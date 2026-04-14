use std::cmp::Ordering;

use common::{query::SortSpec, Data};

use crate::executor::{Operator, SharedDisk};
use crate::row::{decode_block, rows_to_blocks, Row, Schema};

/// Max in-memory row-data bytes before flushing a sorted run to disk.
/// Conservative to stay well inside the 64 MB RLIMIT_AS limit.
const RUN_BUDGET_BYTES: usize = 4 * 1024 * 1024; // 4 MB per run

// ── Public operator ───────────────────────────────────────────────────────────

pub struct SortOperator<'q> {
    sort_specs: &'q [SortSpec],
    disk: SharedDisk,
    child: Option<Box<dyn Operator<'q> + 'q>>,
    schema: Schema,
    output: SortOutput,
}

enum SortOutput {
    /// Not yet initialised — first next() triggers drain + sort.
    Pending,
    /// All rows fit in one buffer; already sorted in memory.
    InMemory { rows: Vec<Row>, idx: usize },
    /// Data spilled across ≥ 2 disk runs; k-way merge in progress.
    External(ExternalMerge),
}

impl<'q> SortOperator<'q> {
    pub fn new(
        sort_specs: &'q [SortSpec],
        child: Box<dyn Operator<'q> + 'q>,
        disk: SharedDisk,
    ) -> Self {
        let schema = child.schema().clone();
        SortOperator {
            sort_specs,
            disk,
            child: Some(child),
            schema,
            output: SortOutput::Pending,
        }
    }

    /// Drain child, generate sorted runs on disk, then set up the output state.
    fn initialize(&mut self) {
        let mut child = self.child.take().expect("SortOperator already initialised");

        let mut run_metas: Vec<RunMeta> = Vec::new();
        let mut buf: Vec<Row> = Vec::new();
        let mut buf_bytes: usize = 0;

        // ── Phase 1: run generation ───────────────────────────────────────────
        while let Some(row) = child.next() {
            buf_bytes += estimate_row_bytes(&row);
            buf.push(row);
            if buf_bytes >= RUN_BUDGET_BYTES {
                let meta = flush_run(&mut buf, self.sort_specs, &self.schema, &self.disk);
                run_metas.push(meta);
                buf.clear();
                buf_bytes = 0;
            }
        }
        drop(child); // release any disk borrows held by the child tree

        if run_metas.is_empty() {
            // Everything fit in one buffer — sort in place, no disk writes needed.
            let specs = self.sort_specs;
            let schema = &self.schema;
            buf.sort_by(|a, b| compare_by_specs(a, b, specs, schema));
            self.output = SortOutput::InMemory { rows: buf, idx: 0 };
            return;
        }

        // Flush the last (possibly partial) batch as another run.
        if !buf.is_empty() {
            let meta = flush_run(&mut buf, self.sort_specs, &self.schema, &self.disk);
            run_metas.push(meta);
        }

        // ── Phase 2: open a RunReader per run; k-way merge happens in next() ──
        //
        // We store owned LocalSortSpec copies inside ExternalMerge so that
        // ExternalMerge is lifetime-free.  This avoids a borrow conflict in
        // next() where &mut self.output and self.sort_specs would otherwise
        // both alias through self.
        let local_specs: Vec<LocalSortSpec> = self
            .sort_specs
            .iter()
            .map(|s| LocalSortSpec {
                column_name: s.column_name.clone(),
                ascending: s.ascending,
            })
            .collect();

        let readers: Vec<RunReader> = run_metas
            .into_iter()
            .map(|meta| RunReader::new(self.disk.clone(), meta, self.schema.clone()))
            .collect();

        self.output = SortOutput::External(ExternalMerge {
            readers,
            sort_specs: local_specs,
            schema: self.schema.clone(),
        });
    }
}

impl<'q> Operator<'q> for SortOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        if matches!(self.output, SortOutput::Pending) {
            self.initialize();
        }
        // ExternalMerge carries its own copies of sort_specs + schema, so
        // the match only borrows self.output — no cross-field conflict.
        match &mut self.output {
            SortOutput::Pending => unreachable!(),
            SortOutput::InMemory { rows, idx } => {
                let r = rows.get(*idx).cloned();
                *idx += 1;
                r
            }
            SortOutput::External(merge) => merge.next_row(),
        }
    }
}

// ── Run flushing ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct RunMeta {
    start_block: u64,
    num_blocks: u64,
}

/// Sort `rows` in place, serialise to blocks, write to anonymous disk region.
fn flush_run(
    rows: &mut Vec<Row>,
    sort_specs: &[SortSpec],
    schema: &Schema,
    disk: &SharedDisk,
) -> RunMeta {
    rows.sort_by(|a, b| compare_by_specs(a, b, sort_specs, schema));
    let block_size = disk.borrow().block_size;            // borrow released
    let blocks = rows_to_blocks(rows, block_size);
    let num_blocks = (blocks.len() / block_size) as u64;
    let start = disk.borrow_mut().alloc_anon_blocks(num_blocks); // borrow released
    disk.borrow_mut().write_blocks(start, &blocks);              // borrow released
    RunMeta { start_block: start, num_blocks }
}

// ── k-way merge ───────────────────────────────────────────────────────────────

/// Owned copy of a SortSpec — lets ExternalMerge be free of the 'q lifetime.
struct LocalSortSpec {
    column_name: String,
    ascending: bool,
}

struct ExternalMerge {
    readers: Vec<RunReader>,
    sort_specs: Vec<LocalSortSpec>,
    schema: Schema,
}

impl ExternalMerge {
    fn next_row(&mut self) -> Option<Row> {
        let min = self.find_min()?;
        let row = self.readers[min].peek().unwrap().clone();
        self.readers[min].advance();
        Some(row)
    }

    /// Linear-scan minimum: O(k) per output row.
    /// Correct for any k; a BinaryHeap upgrade would help when k is large.
    fn find_min(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for i in 0..self.readers.len() {
            if self.readers[i].is_empty() {
                continue;
            }
            best = Some(match best {
                None => i,
                Some(j) => {
                    let a = self.readers[i].peek().unwrap();
                    let b = self.readers[j].peek().unwrap();
                    if self.row_cmp(a, b) == Ordering::Less { i } else { j }
                }
            });
        }
        best
    }

    fn row_cmp(&self, a: &Row, b: &Row) -> Ordering {
        for spec in &self.sort_specs {
            let idx = self
                .schema
                .iter()
                .position(|(n, _)| n == &spec.column_name)
                .unwrap_or_else(|| panic!("sort column '{}' not in schema", spec.column_name));
            let ord = a[idx].partial_cmp(&b[idx]).unwrap_or(Ordering::Equal);
            if ord != Ordering::Equal {
                return if spec.ascending { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    }
}

// ── Run reader (one disk block at a time) ─────────────────────────────────────

struct RunReader {
    disk: SharedDisk,
    start_block: u64,
    total_blocks: u64,
    blocks_read: u64,
    buffer: Vec<Row>,
    buf_idx: usize,
    schema: Schema,
}

impl RunReader {
    fn new(disk: SharedDisk, meta: RunMeta, schema: Schema) -> Self {
        let mut r = RunReader {
            disk,
            start_block: meta.start_block,
            total_blocks: meta.num_blocks,
            blocks_read: 0,
            buffer: Vec::new(),
            buf_idx: 0,
            schema,
        };
        r.load_next_block();
        r
    }

    fn load_next_block(&mut self) {
        loop {
            if self.blocks_read >= self.total_blocks {
                // Run exhausted — mark buffer empty so is_empty() returns true.
                self.buffer.clear();
                self.buf_idx = 0;
                return;
            }
            let abs = self.start_block + self.blocks_read;
            let raw = self.disk.borrow_mut().read_blocks(abs, 1); // borrow released
            let bsz = self.disk.borrow().block_size;               // borrow released
            let rows = decode_block(&raw[..bsz], &self.schema);
            self.blocks_read += 1;
            if !rows.is_empty() {
                self.buffer = rows;
                self.buf_idx = 0;
                return;
            }
            // Rare: empty block — skip to the next one.
        }
    }

    fn peek(&self) -> Option<&Row> {
        self.buffer.get(self.buf_idx)
    }

    fn advance(&mut self) {
        self.buf_idx += 1;
        if self.buf_idx >= self.buffer.len() {
            // Current block exhausted — load the next one.
            self.load_next_block();
        }
    }

    /// True when the in-memory buffer is drained and no more disk blocks remain.
    /// (load_next_block clears the buffer when blocks_read >= total_blocks.)
    fn is_empty(&self) -> bool {
        self.buf_idx >= self.buffer.len()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn compare_by_specs(a: &Row, b: &Row, specs: &[SortSpec], schema: &Schema) -> Ordering {
    for spec in specs {
        let idx = schema
            .iter()
            .position(|(n, _)| n == &spec.column_name)
            .unwrap_or_else(|| panic!("sort column '{}' not in schema", spec.column_name));
        let ord = a[idx].partial_cmp(&b[idx]).unwrap_or(Ordering::Equal);
        if ord != Ordering::Equal {
            return if spec.ascending { ord } else { ord.reverse() };
        }
    }
    Ordering::Equal
}

/// Heap-byte estimate for one Row (used to decide when to flush a run).
/// Over-estimates intentionally to leave safety headroom.
fn estimate_row_bytes(row: &Row) -> usize {
    let data: usize = row
        .iter()
        .map(|v| match v {
            Data::Int32(_) | Data::Float32(_) => 16,
            Data::Int64(_) | Data::Float64(_) => 16,
            Data::String(s) => 32 + s.len(),
        })
        .sum();
    data + 32 // Vec<Data> header + outer Row Vec header
}
