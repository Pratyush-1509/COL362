use std::cmp::Ordering;
use std::collections::BinaryHeap;

use common::{query::SortSpec, Data};

use crate::executor::{Operator, SharedDisk};
use crate::row::{decode_block, rows_to_blocks, Row, Schema};

/// Max in-memory row-data bytes before flushing a sorted run to disk.
/// Peak memory during flush = buf + rows_to_blocks output ≈ 2× this value.
/// With 10 MB budget: ~20 MB sort peak + ~13 MB overhead = ~33 MB total, well under 64 MB RLIMIT_AS.
const RUN_BUDGET_BYTES: usize = 10 * 1024 * 1024; // 10 MB per run

/// Total memory budget for all RunReader row-buffers combined during merge.
const READER_BUFFER_BUDGET: u64 = 16 * 1024 * 1024; // 16 MB

// ── Public operator ───────────────────────────────────────────────────────────

pub struct SortOperator<'q> {
    sort_specs: &'q [SortSpec],
    disk: SharedDisk,
    child: Option<Box<dyn Operator<'q> + 'q>>,
    schema: Schema,
    output: SortOutput,
}

enum SortOutput {
    Pending,
    InMemory { rows: Vec<Row>, idx: usize },
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

    fn initialize(&mut self) {
        let mut child = self.child.take().expect("SortOperator already initialised");

        let mut run_metas: Vec<RunMeta> = Vec::new();
        let mut buf: Vec<Row> = Vec::new();
        let mut buf_bytes: usize = 0;

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
        drop(child);

        if run_metas.is_empty() {
            let specs = self.sort_specs;
            let schema = &self.schema;
            buf.sort_by(|a, b| compare_by_specs(a, b, specs, schema));
            self.output = SortOutput::InMemory { rows: buf, idx: 0 };
            return;
        }

        if !buf.is_empty() {
            let meta = flush_run(&mut buf, self.sort_specs, &self.schema, &self.disk);
            run_metas.push(meta);
        }

        let local_specs: Vec<LocalSortSpec> = self
            .sort_specs
            .iter()
            .map(|s| LocalSortSpec {
                column_name: s.column_name.clone(),
                ascending: s.ascending,
            })
            .collect();

        // Adaptive batch size: cap total reader buffer memory at READER_BUFFER_BUDGET.
        // Assume decoded in-memory bytes per block ≤ 2 × block_size (conservative).
        let block_size = self.disk.borrow().block_size as u64;
        let decoded_per_block = 2 * block_size;
        let num_runs = run_metas.len() as u64;
        let batch_blocks = (READER_BUFFER_BUDGET / (num_runs * decoded_per_block)).max(1).min(64);

        let readers: Vec<RunReader> = run_metas
            .into_iter()
            .map(|meta| RunReader::new(self.disk.clone(), meta, self.schema.clone(), batch_blocks))
            .collect();

        self.output = SortOutput::External(ExternalMerge::new(
            readers,
            local_specs,
            self.schema.clone(),
        ));
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

fn flush_run(
    rows: &mut Vec<Row>,
    sort_specs: &[SortSpec],
    schema: &Schema,
    disk: &SharedDisk,
) -> RunMeta {
    rows.sort_by(|a, b| compare_by_specs(a, b, sort_specs, schema));
    let block_size = disk.borrow().block_size;
    let blocks = rows_to_blocks(rows, block_size);
    let num_blocks = (blocks.len() / block_size) as u64;
    let start = disk.borrow_mut().alloc_anon_blocks(num_blocks);
    disk.borrow_mut().write_blocks(start, &blocks);
    RunMeta { start_block: start, num_blocks }
}

// ── k-way merge with BinaryHeap ───────────────────────────────────────────────

struct LocalSortSpec {
    column_name: String,
    ascending: bool,
}

/// One entry per active run in the min-heap.
/// Memory: k × (~24 byte Vec header + key_len + 8 byte run_idx).
/// For k=500 and 30-byte keys this is ~31 KB — negligible.
struct HeapEntry {
    key: Vec<u8>,
    run_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool { self.key == other.key }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Invert so BinaryHeap (max-heap) gives us the minimum key first.
        other.key.cmp(&self.key)
    }
}

struct ExternalMerge {
    readers: Vec<RunReader>,
    sort_specs: Vec<LocalSortSpec>,
    schema: Schema,
    heap: BinaryHeap<HeapEntry>,
}

impl ExternalMerge {
    fn new(readers: Vec<RunReader>, sort_specs: Vec<LocalSortSpec>, schema: Schema) -> Self {
        let mut merge = ExternalMerge {
            readers,
            sort_specs,
            schema,
            heap: BinaryHeap::new(),
        };
        for i in 0..merge.readers.len() {
            merge.push_next(i);
        }
        merge
    }

    fn push_next(&mut self, run_idx: usize) {
        if let Some(row) = self.readers[run_idx].peek() {
            let key = encode_sort_key(row, &self.sort_specs, &self.schema);
            self.heap.push(HeapEntry { key, run_idx });
        }
    }

    fn next_row(&mut self) -> Option<Row> {
        let entry = self.heap.pop()?;
        let row = self.readers[entry.run_idx].peek().unwrap().clone();
        self.readers[entry.run_idx].advance();
        self.push_next(entry.run_idx);
        Some(row)
    }
}

// ── Binary sort key encoding ──────────────────────────────────────────────────

fn encode_sort_key(row: &Row, specs: &[LocalSortSpec], schema: &Schema) -> Vec<u8> {
    let mut key = Vec::new();
    for spec in specs {
        let idx = schema
            .iter()
            .position(|(n, _)| n == &spec.column_name)
            .unwrap_or_else(|| panic!("sort column '{}' not in schema", spec.column_name));
        encode_key_value(&row[idx], spec.ascending, &mut key);
    }
    key
}

fn encode_key_value(val: &Data, ascending: bool, out: &mut Vec<u8>) {
    match val {
        Data::Int32(v) => {
            // XOR sign bit makes two's-complement ordering equivalent to unsigned ordering.
            let bytes = ((*v as u32) ^ 0x8000_0000u32).to_be_bytes();
            if ascending { out.extend_from_slice(&bytes); }
            else { out.extend(bytes.iter().map(|b| !b)); }
        }
        Data::Int64(v) => {
            let bytes = ((*v as u64) ^ 0x8000_0000_0000_0000u64).to_be_bytes();
            if ascending { out.extend_from_slice(&bytes); }
            else { out.extend(bytes.iter().map(|b| !b)); }
        }
        Data::Float32(v) => {
            // IEEE-754: positive → flip sign bit; negative → flip all bits.
            // Result is unsigned-comparable in the same order as the float values.
            let bits = v.to_bits();
            let bits = if v.is_sign_negative() { !bits } else { bits ^ 0x8000_0000u32 };
            let bytes = bits.to_be_bytes();
            if ascending { out.extend_from_slice(&bytes); }
            else { out.extend(bytes.iter().map(|b| !b)); }
        }
        Data::Float64(v) => {
            let bits = v.to_bits();
            let bits = if v.is_sign_negative() { !bits } else { bits ^ 0x8000_0000_0000_0000u64 };
            let bytes = bits.to_be_bytes();
            if ascending { out.extend_from_slice(&bytes); }
            else { out.extend(bytes.iter().map(|b| !b)); }
        }
        Data::String(s) => {
            // Null terminator (0x00) separates this column from the next in multi-key sorts.
            // TPC-H strings never contain null bytes so the separator is unambiguous.
            if ascending {
                out.extend_from_slice(s.as_bytes());
                out.push(0x00);
            } else {
                out.extend(s.bytes().map(|b| !b));
                out.push(0xff); // complement of 0x00
            }
        }
    }
}

// ── Run reader (64-block batched reads) ───────────────────────────────────────

struct RunReader {
    disk: SharedDisk,
    start_block: u64,
    total_blocks: u64,
    blocks_read: u64,
    buffer: Vec<Row>,
    buf_idx: usize,
    schema: Schema,
    batch_blocks: u64,
}

impl RunReader {
    fn new(disk: SharedDisk, meta: RunMeta, schema: Schema, batch_blocks: u64) -> Self {
        let mut r = RunReader {
            disk,
            start_block: meta.start_block,
            total_blocks: meta.num_blocks,
            blocks_read: 0,
            buffer: Vec::new(),
            buf_idx: 0,
            schema,
            batch_blocks,
        };
        r.load_next_batch();
        r
    }

    fn load_next_batch(&mut self) {
        self.buffer.clear();
        self.buf_idx = 0;
        while self.buffer.is_empty() && self.blocks_read < self.total_blocks {
            let remaining = self.total_blocks - self.blocks_read;
            let to_read = remaining.min(self.batch_blocks);
            let abs = self.start_block + self.blocks_read;
            let raw = self.disk.borrow_mut().read_blocks(abs, to_read);
            let bsz = self.disk.borrow().block_size;
            self.blocks_read += to_read;
            for i in 0..to_read as usize {
                let chunk = &raw[i * bsz..(i + 1) * bsz];
                self.buffer.extend(decode_block(chunk, &self.schema));
            }
        }
    }

    fn peek(&self) -> Option<&Row> {
        self.buffer.get(self.buf_idx)
    }

    fn advance(&mut self) {
        self.buf_idx += 1;
        if self.buf_idx >= self.buffer.len() {
            self.load_next_batch();
        }
    }

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

fn estimate_row_bytes(row: &Row) -> usize {
    let data: usize = row
        .iter()
        .map(|v| match v {
            Data::Int32(_) | Data::Float32(_) => 16,
            Data::Int64(_) | Data::Float64(_) => 16,
            Data::String(s) => 32 + s.len(),
        })
        .sum();
    data + 32
}
