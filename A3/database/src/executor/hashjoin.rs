use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use common::Data;
use crate::executor::{Operator, SharedDisk};
use crate::row::{decode_block, make_join_key, rows_to_blocks, JoinKey, Row, Schema};

/// If the right (build) side stays within this byte budget, skip disk
/// partitioning entirely and probe with a purely in-memory hash table.
const INMEM_BUILD_BUDGET: usize = 16 * 1024 * 1024; // 16 MB

/// Number of hash partitions for the grace hash join fallback.
/// With a 16 MB per-partition budget this handles right sides up to 256 MB.
const NUM_PARTITIONS: usize = 16;

/// Per-partition in-memory buffer before flushing to disk.
const FLUSH_THRESHOLD: usize = 1 * 1024 * 1024; // 1 MB

/// Number of blocks to read from disk in a single request.
/// Batching amortises seek overhead and protocol round-trips.
const READ_BATCH: u64 = 8;

// ── Disk segment metadata ─────────────────────────────────────────────────────

#[derive(Clone)]
struct PartMeta {
    start_block: u64,
    num_blocks: u64,
}

// ── Partition buffer writer ───────────────────────────────────────────────────

struct PartitionBuffers {
    num_partitions: usize,
    buffers: Vec<Vec<Row>>,
    buf_bytes: Vec<usize>,
    segments: Vec<Vec<PartMeta>>,
}

impl PartitionBuffers {
    fn new(num_partitions: usize) -> Self {
        PartitionBuffers {
            num_partitions,
            buffers: vec![Vec::new(); num_partitions],
            buf_bytes: vec![0usize; num_partitions],
            segments: vec![Vec::new(); num_partitions],
        }
    }

    fn push(&mut self, partition: usize, row: Row, disk: &SharedDisk) {
        let bytes = estimate_row_bytes(&row);
        self.buf_bytes[partition] += bytes;
        self.buffers[partition].push(row);
        if self.buf_bytes[partition] >= FLUSH_THRESHOLD {
            Self::flush_one(
                &mut self.buffers[partition],
                &mut self.buf_bytes[partition],
                &mut self.segments[partition],
                disk,
            );
        }
    }

    fn flush_one(
        buf: &mut Vec<Row>,
        buf_bytes: &mut usize,
        segments: &mut Vec<PartMeta>,
        disk: &SharedDisk,
    ) {
        if buf.is_empty() {
            return;
        }
        let block_size = disk.borrow().block_size;
        let bytes = rows_to_blocks(buf, block_size);
        let num_blocks = (bytes.len() / block_size) as u64;
        let start = disk.borrow_mut().alloc_anon_blocks(num_blocks);
        disk.borrow_mut().write_blocks(start, &bytes);
        segments.push(PartMeta { start_block: start, num_blocks });
        buf.clear();
        *buf_bytes = 0;
    }

    fn finish(mut self, disk: &SharedDisk) -> Vec<Vec<PartMeta>> {
        for i in 0..self.num_partitions {
            if !self.buffers[i].is_empty() {
                Self::flush_one(
                    &mut self.buffers[i],
                    &mut self.buf_bytes[i],
                    &mut self.segments[i],
                    disk,
                );
            }
        }
        self.segments
    }
}

// ── Partition reader ──────────────────────────────────────────────────────────

/// Reads rows back from a list of disk segments, fetching READ_BATCH blocks
/// per disk request to reduce seek and protocol overhead.
struct PartitionReader {
    disk: SharedDisk,
    schema: Schema,
    segments: Vec<PartMeta>,
    seg_idx: usize,
    seg_block_offset: u64,
    buffer: Vec<Row>,
    buf_idx: usize,
}

impl PartitionReader {
    fn new(disk: SharedDisk, schema: Schema, segments: Vec<PartMeta>) -> Self {
        let mut r = PartitionReader {
            disk,
            schema,
            segments,
            seg_idx: 0,
            seg_block_offset: 0,
            buffer: Vec::new(),
            buf_idx: 0,
        };
        r.load_next_batch();
        r
    }

    fn load_next_batch(&mut self) {
        loop {
            if self.seg_idx >= self.segments.len() {
                self.buffer.clear();
                self.buf_idx = 0;
                return;
            }
            let seg = &self.segments[self.seg_idx];
            if self.seg_block_offset >= seg.num_blocks {
                self.seg_idx += 1;
                self.seg_block_offset = 0;
                continue;
            }
            let remaining = seg.num_blocks - self.seg_block_offset;
            let to_read = remaining.min(READ_BATCH);
            let abs = seg.start_block + self.seg_block_offset;
            let raw = self.disk.borrow_mut().read_blocks(abs, to_read);
            let bsz = self.disk.borrow().block_size;
            let mut rows: Vec<Row> = Vec::new();
            for i in 0..to_read as usize {
                rows.extend(decode_block(&raw[i * bsz..(i + 1) * bsz], &self.schema));
            }
            self.seg_block_offset += to_read;
            if !rows.is_empty() {
                self.buffer = rows;
                self.buf_idx = 0;
                return;
            }
            // All blocks in this batch were empty — keep scanning.
        }
    }

    fn next_row(&mut self) -> Option<Row> {
        if self.buf_idx >= self.buffer.len() {
            return None;
        }
        let row = self.buffer[self.buf_idx].clone();
        self.buf_idx += 1;
        if self.buf_idx >= self.buffer.len() {
            self.load_next_batch();
        }
        Some(row)
    }
}

// ── Join modes ────────────────────────────────────────────────────────────────

enum JoinMode<'q> {
    /// Right side fit in memory — stream left through an in-memory hash table.
    InMemory {
        left_key_indices: Vec<usize>,
        hash_table: HashMap<JoinKey, Vec<Row>>,
        left: Box<dyn Operator<'q> + 'q>,
        current_left: Option<Row>,
        match_idx: usize,
    },
    /// Right side exceeded the budget — grace hash join with disk partitioning.
    Partitioned {
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        left_schema: Schema,
        right_schema: Schema,
        disk: SharedDisk,
        left_segs: Vec<Vec<PartMeta>>,
        right_segs: Vec<Vec<PartMeta>>,
        cur_part: usize,
        hash_table: HashMap<JoinKey, Vec<Row>>,
        left_reader: Option<PartitionReader>,
        current_left: Option<Row>,
        match_idx: usize,
    },
}

// ── Public operator ───────────────────────────────────────────────────────────

pub struct HashJoinOperator<'q> {
    schema: Schema,
    mode: JoinMode<'q>,
}

fn hash_partition(key: &JoinKey, num_partitions: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % num_partitions
}

fn estimate_row_bytes(row: &Row) -> usize {
    let data: usize = row
        .iter()
        .map(|v| match v {
            Data::Int32(_) | Data::Float32(_) => 8,
            Data::Int64(_) | Data::Float64(_) => 8,
            Data::String(s) => 32 + s.len(),
        })
        .sum();
    data + 32
}

impl<'q> HashJoinOperator<'q> {
    pub fn new(
        mut left: Box<dyn Operator<'q> + 'q>,
        mut right: Box<dyn Operator<'q> + 'q>,
        join_keys: Vec<(usize, usize)>,
        disk: SharedDisk,
    ) -> Self {
        let left_schema = left.schema().clone();
        let right_schema = right.schema().clone();
        let mut schema = left_schema.clone();
        schema.extend_from_slice(&right_schema);

        let left_key_indices: Vec<usize> = join_keys.iter().map(|&(l, _)| l).collect();
        let right_key_indices: Vec<usize> = join_keys.iter().map(|&(_, r)| r).collect();

        // ── Phase 1: attempt to build right side in memory ────────────────────
        let mut hash_table: HashMap<JoinKey, Vec<Row>> = HashMap::new();
        let mut right_bytes = 0usize;

        while let Some(row) = right.next() {
            right_bytes += estimate_row_bytes(&row);
            let key = make_join_key(&row, &right_key_indices);
            hash_table.entry(key).or_default().push(row);
            if right_bytes >= INMEM_BUILD_BUDGET {
                break; // exceeded budget → fall back to grace hash join
            }
        }

        if right_bytes < INMEM_BUILD_BUDGET {
            // Right side fully loaded — no disk I/O needed for this join.
            drop(right);
            return HashJoinOperator {
                schema,
                mode: JoinMode::InMemory {
                    left_key_indices,
                    hash_table,
                    left,
                    current_left: None,
                    match_idx: 0,
                },
            };
        }

        // ── Phase 2: grace hash join ───────────────────────────────────────────
        // Re-partition rows already in the hash table using their existing keys.
        let mut right_bufs = PartitionBuffers::new(NUM_PARTITIONS);
        for (join_key, rows) in hash_table.drain() {
            let part = hash_partition(&join_key, NUM_PARTITIONS);
            for row in rows {
                right_bufs.push(part, row, &disk);
            }
        }
        // Drain the remaining right rows from the operator.
        while let Some(row) = right.next() {
            let key = make_join_key(&row, &right_key_indices);
            let part = hash_partition(&key, NUM_PARTITIONS);
            right_bufs.push(part, row, &disk);
        }
        drop(right);
        let right_segs = right_bufs.finish(&disk);

        // Partition the left (probe) side.
        let mut left_bufs = PartitionBuffers::new(NUM_PARTITIONS);
        while let Some(row) = left.next() {
            let key = make_join_key(&row, &left_key_indices);
            let part = hash_partition(&key, NUM_PARTITIONS);
            left_bufs.push(part, row, &disk);
        }
        drop(left);
        let left_segs = left_bufs.finish(&disk);

        HashJoinOperator {
            schema,
            mode: JoinMode::Partitioned {
                left_key_indices,
                right_key_indices,
                left_schema,
                right_schema,
                disk,
                left_segs,
                right_segs,
                cur_part: 0,
                hash_table: HashMap::new(),
                left_reader: None,
                current_left: None,
                match_idx: 0,
            },
        }
    }
}

impl<'q> Operator<'q> for HashJoinOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        match &mut self.mode {
            // ── In-memory fast path ───────────────────────────────────────────
            JoinMode::InMemory {
                left_key_indices,
                hash_table,
                left,
                current_left,
                match_idx,
            } => loop {
                if current_left.is_some() {
                    let key = {
                        let row = current_left.as_ref().unwrap();
                        make_join_key(row, left_key_indices)
                    };
                    let n = hash_table.get(&key).map(|m| m.len()).unwrap_or(0);
                    if *match_idx < n {
                        let left_row = current_left.as_ref().unwrap().clone();
                        let right_row = hash_table.get(&key).unwrap()[*match_idx].clone();
                        *match_idx += 1;
                        let mut combined = left_row;
                        combined.extend_from_slice(&right_row);
                        return Some(combined);
                    }
                    *current_left = None;
                }
                match left.next() {
                    None => return None,
                    Some(row) => {
                        *match_idx = 0;
                        *current_left = Some(row);
                    }
                }
            },

            // ── Grace hash join (partitioned) ─────────────────────────────────
            JoinMode::Partitioned {
                left_key_indices,
                right_key_indices,
                left_schema,
                right_schema,
                disk,
                left_segs,
                right_segs,
                cur_part,
                hash_table,
                left_reader,
                current_left,
                match_idx,
            } => loop {
                // Drain remaining matches for the current left row.
                if current_left.is_some() {
                    let key = {
                        let row = current_left.as_ref().unwrap();
                        make_join_key(row, left_key_indices)
                    };
                    let n = hash_table.get(&key).map(|m| m.len()).unwrap_or(0);
                    if *match_idx < n {
                        let left_row = current_left.as_ref().unwrap().clone();
                        let right_row = hash_table.get(&key).unwrap()[*match_idx].clone();
                        *match_idx += 1;
                        let mut combined = left_row;
                        combined.extend_from_slice(&right_row);
                        return Some(combined);
                    }
                    *current_left = None;
                }

                // Load the next partition if no reader is open.
                if left_reader.is_none() {
                    if *cur_part >= NUM_PARTITIONS {
                        return None;
                    }
                    // Build hash table from the right partition.
                    hash_table.clear();
                    let rsegs = right_segs[*cur_part].clone();
                    let mut rdr = PartitionReader::new(
                        disk.clone(),
                        right_schema.clone(),
                        rsegs,
                    );
                    while let Some(row) = rdr.next_row() {
                        let key = make_join_key(&row, right_key_indices);
                        hash_table.entry(key).or_default().push(row);
                    }
                    // Open the left partition reader.
                    let lsegs = left_segs[*cur_part].clone();
                    *left_reader = Some(PartitionReader::new(
                        disk.clone(),
                        left_schema.clone(),
                        lsegs,
                    ));
                    *current_left = None;
                    *match_idx = 0;
                }

                // Pull the next row from the left partition.
                let next = left_reader.as_mut().unwrap().next_row();
                match next {
                    Some(row) => {
                        *match_idx = 0;
                        *current_left = Some(row);
                    }
                    None => {
                        // Partition exhausted — advance to the next.
                        *left_reader = None;
                        *cur_part += 1;
                    }
                }
            },
        }
    }
}
