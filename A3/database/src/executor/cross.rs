use crate::executor::{Operator, SharedDisk};
use crate::row::{decode_block, encode_row, Row, Schema};

/// If the spilled right side's estimated in-memory size stays under this
/// threshold, cache all decoded rows in memory so each left row iterates
/// the cache instead of re-reading disk.
/// Factor-of-2 multiplier over raw block bytes is a conservative estimate
/// for Row/Data/String heap overhead.
const RIGHT_CACHE_BYTES: usize = 8 * 1024 * 1024;

/// Blocks per write during spill, and per read during cache load.
const BATCH_BLOCKS: usize = 64;

pub struct CrossOperator<'q> {
    left: Box<dyn Operator<'q> + 'q>,
    right: Option<Box<dyn Operator<'q> + 'q>>,
    disk: SharedDisk,
    schema: Schema,
    right_schema: Schema,

    right_start_block: u64,
    right_num_blocks: u64,

    /// All right rows decoded into memory (Some if right side fit in budget).
    right_cache: Option<Vec<Row>>,
    /// Position in right_cache for the current left row.
    right_cache_idx: usize,

    /// Disk-path state (used only when right_cache is None).
    right_block_idx: u64,
    right_buf: Vec<Row>,
    right_row_idx: usize,

    current_left: Option<Row>,
    initialised: bool,
}

impl<'q> CrossOperator<'q> {
    pub fn new(
        left: Box<dyn Operator<'q> + 'q>,
        right: Box<dyn Operator<'q> + 'q>,
        disk: SharedDisk,
    ) -> Self {
        let mut schema = left.schema().clone();
        let right_schema = right.schema().clone();
        schema.extend_from_slice(&right_schema);

        CrossOperator {
            left,
            right: Some(right),
            disk,
            schema,
            right_schema,
            right_start_block: 0,
            right_num_blocks: 0,
            right_cache: None,
            right_cache_idx: 0,
            right_block_idx: 0,
            right_buf: Vec::new(),
            right_row_idx: 0,
            current_left: None,
            initialised: false,
        }
    }

    fn spill_right(&mut self) {
        let mut right_child = self.right.take().unwrap();
        let block_size = self.disk.borrow().block_size;
        let usable = block_size - 2;

        let mut current_block = vec![0u8; block_size];
        let mut offset = 0usize;
        let mut row_count: u16 = 0;

        let mut write_buf: Vec<u8> = Vec::with_capacity(BATCH_BLOCKS * block_size);
        let mut total_blocks: u64 = 0;
        let mut first_write = true;

        let flush_batch = |disk: &SharedDisk,
                           write_buf: &mut Vec<u8>,
                           total_blocks: &mut u64,
                           right_start_block: &mut u64,
                           first_write: &mut bool| {
            if write_buf.is_empty() {
                return;
            }
            let n = (write_buf.len() / block_size) as u64;
            let start = disk.borrow_mut().alloc_anon_blocks(n);
            if *first_write {
                *right_start_block = start;
                *first_write = false;
            }
            disk.borrow_mut().write_blocks(start, write_buf);
            *total_blocks += n;
            write_buf.clear();
        };

        while let Some(row) = right_child.next() {
            let encoded = encode_row(&row);
            if offset + encoded.len() > usable {
                let rc_bytes = (row_count as u16).to_le_bytes();
                current_block[block_size - 2] = rc_bytes[0];
                current_block[block_size - 1] = rc_bytes[1];
                write_buf.extend_from_slice(&current_block);
                current_block = vec![0u8; block_size];
                offset = 0;
                row_count = 0;

                if write_buf.len() >= BATCH_BLOCKS * block_size {
                    flush_batch(
                        &self.disk,
                        &mut write_buf,
                        &mut total_blocks,
                        &mut self.right_start_block,
                        &mut first_write,
                    );
                }
            }
            current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
            offset += encoded.len();
            row_count += 1;
        }

        let rc_bytes = (row_count as u16).to_le_bytes();
        current_block[block_size - 2] = rc_bytes[0];
        current_block[block_size - 1] = rc_bytes[1];
        write_buf.extend_from_slice(&current_block);
        flush_batch(
            &self.disk,
            &mut write_buf,
            &mut total_blocks,
            &mut self.right_start_block,
            &mut first_write,
        );

        self.right_num_blocks = total_blocks;
    }

    /// After spilling, try to load all right rows into memory.
    /// Uses a conservative 2× multiplier over raw block bytes as the
    /// in-memory size estimate (accounts for Row/Data/String overhead).
    fn try_cache_right(&mut self) {
        let block_size = self.disk.borrow().block_size;
        let estimated = self.right_num_blocks as usize * block_size * 2;
        if estimated > RIGHT_CACHE_BYTES {
            return;
        }
        let mut cache: Vec<Row> = Vec::new();
        let mut blocks_read = 0u64;
        while blocks_read < self.right_num_blocks {
            let remaining = self.right_num_blocks - blocks_read;
            let to_read = remaining.min(BATCH_BLOCKS as u64);
            let abs = self.right_start_block + blocks_read;
            let raw = self.disk.borrow_mut().read_blocks(abs, to_read);
            for i in 0..to_read as usize {
                let chunk = &raw[i * block_size..(i + 1) * block_size];
                cache.extend(decode_block(chunk, &self.right_schema));
            }
            blocks_read += to_read;
        }
        self.right_cache = Some(cache);
    }

    fn load_right_block(&mut self) {
        let block_id = self.right_start_block + self.right_block_idx;
        let raw = self.disk.borrow_mut().read_blocks(block_id, 1);
        let block_size = self.disk.borrow().block_size;
        self.right_buf = decode_block(&raw[..block_size], &self.right_schema);
        self.right_row_idx = 0;
    }

    fn rewind_right(&mut self) {
        if self.right_cache.is_some() {
            self.right_cache_idx = 0;
        } else {
            self.right_block_idx = 0;
            self.right_buf.clear();
            self.right_row_idx = 0;
            if self.right_num_blocks > 0 {
                self.load_right_block();
            }
        }
    }
}

impl<'q> Operator<'q> for CrossOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        if !self.initialised {
            self.spill_right();
            self.initialised = true;
            if self.right_num_blocks == 0 {
                return None;
            }
            self.try_cache_right();
            if self.right_cache.is_none() {
                self.load_right_block();
            }
        }

        loop {
            if let Some(ref left_row) = self.current_left {
                // ── Cached path: zero disk reads per left row ─────────────────
                if let Some(ref cache) = self.right_cache {
                    if self.right_cache_idx < cache.len() {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(&cache[self.right_cache_idx]);
                        self.right_cache_idx += 1;
                        return Some(combined);
                    }
                    self.current_left = None;
                } else {
                    // ── Disk path ─────────────────────────────────────────────
                    if self.right_row_idx < self.right_buf.len() {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(&self.right_buf[self.right_row_idx]);
                        self.right_row_idx += 1;
                        return Some(combined);
                    }
                    self.right_block_idx += 1;
                    if self.right_block_idx < self.right_num_blocks {
                        self.load_right_block();
                        continue;
                    }
                    self.current_left = None;
                }
            }

            match self.left.next() {
                Some(row) => {
                    self.current_left = Some(row);
                    self.rewind_right();
                }
                None => return None,
            }
        }
    }
}