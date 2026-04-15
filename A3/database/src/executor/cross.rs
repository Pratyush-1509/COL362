use crate::executor::{Operator, SharedDisk};
use crate::row::{decode_block, encode_row, Row, Schema};

/// Computes the Cartesian product of two child relations.
///
/// Strategy: spill the RIGHT child to anonymous disk blocks, then stream
/// the LEFT child one row at a time, re-reading the right side from disk
/// for each left row.
///
/// This keeps memory usage to O(block_size) regardless of right-side size.
pub struct CrossOperator<'q> {
    left: Box<dyn Operator<'q> + 'q>,
    right: Option<Box<dyn Operator<'q> + 'q>>,
    disk: SharedDisk,
    schema: Schema,
    right_schema: Schema,

    /// Block ID where the spilled right side starts.
    right_start_block: u64,
    /// Number of blocks spilled.
    right_num_blocks: u64,

    /// Current left row being joined.
    current_left: Option<Row>,
    /// Buffer of decoded right rows from the current disk block.
    right_buf: Vec<Row>,
    /// Which block of the right side we are currently reading (0-indexed).
    right_block_idx: u64,
    /// Index into right_buf.
    right_row_idx: usize,

    /// True once we have spilled the right side.
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
            current_left: None,
            right_buf: Vec::new(),
            right_block_idx: 0,
            right_row_idx: 0,
            initialised: false,
        }
    }

    /// Spill the entire right child to anonymous disk blocks.
    ///
    /// Writes blocks in batches of SPILL_BATCH_BLOCKS to bound peak memory
    /// to O(batch_size × block_size) instead of O(right_side_total_bytes).
    fn spill_right(&mut self) {
        const SPILL_BATCH_BLOCKS: usize = 64; // 64 × 4 KB = 256 KB per write

        let mut right_child = self.right.take().unwrap();
        let block_size = self.disk.borrow().block_size;
        let usable = block_size - 2;

        let mut current_block = vec![0u8; block_size];
        let mut offset = 0usize;
        let mut row_count: u16 = 0;

        // Flat write buffer: holds up to SPILL_BATCH_BLOCKS blocks before flushing.
        let mut write_buf: Vec<u8> = Vec::with_capacity(SPILL_BATCH_BLOCKS * block_size);
        let mut total_blocks: u64 = 0;
        let mut first_write = true;

        let flush_batch = |disk: &crate::executor::SharedDisk,
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
                // Seal the current block and add to write buffer.
                let rc_bytes = (row_count as u16).to_le_bytes();
                current_block[block_size - 2] = rc_bytes[0];
                current_block[block_size - 1] = rc_bytes[1];
                write_buf.extend_from_slice(&current_block);
                current_block = vec![0u8; block_size];
                offset = 0;
                row_count = 0;

                // Flush the write buffer once it reaches the batch size.
                if write_buf.len() >= SPILL_BATCH_BLOCKS * block_size {
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

        // Seal and flush the final (possibly partial) block.
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

    /// Load right rows from block `right_block_idx` into `right_buf`.
    fn load_right_block(&mut self) {
        let block_id = self.right_start_block + self.right_block_idx;
        let raw = self.disk.borrow_mut().read_blocks(block_id, 1);
        let block_size = self.disk.borrow().block_size;
        self.right_buf = decode_block(&raw[..block_size], &self.right_schema);
        self.right_row_idx = 0;
    }

    /// Rewind right side to beginning.
    fn rewind_right(&mut self) {
        self.right_block_idx = 0;
        self.right_buf.clear();
        self.right_row_idx = 0;
        if self.right_num_blocks > 0 {
            self.load_right_block();
        }
    }
}

impl<'q> Operator<'q> for CrossOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        // Spill right side on first call
        if !self.initialised {
            self.spill_right();
            self.initialised = true;
            if self.right_num_blocks == 0 {
                return None;
            }
            self.load_right_block();
        }

        loop {
            if let Some(ref left_row) = self.current_left {
                // Advance within current right block
                if self.right_row_idx < self.right_buf.len() {
                    let mut combined = left_row.clone();
                    combined.extend_from_slice(&self.right_buf[self.right_row_idx]);
                    self.right_row_idx += 1;
                    return Some(combined);
                }
                // Try next right block
                self.right_block_idx += 1;
                if self.right_block_idx < self.right_num_blocks {
                    self.load_right_block();
                    continue;
                }
                // Right side exhausted for this left row — get next left row
                self.current_left = None;
            }

            // Fetch next left row
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
