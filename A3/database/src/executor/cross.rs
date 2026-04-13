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
    fn spill_right(&mut self) {
        let mut right_child = self.right.take().unwrap();
        let block_size = self.disk.borrow().block_size;
        let usable = block_size - 2;

        // We'll build blocks on the fly and write them as they fill up.
        let mut current_block = vec![0u8; block_size];
        let mut offset = 0usize;
        let mut row_count: u16 = 0;
        let mut blocks: Vec<Vec<u8>> = Vec::new();

        while let Some(row) = right_child.next() {
            let encoded = encode_row(&row);
            if offset + encoded.len() > usable {
                // Flush current block
                let rc_bytes = (row_count as u16).to_le_bytes();
                current_block[block_size - 2] = rc_bytes[0];
                current_block[block_size - 1] = rc_bytes[1];
                blocks.push(current_block);
                current_block = vec![0u8; block_size];
                offset = 0;
                row_count = 0;
            }
            current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
            offset += encoded.len();
            row_count += 1;
        }
        // Flush final block (always emit at least one so we know right is empty if 0 rows)
        let rc_bytes = (row_count as u16).to_le_bytes();
        current_block[block_size - 2] = rc_bytes[0];
        current_block[block_size - 1] = rc_bytes[1];
        blocks.push(current_block);

        // Write all blocks to anonymous region
        let num_blocks = blocks.len() as u64;
        let start = self.disk.borrow_mut().alloc_anon_blocks(num_blocks);
        let mut flat: Vec<u8> = Vec::with_capacity(num_blocks as usize * block_size);
        for b in blocks {
            flat.extend_from_slice(&b);
        }
        self.disk.borrow_mut().write_blocks(start, &flat);

        self.right_start_block = start;
        self.right_num_blocks = num_blocks;
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
