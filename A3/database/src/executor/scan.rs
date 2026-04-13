use db_config::DbContext;

use crate::executor::{Operator, SharedDisk};
use crate::row::{decode_block, Row, Schema};

/// How many consecutive blocks to fetch per disk request.
/// Reading in larger chunks gives the disk simulator better sequential I/O credit.
const BLOCKS_PER_READ: u64 = 64;

/// Scans all rows of a single table from disk, buffering `BLOCKS_PER_READ`
/// blocks at a time to minimise the number of disk commands issued.
pub struct ScanOperator {
    disk: SharedDisk,
    schema: Schema,
    /// Absolute block ID of the file's first block on disk.
    file_start: u64,
    /// Total number of blocks in the file.
    file_blocks: u64,
    /// How many blocks we have already read (offset from file_start).
    blocks_read: u64,
    /// Decoded rows from the most recent batch of blocks.
    buffer: Vec<Row>,
    /// Index into `buffer` of the next row to return.
    buf_idx: usize,
}

impl ScanOperator {
    pub fn new(table_id: &str, ctx: &DbContext, disk: SharedDisk) -> Self {
        let table_spec = ctx
            .get_table_specs()
            .iter()
            .find(|t| t.name == table_id)
            .unwrap_or_else(|| panic!("ScanOperator: table '{}' not found in DbContext", table_id));

        let schema: Schema = table_spec
            .column_specs
            .iter()
            .map(|cs| (cs.column_name.clone(), cs.data_type.clone()))
            .collect();

        let file_start = disk.borrow_mut().get_file_start_block(&table_spec.file_id);
        let file_blocks = disk.borrow_mut().get_file_num_blocks(&table_spec.file_id);

        ScanOperator {
            disk,
            schema,
            file_start,
            file_blocks,
            blocks_read: 0,
            buffer: Vec::new(),
            buf_idx: 0,
        }
    }

    /// Attempt to load the next batch of blocks into `self.buffer`.
    /// Returns `false` if the file has been fully consumed.
    fn fill_buffer(&mut self) -> bool {
        if self.blocks_read >= self.file_blocks {
            return false;
        }

        let to_read = BLOCKS_PER_READ.min(self.file_blocks - self.blocks_read);
        let abs_start = self.file_start + self.blocks_read;

        let raw = self.disk.borrow_mut().read_blocks(abs_start, to_read);
        let block_size = self.disk.borrow().block_size;

        self.buffer.clear();
        self.buf_idx = 0;

        for i in 0..to_read as usize {
            let block = &raw[i * block_size..(i + 1) * block_size];
            let rows = decode_block(block, &self.schema);
            self.buffer.extend(rows);
        }

        self.blocks_read += to_read;
        true
    }
}

impl<'q> Operator<'q> for ScanOperator {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        loop {
            if self.buf_idx < self.buffer.len() {
                let row = self.buffer[self.buf_idx].clone();
                self.buf_idx += 1;
                return Some(row);
            }
            // Buffer exhausted — fetch more blocks.
            if !self.fill_buffer() {
                return None;
            }
        }
    }
}
