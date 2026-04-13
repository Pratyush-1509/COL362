use std::io::{BufRead, BufReader, Read, Write};

/// Manages all communication with the Disk Simulator over FD3 (read) and FD4 (write).
///
/// Protocol recap:
///   - Every command we send ends with '\n' and we flush immediately.
///   - Text responses (block-size, file info, anon-start) are a single line ending with '\n'.
///   - Block data responses are exactly `num_blocks * block_size` raw bytes with NO trailing '\n'.
///   - `put block` writes have NO response from the disk.
pub struct DiskManager {
    reader: BufReader<Box<dyn Read>>,
    writer: Box<dyn Write>,
    /// Cached block size (bytes). Queried once at construction.
    pub block_size: usize,
    /// First block ID of the anonymous (writable) region.
    anon_start: u64,
    /// Next free anonymous block we can allocate.
    next_anon_block: u64,
}

impl DiskManager {
    /// Construct a DiskManager, immediately querying block_size and anon_start from disk.
    pub fn new(disk_in: impl Read + 'static, disk_out: impl Write + 'static) -> Self {
        let mut dm = DiskManager {
            reader: BufReader::new(Box::new(disk_in) as Box<dyn Read>),
            writer: Box::new(disk_out) as Box<dyn Write>,
            block_size: 0,
            anon_start: 0,
            next_anon_block: 0,
        };
        dm.block_size = dm.query_block_size();
        dm.anon_start = dm.query_anon_start();
        dm.next_anon_block = dm.anon_start;
        dm
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Write a complete command string (must include trailing '\n') and flush.
    fn send(&mut self, cmd: &str) {
        self.writer
            .write_all(cmd.as_bytes())
            .expect("disk send failed");
        self.writer.flush().expect("disk flush failed");
    }

    /// Read one '\n'-terminated text response line, returning it trimmed.
    fn recv_line(&mut self) -> String {
        let mut line = String::new();
        self.reader
            .read_line(&mut line)
            .expect("disk recv_line failed");
        line.trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string()
    }

    fn query_block_size(&mut self) -> usize {
        self.send("get block-size\n");
        self.recv_line()
            .parse()
            .expect("invalid block-size response")
    }

    fn query_anon_start(&mut self) -> u64 {
        self.send("get anon-start-block\n");
        self.recv_line()
            .parse()
            .expect("invalid anon-start-block response")
    }

    // ── Public API ──────────────────────────────────────────────────────────

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn anon_start(&self) -> u64 {
        self.anon_start
    }

    /// Read `num_blocks` consecutive blocks starting at `start_block`.
    /// Returns exactly `num_blocks * block_size` bytes.
    pub fn read_blocks(&mut self, start_block: u64, num_blocks: u64) -> Vec<u8> {
        let cmd = format!("get block {} {}\n", start_block, num_blocks);
        self.send(&cmd);
        let total = num_blocks as usize * self.block_size;
        let mut buf = vec![0u8; total];
        self.reader
            .read_exact(&mut buf)
            .expect("failed to read block data from disk");
        buf
    }

    /// Write `data` to disk starting at `start_block`.
    /// `data.len()` must be an exact multiple of `block_size`.
    /// All blocks must be in the anonymous region (>= anon_start).
    pub fn write_blocks(&mut self, start_block: u64, data: &[u8]) {
        assert!(
            data.len() % self.block_size == 0,
            "write_blocks: data length {} is not a multiple of block_size {}",
            data.len(),
            self.block_size
        );
        let num_blocks = data.len() / self.block_size;
        // Write header line then raw bytes in one go — do NOT flush between them.
        let cmd = format!("put block {} {}\n", start_block, num_blocks);
        self.writer
            .write_all(cmd.as_bytes())
            .expect("disk write_blocks header failed");
        self.writer
            .write_all(data)
            .expect("disk write_blocks data failed");
        self.writer.flush().expect("disk flush after write_blocks failed");
        // No response from disk for put commands.
    }

    /// Return the starting block ID of a named file.
    pub fn get_file_start_block(&mut self, file_id: &str) -> u64 {
        let cmd = format!("get file start-block {}\n", file_id);
        self.send(&cmd);
        self.recv_line()
            .parse()
            .expect("invalid file start-block response")
    }

    /// Return the number of blocks a named file spans.
    pub fn get_file_num_blocks(&mut self, file_id: &str) -> u64 {
        let cmd = format!("get file num-blocks {}\n", file_id);
        self.send(&cmd);
        self.recv_line()
            .parse()
            .expect("invalid file num-blocks response")
    }

    /// Allocate `count` consecutive anonymous blocks and return the starting block ID.
    /// Simple bump allocator — never frees, which is fine for single-query execution.
    pub fn alloc_anon_blocks(&mut self, count: u64) -> u64 {
        let start = self.next_anon_block;
        self.next_anon_block += count;
        start
    }
}
