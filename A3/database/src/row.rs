use common::{Data, DataType};

/// A single row: one Data value per column, in schema order.
pub type Row = Vec<Data>;

/// The schema for a relation: (column_name, data_type) in column order.
pub type Schema = Vec<(String, DataType)>;

// ── Decoding (disk → Row) ────────────────────────────────────────────────────

/// Decode all rows packed in a single block.
///
/// Block layout:
///   byte 0 .. (block_size-3)  : rows packed tightly, no padding
///   byte (block_size-2)       : row_count low byte  (u16 little-endian)
///   byte (block_size-1)       : row_count high byte
pub fn decode_block(block: &[u8], schema: &Schema) -> Vec<Row> {
    let block_size = block.len();
    let row_count =
        u16::from_le_bytes([block[block_size - 2], block[block_size - 1]]) as usize;

    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 0usize;
    for _ in 0..row_count {
        let (row, consumed) = decode_row_at(&block[offset..], schema);
        rows.push(row);
        offset += consumed;
    }
    rows
}

/// Decode one row starting at `data[0]`. Returns (row, bytes_consumed).
fn decode_row_at(data: &[u8], schema: &Schema) -> (Row, usize) {
    let mut offset = 0usize;
    let mut row = Vec::with_capacity(schema.len());
    for (_, dtype) in schema {
        let (val, consumed) = decode_value(&data[offset..], dtype);
        row.push(val);
        offset += consumed;
    }
    (row, offset)
}

fn decode_value(data: &[u8], dtype: &DataType) -> (Data, usize) {
    match dtype {
        DataType::Int32 => {
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            (Data::Int32(v), 4)
        }
        DataType::Int64 => {
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            (Data::Int64(v), 8)
        }
        DataType::Float32 => {
            let v = f32::from_le_bytes(data[..4].try_into().unwrap());
            (Data::Float32(v), 4)
        }
        DataType::Float64 => {
            let v = f64::from_le_bytes(data[..8].try_into().unwrap());
            (Data::Float64(v), 8)
        }
        DataType::String => {
            // Null-terminated UTF-8
            let null_pos = data
                .iter()
                .position(|&b| b == 0)
                .expect("string column has no null terminator");
            let s = std::str::from_utf8(&data[..null_pos])
                .expect("invalid UTF-8 in string column")
                .to_string();
            (Data::String(s), null_pos + 1)
        }
    }
}

// ── Encoding (Row → disk) ────────────────────────────────────────────────────
// Used when writing intermediate results to the anonymous disk region.

/// Encode a single row to bytes using the same format as table files.
pub fn encode_row(row: &Row) -> Vec<u8> {
    let mut bytes = Vec::new();
    for val in row {
        encode_value(val, &mut bytes);
    }
    bytes
}

fn encode_value(val: &Data, out: &mut Vec<u8>) {
    match val {
        Data::Int32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Data::Int64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Data::Float32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Data::Float64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Data::String(s) => {
            out.extend_from_slice(s.as_bytes());
            out.push(0); // null terminator
        }
    }
}

/// Pack rows into disk blocks.
///
/// Each output block has the same layout as table blocks:
///   rows packed from byte 0, row_count u16-LE at the last 2 bytes.
/// Returns a byte vec whose length is always a multiple of `block_size`.
/// Rows that do not fit in the current block start a new one; a row is always
/// contained within a single block (guaranteed by the spec, and we enforce it
/// here via the panic).
pub fn rows_to_blocks(rows: &[Row], block_size: usize) -> Vec<u8> {
    let usable = block_size - 2; // last 2 bytes reserved for row_count
    let mut all_blocks: Vec<u8> = Vec::new();
    let mut current_block = vec![0u8; block_size];
    let mut offset = 0usize;
    let mut row_count: u16 = 0;

    for row in rows {
        let encoded = encode_row(row);
        assert!(
            encoded.len() <= usable,
            "a single encoded row ({} bytes) exceeds usable block space ({} bytes)",
            encoded.len(),
            usable
        );
        if offset + encoded.len() > usable {
            // Flush current block and start a fresh one
            write_row_count(&mut current_block, block_size, row_count);
            all_blocks.extend_from_slice(&current_block);
            current_block = vec![0u8; block_size];
            offset = 0;
            row_count = 0;
        }
        current_block[offset..offset + encoded.len()].copy_from_slice(&encoded);
        offset += encoded.len();
        row_count += 1;
    }

    // Always emit the final (possibly partial) block so callers get ≥ 1 block.
    write_row_count(&mut current_block, block_size, row_count);
    all_blocks.extend_from_slice(&current_block);

    all_blocks
}

fn write_row_count(block: &mut [u8], block_size: usize, row_count: u16) {
    let bytes = row_count.to_le_bytes();
    block[block_size - 2] = bytes[0];
    block[block_size - 1] = bytes[1];
}

// ── Output formatting (Row → monitor text) ───────────────────────────────────

/// Format a Data value as plain text for the monitor protocol.
pub fn format_value(val: &Data) -> String {
    match val {
        Data::Int32(v) => v.to_string(),
        Data::Int64(v) => v.to_string(),
        Data::Float32(v) => format_f32(*v),
        Data::Float64(v) => format_f64(*v),
        Data::String(s) => s.clone(),
    }
}

/// Format a complete row as "col1|col2|...|colN|" (trailing pipe, no newline).
pub fn format_row(row: &Row) -> String {
    let mut s = String::new();
    for val in row {
        s.push_str(&format_value(val));
        s.push('|');
    }
    s
}

// Float formatting: match SQLite output.
// SQLite prints floats like Rust's Display for most values.
// Details TBD per assignment spec; this is a reasonable approximation.
fn format_f32(v: f32) -> String {
    if v.is_nan() {
        return String::from("NaN");
    }
    if v.is_infinite() {
        return if v > 0.0 {
            String::from("Inf")
        } else {
            String::from("-Inf")
        };
    }
    // SQLite shows whole floats as "X.0"
    if v.fract() == 0.0 && v.abs() < 1e15_f32 {
        format!("{:.1}", v)
    } else {
        // Use enough precision to round-trip
        format!("{}", v)
    }
}

fn format_f64(v: f64) -> String {
    if v.is_nan() {
        return String::from("NaN");
    }
    if v.is_infinite() {
        return if v > 0.0 {
            String::from("Inf")
        } else {
            String::from("-Inf")
        };
    }
    if v.fract() == 0.0 && v.abs() < 1e15_f64 {
        format!("{:.1}", v)
    } else {
        format!("{}", v)
    }
}

// ── Hashable key for hash join ───────────────────────────────────────────────

use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct DataKey(pub common::Data);

impl PartialEq for DataKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (common::Data::Int32(a),   common::Data::Int32(b))   => a == b,
            (common::Data::Int64(a),   common::Data::Int64(b))   => a == b,
            (common::Data::Float32(a), common::Data::Float32(b)) => a.to_bits() == b.to_bits(),
            (common::Data::Float64(a), common::Data::Float64(b)) => a.to_bits() == b.to_bits(),
            (common::Data::String(a),  common::Data::String(b))  => a == b,
            _ => false,
        }
    }
}

impl Eq for DataKey {}

impl Hash for DataKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            common::Data::Int32(v)   => { 0u8.hash(state); v.hash(state); }
            common::Data::Int64(v)   => { 1u8.hash(state); v.hash(state); }
            common::Data::Float32(v) => { 2u8.hash(state); v.to_bits().hash(state); }
            common::Data::Float64(v) => { 3u8.hash(state); v.to_bits().hash(state); }
            common::Data::String(s)  => { 4u8.hash(state); s.hash(state); }
        }
    }
}

pub type JoinKey = Vec<DataKey>;

pub fn make_join_key(row: &Row, indices: &[usize]) -> JoinKey {
    indices.iter().map(|&i| DataKey(row[i].clone())).collect()
}
