use crate::executor::{Operator, SharedDisk};
use crate::row::{Row, Schema};

/// Computes the Cartesian product of two child relations.
///
/// Strategy: materialise the **right** child entirely into memory on the first
/// `next()` call, then stream the left child one row at a time.  For each left
/// row, we yield `left_row ++ right_row` for every buffered right row.
///
/// Why right side in memory?  The right child is re-iterated for every left
/// row, but iterators are single-pass.  Materialising right avoids having to
/// re-open it.  If right is too large, a future upgrade can spill it to the
/// anonymous disk region instead.
///
/// TODO: disk-backed nested-loop join for large right sides.
pub struct CrossOperator<'q> {
    left: Box<dyn Operator<'q> + 'q>,
    /// Right child — Some until the first `next()` call, then taken and drained.
    right: Option<Box<dyn Operator<'q> + 'q>>,
    /// All rows from the right child, kept in memory.
    right_buf: Vec<Row>,
    /// The current left row being joined against every right row.
    current_left: Option<Row>,
    /// Index into `right_buf` for the next right row to pair with `current_left`.
    right_idx: usize,
    /// The disk handle — reserved for the future disk-backed upgrade.
    #[allow(dead_code)]
    disk: SharedDisk,
    schema: Schema,
}

impl<'q> CrossOperator<'q> {
    pub fn new(
        left: Box<dyn Operator<'q> + 'q>,
        right: Box<dyn Operator<'q> + 'q>,
        disk: SharedDisk,
    ) -> Self {
        // Output schema = left columns followed by right columns.
        // The spec guarantees that left and right child schemas have no name collisions.
        let mut schema = left.schema().clone();
        schema.extend_from_slice(right.schema());

        CrossOperator {
            left,
            right: Some(right),
            right_buf: Vec::new(),
            current_left: None,
            right_idx: 0,
            disk,
            schema,
        }
    }

    /// Drain the right child into `right_buf`.  Called at most once.
    fn init_right(&mut self) {
        if let Some(mut right_child) = self.right.take() {
            while let Some(row) = right_child.next() {
                self.right_buf.push(row);
            }
            // right_child dropped here
        }
    }
}

impl<'q> Operator<'q> for CrossOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        // Lazy materialisation of the right side on the first call.
        if self.right.is_some() {
            self.init_right();
        }

        // If the right side is empty, the cross product is empty.
        if self.right_buf.is_empty() {
            return None;
        }

        loop {
            // Try to advance within the right buffer for the current left row.
            if let Some(ref left_row) = self.current_left {
                if self.right_idx < self.right_buf.len() {
                    // Concatenate left_row ++ right_row.
                    let mut combined = left_row.clone();
                    combined.extend_from_slice(&self.right_buf[self.right_idx]);
                    self.right_idx += 1;
                    return Some(combined);
                }
                // All right rows exhausted for this left row — advance left.
                self.current_left = None;
            }

            // Fetch the next left row.
            match self.left.next() {
                Some(row) => {
                    self.current_left = Some(row);
                    self.right_idx = 0;
                }
                None => return None, // Left exhausted — cross product done.
            }
        }
    }
}
