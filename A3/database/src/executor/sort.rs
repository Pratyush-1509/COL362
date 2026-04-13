use common::query::SortSpec;

use crate::executor::{Operator, SharedDisk};
use crate::row::{Row, Schema};

/// Sorts all rows from its child according to a list of sort specifications.
///
/// Current implementation: **in-memory sort**.
/// All rows are materialised into a Vec on the first `next()` call, sorted
/// with `sort_by`, and then returned one by one.
///
/// Limitation: will OOM on very large intermediate results.
/// TODO: replace with an external merge sort using `disk` scratch blocks
///       when memory pressure requires it.
pub struct SortOperator<'q> {
    /// Borrowed from the AST — column names and sort directions.
    sort_specs: &'q [SortSpec],
    /// The disk handle is reserved for the future external-sort upgrade.
    #[allow(dead_code)]
    disk: SharedDisk,
    /// The child operator; wrapped in Option so we can `.take()` it when
    /// draining on the first `next()` call.
    child: Option<Box<dyn Operator<'q> + 'q>>,
    /// All sorted output rows (populated on first `next()` call).
    sorted: Vec<Row>,
    /// Index of the next row to return from `sorted`.
    idx: usize,
    schema: Schema,
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
            sorted: Vec::new(),
            idx: 0,
            schema,
        }
    }

    /// Drain the child, sort all rows, and store them in `self.sorted`.
    /// Called exactly once, on the first `next()` invocation.
    fn materialise_and_sort(&mut self) {
        let mut child = self
            .child
            .take()
            .expect("SortOperator: child already consumed");

        let mut rows: Vec<Row> = Vec::new();
        while let Some(row) = child.next() {
            rows.push(row);
        }
        // child is dropped here — releasing its disk borrows, etc.

        let schema = &self.schema;
        let sort_specs = self.sort_specs;
        rows.sort_by(|a, b| compare_rows(a, b, sort_specs, schema));

        self.sorted = rows;
    }
}

impl<'q> Operator<'q> for SortOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        // Lazy initialisation: drain + sort on the very first call.
        if self.child.is_some() {
            self.materialise_and_sort();
        }

        if self.idx < self.sorted.len() {
            let row = self.sorted[self.idx].clone();
            self.idx += 1;
            Some(row)
        } else {
            None
        }
    }
}

// ── Row comparison ────────────────────────────────────────────────────────────

fn compare_rows(a: &Row, b: &Row, sort_specs: &[SortSpec], schema: &Schema) -> std::cmp::Ordering {
    for spec in sort_specs {
        let idx = schema
            .iter()
            .position(|(name, _)| name == &spec.column_name)
            .unwrap_or_else(|| {
                panic!("SortOperator: sort column '{}' not in schema", spec.column_name)
            });

        let ord = a[idx]
            .partial_cmp(&b[idx])
            .unwrap_or(std::cmp::Ordering::Equal);

        if ord != std::cmp::Ordering::Equal {
            return if spec.ascending { ord } else { ord.reverse() };
        }
    }
    std::cmp::Ordering::Equal
}
