pub mod cross;
pub mod filter;
pub mod project;
pub mod scan;
pub mod sort;

use std::cell::RefCell;
use std::rc::Rc;

use common::query::QueryOp;
use db_config::DbContext;

use crate::disk::DiskManager;
use crate::row::{Row, Schema};

/// A shared, reference-counted handle to the DiskManager.
/// Using Rc<RefCell<>> lets every operator borrow the disk mutably
/// without threading lifetime parameters through the whole tree.
/// This is safe because only one operator's `next()` runs at a time
/// (pull-based, single-threaded pipeline).
pub type SharedDisk = Rc<RefCell<DiskManager>>;

// ── Operator trait ────────────────────────────────────────────────────────────

/// Every node in the query plan implements `Operator`.
///
/// Lifetime `'q` ties operator internals to the lifetime of the `Query`
/// struct owned by `main`, so we never need to clone query AST data.
pub trait Operator<'q> {
    /// The output schema: column names and types in order.
    fn schema(&self) -> &Schema;

    /// Pull the next output row, or `None` when exhausted.
    fn next(&mut self) -> Option<Row>;
}

// ── Tree builder ──────────────────────────────────────────────────────────────

/// Recursively build a boxed operator tree from a QueryOp AST node.
///
/// `'q` is the lifetime of the AST borrow — operators hold slices/refs into
/// the AST so nothing needs to be cloned from `common::query` types.
pub fn build_operator<'q>(
    op: &'q QueryOp,
    ctx: &DbContext,
    disk: SharedDisk,
) -> Box<dyn Operator<'q> + 'q> {
    match op {
        QueryOp::Scan(data) => Box::new(scan::ScanOperator::new(
            &data.table_id,
            ctx,
            Rc::clone(&disk),
        )),

        QueryOp::Filter(data) => {
            let child = build_operator(&data.underlying, ctx, Rc::clone(&disk));
            Box::new(filter::FilterOperator::new(&data.predicates, child))
        }

        QueryOp::Project(data) => {
            let child = build_operator(&data.underlying, ctx, Rc::clone(&disk));
            Box::new(project::ProjectOperator::new(&data.column_name_map, child))
        }

        QueryOp::Sort(data) => {
            let child = build_operator(&data.underlying, ctx, Rc::clone(&disk));
            Box::new(sort::SortOperator::new(
                &data.sort_specs,
                child,
                Rc::clone(&disk),
            ))
        }

        QueryOp::Cross(data) => {
            let left = build_operator(&data.left, ctx, Rc::clone(&disk));
            let right = build_operator(&data.right, ctx, Rc::clone(&disk));
            Box::new(cross::CrossOperator::new(left, right, Rc::clone(&disk)))
        }
    }
}
