pub mod cross;
pub mod filter;
pub mod hashjoin;
pub mod project;
pub mod scan;
pub mod sort;

use std::cell::RefCell;
use std::rc::Rc;

use common::query::{ComparisionValue, Predicate, QueryOp};
use db_config::DbContext;

use crate::disk::DiskManager;
use crate::row::{Row, Schema};

pub type SharedDisk = Rc<RefCell<DiskManager>>;

pub trait Operator<'q> {
    fn schema(&self) -> &Schema;
    fn next(&mut self) -> Option<Row>;
}

fn collect_join_parts<'q>(
    op: &'q QueryOp,
    scans: &mut Vec<&'q QueryOp>,
    predicates: &mut Vec<&'q Predicate>,
) -> bool {
    match op {
        QueryOp::Scan(_) => { scans.push(op); true }
        QueryOp::Filter(data) => {
            for p in &data.predicates { predicates.push(p); }
            collect_join_parts(&data.underlying, scans, predicates)
        }
        QueryOp::Cross(data) => {
            collect_join_parts(&data.left, scans, predicates)
                && collect_join_parts(&data.right, scans, predicates)
        }
        _ => { scans.push(op); true }
    }
}

fn equi_join_keys(
    predicates: &[&Predicate],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Vec<(usize, usize)> {
    let mut keys = Vec::new();
    for pred in predicates {
        if !matches!(pred.operator, common::query::ComparisionOperator::EQ) { continue; }
        if let ComparisionValue::Column(rhs_name) = &pred.value {
            let li = left_schema.iter().position(|(n, _)| n == &pred.column_name);
            let ri = right_schema.iter().position(|(n, _)| n == rhs_name);
            let li2 = left_schema.iter().position(|(n, _)| n == rhs_name);
            let ri2 = right_schema.iter().position(|(n, _)| n == &pred.column_name);
            if let (Some(l), Some(r)) = (li, ri) { keys.push((l, r)); }
            else if let (Some(l), Some(r)) = (li2, ri2) { keys.push((l, r)); }
        }
    }
    keys
}

fn build_leaf<'q>(
    op: &'q QueryOp,
    all_predicates: &[&'q Predicate],
    ctx: &DbContext,
    disk: SharedDisk,
) -> Box<dyn Operator<'q> + 'q> {
    match op {
        QueryOp::Scan(_) => {
            let scan = build_operator(op, ctx, Rc::clone(&disk));
            let scan_schema = scan.schema().clone();
            let pushdown: Vec<&'q Predicate> = all_predicates
                .iter()
                .filter(|p| {
                    let lhs_ok = scan_schema.iter().any(|(n, _)| n == &p.column_name);
                    let rhs_ok = match &p.value {
                        ComparisionValue::Column(rhs) => scan_schema.iter().any(|(n, _)| n == rhs),
                        _ => true,
                    };
                    lhs_ok && rhs_ok
                })
                .copied()
                .collect();
            if pushdown.is_empty() {
                scan
            } else {
                Box::new(filter::OwnedFilterOperator::new(pushdown, scan))
            }
        }
        _ => build_operator(op, ctx, disk),
    }
}

/// Estimate the physical size (file blocks) of a leaf QueryOp.
/// Recursively unwraps Filter/Project wrappers to reach the Scan.
/// Used for join ordering: larger tables should be probe (left) side.
///
/// If the table's stats include a CardinalityData entry we use the maximum
/// cardinality across all columns as a row-count estimate (scaled to blocks
/// via a conservative 128-byte/row assumption so it is comparable to
/// get_file_num_blocks).  This avoids issuing extra disk-protocol commands
/// while still benefiting from metadata on larger datasets.
/// Falls back to get_file_num_blocks when no stats are available.
fn scan_op_blocks(op: &QueryOp, ctx: &DbContext, disk: &SharedDisk) -> u64 {
    match op {
        QueryOp::Scan(data) => {
            if let Some(spec) = ctx.get_table_specs().iter().find(|t| t.name == data.table_id) {
                // Try to read a cardinality stat from any column.
                // The maximum cardinality across all columns is a lower bound
                // on the number of rows (a column cannot have more distinct
                // values than there are rows).  For fact tables like lineitem
                // the PK-ish column (l_orderkey) has cardinality ≈ row count.
                let mut max_card: u64 = 0;
                for col in &spec.column_specs {
                    if let Some(stats) = &col.stats {
                        for stat in stats {
                            if let db_config::statistics::ColumnStat::CardinalityStat(
                                db_config::statistics::CardinalityData(count),
                            ) = stat
                            {
                                if *count > max_card {
                                    max_card = *count;
                                }
                            }
                        }
                    }
                }
                if max_card > 0 {
                    // Convert row-count estimate to a synthetic block count
                    // (128 bytes/row is conservative — avoids underestimating
                    // wide tables like lineitem).
                    return (max_card * 128).div_ceil(4096);
                }
                // Fall back to actual file size from the disk manager.
                disk.borrow_mut().get_file_num_blocks(&spec.file_id)
            } else {
                0
            }
        }
        QueryOp::Filter(data) => scan_op_blocks(&data.underlying, ctx, disk),
        QueryOp::Project(data) => scan_op_blocks(&data.underlying, ctx, disk),
        _ => 0,
    }
}

fn build_join_tree<'q>(
    scan_ops: Vec<&'q QueryOp>,
    all_predicates: Vec<&'q Predicate>,
    ctx: &DbContext,
    disk: SharedDisk,
) -> Box<dyn Operator<'q> + 'q> {
    let n = scan_ops.len();

    // ── Join reordering ─────────────────────────────────────────────────────
    // Pre-compute schemas and file-block counts for all leaf operators.
    let schemas: Vec<Schema> = scan_ops
        .iter()
        .map(|op| build_operator(op, ctx, Rc::clone(&disk)).schema().clone())
        .collect();
    let scan_blocks: Vec<u64> = scan_ops
        .iter()
        .map(|op| scan_op_blocks(op, ctx, &disk))
        .collect();

    // Start with the LARGEST table so it becomes the streaming probe side.
    // This ensures the large fact table (e.g. lineitem) is never the build
    // side of a hash join, avoiding expensive grace-hash-join disk I/O.
    let start = (0..n).max_by_key(|&i| scan_blocks[i]).unwrap_or(0);
    let mut order: Vec<usize> = vec![start];
    let mut remaining: Vec<usize> = (0..n).filter(|&i| i != start).collect();

    while !remaining.is_empty() {
        // Combined schema of the tables joined so far.
        let combined: Schema = order
            .iter()
            .flat_map(|&i| schemas[i].iter().cloned())
            .collect();

        // Primary key: number of equi-join keys connecting this table to the
        // current set (more is better — avoids cross products).
        // Tie-break: larger table first so it goes on the probe side while
        // still small enough to be build side here.
        let best_pos = remaining
            .iter()
            .enumerate()
            .max_by_key(|&(_, &i)| {
                let key_count = equi_join_keys(&all_predicates, &combined, &schemas[i]).len();
                (key_count, scan_blocks[i])
            })
            .map(|(pos, _)| pos)
            .unwrap();

        order.push(remaining.remove(best_pos));
    }

    // ── Build join tree in reordered sequence ───────────────────────────────
    let mut current = build_leaf(scan_ops[order[0]], &all_predicates, ctx, Rc::clone(&disk));

    for step in 1..n {
        let scan_idx = order[step];
        let right = build_leaf(scan_ops[scan_idx], &all_predicates, ctx, Rc::clone(&disk));
        let left_schema = current.schema().clone();
        let right_schema = right.schema().clone();

        let join_keys = equi_join_keys(&all_predicates, &left_schema, &right_schema);

        if join_keys.is_empty() {
            current = Box::new(cross::CrossOperator::new(current, right, Rc::clone(&disk)));
        } else {
            let join_keys_copy = join_keys.clone();
            current = Box::new(hashjoin::HashJoinOperator::new(current, right, join_keys_copy, Rc::clone(&disk)));
        }

        // Apply residual predicates now satisfiable (excluding used equi-join keys)
        let combined_schema = current.schema().clone();
        let residual: Vec<&'q Predicate> = all_predicates
            .iter()
            .filter(|p| {
                let lhs_ok = combined_schema.iter().any(|(n, _)| n == &p.column_name);
                let rhs_ok = match &p.value {
                    ComparisionValue::Column(rhs) => combined_schema.iter().any(|(n, _)| n == rhs),
                    _ => true,
                };
                if !lhs_ok || !rhs_ok { return false; }
                if let ComparisionValue::Column(rhs) = &p.value {
                    if matches!(p.operator, common::query::ComparisionOperator::EQ) {
                        let was_join_key = join_keys.iter().any(|&(li, ri)| {
                            (left_schema.get(li).map(|(n,_)| n == &p.column_name).unwrap_or(false)
                                && right_schema.get(ri).map(|(n,_)| n == rhs).unwrap_or(false))
                            || (left_schema.get(li).map(|(n,_)| n == rhs).unwrap_or(false)
                                && right_schema.get(ri).map(|(n,_)| n == &p.column_name).unwrap_or(false))
                        });
                        if was_join_key { return false; }
                    }
                }
                true
            })
            .copied()
            .collect();

        if !residual.is_empty() {
            current = Box::new(filter::OwnedFilterOperator::new(residual, current));
        }
    }

    current
}

/// Returns true if the data under a Sort node is already in the required order,
/// meaning the Sort can be skipped entirely.
/// Conditions (all must hold):
///   1. Every sort spec is ascending.
///   2. Every sort column has IsPhysicallyOrdered in the db_config for the table.
///   3. The source is a single table (Scan optionally wrapped in Filter/Project),
///      not a join — physical ordering is lost after joining.
fn sort_already_satisfied(underlying: &QueryOp, sort_specs: &[common::query::SortSpec], ctx: &DbContext) -> bool {
    if sort_specs.iter().any(|s| !s.ascending) {
        return false;
    }

    // Unwrap Filter/Project to find the base Scan; bail if we hit a join.
    let mut cur = underlying;
    loop {
        match cur {
            QueryOp::Scan(data) => {
                // Check every sort column is IsPhysicallyOrdered for this table.
                let Some(spec) = ctx.get_table_specs().iter().find(|t| t.name == data.table_id) else {
                    return false;
                };
                return sort_specs.iter().all(|ss| {
                    spec.column_specs.iter().any(|col| {
                        col.column_name == ss.column_name
                            && col.stats.as_ref().map_or(false, |stats| {
                                stats.iter().any(|s| {
                                    matches!(s, db_config::statistics::ColumnStat::IsPhysicallyOrdered)
                                })
                            })
                    })
                });
            }
            QueryOp::Filter(data) => cur = &data.underlying,
            QueryOp::Project(data) => cur = &data.underlying,
            _ => return false, // Cross/Sort/join subtree — physical order not guaranteed
        }
    }
}

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
            let mut scans: Vec<&'q QueryOp> = Vec::new();
            let mut predicates: Vec<&'q Predicate> = Vec::new();

            if collect_join_parts(op, &mut scans, &mut predicates) && scans.len() > 1 {
                // Check for column name overlap between scans
                let schemas: Vec<Schema> = scans.iter()
                    .map(|s| build_operator(s, ctx, Rc::clone(&disk)).schema().clone())
                    .collect();
                let has_overlap = (0..schemas.len()).any(|i| {
                    (i+1..schemas.len()).any(|j| {
                        schemas[i].iter().any(|(n,_)| schemas[j].iter().any(|(m,_)| n == m))
                    })
                });
                if !has_overlap {
                    return build_join_tree(scans, predicates, ctx, Rc::clone(&disk));
                }
            }

            let child = build_operator(&data.underlying, ctx, Rc::clone(&disk));
            Box::new(filter::FilterOperator::new(&data.predicates, child))
        }

        QueryOp::Project(data) => {
            let child = build_operator(&data.underlying, ctx, Rc::clone(&disk));
            Box::new(project::ProjectOperator::new(&data.column_name_map, child))
        }

        QueryOp::Sort(data) => {
            if sort_already_satisfied(&data.underlying, &data.sort_specs, ctx) {
                return build_operator(&data.underlying, ctx, disk);
            }
            let child = build_operator(&data.underlying, ctx, Rc::clone(&disk));
            Box::new(sort::SortOperator::new(&data.sort_specs, child, Rc::clone(&disk)))
        }

        QueryOp::Cross(data) => {
            let left = build_operator(&data.left, ctx, Rc::clone(&disk));
            let right = build_operator(&data.right, ctx, Rc::clone(&disk));
            Box::new(cross::CrossOperator::new(left, right, Rc::clone(&disk)))
        }
    }
}