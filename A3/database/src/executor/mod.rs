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

fn build_join_tree<'q>(
    scan_ops: Vec<&'q QueryOp>,
    all_predicates: Vec<&'q Predicate>,
    ctx: &DbContext,
    disk: SharedDisk,
) -> Box<dyn Operator<'q> + 'q> {
    let mut current = build_leaf(scan_ops[0], &all_predicates, ctx, Rc::clone(&disk));

    for i in 1..scan_ops.len() {
        let right = build_leaf(scan_ops[i], &all_predicates, ctx, Rc::clone(&disk));
        let left_schema = current.schema().clone();
        let right_schema = right.schema().clone();

        let join_keys = equi_join_keys(&all_predicates, &left_schema, &right_schema);

        if join_keys.is_empty() {
            current = Box::new(cross::CrossOperator::new(current, right, Rc::clone(&disk)));
        } else {
            let join_keys_copy = join_keys.clone();
            current = Box::new(hashjoin::HashJoinOperator::new(current, right, join_keys_copy));
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