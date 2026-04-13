use common::{
    query::{ComparisionOperator, ComparisionValue, Predicate},
    Data,
};

use crate::executor::Operator;
use crate::row::{Row, Schema};

/// Passes only rows from its child where ALL predicates evaluate to true.
pub struct FilterOperator<'q> {
    /// Borrowed slice from the query AST — no cloning needed.
    predicates: &'q [Predicate],
    child: Box<dyn Operator<'q> + 'q>,
    schema: Schema,
}

impl<'q> FilterOperator<'q> {
    pub fn new(predicates: &'q [Predicate], child: Box<dyn Operator<'q> + 'q>) -> Self {
        let schema = child.schema().clone();
        FilterOperator {
            predicates,
            child,
            schema,
        }
    }

    fn passes_all(&self, row: &Row) -> bool {
        self.predicates
            .iter()
            .all(|p| evaluate_predicate(row, &self.schema, p))
    }
}

impl<'q> Operator<'q> for FilterOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next()?;
            if self.passes_all(&row) {
                return Some(row);
            }
        }
    }
}

// ── Predicate evaluation ──────────────────────────────────────────────────────

fn evaluate_predicate(row: &Row, schema: &Schema, pred: &Predicate) -> bool {
    let lhs_idx = column_index(schema, &pred.column_name);
    let lhs = &row[lhs_idx];

    let rhs: Data = match &pred.value {
        ComparisionValue::Column(name) => row[column_index(schema, name)].clone(),
        ComparisionValue::I32(v) => Data::Int32(*v),
        ComparisionValue::I64(v) => Data::Int64(*v),
        ComparisionValue::F32(v) => Data::Float32(*v),
        ComparisionValue::F64(v) => Data::Float64(*v),
        ComparisionValue::String(s) => Data::String(s.clone()),
    };

    match &pred.operator {
        ComparisionOperator::EQ => data_eq(lhs, &rhs),
        ComparisionOperator::NE => !data_eq(lhs, &rhs),
        ComparisionOperator::GT => matches!(data_cmp(lhs, &rhs), Some(std::cmp::Ordering::Greater)),
        ComparisionOperator::GTE => matches!(
            data_cmp(lhs, &rhs),
            Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
        ),
        ComparisionOperator::LT => matches!(data_cmp(lhs, &rhs), Some(std::cmp::Ordering::Less)),
        ComparisionOperator::LTE => matches!(
            data_cmp(lhs, &rhs),
            Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
        ),
    }
}

// ── Mixed-type numeric comparison ─────────────────────────────────────────────
//
// The common::Data PartialOrd / PartialEq implementations only handle
// *same-variant* comparisons and return None / false for mixed types.
//
// In practice, a query may have a Float64 column with an I32 literal (e.g.
// `l_quantity <= 10` where the column is Float64 but the literal is I32(10)).
// We resolve this by coercing both sides to f64 for numeric types.
// Strings are only compared to strings.

/// Order comparison that handles mixed numeric types via f64 coercion.
fn data_cmp(a: &Data, b: &Data) -> Option<std::cmp::Ordering> {
    // Fast path: same variant — use the PartialOrd impl from common.
    if let Some(ord) = a.partial_cmp(b) {
        return Some(ord);
    }
    // Slow path: try coercing both to f64.
    match (to_f64(a), to_f64(b)) {
        (Some(x), Some(y)) => x.partial_cmp(&y),
        _ => None, // e.g. String vs numeric — incomparable
    }
}

/// Equality check that handles mixed numeric types.
fn data_eq(a: &Data, b: &Data) -> bool {
    // Fast path: same variant.
    if a == b {
        return true;
    }
    // Slow path: numeric coercion.
    match (to_f64(a), to_f64(b)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

/// Try to represent a Data value as f64 (for mixed-type numeric comparisons).
/// Returns None for non-numeric types (String).
fn to_f64(v: &Data) -> Option<f64> {
    match v {
        Data::Int32(x) => Some(*x as f64),
        Data::Int64(x) => Some(*x as f64),
        Data::Float32(x) => Some(*x as f64),
        Data::Float64(x) => Some(*x),
        Data::String(_) => None,
    }
}

fn column_index(schema: &Schema, name: &str) -> usize {
    schema
        .iter()
        .position(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("FilterOperator: column '{}' not found in schema", name))
}

pub struct OwnedFilterOperator<'q> {
    predicates: Vec<&'q Predicate>,
    child: Box<dyn Operator<'q> + 'q>,
    schema: Schema,
}

impl<'q> OwnedFilterOperator<'q> {
    pub fn new(predicates: Vec<&'q Predicate>, child: Box<dyn Operator<'q> + 'q>) -> Self {
        let schema = child.schema().clone();
        OwnedFilterOperator { predicates, child, schema }
    }
}

impl<'q> Operator<'q> for OwnedFilterOperator<'q> {
    fn schema(&self) -> &Schema { &self.schema }
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = self.child.next()?;
            if self.predicates.iter().all(|p| evaluate_predicate(&row, &self.schema, p)) {
                return Some(row);
            }
        }
    }
}
