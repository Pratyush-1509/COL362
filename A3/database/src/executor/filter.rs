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
        // Keep pulling from child until we find a passing row or exhaust it.
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
    // Resolve LHS column index
    let lhs_idx = column_index(schema, &pred.column_name);
    let lhs = &row[lhs_idx];

    // Resolve RHS value
    let rhs: Data = match &pred.value {
        ComparisionValue::Column(name) => row[column_index(schema, name)].clone(),
        ComparisionValue::I32(v) => Data::Int32(*v),
        ComparisionValue::I64(v) => Data::Int64(*v),
        ComparisionValue::F32(v) => Data::Float32(*v),
        ComparisionValue::F64(v) => Data::Float64(*v),
        ComparisionValue::String(s) => Data::String(s.clone()),
    };

    let cmp = lhs.partial_cmp(&rhs);

    match &pred.operator {
        ComparisionOperator::EQ => lhs == &rhs,
        ComparisionOperator::NE => lhs != &rhs,
        ComparisionOperator::GT => matches!(cmp, Some(std::cmp::Ordering::Greater)),
        ComparisionOperator::GTE => matches!(
            cmp,
            Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
        ),
        ComparisionOperator::LT => matches!(cmp, Some(std::cmp::Ordering::Less)),
        ComparisionOperator::LTE => matches!(
            cmp,
            Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
        ),
    }
}

fn column_index(schema: &Schema, name: &str) -> usize {
    schema
        .iter()
        .position(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("FilterOperator: column '{}' not found in schema", name))
}
