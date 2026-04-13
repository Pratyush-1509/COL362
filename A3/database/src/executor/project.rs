use crate::executor::Operator;
use crate::row::{Row, Schema};

/// Selects and optionally renames a subset of columns from its child.
/// Output row order matches the order of `column_name_map`.
/// Row order from the child is preserved (Project is order-transparent).
pub struct ProjectOperator<'q> {
    /// (original_column_name, output_column_name) pairs, borrowed from the AST.
    column_name_map: &'q [(String, String)],
    /// Pre-computed: for each output column, the index of the source column in the child schema.
    source_indices: Vec<usize>,
    child: Box<dyn Operator<'q> + 'q>,
    schema: Schema,
}

impl<'q> ProjectOperator<'q> {
    pub fn new(
        column_name_map: &'q [(String, String)],
        child: Box<dyn Operator<'q> + 'q>,
    ) -> Self {
        let child_schema = child.schema();

        // Build the output schema and source index map once at construction time.
        let mut schema = Schema::new();
        let mut source_indices = Vec::new();

        for (orig_name, new_name) in column_name_map {
            let idx = child_schema
                .iter()
                .position(|(n, _)| n == orig_name)
                .unwrap_or_else(|| {
                    panic!("ProjectOperator: column '{}' not in child schema", orig_name)
                });
            let dtype = child_schema[idx].1.clone();
            schema.push((new_name.clone(), dtype));
            source_indices.push(idx);
        }

        ProjectOperator {
            column_name_map,
            source_indices,
            child,
            schema,
        }
    }
}

impl<'q> Operator<'q> for ProjectOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.child.next()?;
        // Pick exactly the columns listed in source_indices, in order.
        let projected: Row = self
            .source_indices
            .iter()
            .map(|&idx| row[idx].clone())
            .collect();
        Some(projected)
    }
}
