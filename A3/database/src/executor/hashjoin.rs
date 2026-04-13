use std::collections::HashMap;
use crate::executor::Operator;
use crate::row::{make_join_key, JoinKey, Row, Schema};

pub struct HashJoinOperator<'q> {
    left: Box<dyn Operator<'q> + 'q>,
    schema: Schema,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    hash_table: HashMap<JoinKey, Vec<Row>>,
    current_left: Option<Row>,
    match_idx: usize,
    current_matches: *const Vec<Row>,
    // We need to keep a stable empty vec for the no-match case
    empty: Vec<Row>,
}

impl<'q> HashJoinOperator<'q> {
    pub fn new(
        left: Box<dyn Operator<'q> + 'q>,
        mut right: Box<dyn Operator<'q> + 'q>,
        join_keys: Vec<(usize, usize)>,
    ) -> Self {
        let mut schema = left.schema().clone();
        schema.extend_from_slice(right.schema());

        let left_key_indices: Vec<usize> = join_keys.iter().map(|&(l, _)| l).collect();
        let right_key_indices: Vec<usize> = join_keys.iter().map(|&(_, r)| r).collect();

        let mut hash_table: HashMap<JoinKey, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next() {
            let key = make_join_key(&row, &right_key_indices);
            hash_table.entry(key).or_default().push(row);
        }

        HashJoinOperator {
            left,
            schema,
            left_key_indices,
            right_key_indices,
            hash_table,
            current_left: None,
            match_idx: 0,
            current_matches: std::ptr::null(),
            empty: Vec::new(),
        }
    }

    fn get_matches(&self, key: &JoinKey) -> &Vec<Row> {
        self.hash_table.get(key).unwrap_or(&self.empty)
    }
}

impl<'q> Operator<'q> for HashJoinOperator<'q> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row> {
        loop {
            if let Some(ref left_row) = self.current_left {
                // Safety: current_matches points into hash_table which lives
                // as long as self, and current_left keeps it valid.
                let matches = unsafe { &*self.current_matches };
                if self.match_idx < matches.len() {
                    let mut combined = left_row.clone();
                    combined.extend_from_slice(&matches[self.match_idx]);
                    self.match_idx += 1;
                    return Some(combined);
                }
                self.current_left = None;
            }

            let left_row = self.left.next()?;
            let key = make_join_key(&left_row, &self.left_key_indices);
            let matches = self.get_matches(&key) as *const Vec<Row>;
            self.current_matches = matches;
            self.match_idx = 0;
            self.current_left = Some(left_row);
        }
    }
}
