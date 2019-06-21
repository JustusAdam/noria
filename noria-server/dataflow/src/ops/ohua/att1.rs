
use super::super::grouped::{GroupedOperation, GroupedOperator};
use nom_sql::SqlType;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use prelude::*;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OhuaTestOp {
    /// Assume we only have one ancestor for now
    parent: IndexPair,
    //parents: Vec<IndexPair>,
}

impl OhuaTestOp {
    pub fn new(parents: Vec<NodeIndex>) -> OhuaTestOp {
        assert!(parents.len() == 1);
        Self {
            parent: parents[0].into(),
        }
    }
}

impl Ingredient for OhuaTestOp {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.parent.as_global()]
    }

    fn on_connected(&mut self, _graph: &Graph) {
        // No I need to do something here?
    }

    fn on_commit(&mut self, _you: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.parent.remap(remap)
    }

    fn on_input(
        &mut self,
        _executor: &mut Executor,
        _from: LocalNodeIndex,
        data: Records,
        _tracer: &mut Tracer,
        _replay_key_cols: Option<&[usize]>,
        _domain: &DomainNodes,
        _states: &StateMap,
    ) -> ProcessingResult {
        let res = data
            .iter()
            .map(|d| {
                let spin = d.is_positive();
                let vals = d.rec();
                assert!(vals.len() == 1);
                let ref i = vals[0];
                assert!(i.is_integer());
                let i0: i64 = i.into();
                Record::from((vec![DataType::from(i0 + 40)], spin))
            })
            .collect();

        ProcessingResult {
            results: res,
            ..Default::default()
        }
    }

    fn description(&self, _detailed: bool) -> String {
        "+40".into()
    }

    fn suggest_indexes(&self, _you: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, i: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.parent.as_global(), i)])
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.parent.as_global(), Some(column))]
    }
}
