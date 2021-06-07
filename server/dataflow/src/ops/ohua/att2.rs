
use super::super::grouped::{GroupedOperation, GroupedOperator};
use nom_sql::SqlType;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use crate::prelude::*;
use super::att3::TestCount;


#[derive(Serialize, Deserialize, Clone)]
pub enum GroupingFuncType {
    TestCount(TestCount),
}

impl From<TestCount> for GroupingFuncType {
    fn from(i: TestCount) -> Self {
        GroupingFuncType::TestCount(i)
    }
}

macro_rules! impl_grouping_udf_type {
    ($self:ident, $func:ident, $( $arg:ident ),* ) => {
        match *$self {
            GroupingFuncType::TestCount(ref mut c) => c.$func($($arg),*),
        }
    }
}

trait GroupingUDF {
    fn visit(&mut self, item: &DataType) -> DataType;
    fn unvisit(&mut self, item: &DataType) -> DataType;
}

impl GroupingUDF for TestCount {
    fn visit(&mut self, _item: &DataType) -> DataType {
        self.0 += 1;
        self.0.into()
    }

    fn unvisit(&mut self, _item: &DataType) -> DataType {
        self.0 -= 1;
        self.0.into()
    }
}

impl GroupingUDF for GroupingFuncType {
    fn visit(&mut self, item: &DataType) -> DataType {
        impl_grouping_udf_type!(self, visit, item)
    }
    fn unvisit(&mut self, item: &DataType) -> DataType {
        impl_grouping_udf_type!(self, unvisit, item)
    }
}

fn grouped_function_type_from_string(name: &String) -> GroupingFuncType {
    match name.as_str() {
        "test_count" => TestCount(0).into(),
        _ => unimplemented!(),
    }
}

pub fn grouped_function_from_string(parent: IndexPair, name: String) -> GroupingUDFOp {
    GroupingUDFOp {
        function: grouped_function_type_from_string(&name),
        parent: parent,
        function_name: name,
    }
}


#[derive(Clone, Serialize, Deserialize)]
pub struct GroupingUDFOp {
    function: GroupingFuncType,
    parent: IndexPair,
    function_name: String,
}

impl Ingredient for GroupingUDFOp {
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
        _executor: &mut dyn Executor,
        _from: LocalNodeIndex,
        data: Records,
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
                let res = if spin {
                    self.function.visit(i)
                } else {
                    self.function.unvisit(i)
                };
                Record::from((vec![res], spin))
            })
            .collect();

        ProcessingResult {
            results: res,
            ..Default::default()
        }
    }

    fn description(&self, _detailed: bool) -> String {
        format!("grouping-udf:{}", self.function_name)
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
