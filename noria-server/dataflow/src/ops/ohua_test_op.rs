use super::grouped::{ GroupedOperation, GroupedOperator };
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use prelude::*;

pub trait Semigroup {
    fn append(&self, other: &Self) -> Self;
}

pub trait Monoid: Semigroup {
    fn empty() -> Self;
}

pub trait Group: Monoid {
    fn reverse(&self) -> Self;
}

pub trait Descriptive {
    fn description(detailed: bool) -> String;
}

pub trait Construct<Source> {
    fn construct(src: &Source) -> Self;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TestCount(i64);

impl Construct<DataType> for TestCount {
    fn construct(src: &DataType) -> Self {
        match *src {
            DataType::None => TestCount(0),
            DataType::Int(i) => TestCount(i.into()),
            DataType::BigInt(i) => TestCount(i.clone()),
            _ => unimplemented!(),
        }
    }
}

impl Semigroup for TestCount {
    fn append(&self, other: &TestCount) -> TestCount {
        TestCount(self.0 + other.0)
    }
}

impl Monoid for TestCount {
    fn empty() -> TestCount {
        TestCount(0)
    }
}

impl Group for TestCount {
    fn reverse(&self) -> TestCount {
        TestCount(-self.0)
    }
}

impl Descriptive for TestCount {
    fn description(detailed: bool) -> String {
        "test-count".into()
    }
}

impl From<DataType> for TestCount {
    fn from(i: DataType) -> Self {
        TestCount(i.into())
    }
}

impl Into<DataType> for TestCount {
    fn into(self) -> DataType {
        self.0.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupedUDF<F> {
    over: usize,
    group: Vec<usize>,
    initial: F,
}

enum DiffSum {
    TestCount(TestCount),
    Empty,
}

impl TryFrom<DiffSum> for TestCount {
    type Error = &'static str;
    fn try_from(ds: DiffSum) -> Result<Self, Self::Error> {
        match ds {
            DiffSum::Empty => Ok(TestCount::empty()),
            DiffSum::TestCount(c) => Ok(c),
            _ => Err("Sum variant is inappropriate"),
        }
    }
}

impl Semigroup for DiffSum {
    fn append(&self, other: &DiffSum) -> DiffSum {
        unimplemented!();
        // match *self {
        //     DiffSum::TestCount(c) => {
        //         let conv : TestCount = other.try_into().unwrap();
        //         DiffSum::TestCount(c.append(conv))
        //     },
        //     Empty => match *other {
        //         Empty => Empty,
        //         _ => other.append(&Empty)
        //     }
        // }
    }
}

impl Monoid for DiffSum {
    fn empty() -> Self {
        DiffSum::Empty
    }
}

impl Group for DiffSum {
    fn reverse(&self) -> Self {
        match self {
            DiffSum::Empty => DiffSum::Empty,
            DiffSum::TestCount(c) => DiffSum::TestCount(c.reverse()),
        }
    }
}

impl<F: 'static> GroupedOperation for GroupedUDF<F>
where
    F: Group
        + Into<DataType>
        + From<DataType>
        + Descriptive
        + Clone
        + std::fmt::Debug
        + Construct<DataType>,
{
    type Diff = F;

    fn setup(&mut self, _parent: &Node) {}

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn to_diff(&self, r: &[DataType], positive: bool) -> F {
        let v: F = r.get(self.over).unwrap().clone().into();
        if positive {
            v
        } else {
            v.reverse()
        }
    }

    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut Iterator<Item = Self::Diff>,
    ) -> DataType {
        let v: F = match current {
            None => self.initial.clone(),
            Some(v0) => F::construct(v0),
        };
        diffs.fold(v, |n, d| n.append(&d)).into()
    }

    fn description(&self, detailed: bool) -> String {
        format!("grouped-udf:{}", F::description(detailed))
    }

    fn over_columns(&self) -> Vec<usize> {
        vec![self.over]
    }
}

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

pub fn new_grouped_function_from_string(
    parent: NodeIndex,
    over_col: usize,
    name: String,
    group: Vec<usize>,
) -> GroupedOperator<GroupedUDF<TestCount>> {
    assert!(name == "test_count");
    GroupedOperator::new(
        parent,
        GroupedUDF {
            over: over_col,
            group: group,
            initial: TestCount::empty(),
        },
    )
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
