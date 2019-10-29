
use super::super::grouped::{GroupedOperation, GroupedOperator};
use nom_sql::SqlType;
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
pub struct TestCount(pub i64);

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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Product(f64);

impl Construct<DataType> for Product {
    fn construct(src: &DataType) -> Self {
        Product(src.into())
    }
}

impl Semigroup for Product {
    fn append(&self, other: &Self) -> Self {
        Product(self.0 * other.0)
    }
}

impl Monoid for Product {
    fn empty() -> Self {
        Product(1.0)
    }
}

impl Group for Product {
    fn reverse(&self) -> Self {
        Product(match *self {
            Product(i) if i == 0.0 => 0.0,
            Product(i) => 1.0 / i,
        })
    }
}

impl Descriptive for Product {
    fn description(detailed: bool) -> String {
        "Π".into()
    }
}

impl From<DataType> for Product {
    fn from(dt: DataType) -> Self {
        let f: f64 = (&dt).into();
        Product(f)
    }
}

impl Into<DataType> for Product {
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

pub trait Typed {
    type Type;
    fn typ_static() -> Self::Type;
    fn typ(&self) -> Self::Type {
        Self::typ_static()
    }
}

impl Typed for TestCount {
    type Type = SqlType;
    fn typ_static() -> Self::Type {
        SqlType::Bigint(64)
    }
}

impl Typed for Product {
    type Type = SqlType;
    fn typ_static() -> Self::Type {
        SqlType::Double
    }
}

impl<F: Typed> Typed for GroupedUDF<F> {
    type Type = F::Type;
    fn typ_static() -> Self::Type {
        F::typ_static()
    }
    fn typ(&self) -> Self::Type {
        self.initial.typ()
    }
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


pub fn new_grouped_function_from_string(
    parent: NodeIndex,
    over_cols: Vec<usize>,
    name: String,
    group: Vec<usize>,
) -> NodeOperator {
    match name.as_ref() {
        // <begin(generated-reducing-operator-inits)>
        "ohua.generated/op_s_acc_1_0" => {
            assert_eq!(over_cols.len(), 1);
            super::generated::op_s_acc_1_0::Op_s_acc_1_0::new(parent, 0, group).into()
        },
        "ohua.generated/op_s_acc_0_0" => {
            assert_eq!(over_cols.len(), 1);
            super::generated::op_s_acc_0_0::Op_s_acc_0_0::new(parent, 0, over_cols[0], group).into()
        },
        // <end(generated-reducing-operator-inits)>
        "test_count" => GroupedOperator::new(
            parent,
            GroupedUDF {
                over: over_cols[0],
                group: group,
                initial: TestCount::empty(),
            },
        ).into(),
        "prod" => GroupedOperator::new(
            parent,
            GroupedUDF {
                over: over_cols[0],
                group: group,
                initial: Product::empty(),
            },
        ).into(),
        // TODO Get the categories from somewhere
        "ohua.generated/click_ana_manual" => {
            assert_eq!(over_cols.len(), 2);
            super::click_ana::ClickAna::new(
                parent,
                1,
                2,
                over_cols[0],
                over_cols[1],
                group,
        ).into()},
        _ => panic!("Unknown generated grouping operator: {}", name),
    }
}

pub fn new_simple_function_from_string(
    parent: NodeIndex,
    over_cols: Vec<usize>,
    name: String,
    carry: usize,
) -> NodeOperator {
    match name.as_ref() {
        // <begin(generated-simple-operator-inits)>
        "ohua.generated/op_p_div_or_zero__0" => {
            assert_eq!(over_cols.len(), 2);
            super::generated::op_p_div_or_zero__0::Op_p_div_or_zero__0::new( parent
            , 0
            , over_cols[0]
            , over_cols[1]
            , carry ).into()
        },
        // <end(generated-simple-operator-inits)>
        " " => panic!("This is only for type inference"),
        _ => panic!("Unknown simple generated operator: {}", name)
    }
}
