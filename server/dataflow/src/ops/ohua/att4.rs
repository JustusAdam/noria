
use super::grouped::{GroupedOperation, GroupedOperator};
use nom_sql::SqlType;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use prelude::*;

trait ReversableStatefulFunction {
    type Input;
    type Output;

    fn apply(&mut self, i: Self::Input, positive: bool) -> &Self::Output;
}

struct GroupingUDF {
    on: Vec<usize>,
}
