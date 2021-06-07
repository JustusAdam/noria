use crate::prelude::*;
use nom_sql::SqlType;
use crate::ops::ohua::Typed;

#[derive(Debug)]
pub enum Action {
    Increment
}

#[derive(Debug)]
pub struct Count(i32);

impl Count {
    pub fn apply(&mut self, action: Action, is_positive: bool) {
        if is_positive {
            self.0 += 1;
        } else {
            self.0 -= 1;
        }
    }
    pub fn get_value(&mut self) -> i32 {
        self.0
    }
}

impl SizeOf for Count {

    fn deep_size_of(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
    fn size_of(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
}

impl Default for Count {
    fn default() -> Self {
        Count(0)
    }
}

impl Typed for Count {
    type Type = SqlType;
    fn typ_static() -> Self::Type {
        SqlType::Int(32)
    }
}
