use crate::prelude::*;
use nom_sql::SqlType;
use crate::ops::ohua::Typed;

#[derive(Debug)]
pub enum Action {
    Add(i32)
}

#[derive(Debug)]
pub struct Sum(i32);

impl Sum {
    pub fn apply(&mut self, action: Action, is_positive: bool) {
        match action {
            Action::Add(i) =>
                if is_positive {
                    self.0 += i;
                } else {
                    self.0 -= i;
                }
        }
    }

    pub fn get_sum(&mut self) -> i32 {
        self.0
    }
}

impl SizeOf for Sum {
    fn deep_size_of(&self) -> u64 {
        self.size_of()
    }
    fn size_of(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

}

impl Default for Sum {
    fn default() -> Self {
        Sum(0)
    }
}

impl Typed for Sum {
    type Type = SqlType;
    fn typ_static() -> Self::Type {
        SqlType::Int(32)
    }
}
