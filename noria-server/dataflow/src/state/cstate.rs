
use prelude::*;

/// A memoized computable state
use super::keyed_state::*;
use super::click_ana::ClickAnaState;

struct Memoization<T> {
    computer: T,
    // Because, for the time being, I only expect to use this for grouping UDF's
    // this should only ever contain a single row.
    memoization: Option<Row>,
}

struct StateElement<T> {
    state: KeyedState<Memoization<T>>
}

pub struct MemoizedComputableState<T> {
    states: Vec<StateElement<T>>,
}

impl <T: SizeOf> SizeOf for MemoizedComputableState<T> {
    fn size_of(&self) -> u64 {
        unimplemented!()
    }
    fn deep_size_of(&self) -> u64 {
        unimplemented!()
    }
}

impl super::State for ClickAnaState {
    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        unimplemented!()
    }
    fn is_useful(&self) -> bool {
        !self.states.is_empty()
    }
    fn is_partial(&self) -> bool {
        unimplemented!()
    }
    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
        unimplemented!()
    }
    fn rows(&self) -> usize {
        unimplemented!()
    }
    fn mark_filled(&mut self, key: Vec<DataType>, tag: Tag) {
        unimplemented!()
    }
    fn mark_hole(&mut self, key: &[DataType], tag: Tag) {
        unimplemented!()
    }
    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        unimplemented!()
    }
    fn keys(&self) -> Vec<Vec<usize>> {
        self.states.iter().map(|s| s.key().to_vec()).collect()
    }
    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        unimplemented!()
    }
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        unimplemented!()
    }
    fn evict_keys(&mut self, tag: Tag, keys: &[Vec<DataType>]) -> Option<(&[usize], u64)> {
        unimplemented!()
    }
    fn clear(&mut self) {
        unimplemented!()
    }
    fn as_click_ana_state<'a>(&'a mut self) -> Option<&'a mut ClickAnaState> {
        Option::Some(self)
    }
}
