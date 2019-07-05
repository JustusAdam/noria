
use std::rc::Rc;
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
    state: KeyedState<Rc<Memoization<T>>>,
    key: Vec<usize>,
    partial: bool,
}

impl <T> StateElement<T> {
    fn new(columns: &[usize], partial: bool) -> Self {
        Self {
            key: Vec::from(columns),
            state: columns.into(),
            partial,
        }
    }
}

pub struct MemoizedComputableState<T> {
    states: Vec<StateElement<T>>,
    mem_size: u64,
}

impl <T: SizeOf> SizeOf for MemoizedComputableState<T> {
    fn size_of(&self) -> u64 {
        unimplemented!()
    }
    fn deep_size_of(&self) -> u64 {
        self.mem_size
    }
}

impl super::State for ClickAnaState {
    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        let (i, exists) = if let Some(i) = self.state_for(columns) {
            // already keyed by this key; just adding tags
                (i, true)
        } else {
            // will eventually be assigned
            (self.state.len(), false)
        };

        if let Some(ref p) = partial {
            for &tag in p {
                self.by_tag.insert(tag, i);
            }
        }

        if exists {
            return;
        }

        self.state
            .push(StateElement::new(columns, partial.is_some()));

        if !self.state.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();

            if !old.is_empty() {
                assert!(!old[0].partial());
                for r in old[0].values() {
                    new.insert_row(r.clone());
                }
            }
        }
    }
    fn is_useful(&self) -> bool {
        !self.states.is_empty()
    }
    fn is_partial(&self) -> bool {
        self.states.iter().any(StateElement::partial)
    }
    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
        if self.is_partial() {
            records.retain(|r| {
                // we need to check that we're not erroneously filling any holes
                // there are two cases here:
                //
                //  - if the incoming record is a partial replay (i.e., partial.is_some()), then we
                //    *know* that we are the target of the replay, and therefore we *know* that the
                //    materialization must already have marked the given key as "not a hole".
                //  - if the incoming record is a normal message (i.e., partial.is_none()), then we
                //    need to be careful. since this materialization is partial, it may be that we
                //    haven't yet replayed this `r`'s key, in which case we shouldn't forward that
                //    record! if all of our indices have holes for this record, there's no need for us
                //    to forward it. it would just be wasted work.
                //
                //    XXX: we could potentially save come computation here in joins by not forcing
                //    `right` to backfill the lookup key only to then throw the record away
                match *r {
                    Record::Positive(ref r) => unimplemented!(),
                    Record::Negative(ref r) => unimplemented!(),
                }
            });
        } else {
            for r in records.iter() {
                // This should have been handled by the operator, so I only do
                // some checking that the values line up
                match *r {
                    Record::Positive(ref r) => {
                        debug_assert!(self.is_the_same(r))
                    }
                    Record::Negative(ref r) => {
                        debug_assert!(self.isnt_contained(r))
                    }
                }
            }
        }
    }
    fn rows(&self) -> usize {
        self.state.iter().map(StateElement::rows).sum()
    }
    fn mark_filled(&mut self, key: Vec<DataType>, tag: Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[&tag];
        self.state[index].mark_filled(key);
    }
    fn mark_hole(&mut self, key: &[DataType], tag: Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[&tag];
        let freed_bytes = self.state[index].mark_hole(key);
        self.mem_size = self.mem_size.checked_sub(freed_bytes).unwrap();
    }
    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        let index = self
            .state_for(columns)
            .expect("lookup on non-indexed column set");
        self.state[index].lookup(key)
    }
    fn keys(&self) -> Vec<Vec<usize>> {
        self.states.iter().map(|s| s.key().to_vec()).collect()
    }
    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        assert!(!self.state[0].partial());
        self.state[0].values().flat_map(fix).collect()
    }
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys, bytes_freed)
    }
    fn evict_keys(&mut self, tag: Tag, keys: &[Vec<DataType>]) -> Option<(&[usize], u64)> {
        self.by_tag.get(&tag).cloned().map(move |index| {
            let bytes = self.state[index].evict_keys(keys);
            self.mem_size = self.mem_size.saturating_sub(bytes);
            (self.state[index].key(), bytes)
        })
    }
    fn clear(&mut self) {
        for state in &mut self.state {
            state.clear();
        }
        self.mem_size = 0;
    }
    fn as_click_ana_state<'a>(&'a mut self) -> Option<&'a mut ClickAnaState> {
        Option::Some(self)
    }
}

impl ClickAnaState {
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| s.key() == cols)
    }

    fn is_the_same(&self, row: &[DataType]) -> bool {
        self.states.iter().all(|s| s.is_same_if_contained(row))
    }

    fn isnt_contained(&self, row: &[DataType]) -> bool {
        unimplemented!()
    }
}
