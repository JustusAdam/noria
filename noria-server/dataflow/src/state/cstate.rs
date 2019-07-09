use prelude::*;
use rand::{Rng, ThreadRng};
use std::collections::HashMap;
use std::rc::Rc;

use super::click_ana::ClickAnaState;
use super::keyed_state::*;
use super::mk_key::{ MakeKey, key_type_from_row };

#[derive(Debug)]
struct Memoization<T> {
    computer: T,
    // Because, for the time being, I only expect to use this for grouping UDF's
    // this should only ever contain a single row.
    memoization: Option<Row>,
}

impl<T> Memoization<T> {
    fn value<'a>(&'a self) -> &'a Row {
        &self.memoization.as_ref().unwrap()
    }
}

use std::ops::Index;

impl<T> Index<usize> for Memoization<T> {
    type Output = DataType;
    fn index(&self, index: usize) -> &DataType {
        self.value().index(index)
    }
}

#[derive(Debug)]
struct MemoElem<T>(Rc<Memoization<T>>);

impl<T> MemoElem<T> {
    fn value<'a>(&'a self) -> &'a Row {
        self.0.value()
    }
}

impl<T> Clone for MemoElem<T> {
    fn clone(&self) -> Self {
        MemoElem(self.0.clone())
    }
}

// Same assumptions as with `Row` apply here
unsafe impl<T> Send for MemoElem<T> {}
unsafe impl<T> Sync for MemoElem<T> {}

impl<T: SizeOf> SizeOf for Memoization<T> {
    fn size_of(&self) -> u64 {
        unimplemented!()
    }

    fn deep_size_of(&self) -> u64 {
        unimplemented!()
    }
}

impl<T: SizeOf> DeallocSize for MemoElem<T> {
    fn dealloc_size(&self) -> u64 {
        if Rc::strong_count(&self.0) == 1 {
            self.0.deep_size_of()
        } else {
            0
        }
    }
}

struct StateElement<T> {
    state: KeyedState<Option<MemoElem<T>>>,
    key: Vec<usize>,
    partial: bool,
}

macro_rules! insert_row_match_impl {
    ($self:ident, $r:ident, $map:ident) => {{
        let key = MakeKey::from_row(&$self.key, $r.0.value());
        match $map.entry(key) {
            Entry::Occupied(mut rs) => {
                let res = rs.get_mut().replace($r);
                debug_assert!(res.is_none(), "Key already exists");
            }
            Entry::Vacant(..) if $self.partial => return false,
            rs @ Entry::Vacant(..) => {
                let res = rs.or_default().replace($r);
                debug_assert!(res.is_none(), "How could this happen?");
            }
        }
    }};
}

use std::hash::{BuildHasher, Hash};

impl<T> StateElement<T> {
    fn new(columns: &[usize], partial: bool) -> Self {
        Self {
            key: Vec::from(columns),
            state: columns.into(),
            partial,
        }
    }

    fn rows(&self) -> usize {
        unimplemented!()
    }

    fn key<'a>(&'a self) -> &'a [usize] {
        &self.key
    }

    fn partial(&self) -> bool {
        self.partial
    }

    fn values<'a>(&'a self) -> Box<Iterator<Item = &'a MemoElem<T>> + 'a> {
        fn val_helper<'a, K: Eq + Hash, V, H: BuildHasher>(
            map: &'a rahashmap::HashMap<K, Option<V>, H>,
        ) -> Box<Iterator<Item = &'a V> + 'a> {
            Box::new(map.values().flat_map(Option::iter))
        }
        match self.state {
            KeyedState::Single(ref map) => val_helper(map),
            KeyedState::Double(ref map) => val_helper(map),
            KeyedState::Tri(ref map) => val_helper(map),
            KeyedState::Quad(ref map) => val_helper(map),
            KeyedState::Quin(ref map) => val_helper(map),
            KeyedState::Sex(ref map) => val_helper(map),
        }
    }

    fn insert(&mut self, e: MemoElem<T>) -> bool {
        use rahashmap::Entry;
        match self.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&e.value()[self.key[0]]) {
                    unimplemented!("Can this occur?");
                } else if self.partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(e.0[self.key[0]].clone(), Option::Some(e));
            }
            KeyedState::Double(ref mut map) => insert_row_match_impl!(self, e, map),
            KeyedState::Tri(ref mut map) => insert_row_match_impl!(self, e, map),
            KeyedState::Quad(ref mut map) => insert_row_match_impl!(self, e, map),
            KeyedState::Quin(ref mut map) => insert_row_match_impl!(self, e, map),
            KeyedState::Sex(ref mut map) => insert_row_match_impl!(self, e, map),
        }
        true
    }

    fn clear(&mut self) {
        self.state.clear()
    }

    fn is_contained(&self, key: &[DataType]) -> bool {
        self.values()
            .any(|v| v.0.memoization.as_ref().map_or(false, |d| **d == key))
    }

    fn mark_hole(&mut self, key: &[DataType]) -> u64
    where
        T: SizeOf,
    {
        self.state.mark_hole(key)
    }

    fn mark_filled(&mut self, key: Vec<DataType>) {
        self.state.mark_filled(key, Option::None)
    }

    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> LookupResult<'a>
    where
        T: std::fmt::Debug,
    {
        if let Some(rs) = self.state.lookup(key) {
            LookupResult::Some(RecordResult::Borrowed(std::slice::from_ref(
                rs.as_ref().unwrap().value(),
            )))
        } else if self.partial() {
            // partially materialized, so this is a hole (empty results would be vec![])
            LookupResult::Missing
        } else {
            LookupResult::Some(RecordResult::Owned(vec![]))
        }
    }

    fn lookup_row<'a>(&'a self, row: &[DataType]) -> Option<&'a Option<MemoElem<T>>>
    where
        T: std::fmt::Debug
    {
        self.state.lookup(&key_type_from_row(&self.key, row))
    }

    fn evict_keys(&mut self, keys: &[Vec<DataType>]) -> u64
    where
        T: SizeOf,
    {
        keys.iter().map(|k| self.state.evict(k)).sum()
    }

    fn evict_random_keys(&mut self, count: usize, rng: &mut ThreadRng) -> (u64, Vec<Vec<DataType>>)
    where
        T: SizeOf,
    {
        self.state.evict_random_keys(count, rng)
    }
}

pub struct MemoizedComputableState<T> {
    state: Vec<StateElement<T>>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
}

impl <T> MemoizedComputableState<T> {
    pub fn new() -> Self {
        MemoizedComputableState {
            state: Vec::new(),
            by_tag: HashMap::default(),
            mem_size: 0,
        }
    }
}

impl<T: SizeOf> SizeOf for MemoizedComputableState<T> {
    fn size_of(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
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
                    new.insert((*r).clone());
                }
            }
        }
    }
    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }
    fn is_partial(&self) -> bool {
        self.state.iter().any(StateElement::partial)
    }
    /// Because in this state type I'm mostly circumventing the standard state
    /// interface this function doesn't actually insert records but only ensures
    /// that the processing node the processing in the node was proper.
    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
        if self.is_partial() {
            records.retain(|r| {
                match *r {
                    Record::Positive(ref r) => self
                        .record_save_state(r)
                        .expect("There should have been a key for this!"),
                    Record::Negative(ref r) => {
                        // I'm panicking here, but it may be okay for the key to
                        // be missing, not sure.
                        self.record_save_state(r)
                            .expect("I think this key might have to exist.")
                    }
                }
            });
        } else {
            for r in records.iter() {
                // This should have been handled by the operator, so I only do
                // some checking that the values line up
                match *r {
                    Record::Positive(ref r) => debug_assert!(self.is_contained(r)),
                    Record::Negative(ref r) => debug_assert!(!self.is_contained(r)),
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
        self.state.iter().map(|s| s.key().to_vec()).collect()
    }
    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        assert!(!self.state[0].partial());
        self.state[0]
            .values()
            .map(|v| (**v.value()).clone())
            .collect()
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

    fn is_contained(&self, row: &[DataType]) -> bool {
        self.state.iter().any(|s| s.is_contained(row))
    }
    /// Return how the provided row is stored. If the result is `Some`
    /// there was a key matching the record found in the states. If additionally
    /// the boolean is `true` there was also a value for that key found.
    fn record_save_state(&self, r: &[DataType]) -> Option<bool> {
        let mut material = false;
        let mut exists = false;
        for s in self.state.iter() {
            s.lookup_row(r).map(|o| {
                o.as_ref().map(|m| {
                    exists = true;
                    m.0.memoization.as_ref().map(|_| material = true)
                })
            });
        }
        if exists {
            Option::Some(material)
        } else {
            Option::None
        }
    }
}
