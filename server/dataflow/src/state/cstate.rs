use crate:: prelude::*;
//use rand::prelude::{Rng, ThreadRng};
//use std::collections::HashMap;
//use std::rc::Rc;
//use std::cell::RefCell;

use super::click_ana::ClickAnaState;
use super::keyed_state::*;
use super::single_state::Leaf;
//use super::mk_key::{ MakeKey, key_type_from_row };

static mut EMPTY_ROWS : Option<Rows> = None;

#[derive(Clone)]
enum Memoization<T> {
    Empty,
    Store(Rows),
    Computing {
        computer: T,
        // Because, for the time being, I only expect to use this for grouping UDF's
        // this should only ever contain a single row.
        memoization: Rows,
    },
}

fn singleton_rows(row: Row) -> Rows {
    let mut b = Rows::default();
    b.insert(row);
    b
}

use Memoization::*;

impl<T> Memoization<T> {
    fn value<'a>(&'a self) -> &'a Row {
        self.value_may().unwrap()
    }

    fn value_may<'a>(&'a self) -> Option<&'a Row> {
        match self {
            Empty => Option::None,
            Store(r) | Computing{ memoization: r, .. } => r.iter().next(),
        }
    }

    fn computer_may_mut<'a>(&'a mut self) -> Option<&'a mut T> {
        match self {
            Memoization::Computing { computer, .. } => Option::Some(computer),
            _ => Option::None,
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Row> {
        self.value_may().into_iter()
    }

    fn replace_row(&mut self, item: Row) -> Option<Row> {
        match self {
            Empty => {
                *self = Memoization::Store(singleton_rows(item));
                Option::None
            }
            Store(ref mut e)
                | Computing{ memoization: ref mut e, .. } => {
                let x = e.drain().next();
                e.insert(item);
                x.map(|e| e.0)
            }
        }
    }

    fn into_row(self) -> Option<Row> {
        match self {
            Empty => Option::None,
            Store(e) | Computing { memoization: e, .. } => e.into_iter().next().map(|n| n.0),
        }
    }

    fn into_rows(self) -> Rows {
        match self {
            Empty => Rows::default(),
            Store(r) | Computing { memoization: r, .. } => r
        }
    }

    fn drop_row(&mut self) -> Option<Row> {
        match self {
            Computing { memoization: ref mut e, .. }
            | Store(ref mut e) =>
                e.drain().next().map(|x| x.0),
            _ => None
        }
    }
}

impl<T> Default for Memoization<T> {
    fn default() -> Self {
        Memoization::Empty
    }
}

use std::ops::Index;

impl<T> Index<usize> for Memoization<T> {
    type Output = DataType;
    fn index(&self, index: usize) -> &DataType {
        self.value().index(index)
    }
}

pub(crate) struct MemoElem<T>(Memoization<T>);

impl<T> MemoElem<T> {
    pub fn value<'a>(&'a self) -> &'a Row {
        self.0.value()
    }

    pub fn singleton_row(r: Row) -> MemoElem<T> {
        MemoElem(Memoization::Store(singleton_rows(r)))
    }

    pub fn value_may<'a>(&'a self) -> Option<&'a Row> {
        self.0.value_may()
    }

    pub fn get_or_init_compute_mut<'a>(&'a mut self) -> &'a mut T
    where
        T: Default,
    {
        match self.0 {
            Memoization::Computing {
                ref mut computer, ..
            } => computer,
            _ => {
                let new = unsafe { Memoization::Computing {
                    computer: Default::default(),
                    memoization: std::mem::uninitialized(),
                } };
                let old = std::mem::replace(&mut self.0, new);
                if let Memoization::Computing {
                    ref mut computer,
                    ref mut memoization,
                } = self.0
                {
                    std::mem::replace(memoization, old.into_rows());
                    computer
                } else {
                    unreachable!()
                }
            }
        }
    }
}

impl<T> Default for MemoElem<T> {
    fn default() -> Self {
        MemoElem(Memoization::default())
    }
}

impl<T: SizeOf> SizeOf for MemoElem<T> {
    fn size_of(&self) -> u64 {
        self.0.size_of()
    }

    fn deep_size_of(&self) -> u64 {
        self.0.deep_size_of()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> Leaf for MemoElem<T> {
    fn push(&mut self, item: Row) {
        let i = self.0.replace_row(item);
        debug_assert!(i.is_none());
    }
    fn remove(&mut self, row: &[DataType]) -> Option<Row> {
        if self.0.value_may().map_or(false, |i| &i[..] == row) {
            self.0.drop_row()
        } else {
            Option::None
        }
    }
    fn as_rows(&self) -> &Rows {
        match self.0 {
            Memoization::Store(ref rows) | Memoization::Computing{memoization: ref rows,..} => rows,
            _ => unsafe { EMPTY_ROWS.get_or_insert_with(|| Rows::default()) },
        }
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
    fn is_empty(&self) -> bool {
        use Memoization::*;
        match self {
            Store(r) | Computing{memoization: r, ..} => r.is_empty(),
            _ => true
        }
    }
}

pub(crate) struct SpecialStateWrapper<T>(pub MemoryState<T>);

impl<T> SpecialStateWrapper<T> {
    pub fn new() -> Self {
        SpecialStateWrapper(MemoryState::default())
    }
}

impl<T: SizeOf> SizeOf for SpecialStateWrapper<T> {
    fn size_of(&self) -> u64 {
        self.0.size_of()
    }

    fn deep_size_of(&self) -> u64 {
        self.0.deep_size_of()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl State for super::click_ana::ClickAnaState {
    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        self.0.add_key(columns, partial)
    }

    fn is_useful(&self) -> bool {
        self.0.is_useful()
    }

    fn is_partial(&self) -> bool {
        self.0.is_partial()
    }

    fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
        self.0.process_records(records, partial_tag)
    }

    fn mark_hole(&mut self, key: &[DataType], tag: Tag) {
        self.0.mark_hole(key, tag)
    }

    fn mark_filled(&mut self, key: Vec<DataType>, tag: Tag) {
        self.0.mark_filled(key, tag)
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        self.0.lookup(columns, key)
    }

    fn rows(&self) -> usize {
        self.0.rows()
    }

    fn keys(&self) -> Vec<Vec<usize>> {
        self.0.keys()
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        self.0.cloned_records()
    }

    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        self.0.evict_random_keys(count)
    }

    fn evict_keys(&mut self, tag: Tag, keys: &[Vec<DataType>]) -> Option<(&[usize], u64)> {
        self.0.evict_keys(tag, keys)
    }

    fn clear(&mut self) {
        self.0.clear()
    }

    fn as_click_ana_state<'a>(&'a mut self) -> Option<&'a mut ClickAnaState> {
        Option::Some(self)
    }
}

// struct StateElement<T> {
//     state: KeyedState<MemoElem<T>>,
//     key: Vec<usize>,
//     partial: bool,
// }

// macro_rules! insert_row_match_impl {
//     ($self:ident, $r:ident, $map:ident) => {{
//         let key = MakeKey::from_row(&$self.key, &$r);
//         match $map.entry(key) {
//             Entry::Occupied(mut rs) => {
//                 let res = rs.get_mut().0.replace_row($r);
//                 debug_assert!(res.is_none(), "Key already exists");
//             }
//             Entry::Vacant(..) if $self.partial => return false,
//             rs @ Entry::Vacant(..) => {
//                 let res = rs.or_default().0.replace_row($r);
//                 debug_assert!(res.is_none(), "How could this happen?");
//             }
//         }
//     }};
// }

// use std::hash::{BuildHasher, Hash};

// impl<T> StateElement<T> {
//     fn new(columns: &[usize], partial: bool) -> Self {
//         Self {
//             key: Vec::from(columns),
//             state: columns.into(),
//             partial,
//         }
//     }

//     fn rows(&self) -> usize {
//         unimplemented!()
//     }

//     fn key<'a>(&'a self) -> &'a [usize] {
//         &self.key
//     }

//     fn partial(&self) -> bool {
//         self.partial
//     }

//     fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a MemoElem<T>> + 'a> {
//         fn val_helper<'a, K: Eq + Hash, V, H: BuildHasher>(
//             map: &'a rahashmap::HashMap<K, V, H>,
//         ) -> Box<dyn Iterator<Item = &'a V> + 'a> {
//             Box::new(map.values())
//         }
//         match self.state {
//             KeyedState::Single(ref map) => val_helper(map),
//             KeyedState::Double(ref map) => val_helper(map),
//             KeyedState::Tri(ref map) => val_helper(map),
//             KeyedState::Quad(ref map) => val_helper(map),
//             KeyedState::Quin(ref map) => val_helper(map),
//             KeyedState::Sex(ref map) => val_helper(map),
//         }
//     }

//     fn insert(&mut self, e: Row) -> bool {
//         use rahashmap::Entry;
//         match self.state {
//             KeyedState::Single(ref mut map) => {
//                 // treat this specially to avoid the extra Vec
//                 debug_assert_eq!(self.key.len(), 1);
//                 // i *wish* we could use the entry API here, but it would mean an extra clone
//                 // in the common case of an entry already existing for the given key...
//                 if let Some(ref mut rs) = map.get_mut(&e[self.key[0]]) {
//                     let res = rs.0.replace_row(e);
//                     debug_assert!(res.is_none(), "Can this happen?");
//                 } else if self.partial {
//                     // trying to insert a record into partial materialization hole!
//                     return false;
//                 } else {
//                     map.insert(e.0[self.key[0]].clone(), MemoElem::singleton_row(e));
//                 }
//             }
//             KeyedState::Double(ref mut map) => insert_row_match_impl!(self, e, map),
//             KeyedState::Tri(ref mut map) => insert_row_match_impl!(self, e, map),
//             KeyedState::Quad(ref mut map) => insert_row_match_impl!(self, e, map),
//             KeyedState::Quin(ref mut map) => insert_row_match_impl!(self, e, map),
//             KeyedState::Sex(ref mut map) => insert_row_match_impl!(self, e, map),
//         }
//         true
//     }

//     fn clear(&mut self) {
//         self.state.clear()
//     }

//     fn is_contained(&self, key: &[DataType]) -> bool {
//         self.values()
//             .any(|v| v.0.value_may().map_or(false, |d| **d == key))
//     }

//     fn mark_hole(&mut self, key: &[DataType]) -> u64
//     where
//         T: SizeOf,
//     {
//         self.state.mark_hole(key)
//     }

//     fn mark_filled(&mut self, key: Vec<DataType>) {
//         self.state.mark_filled(key, MemoElem::default())
//     }

//     pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> LookupResult<'a>
//     where
//         T: std::fmt::Debug,
//     {
//         if let Some(rs) = self.state.lookup(key) {
//             LookupResult::Some(RecordResult::Borrowed(std::slice::from_ref(
//                 rs.value(),
//             )))
//         } else if self.partial() {
//             // partially materialized, so this is a hole (empty results would be vec![])
//             LookupResult::Missing
//         } else {
//             LookupResult::Some(RecordResult::Owned(vec![]))
//         }
//     }

//     fn lookup_row<'a>(&'a self, row: &[DataType]) -> Option<&'a MemoElem<T>>
//     where
//         T: std::fmt::Debug
//     {
//         self.state.lookup(&key_type_from_row(&self.key, row))
//     }

//     fn lookup_computer_mut<'a>(&'a mut self, key: &KeyType) -> Option<Option<&'a mut T>>
//     where
//         T: std::fmt::Debug
//     {
//         self.state.lookup_mut(key).map(|me| me.0.computer_may_mut())
//     }

//     fn evict_keys(&mut self, keys: &[Vec<DataType>]) -> u64
//     where
//         T: SizeOf,
//     {
//         keys.iter().map(|k| self.state.evict(k)).sum()
//     }

//     fn evict_random_keys(&mut self, count: usize, rng: &mut ThreadRng) -> (u64, Vec<Vec<DataType>>)
//     where
//         T: SizeOf,
//     {
//         self.state.evict_random_keys(count, rng)
//     }
// }

// pub struct MemoizedComputableState<T> {
//     state: Vec<StateElement<T>>,
//     by_tag: HashMap<Tag, usize>,
//     mem_size: u64,
// }

// impl <T> MemoizedComputableState<T> {
//     pub fn new() -> Self {
//         MemoizedComputableState {
//             state: Vec::new(),
//             by_tag: HashMap::default(),
//             mem_size: 0,
//         }
//     }
//     fn lookup_computer_mut<'a>(&'a mut self, columns: &[usize], key: &KeyType) -> Option<Option<&'a mut T>>
//     where
//         T: std::fmt::Debug
//     {
//         let index = self
//             .state_for(columns)
//             .expect("lookup on non-indexed column set");
//         self.state[index].lookup_computer_mut(key)
//     }
//     fn state_for(&self, cols: &[usize]) -> Option<usize> {
//         self.state.iter().position(|s| s.key() == cols)
//     }
// }

// impl<T: SizeOf> SizeOf for MemoizedComputableState<T> {
//     fn size_of(&self) -> u64 {
//         std::mem::size_of::<Self>() as u64
//     }
//     fn deep_size_of(&self) -> u64 {
//         self.mem_size
//     }
// }

// impl super::State for ClickAnaState {
//     fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
//         let (i, exists) = if let Some(i) = self.state_for(columns) {
//             // already keyed by this key; just adding tags
//             (i, true)
//         } else {
//             // will eventually be assigned
//             (self.state.len(), false)
//         };

//         if let Some(ref p) = partial {
//             for &tag in p {
//                 self.by_tag.insert(tag, i);
//             }
//         }

//         if exists {
//             return;
//         }

//         self.state
//             .push(StateElement::new(columns, partial.is_some()));

//         if !self.state.is_empty() && partial.is_none() {
//             // we need to *construct* the index!
//             let (new, old) = self.state.split_last_mut().unwrap();

//             if !old.is_empty() {
//                 assert!(!old[0].partial());
//                 for r in old[0].values().map(MemoElem::value_may).flat_map(Option::into_iter) {
//                     new.insert(( *r ).clone());
//                 }
//             }
//         }
//     }
//     fn is_useful(&self) -> bool {
//         !self.state.is_empty()
//     }
//     fn is_partial(&self) -> bool {
//         self.state.iter().any(StateElement::partial)
//     }
//     /// Because in this state type I'm mostly circumventing the standard state
//     /// interface this function doesn't actually insert records but only ensures
//     /// that the processing node the processing in the node was proper.
//     fn process_records(&mut self, records: &mut Records, partial_tag: Option<Tag>) {
//         if self.is_partial() {
//             records.retain(|r| {
//                 match *r {
//                     Record::Positive(ref r) => self
//                         .record_save_state(r)
//                         .expect("There should have been a key for this!"),
//                     Record::Negative(ref r) => {
//                         // I'm panicking here, but it may be okay for the key to
//                         // be missing, not sure.
//                         self.record_save_state(r)
//                             .expect("I think this key might have to exist.")
//                     }
//                 }
//             });
//         } else {
//             for r in records.iter() {
//                 // This should have been handled by the operator, so I only do
//                 // some checking that the values line up
//                 match *r {
//                     Record::Positive(ref r) => debug_assert!(self.is_contained(r)),
//                     Record::Negative(ref r) => debug_assert!(!self.is_contained(r)),
//                 }
//             }
//         }
//     }
//     fn rows(&self) -> usize {
//         self.state.iter().map(StateElement::rows).sum()
//     }
//     fn mark_filled(&mut self, key: Vec<DataType>, tag: Tag) {
//         debug_assert!(!self.state.is_empty(), "filling uninitialized index");
//         let index = self.by_tag[&tag];
//         self.state[index].mark_filled(key);
//     }
//     fn mark_hole(&mut self, key: &[DataType], tag: Tag) {
//         debug_assert!(!self.state.is_empty(), "filling uninitialized index");
//         let index = self.by_tag[&tag];
//         let freed_bytes = self.state[index].mark_hole(key);
//         self.mem_size = self.mem_size.checked_sub(freed_bytes).unwrap();
//     }
//     fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
//         let index = self
//             .state_for(columns)
//             .expect("lookup on non-indexed column set");
//         self.state[index].lookup(key)
//     }
//     fn keys(&self) -> Vec<Vec<usize>> {
//         self.state.iter().map(|s| s.key().to_vec()).collect()
//     }
//     fn cloned_records(&self) -> Vec<Vec<DataType>> {
//         assert!(!self.state[0].partial());
//         self.state[0]
//             .values()
//             .map(|v| (**v.value()).clone())
//             .collect()
//     }
//     fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
//         let mut rng = rand::thread_rng();
//         let index = rng.gen_range(0, self.state.len());
//         let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
//         self.mem_size = self.mem_size.saturating_sub(bytes_freed);
//         (self.state[index].key(), keys, bytes_freed)
//     }
//     fn evict_keys(&mut self, tag: Tag, keys: &[Vec<DataType>]) -> Option<(&[usize], u64)> {
//         self.by_tag.get(&tag).cloned().map(move |index| {
//             let bytes = self.state[index].evict_keys(keys);
//             self.mem_size = self.mem_size.saturating_sub(bytes);
//             (self.state[index].key(), bytes)
//         })
//     }
//     fn clear(&mut self) {
//         for state in &mut self.state {
//             state.clear();
//         }
//         self.mem_size = 0;
//     }
//     fn as_click_ana_state<'a>(&'a mut self) -> Option<&'a mut ClickAnaState> {
//         Option::Some(self)
//     }
// }

// impl ClickAnaState {

//     fn is_contained(&self, row: &[DataType]) -> bool {
//         self.state.iter().any(|s| s.is_contained(row))
//     }
//     /// Return how the provided row is stored. If the result is `Some`
//     /// there was a key matching the record found in the states. If additionally
//     /// the boolean is `true` there was also a value for that key found.
//     fn record_save_state(&self, r: &[DataType]) -> Option<bool> {
//         let mut material = false;
//         let mut exists = false;
//         for s in self.state.iter() {
//             s.lookup_row(r).map(|o| {
//                 exists = true;
//                 o.value_may().map(|_| {
//                     material = true
//                 })
//             });
//         }
//         if exists {
//             Option::Some(material)
//         } else {
//             Option::None
//         }
//     }

// }
