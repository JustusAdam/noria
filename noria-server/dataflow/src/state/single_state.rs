
use super::mk_key::MakeKey;
use common::SizeOf;
use prelude::*;
use rand::prelude::*;
use state::keyed_state::{ KeyedState, DeallocSize };
use std::rc::Rc;

pub(super) struct SingleState<T> {
    key: Vec<usize>,
    state: KeyedState<T>,
    partial: bool,
    rows: usize,
}

macro_rules! insert_row_match_impl {
    ($self:ident, $r:ident, $map:ident) => {{
        let key = MakeKey::from_row(&$self.key, &*$r);
        match $map.entry(key) {
            Entry::Occupied(mut rs) => rs.get_mut().push($r),
            Entry::Vacant(..) if $self.partial => return false,
            rs @ Entry::Vacant(..) => rs.or_default().push($r),
        }
    }};
}

macro_rules! remove_row_match_impl {
    ($self:ident, $r:ident, $do_remove:ident, $map:ident) => {{
        // TODO: can we avoid the Clone here (in `MakeKey::from_row`)?
        let key = MakeKey::from_row(&$self.key, $r);
        if let Some(ref mut rs) = $map.get_mut(&key) {
            return $do_remove(&mut $self.rows, rs);
        }
    }};
}

pub(crate) trait Leaf : Default {
    fn push(&mut self, item: Row);
    fn remove(&mut self, row: &[DataType]) -> Option<Row>;
    fn row_slice(&self) -> &[Row];
    fn singleton(item: Row) -> Self {
        let mut n = Self::default();
        n.push(item);
        n
    }
}

impl Leaf for Vec<Row> {
    fn push(&mut self, item: Row) {
        Vec::push(self,item)
    }
    fn remove(&mut self, r: &[DataType]) -> Option<Row> {
        if self.len() == 1 {
            // it *should* be impossible to get a negative for a record that we don't have
            debug_assert_eq!(r, &self[0][..]);
            Some(self.swap_remove(0))
        } else if let Some(i) = self.iter().position(|rsr| &rsr[..] == r) {
            Some(self.swap_remove(i))
        } else {
            None
        }
    }
    fn row_slice(&self) -> &[Row] {
        self
    }
    fn singleton(item: Row) -> Self {
        vec![item]
    }
}

impl<T: Leaf + DeallocSize> SingleState<T> {
    pub(super) fn new(columns: &[usize], partial: bool) -> Self {
        Self {
            key: Vec::from(columns),
            state: columns.into(),
            partial,
            rows: 0,
        }
    }

    /// Inserts the given record, or returns false if a hole was encountered (and the record hence
    /// not inserted).
    pub(super) fn insert_row(&mut self, r: Row) -> bool
    where
        T: Default
    {
        use rahashmap::Entry;
        match self.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    self.rows += 1;
                    rs.push(r);
                    return true;
                } else if self.partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(r[self.key[0]].clone(), T::singleton(r));
            }
            KeyedState::Double(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Tri(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Quad(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Quin(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Sex(ref mut map) => insert_row_match_impl!(self, r, map),
        }

        self.rows += 1;
        true
    }

    /// Attempt to remove row `r`.
    pub(super) fn remove_row(&mut self, r: &[DataType], hit: &mut bool) -> Option<Row> {
        let mut do_remove = |self_rows: &mut usize, rs: &mut T| -> Option<Row> {
            *hit = true;
            let rm = rs.remove(r);
            if rm.is_some() {
                *self_rows = self_rows.checked_sub(1).unwrap();
            }
            rm
        };

        match self.state {
            KeyedState::Single(ref mut map) => {
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Double(ref mut map) => remove_row_match_impl!(self, r, do_remove, map),
            KeyedState::Tri(ref mut map) => remove_row_match_impl!(self, r, do_remove, map),
            KeyedState::Quad(ref mut map) => remove_row_match_impl!(self, r, do_remove, map),
            KeyedState::Quin(ref mut map) => remove_row_match_impl!(self, r, do_remove, map),
            KeyedState::Sex(ref mut map) => remove_row_match_impl!(self, r, do_remove, map),
        }
        None
    }

    pub(super) fn mark_filled(&mut self, key: Vec<DataType>)
    where
        T: Default
    {
        self.state.mark_filled(key, T::default())
    }

    pub(super) fn mark_hole(&mut self, key: &[DataType]) -> u64 {
        self.state.mark_hole(key)
    }

    pub(super) fn clear(&mut self) {
        self.rows = 0;
        self.state.clear()
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub(super) fn evict_random_keys(
        &mut self,
        count: usize,
        rng: &mut ThreadRng,
    ) -> (u64, Vec<Vec<DataType>>)
    {
        self.state.evict_random_keys(count, rng)
    }

    /// Evicts a specified key from this state, returning the number of bytes freed.
    pub(super) fn evict_keys(&mut self, keys: &[Vec<DataType>]) -> u64 {
        keys.iter().map(|k| self.state.evict(k)).sum()
    }

    pub(super) fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
        match self.state {
            KeyedState::Single(ref map) => Box::new(map.values()),
            KeyedState::Double(ref map) => Box::new(map.values()),
            KeyedState::Tri(ref map) => Box::new(map.values()),
            KeyedState::Quad(ref map) => Box::new(map.values()),
            KeyedState::Quin(ref map) => Box::new(map.values()),
            KeyedState::Sex(ref map) => Box::new(map.values()),
        }
    }
    pub(super) fn key(&self) -> &[usize] {
        &self.key
    }
    pub(super) fn partial(&self) -> bool {
        self.partial
    }
    pub(super) fn rows(&self) -> usize {
        self.rows
    }
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> LookupResult<'a>
    where
        T: std::fmt::Debug
    {
        if let Some(rs) = self.state.lookup(key) {
            LookupResult::Some(RecordResult::Borrowed(&rs.row_slice()[..]))
        } else if self.partial() {
            // partially materialized, so this is a hole (empty results would be vec![])
            LookupResult::Missing
        } else {
            LookupResult::Some(RecordResult::Owned(vec![]))
        }
    }
    pub(super) fn lookup_leaf<'a>(&'a self, key: &KeyType) -> Option<&'a T>
    where
        T: std::fmt::Debug
    {
        self.state.lookup(key)
    }
    pub(super) fn lookup_leaf_mut<'a>(&'a mut self, key: &KeyType) -> Option<&'a mut T>
    where
        T: std::fmt::Debug
    {
        self.state.lookup_mut(key)
    }
}
