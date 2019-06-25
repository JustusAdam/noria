use fnv::FnvBuildHasher;
use rahashmap::HashMap as RaHashMap;
use std::rc::Rc;

use common::SizeOf;
use prelude::*;

type FnvHashMap<K, V> = RaHashMap<K, V, FnvBuildHasher>;

#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub(super) enum KeyedState<T> {
    Single(FnvHashMap<DataType, T>),
    Double(FnvHashMap<(DataType, DataType), T>),
    Tri(FnvHashMap<(DataType, DataType, DataType), T>),
    Quad(FnvHashMap<(DataType, DataType, DataType, DataType), T>),
    Quin(FnvHashMap<(DataType, DataType, DataType, DataType, DataType), T>),
    Sex(FnvHashMap<(DataType, DataType, DataType, DataType, DataType, DataType), T>),
}

impl<T> KeyedState<T> {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a T>
    where
        T: std::fmt::Debug,
    {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => m.get(k),
            (st, kt) => panic!("State: {:?}, key: {:?}", st, kt),
            (st, kt) => panic!(
                "Stat: {}, key: {}",
                match *st {
                    KeyedState::Single(_) => 1,
                    KeyedState::Double(_) => 2,
                    KeyedState::Tri(_) => 3,
                    KeyedState::Quad(_) => 4,
                    KeyedState::Quin(_) => 5,
                    KeyedState::Sex(_) => 6,
                },
                match *kt {
                    KeyType::Single(_) => 1,
                    KeyType::Double(_) => 2,
                    KeyType::Tri(_) => 3,
                    KeyType::Quad(_) => 4,
                    KeyType::Quin(_) => 5,
                    KeyType::Sex(_) => 6,
                }
            ),
        }
    }

    /// Remove all rows for the first key at or after `index`, returning that key along with the
    /// number of bytes freed. Returns None if already empty.
    pub(super) fn evict_at_index(&mut self, index: usize) -> Option<(u64, Vec<DataType>)>
    where
        T: DeallocSize,
    {
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) => m.remove_at_index(index).map(|(k, rs)| (rs, vec![k])),
            KeyedState::Double(ref mut m) => {
                m.remove_at_index(index).map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::Tri(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2])),
            KeyedState::Quad(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3])),
            KeyedState::Quin(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4])),
            KeyedState::Sex(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4, k.5])),
        }?;
        Some((rs.dealloc_size(), key))
    }

    /// Remove all rows for the given key, returning the number of bytes freed.
    pub(super) fn evict(&mut self, key: &[DataType]) -> u64
    where
        T: DeallocSize,
    {
        match *self {
            KeyedState::Single(ref mut m) => m.remove(&(key[0])),
            KeyedState::Double(ref mut m) => m.remove(&(key[0].clone(), key[1].clone())),
            KeyedState::Tri(ref mut m) => {
                m.remove(&(key[0].clone(), key[1].clone(), key[2].clone()))
            }
            KeyedState::Quad(ref mut m) => m.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
            )),
            KeyedState::Quin(ref mut m) => m.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
            )),
            KeyedState::Sex(ref mut m) => m.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
                key[5].clone(),
            )),
        }
        .map(|r| r.dealloc_size())
        .unwrap_or(0)
    }
}

/// Real number of bytes freed by deallocating this element. In particular for
/// smart references this should 0 unless this is the only existing copy of the
/// reference.
pub(crate) trait DeallocSize {
    fn dealloc_size(&self) -> u64;
}

impl DeallocSize for Row {
    fn dealloc_size(&self) -> u64 {
        if Rc::strong_count(&self.0) == 1 {
            self.deep_size_of()
        } else {
            0
        }
    }
}

impl<T: DeallocSize> DeallocSize for Vec<T> {
    fn dealloc_size(&self) -> u64 {
        self.iter().map(DeallocSize::dealloc_size).sum()
    }
}

impl<'a, T> Into<KeyedState<T>> for &'a [usize] {
    fn into(self) -> KeyedState<T> {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(FnvHashMap::default()),
            2 => KeyedState::Double(FnvHashMap::default()),
            3 => KeyedState::Tri(FnvHashMap::default()),
            4 => KeyedState::Quad(FnvHashMap::default()),
            5 => KeyedState::Quin(FnvHashMap::default()),
            6 => KeyedState::Sex(FnvHashMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
