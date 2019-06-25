
/// A memoized computable state
use super::keyed_state::*;

struct Memoization<T> {
    computer: T,
    memoization: Vec<DataType>
}

struct StateElement<T> {
    state: KeyedState<Memoization<T>>
}

pub struct MemoizedComputableState<T> {
    states: Vec<StateElement<T>>,
}

impl super::State for MemoizedComputableState<T> {
}
