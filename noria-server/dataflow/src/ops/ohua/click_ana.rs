use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::state::click_ana::{ClickAnaState};
use prelude::*;

#[derive(Debug)]
pub enum FreeGroup<A> {
    One(A),
    And(Box<FreeGroup<A>>, Box<FreeGroup<A>>),
    Not(Box<FreeGroup<A>>),
    Empty,
}


type Timestamp = i32;
type Category = i32;

pub struct ClickAna {
    // Function arguments
    start_cat: Category,
    end_cat: Category,

    // Index for the input
    // Perhaps at some point this should allow multiple ancestors?
    src: IndexPair,

    // This *should* be `Option<usize>`
    cols: usize,

    // Index into the cache
    local_index: Option<IndexPair>,

    // Precomputed data structures, copied from `dataflow::ops::grouped`
    group_by: Vec<usize>,
    out_key: Vec<usize>,
    colfix: Vec<usize>,
}

impl ClickAna {
    pub fn new(
        src: NodeIndex,
        start_cat: Category,
        end_cat: Category,
        mut group_by: Vec<usize>,
    ) -> ClickAna {
        group_by.sort();
        let out_key = (0..group_by.len()).collect();
        ClickAna {
            src: src.into(),

            start_cat: start_cat,
            end_cat: end_cat,

            local_index: None,
            cols: 0, // Actually initialized in `on_connected`

            group_by: group_by,
            out_key: out_key,
            colfix: Vec::new(),
        }
    }

    fn on_input_mut(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        replay_key_cols: &Option<&[usize]>,
        state: &mut StateMap,
    ) -> ProcessingResult {

        debug_assert_eq!(from, *self.src);

        let concat = self.concat;

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                ..Default::default()
            };
        }

        let group_by = &self.group_by;
        // Are the columns equal that we group over
        let cmp = |a: &Record, b: &Record| {
            group_by
                .iter()
                .map(|&col| &a[col])
                .cmp(group_by.iter().map(|&col| &b[col]))
        };

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        let mut rs: Vec<_> = rs.into();
        rs.sort_by(&cmp);

        // find the current value for this group
        let us = self.local_index.unwrap();
        let db = state
            .get(*us)
            .expect("grouped operators must have their own state materialized")
            .as_click_ana_state()
            .unwrap();

        let mut misses = Vec::new();
        let mut lookups = Vec::new();
        let mut out = Vec::new();
        {
            let out_key = &self.out_key;
            let mut handle_group =
                |group_rs: ::std::vec::Drain<Record>, mut diffs: ::std::vec::Drain<_>| {
                    let mut group_rs = group_rs.peekable();

                    // Retrieve the values for this group
                    let group = get_group_values(group_by, group_rs.peek().unwrap());

                    let mut m = {
                        if let mut Option::Some(o) = db.lookup_memoizer_mut(&out_key[..], &KeyType::from(&group[..])) {
                            if replay_key_cols.is_some() {
                                lookups.push(Lookup {
                                    on: *us,
                                    cols: out_key.clone(),
                                    key: group.clone(),
                                });
                            };
                            if !o.is_some() {
                                o.replace(Memoization::default())
                            };
                            o.as_ref().unwrap()
                        } else {
                            misses.extend(group_rs.map(|r| Miss {
                                on: *us,
                                lookup_idx: out_key.clone(),
                                lookup_cols: group_by.clone(),
                                replay_cols: replay_key_cols.map(Vec::from),
                                record: r.extract().0,
                            }));
                            return;
                        }
                    };

                    m.apply(diffs);
                    let new = concat(m);

                    match m.memoization {
                        Some(ref old) if new == **current => {
                            return;
                        }
                    }

                    if let Option::Some(old) = m.memoization.replace()
                        _ => {
                            if let Some(old) = old {
                                // revoke old value
                                debug_assert!(current.is_some());
                                out.push(Record::Negative(old.into_owned()));
                            }

                            // emit positive, which is group + new.
                            let mut rec = group;
                            rec.push(new);
                            out.push(Record::Positive(rec));
                        }
                    }
                };

            let mut diffs = Vec::new();
            let mut group_rs = Vec::new();
            for r in rs {
                // This essentially is
                // rs.chunk_by(group_is_equal(self.group_by)).map(|chunk| {
                //     handle_group(chunk.map(self.inner.to_diff(...)))
                // })
                if !group_rs.is_empty() && cmp(&group_rs[0], &r) != Ordering::Equal {
                    handle_group(&mut self.inner, group_rs.drain(..), diffs.drain(..));
                }

                diffs.push(self.inner.to_diff(&r[..], r.is_positive()));
                group_rs.push(r);
            }
            assert!(!diffs.is_empty());
            handle_group(&mut self.inner, group_rs.drain(..), diffs.drain(..));
        }

        ProcessingResult {
            results: out.into(),
            lookups,
            misses,
        }
    }
}

impl Ingredient for ClickAna {
    fn take(&mut self) -> NodeOperator {
        unimplemented!()
        //Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    /// TODO check this is can be copied like this
    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];

        // group by all columns
        self.cols = srcn.fields().len();

        // build a translation mechanism for going from output columns to input columns
        let colfix: Vec<_> = (0..self.cols)
            .filter(|col| {
                // since the generated value goes at the end,
                // this is the n'th output value
                // otherwise this column does not appear in output
                self.group_by.iter().any(|c| c == col)
            })
            .collect();
        self.colfix.extend(colfix.into_iter());
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        // who's our parent really?
        self.src.remap(remap);

        // who are we?
        self.local_index = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        _: &mut Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_cols: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult { unimplemented!() }

    fn on_input_raw_mut(
        &mut self,
        _: &mut Executor,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        replay_key_cols: &ReplayContext,
        _: &DomainNodes,
        state: &mut StateMap,
    ) -> RawProcessingResult {
        RawProcessingResult::Regular(self.on_input_mut(from, data, replay_key_cols.key(), state))
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // index by our primary key
        Some((this, self.out_key.clone())).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        if col == self.colfix.len() {
            return None;
        }
        Some(vec![(self.src.as_global(), self.colfix[col])])
    }

    fn description(&self, detailed: bool) -> String {
        "click-ana".to_string()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        if column == self.colfix.len() {
            return vec![(self.src.as_global(), None)];
        }
        vec![(self.src.as_global(), Some(self.colfix[column]))]
    }

    fn is_selective(&self) -> bool {
        true
    }

    fn make_special_state(&self) -> Option<Box<State>> {
        Option::Some(Box::new(ClickAnaState::new()))
    }

}




// pub mod itree {
//     enum Direction {
//         Left,
//         Right,
//     }
//     enum Span<T> {
//         Closed {
//             span: (T, T),
//         },
//         HalfOpen {
//             direction: Direction,
//             bound: T,
//         },
//         Open,
//     }
//     pub struct Tree<T,E>(Box<Node<T,E>>);
//     struct Node<T,E>
//         {
//             span: Span<T,E>,
//             elems: Vec<(T,E)>,
//             left: Tree<T,E>,
//             right: Tree<T,E>,
//         }
//     pub enum Action<T,E> {
//         Open(T),
//         Close(T),
//         Record(T,E),
//     }

//     impl <T,E> Node {
//         fn new(span: Span<T,E>) -> Node<T,E> {
//             Node::new_with_elems(span, Vec::empty())
//         }
//         fn new_with_elems(span: Span<T,E>, elems: Vec<(T,E)>) -> Node<T,E> {
//             Node { span: span, elems: elems }
//         }
//         fn empty() -> Node<T,E> {
//             Node::Empty
//         }
//         fn new_open(elems: Vec<(T,E)>) {
//             Node::Node{ span: Open, elems: elems }
//         }
//     }

//     use std::cmp::Ordering;

//     impl <T> Span {

//         fn contains(&self, e: &T) -> bool {
//             match *self {
//                 Open => true,
//                 HalfOpen { direction: Left, bound } => e <= bound,
//                 HalfOpen { direction: Right, bound} => bound < e,
//                 Closed { span: (lower, upper) } => lower <= e && e < upper,
//             }
//         }

//         fn is_open(&self) {
//             match *self {
//                 Closed{} => false,
//                 _ => true,
//             }
//         }
//     }

//     fn split_vec<F: Fn(T) -> bool, T>(predicate: F, v: Vec<T>) -> (Vec<T>, Vec<T>) {
//         let v1 = Vec::new();
//         let v2 = Vec::new();

//         for i in v.drain(..) {
//             if predicate(i) {
//                 v1.push(i)
//             } else {
//                 v2.push(i)
//             }
//         }
//         (v1, v2)
//     }

//     impl <T : std::cmp::Ord,E> Tree {
//         fn new_node(node: Node<T,E>) -> Tree<T,E> {
//             Tree(Box::new(node))
//         }
//         fn new_half_open(direction: Direction, bound: T) -> Tree<T,E> {
//             Tree::new_node(
//                 Node::new(HalfOpen { direction: direction,
//                                      bound: bound,
//                                      child: Tree::empty(),
//                 }))
//         }
//         fn empty() -> Tree<T,E> {
//             Tree::new_node(Node { span: Open, elems: Vec::new()})
//         }
//         fn new_open(elems: Vec<(T,E)>) -> Tree<T,E> {
//             Tree::new_node(Node::new_open(elems))
//         }
//         pub fn open_interval(&mut self, bound: T) {
//             let ref mut elem = self;
//             loop {
//                 let node@Node{span, ..} = *elem.0;
//                 match span {

//                     Option::None => {
//                         let new_node = match span {
//                             HalfOpen{ direction: Left, bound: bound0, child } => {
//                                 let (smaller, larger) = split_vec(|a| a.0 < bound, elems);
//                                 Tree:new_node(
//                                     Node::new_with_elems(
//                                         Span::Closed {
//                                             span: (bound, bound0),
//                                             right: child,
//                                             left: Tree::new_open(smaller),
//                                         },
//                                         larger,
//                                     )
//                                 );
//                             },
//                             HalfOpen { direction: Right, ..} => {
//                                 let (smaller, larger) = split_vec(|a| a.0 < bound, elems);

//                             },
//                             Open => {
//                                 let (smaller, larger) = split_vec(|a| a.0 < bound, elems);
//                                 Tree::new_node(
//                                     Node::new_with_elems(
//                                         HalfOpen {
//                                             direction: Right,
//                                             bound: bound,
//                                             child: Tree::new_open(smaller),
//                                         },
//                                         larger,
//                                     )
//                                 )
//                             }
//                             elem.0 = new_node;
//                             return;
//                         },
//                         Option::Some(child) => {
//                             elem = child;
//                         }
//                     }
//                 }
//             }

//         }
//     }
// }
