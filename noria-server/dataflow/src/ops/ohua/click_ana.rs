/// The interval tree allows for association of elements in intervals
pub mod iseq {
    pub struct Seq<T>(Vec<Interval<T>>);

    struct Interval<T> {
        lower_bound: Option<T>,
        upper_bound: Option<T>,
        elems: Vec<T>,
    }

    impl<T> Interval<T> {
        fn bounded(lower: T, upper: T, elems: Vec<T>) -> Interval<T>
        where
            T: std::cmp::Ord,
        {
            assert!(lower <= upper);
            Interval {
                lower_bound: Option::Some(lower),
                upper_bound: Option::Some(upper),
                elems: elems,
            }
        }

        fn with_upper_bound(upper: T, elems: Vec<T>) -> Interval<T> {
            Interval {
                lower_bound: Option::None,
                upper_bound: Option::Some(upper),
                elems: elems,
            }
        }

        fn with_lower_bound(lower: T, elems: Vec<T>) -> Interval<T> {
            Interval {
                lower_bound: Option::Some(lower),
                upper_bound: Option::None,
                elems: elems,
            }
        }

        fn with_bound(lower: bool, bound: T, elems: Vec<T>) -> Interval<T> {
            if lower {
                Self::with_lower_bound(bound, elems)
            } else {
                Self::with_upper_bound(bound, elems)
            }
        }

        fn compare_elem(&self, elem: &T) -> std::cmp::Ordering
        where
            T: std::cmp::Ord,
        {
            use std::cmp::Ordering;
            match (&self.lower_bound, &self.upper_bound) {
                (Option::Some(ref l), Option::Some(ref u)) => {
                    if elem < l {
                        Ordering::Less
                    } else if elem < u {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    }
                }
                (Option::Some(b), Option::None) | (Option::None, Option::Some(b)) =>
                // TODO recheck if this is correct
                {
                    elem.cmp(&b)
                }
                // Invariant: elems is never empty (if no bounds exist)
                (Option::None, Option::None) => elem.cmp(&self.elems[0]),
            }
        }

        fn has_lower_bound(&self) -> bool {
            self.lower_bound.is_some()
        }

        fn insert_elem(&mut self, elem: T)
        where
            T: std::cmp::Ord,
        {
            assert!(
                self.lower_bound.as_ref().map_or(true, |b| b <= &elem)
                    && self.upper_bound.as_ref().map_or(true, |b| &elem < b)
            );
            self.elems.push(elem);
        }

        fn remove_element(&mut self, elem: &T) -> bool
        where
            T: std::cmp::Eq,
        {
            self.elems
                .iter()
                .position(|e| e == elem)
                .map(|p| self.elems.remove(p))
                .is_some()
        }

        fn has_upper_bound(&self) -> bool {
            self.upper_bound.is_some()
        }

        fn is_closed(&self) -> bool {
            self.has_upper_bound() && self.has_lower_bound()
        }

        fn new(elems: Vec<T>) -> Interval<T> {
            Interval {
                elems: elems,
                upper_bound: Option::None,
                lower_bound: Option::None,
            }
        }

        /// Split off all elements which are larger (`true`) or smaller
        /// (`false`) than the provided pivot element. The larger section is
        /// always inclusive with respect to the pivot.
        fn steal_elems(&mut self, splitter: &T, larger: bool) -> Vec<T>
        where
            T: std::cmp::Ord,
        {
            self.elems.sort();
            // We can collapse here, because in every case the index points to the
            // element that should be the first element in v2.
            let idx = collapse_result(self.elems.binary_search(splitter));
            let mut v2 = self.elems.split_off(idx);
            if !larger {
                std::mem::swap(&mut self.elems, &mut v2);
            }
            v2
        }

        fn adjust_upper_bound(&mut self, bound: T) -> Option<Interval<T>>
        where
            T: std::cmp::Ord,
        {
            self.adjust_bound(bound, false)
        }

        fn adjust_lower_bound(&mut self, bound: T) -> Option<Interval<T>>
        where
            T: std::cmp::Ord,
        {
            self.adjust_bound(bound, true)
        }

        fn get_bound_mut<'a>(&'a mut self, lower: bool) -> &'a mut Option<T>
        where
            T: 'a,
        {
            if lower {
                &mut self.lower_bound
            } else {
                &mut self.upper_bound
            }
        }

        fn get_bound<'a>(&'a self, lower: bool) -> &'a Option<T>
        where
            T: 'a,
        {
            if lower {
                &self.lower_bound
            } else {
                &self.upper_bound
            }
        }

        fn adjust_bound(&mut self, bound: T, lower: bool) -> Option<Interval<T>>
        where
            T: std::cmp::Ord,
        {
            assert!(
                (lower && self.upper_bound.as_ref().map_or(true, |b| &bound <= b))
                    || (lower && self.lower_bound.as_ref().map_or(true, |b| b <= &bound)),
                "The provided bound is invalid"
            );
            let other_elements = if self
                .get_bound(lower)
                .as_ref()
                .map(|a| if lower { a >= &bound } else { a <= &bound })
                .unwrap_or(false)
            {
                Vec::new()
            } else {
                self.steal_elems(&bound, !lower)
            };
            let old_bound = self.get_bound_mut(lower).replace(bound);
            if other_elements.is_empty() && old_bound.is_none() {
                Option::None
            } else {
                let mut new_elem = Interval::new(other_elements);
                old_bound.map(|b| new_elem.get_bound_mut(lower).replace(b));
                Option::Some(new_elem)
            }
        }

        fn needs_cleanup(&self) -> bool {
            !self.has_lower_bound() && !self.has_upper_bound() && self.elems.is_empty()
        }
    }

    fn collapse_result<T>(r: Result<T, T>) -> T {
        match r {
            Ok(t) | Err(t) => t,
        }
    }

    /// Split the elements vector into two parts on an element `e` such that all
    /// elements of of the first vector are strictly smaller than `e` and the
    /// elements of the second larger or equal to `e`.

    impl<T: std::cmp::Ord> Seq<T> {
        fn open_interval(&mut self, bound: T) {
            self.expand_interval(bound, true)
        }

        fn close_interval(&mut self, bound: T) {
            self.expand_interval(bound, false)
        }

        fn insert_element(&mut self, elem: T) {
            match self.find_target_index(&elem) {
                Ok(idx) => self.0[idx].insert_elem(elem),
                Err(idx) => self.0.insert(idx, Interval::new(vec![elem])),
            }
        }

        fn remove_element(&mut self, elem: &T) {
            let success = self
                .find_target_index(elem)
                .map(|idx| self.0[idx].remove_element(elem))
                .unwrap_or(false);
            assert!(success);
        }

        fn contract_interval(&mut self, bound: &T, lower: bool) {
            let idx = self.find_exact_bound(bound, lower);
            let neighbor = if lower { idx - 1 } else { idx + 1 };

            let do_cleanup = {
                let ref mut target = self.0[idx];
                let removed = if lower {
                    target.lower_bound.take()
                } else {
                    target.upper_bound.take()
                };
                assert!(&removed.unwrap() == bound);
                target.needs_cleanup()
            };

            if do_cleanup {
                self.0.remove(idx);
            } else if self
                .0
                .get(neighbor)
                .map_or(false, |b| b.get_bound(!lower).is_none())
            {
                let mut target = self.0.remove(idx);
                let ref mut other = self.0[if lower { neighbor } else { neighbor - 1 }];
                other.elems.append(&mut target.elems);
                std::mem::swap(other.get_bound_mut(!lower), target.get_bound_mut(!lower))
            }
        }

        fn expand_interval(&mut self, bound: T, lower: bool) {
            match self.find_target_index(&bound) {
                Ok(idx) => {
                    let ref mut target = self.0[idx];
                    target
                        .adjust_bound(bound, lower)
                        .map(|new_node| self.0.insert(if lower { idx } else { idx + 1 }, new_node));
                }
                Err(idx) => self.0.insert(
                    idx,
                    if lower {
                        Interval::with_lower_bound(bound, Vec::new())
                    } else {
                        Interval::with_upper_bound(bound, Vec::new())
                    },
                ),
            }
        }

        /// Find an exact interval index with this bound
        fn find_exact_bound(&self, bound: &T, lower: bool) -> usize {
            self.0
                .binary_search_by(|e| {
                    if (lower && e.lower_bound.as_ref().map_or(false, |b| b == bound))
                        || (!lower && e.upper_bound.as_ref().map_or(false, |b| b == bound))
                    {
                        std::cmp::Ordering::Equal
                    } else {
                        e.compare_elem(bound)
                    }
                })
                .expect(
                    "Invariant broken, a bound to be removed should be present in the sequence.",
                )
        }

        /// Finds an index where new elements should be inserted to preserve
        /// ordering. If the result is `Ok` the index points to a *closed*
        /// interval that contains the element. For `Err` there may be open
        /// intervals before or after the index that *can* contain the element
        fn find_insert_index(&self, idx: &T) -> Result<usize, usize> {
            self.0.binary_search_by(|e| e.compare_elem(idx))
        }

        /// This is the more comprehensive version of `find_insert_index`. The
        /// `Ok` value here may point either to a bounded interval that should
        /// contain the element or the only available partially bounded interval
        /// that should contain the element. An `Err` value indicates that no
        /// suitable interval was found and a new one will have to be created to
        /// contain the element.
        fn find_target_index(&self, elem: &T) -> Result<usize, usize> {
            // An important invariant is that there is only ever one partially
            // bounded interval. A situation like [b1, ...), (..., b2] cannot
            // happen, because b2 would have become the upper bound for the
            // first interval and vice versa.
            self.find_insert_index(elem).or_else(|idx| {
                let candidates = (self.0.get(idx - 1), self.0.get(idx));
                assert!(
                    candidates.0.map_or(true, |e| e.has_upper_bound())
                        || candidates.0.map_or(true, |e| e.has_lower_bound()),
                    "Invariant broken, both intervals lack bounds."
                );
                match candidates {
                    (Option::Some(t), _) if !t.has_upper_bound() => Ok(idx - 1),
                    (_, Option::Some(t)) if !t.has_lower_bound() => Ok(idx),
                    _ => Err(idx),
                }
            })
        }
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
