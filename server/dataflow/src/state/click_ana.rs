
use super::cstate::*;

#[derive(Debug)]
pub enum FreeGroup<A> {
    One(A),
    And(Box<FreeGroup<A>>, Box<FreeGroup<A>>),
    Not(Box<FreeGroup<A>>),
    Empty,
}

impl<A> FreeGroup<A> {
    fn empty() -> FreeGroup<A> {
        FreeGroup::Empty
    }

    fn and(self, other: FreeGroup<A>) -> FreeGroup<A> {
        FreeGroup::And(Box::new(self), Box::new(other))
    }

    fn not(self) -> FreeGroup<A> {
        match self {
            FreeGroup::Not(s) => *s,
            _ => FreeGroup::Not(Box::new(self)),
        }
    }

    fn one(item: A) -> FreeGroup<A> {
        FreeGroup::One(item)
    }

    // pub fn iter<'a>(&'a self) -> &'a impl Iterator<Item = A> {
    //     self.iter_helper(false)
    // }

    // fn iter_helper<'a>(&'a self, negated: bool) -> &'a impl Iterator<Item=(&'a A, bool)> {
    //     use std::iter::*;
    //     match self {
    //         FreeGroup::One(a) => &once((a, negated)),
    //         FreeGroup::Not(inner) => &inner.iter_helper(!negated),
    //         FreeGroup::Empty => &empty(),
    //         FreeGroup::And(one, two) => &one.iter_helper(negated).chain(two.iter_helper(negated))
    //     }
    // }
}
pub(crate) type ClickAnaState = SpecialStateWrapper<MemoElem<iseq::Seq<I32Wrap>>>;

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
pub struct I32Wrap(pub i32);


pub trait Computer {
    type Action;
    type Output;
    fn apply(&mut self, action: Self::Action, positive: bool);
    fn compute_new_value(&mut self) -> Self::Output;
}

pub mod iseq {
    pub (crate) type ClickAnaInner = Seq<i32>;
    #[derive(Debug)]
    pub struct Seq<T>(Vec<Interval<T>>);

    use common::SizeOf;
    use super::I32Wrap;

    impl SizeOf for Seq<i32> {
        fn size_of(&self) -> u64 {
            std::mem::size_of::<Self>() as u64
        }
        fn deep_size_of(&self) -> u64 {
            // TODO do a proper implementation here and find out if its even should have one
            self.size_of()
        }
    }

    impl SizeOf for Seq<I32Wrap> {
        fn size_of(&self) -> u64 {
            std::mem::size_of::<Self>() as u64
        }
        fn deep_size_of(&self) -> u64 {
            // TODO do a proper implementation here and find out if its even should have one
            self.size_of()
        }
    }

    impl<T> Default for Seq<T> {
        fn default() -> Self {
            Seq(Vec::new())
        }
    }

    use nom_sql::SqlType;
    use crate::ops::ohua::Typed;

    impl Typed for Seq<i32> {
        type Type = SqlType;
        fn typ_static() -> Self::Type {
            SqlType::Double
        }
    }

    impl Typed for Seq<I32Wrap> {
        type Type = SqlType;
        fn typ_static() -> Self::Type {
            SqlType::Double
        }
    }

    #[derive(Debug)]
    pub struct Interval<T> {
        lower_bound: Option<T>,
        upper_bound: Option<T>,
        elems: Vec<T>,
    }

    impl<T: std::fmt::Debug> Interval<T> {
        pub fn len(&self) -> usize {
            self.elems.len()
        }

        fn bounded(lower: T, upper: T, elems: Vec<T>) -> Interval<T>
        where
            T: std::cmp::Ord,
        {
            debug_assert!(lower <= upper);
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
                        Ordering::Greater
                    } else if elem < u {
                        Ordering::Equal
                    } else {
                        Ordering::Less
                    }
                }
                (Option::Some(b), Option::None) | (Option::None, Option::Some(b)) =>
                // TODO recheck if this is correct
                {
                    b.cmp(elem)
                    //elem.cmp(&b)
                }
                // Invariant: elems is never empty (if no bounds exist)
                (Option::None, Option::None) => {
                    debug_assert!(self.elems.len() != 0);
                    self.elems[0].cmp(elem)
                }
            }
        }

        fn has_lower_bound(&self) -> bool {
            self.lower_bound.is_some()
        }

        fn insert_elem(&mut self, elem: T)
        where
            T: std::cmp::Ord,
        {
            debug_assert!(self.is_in_bounds(&elem));
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

        pub fn has_upper_bound(&self) -> bool {
            self.upper_bound.is_some()
        }

        pub fn is_closed(&self) -> bool {
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

        pub fn is_in_lower_bound(&self, item: &T) -> bool
        where
            T: std::cmp::Ord,
        {
            self.lower_bound.as_ref().map_or(true, |b| b <= item)
        }

        pub fn is_in_upper_bound(&self, item: &T) -> bool
        where
            T: std::cmp::Ord,
        {
            self.upper_bound.as_ref().map_or(true, |b| item <= b)
        }

        pub fn is_in_bounds(&self, item: &T) -> bool
        where
            T: std::cmp::Ord,
        {
            self.is_in_upper_bound(item) && self.is_in_lower_bound(item)
        }

        fn adjust_bound(&mut self, bound: T, lower: bool) -> Option<Interval<T>>
        where
            T: std::cmp::Ord,
            T: std::fmt::Debug,
        {
            debug_assert!(
                if lower {
                    self.is_in_upper_bound(&bound)
                } else {
                    self.is_in_lower_bound(&bound)
                },
                "The provided {} bound {:?} is invalid in \n{}",
                if lower { "lower" } else { "upper" },
                bound,
                self.draw()
            );
            let new_bound_is_larger = self
                .get_bound(lower)
                .as_ref()
                .map(|a| if lower { a >= &bound } else { a <= &bound })
                .unwrap_or(false);
            let other_elements = if new_bound_is_larger {
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

        #[cfg(test)]
        pub fn bounds_are_valid(&self) -> bool
        where
            T: std::cmp::Ord,
        {
            self.lower_bound
                .as_ref()
                .map_or(true, |l| self.is_in_upper_bound(l))
        }

        #[cfg(test)]
        pub fn assert_is_left_of(&self, other: &Interval<T>)
        where
            T: std::cmp::Ord + std::fmt::Debug,
        {
            if let Option::Some(b1) = self.lower_bound.as_ref() {
                assert!(
                    !other.has_lower_bound() || !other.is_in_lower_bound(b1),
                    "lower bound is wrong between\n{}\n{}",
                    self.draw(),
                    other.draw()
                );
                assert!(
                    !other.has_upper_bound() || other.is_in_upper_bound(b1),
                    "lower bound is wrong between\n{}\n{}",
                    self.draw(),
                    other.draw()
                );
            }
            if let Option::Some(b1) = self.upper_bound.as_ref() {
                assert!(
                    !other.has_lower_bound() || !other.is_in_lower_bound(b1),
                    "upper bound is wrong between\n{}\n{}",
                    self.draw(),
                    other.draw()
                );
                assert!(
                    !other.has_upper_bound() || other.is_in_upper_bound(b1),
                    "upper bound is wrong between \n{}\n{}",
                    self.draw(),
                    other.draw()
                );
            }
        }

        #[cfg(test)]
        pub fn assert_no_common_element_with(&self, other: &Interval<T>)
        where
            T: std::cmp::PartialEq + std::fmt::Debug,
        {
            for e in self.elems.iter() {
                for e2 in other.elems.iter() {
                    assert!(
                        e != e2,
                        "Element {:?} is the same in \n{}\n{}",
                        e,
                        self.draw(),
                        other.draw()
                    );
                }
            }
        }

        #[cfg(test)]
        pub fn assert_all_elems_in_bound(&self)
        where
            T: std::cmp::Ord,
        {
            for elem in self.elems.iter() {
                assert!(
                    self.is_in_bounds(elem),
                    "Element {:?} is out of bounds\n{}",
                    elem,
                    self.draw()
                );
            }
        }

        fn draw(&self) -> String
        where
            T: std::fmt::Debug,
        {
            format!(
                "[{},{}) <{}>",
                self.lower_bound
                    .as_ref()
                    .map_or("...".to_string(), |a| format!("{:?}", a)),
                self.upper_bound
                    .as_ref()
                    .map_or("...".to_string(), |a| format!("{:?}", a)),
                format!("{:?}", self.elems)
            )
        }
    }

    fn collapse_result<T>(r: Result<T, T>) -> T {
        match r {
            Ok(t) | Err(t) => t,
        }
    }

    #[derive(Clone, Eq, PartialEq, Hash, Debug)]
    pub enum Action<B> {
        Open(B),
        Close(B),
        Insert(B),
    }

    /// Split the elements vector into two parts on an element `e` such that all
    /// elements of of the first vector are strictly smaller than `e` and the
    /// elements of the second larger or equal to `e`.
    use super::FreeGroup;

    impl<T: std::cmp::Ord + std::fmt::Debug> Seq<T> {


        pub fn apply(&mut self, action: Action<T>, positive: bool) {
            self.handle_helper(action, !positive)
        }

        pub fn compute_new_value(&mut self) -> f64 {
            let lens : Vec<usize> = self.complete_intervals().map(|i| i.elems.len()).collect();
            let l = lens.len();
            match l {
                0 => 0.0,
                _ => {
                    let sum : usize = lens.into_iter().sum();
                    sum as f64 / l as f64
                }
            }
        }

        #[cfg(test)]
        fn from_intervals(intervals: Vec<Interval<T>>) -> Seq<T> {
            Seq(intervals)
        }

        pub fn new() -> Seq<T> {
            Seq(Vec::new())
        }

        pub fn handle(&mut self, action: FreeGroup<Action<T>>)
        where
            T: std::fmt::Debug,
        {
            match dbg!(action) {
                FreeGroup::One(ac) => self.handle_helper(ac, false),
                FreeGroup::Not(n) => match *n {
                    FreeGroup::One(ac) => self.handle_helper(ac, true),
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            }
        }

        pub fn handle_helper(&mut self, action: Action<T>, negate: bool) {
            match action {
                Action::Open(i) => {
                    if !negate {
                        self.open_interval(i)
                    } else {
                        self.reverse_open_interval(&i)
                    }
                }
                Action::Close(i) => {
                    if !negate {
                        self.close_interval(i)
                    } else {
                        self.reverse_close_interval(&i)
                    }
                }
                Action::Insert(i) => {
                    if !negate {
                        self.insert_element(i)
                    } else {
                        self.remove_element(&i)
                    }
                }
            }
        }

        fn open_interval(&mut self, bound: T) {
            self.expand_interval(bound, true)
        }

        fn reverse_open_interval(&mut self, bound: &T) {
            self.contract_interval(bound, true)
        }

        fn close_interval(&mut self, bound: T) {
            self.expand_interval(bound, false)
        }

        fn reverse_close_interval(&mut self, bound: &T) {
            self.contract_interval(bound, false)
        }

        fn insert_element(&mut self, elem: T) {
            match self.find_target_index(&elem) {
                Ok(idx) => self.0[idx].insert_elem(elem),
                Err(idx) => self.0.insert(idx, Interval::new(vec![elem])),
            }
        }

        fn remove_element(&mut self, elem: &T) {
            if let Result::Ok(idx) = self.find_target_index(elem) {
                let cleanup_necessary = {
                    let ref mut target = self.0[idx];
                    debug_assert!(target.remove_element(elem));
                    target.needs_cleanup()
                };
                if cleanup_necessary {
                    self.0.remove(idx);
                }
            } else {
                panic!("Element-to-remove is not present");
            }
        }

        pub fn complete_intervals<'a>(&'a self) -> impl Iterator<Item = &'a Interval<T>> {
            self.0.iter().filter(|e| e.is_closed())
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
                debug_assert!(&removed.unwrap() == bound);
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
            #[cfg(test)]
            eprintln!(
                "Expanding intervals with {} bound {:?}",
                if lower { "lower" } else { "upper" },
                &bound
            );

            match self.find_target_index(&bound) {
                Ok(idx) => {
                    let ref mut target = self.0[idx];
                    //eprintln!("Targeting {}", target.draw());
                    target.adjust_bound(bound, lower).map(|new_node| {
                        let iidx = if lower { idx } else { idx + 1 };
                        // eprintln!(
                        //     "Inserting leftover interval at index {}\n{}",
                        //     iidx,
                        //     new_node.draw()
                        // );
                        self.0.insert(iidx, new_node)
                    });
                }
                Err(idx) => {
                    //eprintln!("Inserting new interval at {}", idx);
                    self.0.insert(
                        idx,
                        if lower {
                            Interval::with_lower_bound(bound, Vec::new())
                        } else {
                            Interval::with_upper_bound(bound, Vec::new())
                        },
                    )
                }
            }
        }

        /// Find an exact interval index with this bound
        fn find_exact_bound(&self, bound: &T, lower: bool) -> usize {
            self.0
                .binary_search_by(|e| {
                    if (if lower {
                        e.lower_bound.as_ref().map_or(false, |b| b == bound)
                    } else {
                        e.upper_bound.as_ref().map_or(false, |b| b == bound)
                    }) {
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

        fn prev<'a>(&'a self, idx: usize) -> Option<&'a Interval<T>> {
            if idx == 0 {
                Option::None
            } else {
                self.get(idx - 1)
            }
        }

        fn get<'a>(&'a self, idx: usize) -> Option<&'a Interval<T>> {
            self.0.get(idx)
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
                let candidates = (self.prev(idx), self.get(idx));
                debug_assert!(
                    candidates.0.map_or(true, |e| e.has_upper_bound())
                        || candidates.1.map_or(true, |e| e.has_lower_bound()),
                    "Invariant broken, both intervals lack bounds."
                );
                match candidates {
                    (Option::Some(t), _) if !t.has_upper_bound() => Ok(idx - 1),
                    (_, Option::Some(t)) if !t.has_lower_bound() => Ok(idx),
                    _ => Err(idx),
                }
            })
        }

        #[cfg(test)]
        pub fn check_invariants(&self)
        where
            T: std::cmp::Ord + std::fmt::Debug,
        {
            for (idx, iv) in self.0.iter().enumerate() {
                assert!(iv.bounds_are_valid(), "bounds invalid");
                assert!(
                    iv.has_upper_bound()
                        || self.0.get(idx + 1).map_or(true, Interval::has_lower_bound),
                    "Two open intervals next to each other"
                );
                iv.assert_all_elems_in_bound();
                for iv2 in self.0[idx + 1..].iter() {
                    iv.assert_is_left_of(iv2);
                    iv.assert_no_common_element_with(iv2);
                }
            }
        }

        pub fn draw(&self) -> String
        where
            T: std::fmt::Debug,
        {
            format!(
                "Seq({})\n{}",
                self.0.len(),
                self.0
                    .iter()
                    .map(Interval::draw)
                    .collect::<Vec<String>>()
                    .join("\n")
            )
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rand::random;
        use std::collections::HashSet;

        #[test]
        fn test_iseq_insert() {
            let mut seq = Seq::new();

            seq.insert_element(1);
            println!("{}", seq.draw());
            seq.insert_element(2);

            seq.check_invariants();
            println!("{}", seq.draw());
        }

        #[test]
        fn test_iseq_action_around_closed_interval() {
            for t in &[(1, 2), (-2, -1)] {
                let mut seq = Seq::new();
                println!("\nTesting {:?}", &t);

                let (b1, b2): &(i32, i32) = t;

                seq.open_interval(b1);
                seq.close_interval(b2);
                seq.check_invariants();

                seq.open_interval(&-3);
                seq.check_invariants();
                seq.close_interval(&3);
                seq.check_invariants();
            }
        }

        #[test]
        fn test_iseq_double_close() {
            for t in &[(1, 2), (2, 1), (-1, -2), (-2, -1)] {
                let (b1, b2): &(i32, i32) = t;
                println!("\nTesting {:?}", *t);
                let mut seq = Seq::new();

                seq.close_interval(b1);
                seq.close_interval(b2);

                println!("{}", seq.draw());
                seq.check_invariants();
            }
        }

        #[test]
        fn test_interval_compare() {
            use std::cmp::Ordering;
            {
                let iv = Interval::<i32>::bounded(1, 10, Vec::new());
                use std::cmp::Ordering;
                assert!(iv.compare_elem(&0) == Ordering::Greater);
                assert!(iv.compare_elem(&2) == Ordering::Equal);
                assert!(iv.compare_elem(&11) == Ordering::Less);
            }
            {
                let iv = Interval::<i32>::with_lower_bound(1, Vec::new());
                assert!(iv.compare_elem(&0) == Ordering::Greater);
                // Not sure this should be equal
                assert!(iv.compare_elem(&1) == Ordering::Equal);
                assert!(iv.compare_elem(&2) == Ordering::Less);
            }
        }

        // #[test]
        // fn test_search_index() {
        //     let seq = Seq::from_intervals(
        //         vec![Interval::bounded(1,2), Interval::bounded()]
        //     );
        // }

        #[test]
        fn test_iseq_random() {
            let mut taken = HashSet::new();

            let mut seq = Seq::new();

            for i in 0..100 {
                let ac = random_action();
                if taken.insert(ac.clone()) {
                    seq.handle(FreeGroup::one(ac));
                    seq.check_invariants();
                }
            }

            for i in 0..100 {
                let _: u32 = i;
                if i % 2 == 0 {
                    let ac = random_action();
                    if taken.insert(ac.clone()) {
                        seq.handle(FreeGroup::one(ac));
                    }
                } else {
                    let ac = taken.drain().next().unwrap();
                    seq.handle(FreeGroup::not(FreeGroup::one(ac)));
                }
                seq.check_invariants();
            }
            for ac in taken.drain() {
                seq.handle(FreeGroup::not(FreeGroup::one(ac)));
                seq.check_invariants();
            }
        }

        fn random_action() -> Action<i32> {
            let v = random();
            match random::<u8>() % 3 {
                0 => Action::Open(v),
                1 => Action::Close(v),
                2 => Action::Insert(v),
                i => panic!("{}", i),
            }
        }

    }
}
