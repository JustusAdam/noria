use std::sync::mpsc::channel;
use std::thread;
use std::time;
use tokio::prelude::*;

use serde_derive::Deserialize;

use noria::DataType;

#[derive(Deserialize,Default)]
pub struct Conf {
    num_probes: usize,
    num_probings: u32,
    num_noise_makers: usize,
    noise_pool_size: usize,
    pub shards: usize,
    num_users: usize,
    pub table_name: String,
    pub query_name: String,
    pub query_file: String,
    measure_chunk_size: usize,
}

type User = usize;
type Id = usize;
type Start = std::sync::Barrier;

fn random_range<T: rand::distributions::uniform::SampleUniform, R: rand::Rng>(rng: &mut R, lo: T, hi: T) -> T
where
    rand::distributions::Standard: rand::distributions::Distribution<T>
{
    rng.gen_range(lo, hi)
}

fn random_user_not<R:rand::Rng>(rng: &mut R, max_user: User, avoid: &[User]) -> User {
    loop {
        let usr = random_range(rng, 0, max_user);
        if !avoid.contains(&usr) {
            return usr;
        }
    }
    panic!()
}

fn random_ev_type<R: rand::Rng>(rng: &mut R) -> i32 {
    let i = random_range(rng, 0,10);
    assert!(i < 10);
    if i < 8 {
        0
    } else {
        i - 7
    }
}

type Event = (Id,User,i32, i32);

fn random_event_raw<R: rand::Rng>(rng: &mut R, id: Id, usr: Result<User,&[User]>, max_user: User, ts: &mut i32) -> Event {
    *ts += 1;
    (id.into(),
     match usr {
         Ok(usr) => usr,
         Err(avoid) => random_user_not(rng, max_user, avoid)
     },
     random_ev_type(rng),
     *ts
    )
}

fn event_to_db_format(ev: Event) -> Vec<DataType> {
    let (id, u, ty, ts) = ev;
    vec![id.into(), u.into(), ty.into(), ts.into()]
}

fn random_event<R: rand::Rng>(rng: &mut R, id: usize, usr: Result<User,&[User]>, max_user: User, ts: &mut i32) -> Vec<DataType> {
    event_to_db_format(random_event_raw(rng, id, usr, max_user, ts))
}

struct Probe<'a, R> {
    requests: Vec<(bool, Event, Vec<DataType>)>,
    iseq: iseq::Seq<i32>,
    max_user: User,
    my_user: User,
    probings: u32,
    my_val: f32,
    rng: &'a mut R,
    chunk_size: usize,
}

fn iseq_apply(s: &mut iseq::Seq<i32>, positive: bool, ev: &Event) {
    use iseq::Action;
    let val = ev.3;
    s.apply(
        match ev.2 {
            0 => Action::Insert(val),
            1 => Action::Open(val),
            2 => Action::Close(val),
            i => panic!("{}?", i),
        },
        positive);
}

impl <'a, R: rand::Rng> Probe<'a, R> {
    fn new(rng: &'a mut R, user: User, id_start: Id, pool_size: usize, probings: u32, max_user: User, chunk_size: usize) -> Self {
        let ref mut ts = 0;
        let mut requests = (id_start..id_start+pool_size).map(| id | {
            let ev = random_event_raw(rng, id, Ok(user), max_user, ts);
            (true, ev, event_to_db_format(ev.clone()))
        }).collect();
        let mut s = Default::default();
        Self {
            requests,
            iseq:s,
            max_user,
            my_user: user,
            probings,
            my_val: 0.0,
            rng,
            chunk_size,
        }
    }
    fn init(&mut self, db: &mut Db) {
        for (ref mut pos, ref raw, ref dt) in self.requests.iter_mut() {
            iseq_apply(&mut self.iseq, *pos, raw);
            db.apply(pos, dt.clone());
        }
        for (p, _, _) in self.requests.iter() {
            assert!(!p);
        }
        self.my_val = self.iseq.compute_new_value() as f32;
    }
    fn run(&mut self, db: &mut Db) -> Vec<(User,time::Duration)> {
        let mut measurements = Vec::new();
        let mut db_seen = None;
        let mut probings = self.probings;
        let mut reqs = Vec::with_capacity(self.chunk_size);
        while probings > 0 {
            // eprintln!("{}: Starting probe {}", self.my_user, probings);
            let l = self.requests.len();
            let mut chunk = self.chunk_size;
            while chunk != 0 {
                let idx = random_range(self.rng, 0, l);
                let (ref mut positive, ref raw, ref req) = self.requests.get_mut(idx).unwrap();
                let pos = *positive;
                iseq_apply(&mut self.iseq, pos, raw);
                let new_val = self.iseq.compute_new_value() as f32 ;
                *positive = !pos;
                reqs.push(Ok((pos, req.clone())));
                if new_val != self.my_val {
                    self.my_val = new_val;
                    reqs.push(Err(new_val));
                };
                chunk -= 1;
            }
            // eprintln!("{}: {} Requests prepared", self.my_user, reqs.len());
            let t1 = time::Instant::now();
            let mut n_reqs = 0;
            for r in reqs.drain(..) {
                match r {
                    Ok((mut positive, req)) => {
                        let ref mut p = positive;
                        db.apply(p, req)
                    },
                    Err(val) =>
                        while db_seen != Some(val) {
                            db_seen = db.get_value(self.my_user);
                            n_reqs += 1;
                        },
                }
            }
            let t2 = time::Instant::now();
            measurements.push((n_reqs, t2 - t1));
            probings -= 1;
            thread::yield_now();
            // eprintln!("{}: Requests answered", self.my_user);
        }
        // eprintln!("{}: Measuring finished", self.my_user);
        measurements
    }
}

struct NoiseMaker<'a, R> {
    requests: Vec<(bool, Vec<DataType>)>,
    rng: &'a mut R,
}

impl <'a, R: rand::Rng> NoiseMaker<'a, R> {
    fn new(rng: &'a mut R, pool_size: usize, id_start: Id, probed_users: &[User], max_user: User) -> Self {
        let ref mut ts = 0;
        let mut requests = (id_start..id_start+pool_size).map(| id | {
            let ev = random_event(rng, id, Err(probed_users), max_user, ts);
            (true, ev)
        }).collect();
        NoiseMaker {
            requests,
            rng
        }
    }

    fn init(&mut self, db: &mut Db) {
        for (ref mut pos, ref r) in self.requests.iter_mut() {
            db.apply(pos, r.clone());
        }
    }

    fn run(&mut self, db: &mut Db) {
        let l = self.requests.len();
        loop {
            for _ in 0..20 {
                let ref mut e = self.requests[random_range(self.rng, 0, l)];
                db.apply(&mut e.0, e.1.clone());
            };
            thread::yield_now();
        }
    }
}

struct Db {
    table: noria::SyncTable,
    view: noria::SyncView,
}

impl Db {
    fn apply(&mut self, positive: &mut bool, r: Vec<DataType>) {
        if *positive {
            self.table.insert(r)
        } else {
            self.table.delete(vec![r[0].clone()])
        }.unwrap();
        *positive = !(*positive);
    }
    fn get_value(&mut self, usr: User) -> Option<f32> {
        let res = self.view.lookup(&[usr.into()], true).unwrap();
        if res.len() == 0 {
            None
        } else {
            assert_eq!(res.len(),1);
            let f : f64 = (&res[0][1]).into();
            Some(f as f32)
        }
    }

    fn new(table: noria::SyncTable, view: noria::SyncView) -> Self {
        Db {
            view, table
        }
    }
}

fn read_file(path: &str) -> std::io::Result<String> {
    use std::fs::File;
    use std::io::{BufReader};
    let mut s = String::new();
    BufReader::new(File::open(path)?).read_to_string(&mut s)?;
    Ok(s)
}

pub fn main(conf: &Conf) {
    let zk_addr = "127.0.0.1:2181";
    let mut rt : tokio::runtime::Runtime = tokio::runtime::Builder::new().build().unwrap();
    let mut server_handle : noria::Handle<noria::LocalAuthority> = {
        let mut b = noria::Builder::default();
        b.set_sharding(Some(conf.shards));
        b.disable_partial();
        // let auth = noria::consensus::ZookeeperAuthority::new(zk_addr).unwrap();
        // rt.block_on(b.start(Arc::new(auth))).unwrap()
        rt.block_on(b.start_local()).unwrap()
    };
    let get_handle = || {
        server_handle.clone()
        // noria::ControllerHandle::from_zk(zk_addr).wait().unwrap()
    };
    // }
    // else {
    //     noria::ControllerHandle::from_zk("127.0.0.1:2181/redesign").wait().unwrap()
    // };
    let exp_query = read_file(&conf.query_file).unwrap();
    rt.block_on(get_handle().install_recipe(&exp_query)).unwrap();
    let (to_probe, from_probe) = channel();
    use std::sync::{Arc,Barrier};
    let starter = Arc::new(Barrier::new(conf.num_probes + conf.num_noise_makers + 1));
    let seed : [u8;32] = [238, 10, 195, 186, 86, 124, 83, 85, 222, 39, 150, 38, 153, 110, 82, 70, 111, 168, 61, 84, 17, 242, 144, 82, 16, 79, 238, 192, 27, 93, 93, 179];

    let mut source_rng : rand::rngs::StdRng = rand::SeedableRng::from_seed(seed);
    let mut mk_id_start = {
        let mut id = 0;
        let pool_sz = conf.noise_pool_size;
        move || {
            let slc = id;
            id += pool_sz;
            slc
        }
    };
    let probed_users = {
        let mut probed_users = Vec::new();
        let usrs = conf.num_users;
        let mut usr_rng : rand::rngs::StdRng = rand::SeedableRng::from_rng(&mut source_rng).unwrap();
        let mut new_user = || {
            let usr = random_user_not(&mut usr_rng, usrs, &probed_users);
            probed_users.push(usr);
            usr
        };
        let probes : Vec<_> = (0..conf.num_probes).map(|_| {
            let start = starter.clone();
            let chn = to_probe.clone();
            let probings = conf.num_probings;
            let usr = new_user();
            let slc = mk_id_start();
            let noise_pool_size = conf.noise_pool_size;
            let mut h = get_handle();
            let mut db = Db::new(
                rt.block_on(h.table(conf.table_name.as_ref())).unwrap().into_sync(),
                rt.block_on(h.view(conf.query_name.as_ref())).unwrap().into_sync(),
                );
            let mut rng : rand::rngs::StdRng = rand::SeedableRng::from_rng(&mut source_rng).unwrap();
            let max_user = conf.num_users;
            let chunk_size = conf.measure_chunk_size;
            eprintln!("User {} is being probed", usr);
            thread::spawn(move || {
                let mut prb = Probe::new(&mut rng, usr, slc, noise_pool_size, probings, max_user, chunk_size);
                prb.init(&mut db);
                start.wait();
                let res = prb.run(&mut db);
                chn.send((usr, res)).unwrap();
            })
        }).collect();
        //eprintln!("{} Probes spawned", probes.len());
        probed_users
    };
    for _ in 0..conf.num_noise_makers {
        let start = starter.clone();
        let noise_pool_size = conf.noise_pool_size;
        let slc = mk_id_start();
        let mut h = get_handle();
        let mut db = Db::new(
            rt.block_on(h.table(conf.table_name.as_ref())).unwrap().into_sync(),
            rt.block_on(h.view(conf.query_name.as_ref())).unwrap().into_sync(),
        );
        let pu = probed_users.clone();
        let max_user = conf.num_users;
        let mut rng : rand::rngs::StdRng = rand::SeedableRng::from_rng(&mut source_rng).unwrap();
        thread::spawn(move || {
            let mut nm = NoiseMaker::new(&mut rng, noise_pool_size, slc, &pu, max_user);
            nm.init(&mut db);
            start.wait();
            nm.run(&mut db)
        });
    }
    // eprintln!("Noise spawned");
    starter.wait();
    //eprintln!("Sync!");
    for i in 0..conf.num_probes {
        let (usr, probes) = from_probe.recv().unwrap();
        for (reqs, time) in probes.into_iter() {
            println!("{},{},{}", usr, reqs, time.as_millis());
        }
    }
    server_handle.shutdown();
}
pub mod iseq {
    pub (crate) type ClickAnaInner = Seq<i32>;
    #[derive(Debug)]
    pub struct Seq<T>(Vec<Interval<T>>);

    impl<T> Default for Seq<T> {
        fn default() -> Self {
            Seq(Vec::new())
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
