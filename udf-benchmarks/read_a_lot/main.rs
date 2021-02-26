extern crate noria;
extern crate rand;
extern crate serde_derive;
extern crate tokio;
extern crate toml;

use std::io::Read;

use serde_derive::Deserialize;

use noria::{DataType};

use tokio::runtime::Runtime;

const TABLE_NAME: &'static str = "clicks";
const QUERY_NAME: &'static str = "clickstream_ana";

#[derive(Deserialize, Default)]
struct EConf {
    query_file: String,
    sharding: Option<usize>,
    num_readers: usize,
    read_batch_size: usize,
    read_num_batches: usize,
    preload_load: usize,
    num_users: usize,
    memory_limit: usize,
    distribution: String,
}

type User = usize;
type Id = usize;

fn read_file(path: &str) -> std::io::Result<String> {
    use std::fs::File;
    use std::io::BufReader;
    let mut s = String::new();
    BufReader::new(File::open(path)?).read_to_string(&mut s)?;
    Ok(s)
}

type Controller = noria::Handle<noria::LocalAuthority>;

type Rng = rand::rngs::StdRng;

fn random_range<T: rand::distributions::uniform::SampleUniform, R: rand::Rng>(
    rng: &mut R,
    lo: T,
    hi: T,
) -> T
where
    rand::distributions::Standard: rand::distributions::Distribution<T>,
{
    rng.gen_range(lo, hi)
}

fn random_ev_type<R: rand::Rng>(rng: &mut R) -> i32 {
    let i = random_range(rng, 0, 10);
    assert!(i < 10);
    if i < 8 {
        0
    } else {
        i - 7
    }
}

type Event = (Id, User, i32, i32);

fn random_event_raw<R: rand::Rng>(rng: &mut R, id: Id, usr: User, ts: &mut i32) -> Event {
    *ts += 1;
    (id, usr, random_ev_type(rng), *ts)
}

fn event_to_db_format(ev: Event) -> Vec<DataType> {
    let (id, u, ty, ts) = ev;
    vec![id.into(), u.into(), ty.into(), ts.into()]
}

fn random_event<R: rand::Rng>(rng: &mut R, id: usize, usr: User, ts: &mut i32) -> Vec<DataType> {
    event_to_db_format(random_event_raw(rng, id, usr, ts))
}

fn preload(conf: &EConf, controller: &mut Controller, rt: &mut Runtime, source_rng: &mut Rng) {
    let mut id = 0;
    let mut table = rt
        .block_on(controller.table(TABLE_NAME))
        .unwrap()
        .into_sync();
    for i in 0..conf.num_users {
        let ref mut ts = 0;
        for _ in 0..conf.preload_load {
            table.insert(random_event(source_rng, id, i, ts)).unwrap();
            id += 1;
        }
    }
}

enum Sampler {
    Uniform(rand::distributions::Uniform<usize>),
}

impl Sampler {
    fn new(conf: &EConf) -> Self {
        match conf.distribution.as_ref() {
            "uniform" => Sampler::Uniform(rand::distributions::Uniform::from(0..conf.num_users)),
            d => panic!("Unknown distribution {}", d),
        }
    }

    fn sample(&self, rng: &mut Rng) -> User {
        use rand::distributions::Distribution;
        match self {
            Sampler::Uniform(dist) => dist.sample(rng),
        }
    }
}

fn run(conf: &EConf, controller: &mut Controller, rt: &mut Runtime, source_rng: &mut Rng) {
    use std::sync::{Arc, Barrier};
    use std::thread::{spawn, JoinHandle};
    use std::time;

    let starter = Arc::new(Barrier::new(conf.num_readers + 1));

    let mut readers: Vec<JoinHandle<Vec<_>>> = (0..conf.num_readers)
        .map(|_| {
            let mut query = rt
                .block_on(controller.view(QUERY_NAME))
                .unwrap()
                .into_sync();
            let dist = Sampler::new(conf);
            let start = starter.clone();
            let mut batches: Vec<Vec<_>> = (0..conf.read_num_batches)
                .map(|_| {
                    (0..conf.read_batch_size)
                        .map(|_| dist.sample(source_rng))
                        .collect()
                })
                .collect();
            spawn(move || {
                start.wait();
                batches
                    .drain(..)
                    .map(|mut batch| {
                        let t1 = time::Instant::now();
                        for r in batch.drain(..) {
                            let res = query.lookup(&[r.into()], true).unwrap();
                            assert_eq!(res.len(), 1, "Reading user {} failed", r);
                            // let f: f64 = (&res[0][1]).into();
                            // f as f32
                        }
                        let t2 = time::Instant::now();
                        t2 - t1
                    })
                    .collect()
            })
        })
        .collect();

    starter.wait();

    for reader in readers.drain(..) {
        for m in reader.join().unwrap().drain(..) {
            println!("{}", m.as_millis())
        }
    }
    ()
}

fn main() {
    let mut args = std::env::args();
    let conf: EConf = {
        args.next().unwrap();
        let conf_file = args.next().unwrap();
        let mut buf = String::new();
        std::fs::File::open(conf_file)
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        toml::from_str(&mut buf).unwrap()
    };

    let query = read_file(&conf.query_file).unwrap();

    let mut rt = tokio::runtime::Builder::new().build().unwrap();

    let mut controller = {
        let mut b = noria::Builder::default();
        b.set_sharding(conf.sharding);
        b.set_memory_limit(conf.memory_limit, std::time::Duration::from_millis(50));
        // let auth = noria::consensus::ZookeeperAuthority::new(zk_addr).unwrap();
        // rt.block_on(b.start(Arc::new(auth))).unwrap()
        rt.block_on(b.start_local()).unwrap()
    };

    rt.block_on(controller.install_recipe(&query)).unwrap();

    let seed: [u8; 32] = [
        238, 10, 195, 186, 86, 124, 83, 85, 222, 39, 150, 38, 153, 110, 82, 70, 111, 168, 61, 84,
        17, 242, 144, 82, 16, 79, 238, 192, 27, 93, 93, 179,
    ];
    let mut source_rng: rand::rngs::StdRng = rand::SeedableRng::from_seed(seed);

    preload(&conf, &mut controller, &mut rt, &mut source_rng);

    run(&conf, &mut controller, &mut rt, &mut source_rng);
}
