mod click_event {
    const LARGEST_TIMESTAMP_JUMP: Timestamp = 10;
    pub type UID = i32;
    pub type Timestamp = u64;
    pub type Category = i32;

    pub struct Event {
        pub user: UID,
        pub category: Category,
        pub ts: Timestamp,
    }

    use std::ops::Range;

    fn random_from_range<T>(r: &Range<T>) -> T
    where
        rand::distributions::Standard: rand::distributions::Distribution<T>,
        T: std::ops::Rem<Output = T> + std::ops::Sub<Output = T> + std::ops::Add<Output = T> + Copy,
    {
        r.start + (rand::random() % (r.end - r.start))
    }

    pub fn generate(
        uid_range: Range<UID>,
        cat_range: Range<Category>,
    ) -> impl Iterator<Item = Event> {
        use rand::random;
        use std::time::{Duration, SystemTime};
        let mut ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        std::iter::from_fn(move || {
            ts += random::<u64>() % LARGEST_TIMESTAMP_JUMP;

            Option::Some(Event {
                ts,
                user: random_from_range(&uid_range),
                category: random_from_range(&cat_range),
            })
        })
    }

    pub fn dump<W: std::io::Write, I: Iterator<Item = Event>>(w: &mut W, events: I) {
        for ev in events {
            writeln!(w, "{},{},{}", ev.user, ev.category, ev.user).unwrap()
        }
    }

    pub fn parse<R: std::io::BufRead>(r: R) -> impl Iterator<Item = Event> {
        r.lines().map(|l| {
            let line = l.unwrap();
            let mut fields = line.split(',');
            Event {
                user: fields.next().unwrap().parse().unwrap(),
                category: fields.next().unwrap().parse().unwrap(),
                ts: fields.next().unwrap().parse().unwrap(),
            }
        })
    }
}

#[macro_use]
extern crate clap;

fn main() {
    use clap::*;
    let app = clap_app!(clickstream =>
                            (@arg DATAFILE: -f --datafile [FILE] "Where to read the events from or write them to")
                            (@subcommand generate =>
                             (@arg COUNT: <NUM> "How many entries to generate")
                            )
                            (@subcommand run =>
                            )

    );
    let matches = app.get_matches();
    let f = matches.value_of("datafile").unwrap_or("data.csv");

    use crate::click_event::*;
    use std::fs::File;

    match matches.subcommand() {
        ("generate", Some(gen_opts)) => dump(
            &mut File::create(f).unwrap(),
            generate(0..20, 0..5).take(gen_opts.value_of("COUNT").unwrap().parse().unwrap()),
        ),
        ("run", _) => {
            let mut f = std::io::BufReader::new(File::open(f).unwrap());
            let it = parse(&mut f);
        }
        ("", _) => println!("No subcommand specified!"),
        (sub, _) => println!("unknown subcommand '{}'", sub),
    }
}
