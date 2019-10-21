extern crate noria;
extern crate serde_derive;
extern crate toml;

use std::io::Read;

use serde_derive::Deserialize;

use noria::{Builder, DataType, TableOperation};

#[derive(Deserialize)]
struct EConf {
    query_file: String,
    data_file: String,
    lookup_file: String,
    sharding: Option<usize>,
    logging: Option<bool>,
}

fn make_test_builder(conf: &EConf) -> Builder {
    let mut b = Builder::default();
    b.set_sharding(conf.sharding);
    if conf.logging == Some(true) {
        b.log_with(noria::logger_pls());
    }
    b
}

fn make_test_instance(conf: &EConf) -> noria::SyncHandle<noria::consensus::LocalAuthority> {
    make_test_builder(conf).start_simple().unwrap()
}

fn read_file(path: &str) -> std::io::Result<String> {
    use std::fs::File;
    use std::io::{BufReader};
    let mut s = String::new();
    BufReader::new(File::open(path)?).read_to_string(&mut s)?;
    Ok(s)
}

fn get_inputs() -> std::io::Result<(EConf,String, String, String)> {
    let mut args = std::env::args();
    args.next().unwrap();
    let conf_file = args.next().unwrap();
    let mut buf = String::new();
    std::fs::File::open(conf_file)?.read_to_string(&mut buf).unwrap();
    let conf :EConf = toml::from_str(&mut buf).unwrap();
    let exp_query = read_file(&conf.query_file)?;
    let exp_data = read_file(&conf.data_file)?;
    let exp_lookups = read_file(&conf.lookup_file)?;
    Ok((conf, exp_query, exp_data, exp_lookups))
}

fn make_deser<T: std::str::FromStr + Into<DataType>>() -> Box<dyn Fn(&str) -> DataType>
where
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    Box::new(|s| s.parse::<T>().unwrap().into())
}

fn to_deser(s: &str) -> Box<dyn Fn(&str) -> DataType> {
    match s {
        "i32" => make_deser::<i32>(),
        "i64" => make_deser::<i64>(),
        _ => panic!("Unknown type {}", s),
    }
}

fn deser_lines(lines: &mut std::str::Lines) -> (Vec<Vec<DataType>>, Option<String>) {
    let line1 = lines.next().unwrap();
    let deser: Vec<_> = line1.split(',').map(to_deser).collect();
    let mut chunk = Vec::new();

    for line in lines {
        if line.chars().next().unwrap() == '#' {
            return (chunk, Some(line.to_string()));
        }
        let item = line
            .split(',')
            .zip(deser.iter())
            .map(|(s, f)| (**f)(s))
            .collect::<Vec<DataType>>();
        chunk.push(item);
    }
    (chunk, None)
}

type TaggedRows = (String, Vec<Vec<DataType>>);

fn get_data() -> (EConf, String, Vec<TaggedRows>, TaggedRows) {
    let (conf, exp_query, exp_data, exp_lookups) = get_inputs().unwrap();
    let mut processed_data = Vec::new();

    let mut lines = exp_data.lines();

    let mut current_label = lines.next().map(str::to_string);

    while let Some(mut label) = current_label {
        let (chunk, next_label) = deser_lines(&mut lines);
        label.remove(0);
        processed_data.push((label, chunk));
        current_label = next_label;
    }

    let mut lookup_lines = exp_lookups.lines();

    let mut lookup_label = lookup_lines.next().unwrap().to_string();
    lookup_label.remove(0);

    let (processed_lookups, _) = deser_lines(&mut lookup_lines);

    (conf, exp_query, processed_data, (lookup_label, processed_lookups))
}

const SETUP_FOR_VERIFY_STR: Option<&'static str> = option_env!("VERIFY");

fn is_setup_for_verify() -> bool {
    SETUP_FOR_VERIFY_STR.is_some()
}

fn main() {
    let (conf, query, data, lookups) = get_data();
    eprintln!("Finished parsing input data");
    let mut builder = make_test_instance(&conf);

    builder.install_recipe(query).unwrap();

    use std::time::Instant;

    for (table_name, data) in data {
        let mut table = builder.table(&table_name).unwrap().into_sync();
        let t0 = Instant::now();
        table
            .perform_all(data.into_iter().map(TableOperation::Insert))
            .unwrap();
        let t1 = Instant::now();
        if !is_setup_for_verify() {
            println!("load,{},{}", table_name, (t1 - t0).as_nanos())
        };
    }

    eprintln!("Finished loading data");

    let (view_name, mut lookup_data) = lookups;

    let mut query = builder.view(&view_name).unwrap().into_sync();
    let t0 = Instant::now();

    for i in lookup_data.drain(..) {
        let res = query.lookup(&i, true).unwrap();
        if is_setup_for_verify() {
            let k :i32 = (&i[0]).clone().into();
            let v :f64 = if res.len() == 0 {
                0.0
            } else {
                assert_eq!(res.len(),1);
                (&res[0][1]).into()
            };
            println!("{},{}", k, v);
        }
    }
    let t1 = Instant::now();
    if !is_setup_for_verify() {
        println!("lookup,{},{}", view_name, (t1 - t0).as_nanos())
    };

    eprintln!("Finished running queries");
}
