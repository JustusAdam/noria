extern crate noria;
extern crate serde_derive;
extern crate toml;
extern crate rand;
extern crate tokio;

mod redesign;

use std::io::Read;

use serde_derive::Deserialize;

use noria::{Builder, DataType, TableOperation};

#[derive(Deserialize,Default)]
struct EConf {
    table_file: String,
    query_file: String,
    data_file: String,
    lookup_file: String,
    sharding: Option<usize>,
    logging: Option<bool>,
    separate_ops: Option<Vec<String>>,
    dump_graph: Option<String>,
    separate_ops_exclusive: Option<bool>,
}

fn make_test_builder(sharding: Option<usize>, logging: bool) -> Builder {
    let mut b = Builder::default();
    b.set_sharding(sharding);
    if logging {
        b.log_with(noria::logger_pls());
    }
    b
}

fn make_test_instance(sharding: Option<usize>, logging: bool) -> noria::SyncHandle<noria::consensus::LocalAuthority> {
    make_test_builder(sharding, logging).start_simple().unwrap()
}

fn read_file(path: &str) -> std::io::Result<String> {
    use std::fs::File;
    use std::io::{BufReader};
    let mut s = String::new();
    BufReader::new(File::open(path)?).read_to_string(&mut s)?;
    Ok(s)
}

fn get_inputs() -> std::io::Result<(EConf,String, String, String, String)> {
    let mut args = std::env::args();
    args.next().unwrap();
    let conf_file = args.next().unwrap();
    let mut buf = String::new();
    std::fs::File::open(conf_file)?.read_to_string(&mut buf).unwrap();
    let conf :EConf = toml::from_str(&mut buf).unwrap();
    let exp_query = read_file(&conf.query_file)?;
    let exp_tables = read_file(&conf.table_file)?;
    let exp_data = read_file(&conf.data_file)?;
    let exp_lookups = read_file(&conf.lookup_file)?;
    Ok((conf, exp_tables, exp_query, exp_data, exp_lookups))
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

fn get_data() -> (EConf, String, String, Vec<TaggedRows>, TaggedRows) {
    let (conf, exp_tables, exp_query, exp_data, exp_lookups) = get_inputs().unwrap();
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

    (conf, exp_tables, exp_query, processed_data, (lookup_label, processed_lookups))
}

const SETUP_FOR_VERIFY_STR: Option<&'static str> = option_env!("VERIFY");

fn is_setup_for_verify() -> bool {
    SETUP_FOR_VERIFY_STR.is_some()
}

fn main0() {
    let (conf, tables, query, data, lookups) = get_data();
    if let Some(ref l) = conf.separate_ops {
        eprintln!("Setting domain control for process {}", std::process::id());
        unsafe {
            noria::make_single_list = Some(l.clone());
        }
    }
    if let Some(b) = conf.separate_ops_exclusive {
        unsafe {
            noria::make_singles_completely_exclusive = b;
        }
    }
    eprintln!("Finished parsing input data");

    // {
    //     let mut controller = make_test_instance(&conf);
    //     controller.install_recipe(&tables).unwrap();
    //     for (table_name, data) in data.clone() {
    //         let mut table = controller.table(&table_name).unwrap().into_sync();
    //         let t0 = Instant::now();
    //         table
    //             .perform_all(data.into_iter().map(TableOperation::Insert))
    //             .unwrap();
    //         let t1 = Instant::now();
    //         if !is_setup_for_verify() {
    //             println!("raw_load,{},{}", table_name, (t1 - t0).as_nanos())
    //         };
    //     }
    // }

    // eprintln!("Finished loading probe");

    let mut controller = make_test_instance(conf.sharding, conf.logging.unwrap_or(false));

    controller.install_recipe(tables).unwrap();
    controller.extend_recipe(query).unwrap();

    if let Some(file) = conf.dump_graph {
        use std::io::Write;
        let gr = controller.graphviz();
        write!(std::fs::File::create(file).unwrap(), "{}", gr.unwrap()).unwrap();
    }

    use std::time::Instant;

    for (table_name, data) in data {
        let mut table = controller.table(&table_name).unwrap().into_sync();
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

    let mut query = controller.view(&view_name).unwrap().into_sync();
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



fn main2() {
    let mut args = std::env::args();
    args.next().unwrap();
    let conf_file = args.next().unwrap();
    let mut buf = String::new();
    std::fs::File::open(conf_file).unwrap().read_to_string(&mut buf).unwrap();
    let conf : redesign::Conf = toml::from_str(&mut buf).unwrap();

    redesign::main(&conf);
}

fn main2() {
    let conf = Default::default();
    let mut contr = make_test_instance(&conf);
    contr.install_recipe("CREATE TABLE test (x int);").unwrap();
    let udtf_args = vec!["test"];
    contr.install_udtf("main", &udtf_args).unwrap();
    let mut main_v = contr.view("main").unwrap().into_sync();
    print!("{:?}", main_v.lookup(&[0.into()], true).unwrap());
}

fn main() {
    main2()
}
