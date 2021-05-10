extern crate noria;
extern crate futures;
extern crate nom_sql;
extern crate rand;
extern crate chrono;

use rand::prelude as rp;
use rp::{Rng};

const SCHEMA : &'static str = include_str!("../../noria-benchmarks/lobsters/db-schema/noria.sql");

struct DataGen {
    rng: rp::ThreadRng,
}

impl DataGen {
    fn new() -> Self {
        DataGen{ rng: rp::thread_rng()}
    }

    fn gen_data_item(&mut self, col: &nom_sql::ColumnSpecification) -> noria::DataType {
        use nom_sql::SqlType::*;
        match &col.sql_type {
            Bool => (self.rng.gen::<bool>() as i32).into(),
            Varchar(i) => self.gen_str(*i as usize),
            Tinyint(_) => (self.rng.gen::<i8>() as i32).into(),
            Int(_i) => self.rng.gen::<i32>().into(),
            Bigint(_i) => self.rng.gen::<i32>().into(),
            Double => self.rng.gen::<f64>().into(),
            Float => (self.rng.gen::<f32>() as f64).into(),
            Tinytext => self.gen_str(10),
            Mediumtext => self.gen_str(50),
            Longtext => self.gen_str(1000),
            Text => self.gen_str(100),
            Timestamp => noria::DataType::Timestamp(chrono::NaiveDateTime::from_timestamp(self.rng.gen_range(chrono::MIN_DATE.and_hms(0,0,0).timestamp(), chrono::MAX_DATE.and_hms(0,0,0).timestamp()) , 0)),
            DateTime(_i) => noria::DataType::Timestamp(chrono::NaiveDateTime::from_timestamp(self.rng.gen_range(chrono::MIN_DATE.and_hms(0,0,0).timestamp(), chrono::MAX_DATE.and_hms(0,0,0).timestamp()) , 0)),
            Decimal(i1, i2) => self.rng.gen::<f64>().into(),
            t => unimplemented!("Data generation for {} is not supported yet", t),
        }
    }

    fn gen_str(&mut self, i: usize) -> noria::DataType {
        let l = self.rng.gen_range(0, i);
        self.rng.sample_iter::<char,_>(&rand::distributions::Standard).take(l).collect::<String>().into()
    }
}

fn main() {
    let mut ctrl = {
        let mut b = noria::Builder::default();
        b.log_with(noria::logger_pls());
        b.start_simple().unwrap()
    };

    let mut current_q = String::new();
    let gen_num_rows = 20;

    for line in SCHEMA.lines().filter(|l| !l.starts_with("--") && !l.is_empty() ) {
        if !current_q.is_empty() {
            current_q.push_str(" ");
        }
        current_q.push_str(line);

        if current_q.ends_with(';') {
            let mut w_iter = current_q.split_whitespace();
            let w1 = w_iter.next().unwrap();
            let w2 = w_iter.next().unwrap();
            println!("Handling {} {} query", w1, w2);
            if w1 != "DROP" && w1 != "INSERT" // skipping these for now. Figure out correct handling later
            {
                let ar = ctrl.extend_recipe(&current_q).unwrap();
                println!("Installed tables: {:?}", ar.new_nodes.keys().collect::<Vec<_>>())
            }
            if w1 == "CREATE" && w2 == "TABLE" {
                match nom_sql::parse_query(&current_q).unwrap() {
                    nom_sql::SqlQuery::CreateTable(ct) => {
                        let mut g = DataGen::new();
                        let mut t = ctrl.table(ct.table.name).unwrap().into_sync();
                        for _i in 0..gen_num_rows {
                            t.insert(ct.fields.iter().map(|spec| g.gen_data_item(spec)).collect::<Vec<_>>()).unwrap();
                        }
                    }

                    _ => unreachable!(),
                }
            }

            current_q.clear();
        }
    }

    // ctrl.install_udtf("main", vec!["read_ribbons", "stories", "comments", "comments", "votes"]).unwrap();


}
