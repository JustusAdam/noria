extern crate noria;
extern crate futures;
extern crate nom_sql;
extern crate rand;
extern crate chrono;
extern crate failure;

extern crate tokio;

use rand::prelude as rp;
use rp::{Rng, SeedableRng};

const SCHEMA : &'static str = include_str!("tables.sql");

const SQL_Q : &'static str = include_str!("repl_comm.sql");

const NULL : noria::DataType = noria::DataType::None;

struct DataGen {
    rng: rp::StdRng,
}

impl DataGen {
    fn new() -> Self {
        DataGen{ rng: rp::StdRng::seed_from_u64(44651965920185619)}
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
            Timestamp => self.gen_date(),
            DateTime(_i) => self.gen_date(),
            Decimal(i1, i2) => self.rng.gen::<f64>().into(),
            t => unimplemented!("Data generation for {} is not supported yet", t),
        }
    }

    fn gen_str(&mut self, i: usize) -> noria::DataType {
        let l = self.rng.gen_range(0, i);
        self.rng.sample_iter::<char,_>(&rand::distributions::Alphanumeric).take(l).collect::<String>().into()
    }

    fn gen_date(&mut self) -> noria::DataType {
        noria::DataType::Timestamp(chrono::NaiveDateTime::from_timestamp(self.rng.gen_range(chrono::MIN_DATE.and_hms(0,0,0).timestamp(), chrono::MAX_DATE.and_hms(0,0,0).timestamp()) , 0))
    }

    async fn load_comments(&mut self, ctrl: &mut Handle) -> LoadRes {
        let ref mut table = ctrl.table("comments").await?;
        // `id` int unsigned NOT NULL PRIMARY KEY
        // `created_at` datetime NOT NULL
        // `updated_at` datetime
        // `short_id` varchar(10) DEFAULT '' NOT NULL
        // `story_id` int unsigned NOT NULL
        // `user_id` int unsigned NOT NULL
        // `parent_comment_id` int unsigned
        // `thread_id` int unsigned
        // `comment` mediumtext NOT NULL
        // `markeddown_comment` mediumtext
        // `is_deleted` tinyint(1) DEFAULT 0
        // `is_moderated` tinyint(1) DEFAULT 0
        // `is_from_email` tinyint(1) DEFAULT 0
        // `hat_id` int



        table.insert(vec![10.into(), self.gen_date(), self.gen_date(), "".into(), 5.into(), 6.into(), NULL, NULL, "First Comment".into(), NULL, 0.into(), 0.into(), 0.into(), NULL]).await?;
        table.insert(vec![11.into(), self.gen_date(), self.gen_date(), "".into(), 5.into(), 6.into(), 10.into(), NULL, "Second Comment".into(), NULL, 0.into(), 0.into(), 0.into(), NULL]).await?;
        Ok(())
    }

    async fn load_ribbons(&mut self, ctrl: &mut Handle) -> LoadRes {
        let mut table = ctrl.table("read_ribbons").await?;
        // `id` bigint NOT NULL PRIMARY KEY
        // `is_following` tinyint(1) DEFAULT 1
        // `created_at` datetime NOT NULL
        // `updated_at` datetime NOT NULL
        // `user_id` bigint
        // `story_id` bigint
        table.insert(vec![0.into(), 1.into(), self.gen_date(), self.gen_date(), 0.into(), 2.into()]).await?;
        table.insert(vec![1.into(), 1.into(), self.gen_date(), self.gen_date(), 1.into(), 1.into()]).await?;
        table.insert(vec![2.into(), 1.into(), self.gen_date(), self.gen_date(), 2.into(), 0.into()]).await?;

        table.insert(vec![5.into(), 1.into(), NULL, NULL, 5.into(), 5.into()]).await?;
        table.insert(vec![6.into(), 1.into(), NULL, NULL, 6.into(), 5.into()]).await?;

        Ok(())
    }

    async fn load_stories(&mut self, ctrl: &mut Handle) -> LoadRes {
        let mut table = ctrl.table("stories").await?;
        // `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
        // `always_null` int,
        // `created_at` datetime,
        // `user_id` int unsigned,
        // `url` varchar(250) DEFAULT '',
        // `title` varchar(150) DEFAULT '' NOT NULL,
        // `description` mediumtext,
        // `short_id` varchar(6) DEFAULT '' NOT NULL,
        // `is_expired` tinyint(1) DEFAULT 0 NOT NULL,
        // `is_moderated` tinyint(1) DEFAULT 0 NOT NULL,
        // `markeddown_description` mediumtext,
        // `story_cache` mediumtext,
        // `merged_story_id` int,
        // `unavailable_at` datetime,
        // `twitter_id` varchar(20),
        // `user_is_author` tinyint(1) DEFAULT 0,
        table.insert(
            vec![ 1.into(), // id
                  0.into(), // always_null
                  self.gen_date(), // created_at
                  1.into(), // user_id
                  self.gen_str(20), // url
                  "First Story".into(), // title
                  NULL, // description
                  "".into(), // short_id
                  0.into(), // is_expired
                  0.into(), // is_moderated
                  NULL, // markeddown_description
                  NULL, // story_cache
                  NULL, // merged_story_id
                  NULL, // unavailable_at
                  NULL, // twitter_id
                  0.into() // user_is_author
            ]).await?;

        table.insert(
            vec![ 2.into(), // id
                  0.into(), // always_null
                  self.gen_date(), // created_at
                  5.into(), // user_id
                  self.gen_str(20), // url
                  "Second Story".into(), // title
                  NULL, // description
                  "".into(), // short_id
                  0.into(), // is_expired
                  0.into(), // is_moderated
                  NULL, // markeddown_description
                  NULL, // story_cache
                  NULL, // merged_story_id
                  NULL, // unavailable_at
                  NULL, // twitter_id
                  0.into() // user_is_author
            ]).await?;

        table.insert(
            vec![ 5.into(), // id
                  0.into(), // always_null
                  self.gen_date(), // created_at
                  5.into(), // user_id
                  self.gen_str(20), // url
                  "Third Story".into(), // title
                  NULL, // description
                  "".into(), // short_id
                  0.into(), // is_expired
                  0.into(), // is_moderated
                  NULL, // markeddown_description
                  NULL, // story_cache
                  NULL, // merged_story_id
                  NULL, // unavailable_at
                  NULL, // twitter_id
                  0.into() // user_is_author
            ]).await?;

        table.insert(
            vec![ 6.into(), // id
                  0.into(), // always_null
                  self.gen_date(), // created_at
                  5.into(), // user_id
                  self.gen_str(20), // url
                  "Fourth Story".into(), // title
                  NULL, // description
                  "".into(), // short_id
                  0.into(), // is_expired
                  0.into(), // is_moderated
                  NULL, // markeddown_description
                  NULL, // story_cache
                  NULL, // merged_story_id
                  NULL, // unavailable_at
                  NULL, // twitter_id
                  0.into() // user_is_author
            ]).await?;

        table.insert(
            vec![ 0.into(), // id
                  0.into(), // always_null
                  self.gen_date(), // created_at
                  5.into(), // user_id
                  self.gen_str(20), // url
                  "First Story".into(), // title
                  NULL, // description
                  "".into(), // short_id
                  0.into(), // is_expired
                  0.into(), // is_moderated
                  NULL, // markeddown_description
                  NULL, // story_cache
                  NULL, // merged_story_id
                  NULL, // unavailable_at
                  NULL, // twitter_id
                  0.into() // user_is_author
            ]).await?;

        Ok(())
    }

    async fn load_users(&mut self, ctrl: &mut Handle) -> LoadRes {
        let mut table = ctrl.table("users").await?;
        // `id` int unsigned NOT NULL PRIMARY KEY
        // `username` varchar(50)
        // `email` varchar(100)
        // `password_digest` varchar(75)
        // `created_at` datetime
        // `is_admin` tinyint(1) DEFAULT 0
        // `password_reset_token` varchar(75)
        // `session_token` varchar(75) DEFAULT '' NOT NULL
        // `about` mediumtext
        // `invited_by_user_id` int
        // `is_moderator` tinyint(1) DEFAULT 0
        // `pushover_mentions` tinyint(1) DEFAULT 0
        // `rss_token` varchar(75)
        // `mailing_list_token` varchar(75)
        // `mailing_list_mode` int DEFAULT 0
        // `karma` int DEFAULT 0 NOT NULL
        // `banned_at` datetime
        // `banned_by_user_id` int
        // `banned_reason` varchar(200)
        // `deleted_at` datetime
        // `disabled_invite_at` datetime
        // `disabled_invite_by_user_id` int
        // `disabled_invite_reason` varchar(200)
        // `settings` text
        table.insert(vec![0.into(), "first".into(), 10.into()]).await?;
        table.insert(vec![1.into(), "second".into(), 11.into()]).await?;
        table.insert(vec![2.into(), "third".into(), 12.into()]).await?;

        table.insert(vec![5.into(), "fourth".into(), 13.into()]).await?;
        table.insert(vec![6.into(), "fifth".into(), 14.into()]).await?;
        Ok(())
    }

    async fn load_votes(&mut self, ctrl: &mut Handle) -> LoadRes {
        let mut table = ctrl.table("votes").await?;
        // `id` bigint unsigned NOT NULL PRIMARY KEY
        // `user_id` int unsigned NOT NULL
        // `story_id` int unsigned NOT NULL
        // `comment_id` int unsigned
        // `vote` tinyint NOT NULL
        // `reason` varchar(1)
        table.insert(vec![0.into(), 0.into(), 0.into(), 0.into(), 1.into(), NULL]).await?;
        table.insert(vec![1.into(), 1.into(), 0.into(), 1.into(), 0.into(), NULL]).await?;
        table.insert(vec![2.into(), 2.into(), 0.into(), 2.into(), 1.into(), NULL]).await?;
        table.insert(vec![3.into(), 0.into(), 1.into(), 1.into(), 0.into(), NULL]).await?;
        table.insert(vec![4.into(), 2.into(), 1.into(), 0.into(), 1.into(), NULL]).await?;
        table.insert(vec![5.into(), 1.into(), 1.into(), 2.into(), 0.into(), NULL]).await?;
        table.insert(vec![6.into(), 1.into(), 2.into(), 1.into(), 1.into(), NULL]).await?;
        table.insert(vec![7.into(), 0.into(), 2.into(), 1.into(), 0.into(), NULL]).await?;
        table.insert(vec![8.into(), 2.into(), 2.into(), 2.into(), 1.into(), NULL]).await?;

        table.insert(vec![9.into(), 5.into(), 2.into(), 11.into(), 1.into(), NULL]).await?;
        table.insert(vec![10.into(), 6.into(), 2.into(), 11.into(), 1.into(), NULL]).await?;
        table.insert(vec![11.into(), 5.into(), 5.into(), 11.into(), 1.into(), NULL]).await?;
        table.insert(vec![12.into(), 6.into(), 6.into(), 11.into(), 1.into(), NULL]).await?;
        Ok(())
    }

    async fn load_data(&mut self, ctrl: &mut Handle) -> () {
        self.load_users(ctrl).await.unwrap();
        self.load_stories(ctrl).await.unwrap();
        self.load_comments(ctrl).await.unwrap();
        self.load_ribbons(ctrl).await.unwrap();
        self.load_votes(ctrl).await.unwrap();
    }
}

type Handle = noria::Handle<noria::LocalAuthority>;
type LoadRes = Result<(), failure::Error>;



#[tokio::main]
async fn main() {
    let mut a_it = std::env::args();
    a_it.next().expect("Missing executable name");
    let func = a_it.next().expect("Expected view name as singular argument to this executable");
    let (mut ctrl, done ) = {
        let mut b = noria::Builder::default();
        b.log_with(noria::logger_pls());
        b.disable_partial();
        b.start_local().await.unwrap()
    };
    eprintln!("Noria started");

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
                let ar = ctrl.extend_recipe(&current_q).await.unwrap();
                println!("Installed tables: {:?}", ar.new_nodes.keys().collect::<Vec<_>>())
            }
            // if w1 == "CREATE" && w2 == "TABLE" {
            //     match nom_sql::parse_query(&current_q).unwrap() {
            //         nom_sql::SqlQuery::CreateTable(ct) => {
            //             let mut g = DataGen::new();
            //             let mut t = ctrl.table(ct.table.name).unwrap().into_sync();
            //             for _i in 0..gen_num_rows {
            //                 t.insert(ct.fields.iter().map(|spec| g.gen_data_item(spec)).collect::<Vec<_>>()).unwrap();
            //             }
            //         }

            //         _ => unreachable!(),
            //     }
            // }

            current_q.clear();
        }
    }

    eprintln!("Tables installed");

    // ctrl.extend_recipe(SQL_Q).await.unwrap();
    // println!("Query installed");

    DataGen::new().load_data(&mut ctrl).await;
    let (udf, udf_input_tables) =
        // ("main", vec!["read_ribbons", "stories", "comments", "comments", "votes"])
        // ("main0", vec!["read_ribbons"])
        // ("main1", vec!["read_ribbons", "comments"])
        // ("main2", vec!["read_ribbons", "stories", "comments"])
        // ("main3", vec!["read_ribbons", "stories", "comments", "comments"])
        // ("main4", vec!["read_ribbons", "stories", "comments", "comments", "votes"])
          //("main5", vec!["read_ribbons", "stories", "comments", "comments", "votes"])
        // ("main7", vec![])
    // ("main8", vec![])
        (&func, vec![])
        ;

    ctrl.install_udtf(udf, &udf_input_tables).await.unwrap();
    println!("UDTF installed");

    tokio::time::delay_for(std::time::Duration::from_secs(10)).await;


    {
        use std::io::Write;
        let gr = ctrl.graphviz().await;
        write!(std::fs::File::create("graph.dot").unwrap(), "{}", gr.unwrap()).unwrap();
    }
    {
        let mut view = ctrl.view(udf).await.expect("UDTF not found");
        // let cols = view.columns();

        for r in view.lookup(&vec![0.into()], true).await.unwrap().into_iter() {
            println!("{:?}", r.into_iter().zip(view.columns().iter()).collect::<Vec<_>>());
        }
    }
    // Currently does not work. Is it the nested selects?
    // {
    //     let mut view = ctrl.view("replying_comments_original").expect("Replying comments view not found").into_sync();

    //     for mut r in view.lookup(&vec![0.into()], true).unwrap().drain(..) {
    //         println!("{:?}", r.drain(..).zip(view.columns().iter()).collect::<Vec<_>>());
    //     }
    // }


    done.await;
}
