use crate::controller::migrate::Migration;
use crate::startup::Event;
use dataflow::prelude::*;
use futures::try_ready;
use noria::consensus::Authority;
use noria::prelude::*;
use noria::SyncControllerHandle;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use stream_cancel::Trigger;
use tokio::prelude::*;
use tokio_io_pool;

/// A handle to a controller that is running in the same process as this one.
pub struct Handle<A: Authority + 'static> {
    c: Option<ControllerHandle<A>>,
    #[allow(dead_code)]
    event_tx: Option<futures::sync::mpsc::UnboundedSender<Event>>,
    kill: Option<Trigger>,
    iopool: Option<tokio_io_pool::Runtime>,
}

impl<A: Authority> Deref for Handle<A> {
    type Target = ControllerHandle<A>;
    fn deref(&self) -> &Self::Target {
        self.c.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for Handle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.c.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> Handle<A> {
    pub(super) fn new(
        authority: Arc<A>,
        event_tx: futures::sync::mpsc::UnboundedSender<Event>,
        kill: Trigger,
        io: tokio_io_pool::Runtime,
    ) -> impl Future<Item = Self, Error = failure::Error> {
        ControllerHandle::make(authority).map(move |c| Handle {
            c: Some(c),
            event_tx: Some(event_tx),
            kill: Some(kill),
            iopool: Some(io),
        })
    }

    /// A future that resolves when the controller can accept more messages.
    ///
    /// When this future resolves, you it is safe to call any methods on the wrapped
    /// `ControllerHandle` that require `poll_ready` to have returned `Async::Ready`.
    pub fn ready(self) -> impl Future<Item = Self, Error = failure::Error> {
        let mut rdy = Some(self);
        future::poll_fn(move || -> Poll<_, failure::Error> {
            try_ready!(rdy.as_mut().unwrap().poll_ready());
            Ok(Async::Ready(rdy.take().unwrap()))
        })
    }

    #[cfg(test)]
    pub(super) fn backend_ready<E>(self) -> impl Future<Item = Self, Error = E> {
        let snd = self.event_tx.clone().unwrap();
        future::loop_fn((self, snd), |(this, snd)| {
            let (tx, rx) = futures::sync::oneshot::channel();
            snd.unbounded_send(Event::IsReady(tx)).unwrap();
            rx.map_err(|_| unimplemented!("worker loop went away"))
                .and_then(|v| {
                    if v {
                        future::Either::A(future::ok(future::Loop::Break(this)))
                    } else {
                        use std::time;
                        future::Either::B(
                            tokio::timer::Delay::new(
                                time::Instant::now() + time::Duration::from_millis(50),
                            )
                            .map(move |_| future::Loop::Continue((this, snd)))
                            .map_err(|_| unimplemented!("no timer available")),
                        )
                    }
                })
        })
    }

    #[doc(hidden)]
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (ret_tx, ret_rx) = futures::sync::oneshot::channel();
        let (fin_tx, fin_rx) = futures::sync::oneshot::channel();
        let b = Box::new(move |m: &mut Migration| {
            if ret_tx.send(f(m)).is_err() {
                unreachable!("could not return migration result");
            }
        });

        self.event_tx
            .clone()
            .unwrap()
            .unbounded_send(Event::ManualMigration { f: b, done: fin_tx })
            .unwrap();

        match fin_rx.wait() {
            Ok(()) => ret_rx.wait().unwrap(),
            Err(e) => unreachable!("{:?}", e),
        }
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub fn set_security_config(
        &mut self,
        p: String,
    ) -> impl Future<Item = (), Error = failure::Error> {
        self.rpc("set_security_config", p, "failed to set security config")
    }

    /// Install a new set of policies on the controller.
    #[must_use]
    pub fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> impl Future<Item = (), Error = failure::Error> {
        let mut c = self.c.clone().unwrap();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        self.rpc::<_, ()>(
            "create_universe",
            &context,
            "failed to create security universe",
        )
        .and_then(move |_| {
            // Write to Context table
            let bname = match context.get("group") {
                None => format!("UserContext_{}", uid.to_string()),
                Some(g) => format!("GroupContext_{}_{}", g.to_string(), uid.to_string()),
            };

            let mut fields: Vec<_> = context.keys().collect();
            fields.sort();
            let record: Vec<DataType> = fields.iter().map(|&f| context[f].clone()).collect();

            c.table(&bname).and_then(|table| {
                table
                    .insert(record)
                    .map_err(|e| format_err!("failed to make table: {:?}", e))
                    .map(|_| ())
            })
        })
    }

    /// Inform the local instance that it should exit.
    pub fn shutdown(&mut self) {
        if let Some(io) = self.iopool.take() {
            drop(self.c.take());
            drop(self.event_tx.take());
            drop(self.kill.take());
            io.shutdown_on_idle();
        }
    }
}

impl<A: Authority> Drop for Handle<A> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A synchronous handle to a worker.
pub struct SyncHandle<A: Authority + 'static> {
    rt: Option<tokio::runtime::Runtime>,
    wh: Handle<A>,
    // this is an Option so we can drop it
    sh: Option<SyncControllerHandle<A, tokio::runtime::TaskExecutor>>,
}

impl<A: Authority> SyncHandle<A> {
    /// Construct a new synchronous handle on top of an existing runtime.
    ///
    /// Note that the given `Handle` must have been created through the given `Runtime`.
    pub fn from_existing(rt: tokio::runtime::Runtime, wh: Handle<A>) -> Self {
        let sch = wh.sync_handle(rt.executor());
        SyncHandle {
            rt: Some(rt),
            wh,
            sh: Some(sch),
        }
    }

    /// Construct a new synchronous handle on top of an existing external runtime.
    ///
    /// Note that the given `Handle` must have been created through the `Runtime` backing the
    /// given executor.
    pub fn from_executor(ex: tokio::runtime::TaskExecutor, wh: Handle<A>) -> Self {
        let sch = wh.sync_handle(ex);
        SyncHandle {
            rt: None,
            wh,
            sh: Some(sch),
        }
    }

    /// Stash away the given runtime inside this worker handle.
    pub fn wrap_rt(&mut self, rt: tokio::runtime::Runtime) {
        self.rt = Some(rt);
    }

    /// Run an operation on the underlying asynchronous worker handle.
    pub fn on_worker<F, FF>(&mut self, f: F) -> Result<FF::Item, FF::Error>
    where
        F: FnOnce(&mut Handle<A>) -> FF,
        FF: IntoFuture,
        FF::Future: Send + 'static,
        FF::Item: Send + 'static,
        FF::Error: Send + 'static,
    {
        let fut = f(&mut self.wh);
        self.sh.as_mut().unwrap().run(fut)
    }

    #[doc(hidden)]
    pub fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.on_worker(move |w| -> Result<_, ()> { Ok(w.migrate(f)) })
            .unwrap()
    }
}

impl<A: Authority> Deref for SyncHandle<A> {
    type Target = SyncControllerHandle<A, tokio::runtime::TaskExecutor>;
    fn deref(&self) -> &Self::Target {
        self.sh.as_ref().unwrap()
    }
}

impl<A: Authority> DerefMut for SyncHandle<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.sh.as_mut().unwrap()
    }
}

impl<A: Authority + 'static> Drop for SyncHandle<A> {
    fn drop(&mut self) {
        drop(self.sh.take());
        self.wh.shutdown();
        if let Some(rt) = self.rt.take() {
            rt.shutdown_on_idle().wait().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    #[cfg_attr(not(debug_assertions), allow_fail)]
    fn limit_mutator_creation() {
        use crate::Builder;
        let r_txt = "CREATE TABLE a (x int, y int, z int);\n
                     CREATE TABLE b (r int, s int);\n";

        let mut c = Builder::default().start_simple().unwrap();
        assert!(c.install_recipe(r_txt).is_ok());
        for _ in 0..2500 {
            let _ = c.table("a").unwrap();
        }
    }

    fn make_test_builder(log: bool) -> crate::Builder {
        let mut b = crate::Builder::default();
        b.set_sharding(None);
        if log {
            b.log_with(crate::logger_pls());
        }
        b
    }
    use futures::future::Future;

    type ClickAnaTestDBHandle = super::SyncHandle<noria::consensus::LocalAuthority>;

    fn make_test_instance(log: bool) -> ClickAnaTestDBHandle {
        make_test_builder(log).start_simple().unwrap()
    }

    const CLICK_ANA_TABLE_SQL : &str =
        "CREATE TABLE clicks (user_id int, pagetype int, ts int)";

    const CLICK_ANA_SQL : &str = include_str!("click_ana.sql");

    fn prepare_click_ana_db(recipe: &str) -> ClickAnaTestDBHandle {
        use futures::future::Future;
        let mut b = make_test_instance(false);
        b.install_recipe(
            CLICK_ANA_TABLE_SQL
        ).unwrap();

        b.extend_recipe(
            recipe
        ).unwrap();

        {
            //use chrono::{NaiveDateTime};
            use noria::{ TableOperation, DataType };
            fn row(grp: i32, cat: i32, ts: i32) -> Vec<DataType> {
                vec![grp.into(), cat.into(), ts.into()]
            }
            let mut data = vec![
                row(1,1,1),
                row(1,3,2),
                row(1,3,5),
                row(1,0,6),
                row(1,2,10),

                row(1,3,12),

                row(1,1,14),
                row(1,8,15),
                row(1,3,16),
                row(1,7,17),
                row(1,77,18),
                row(1,7,22),
                row(1,2,25),
            ];
            use rand::Rng;
            rand::thread_rng().shuffle(&mut data);
            let ops = data.into_iter().map(TableOperation::Insert);

            b.table("tab").unwrap().perform_all(ops).wait().unwrap();
        }
        b
    }

    #[test]
    fn simple_click_ana_test() {
        // TODO do this test with a timestamp instead
        let mut b = prepare_click_ana_db(
            "VIEW test: SELECT grp, click_ana(pagetype, ts)
                        FROM tab
                        WHERE user_id = ?
                        GROUP BY user_id;"
        );

        let res = b.view("test").unwrap().lookup(&[1.into()], true).wait().unwrap().1;

        println!("{:?}", res);

        assert!(res.len() == 1);
        assert!(res[0][1] == 4.0.into());
    }

    #[test]
    fn simple_pure_sql_click_ana_test() {
        let mut b = prepare_click_ana_db(CLICK_ANA_SQL
        );

        let res = b.view("click_ana_sql").unwrap().lookup(&[1.into()], true).wait().unwrap().1;

        println!("{:?}", res);

        assert!(res.len() == 1);
        assert!(res[0][1] == 4.0.into());
    }

    #[test]
    // TODO Test that this works with more complex subexpressions in the UDF invokation
    fn very_simple_ohua_integration() {
        let mut b = make_test_instance(false);
        b.install_recipe(
            "CREATE TABLE k (x int, PRIMARY KEY(x));
             CREATE TABLE a (x int, y int, z int, PRIMARY KEY(z));",
        )
        .unwrap();

        b.extend_recipe(
            "VIEW test: SELECT k.x, test_count(a.y)
                                    FROM a JOIN k ON (a.x = k.x)
                                    WHERE k.x = ?
                                    GROUP BY k.x;",
        )
        .unwrap();

        use futures::future::Future;
        use std::dbg;
        use std::io::Write;

        // let mut f_1 = dbg!(std::fs::File::create("noria-test-graph.gr").unwrap());
        // write!(f_1, "{}", b.graphviz().unwrap()).unwrap();

        b.table("k").unwrap().insert(vec![1.into()]).wait().unwrap();

        {
            let a = b.table("a").unwrap();

            let a1 = a
                .insert(vec![1.into(), 1.into(), 0.into()])
                .and_then(|a| a.insert(vec![1.into(), 5.into(), 1.into()]))
                .and_then(|a| a.insert(vec![1.into(), 8.into(), 2.into()]))
                .wait()
                .unwrap();
            println!("Columns: {:?}", a1.columns());
        }

        let v = b.view("test").unwrap();

        assert!(v.lookup(&[1.into()], true).wait().unwrap().1[0][1] == 14.into());

        b.table("a").unwrap().delete(vec![2.into()]).wait().unwrap();

        assert!(b.view("test").unwrap().lookup(&[1.into()], true).wait().unwrap().1[0][1] == 6.into());
    }

    #[test]
    fn product_udf() {
        let mut b = make_test_instance(false);

        b.install_recipe(
            //"CREATE TABLE k (k int, PRIMARY KEY(k));
            "CREATE TABLE a (y int, x int, sk int, PRIMARY KEY(sk));",
        )
        .unwrap();

        //b.table("k").unwrap().insert(vec![1.into()]).wait().unwrap();

        use futures::future::Future;

        b.table("a")
            .unwrap()
            .insert(vec![2.into(), 1.into(), 0.into()])
            .and_then(|a| a.insert(vec![3.into(), 1.into(), 1.into()]))
            .and_then(|a| a.insert(vec![4.into(), 1.into(), 2.into()]))
            .wait()
            .unwrap();

        // b.extend_recipe(
        //     "VIEW test: SELECT a.x, sum(a.y)
        //                 FROM k JOIN a ON (a.x = k.k)
        //                 WHERE k.k = ?
        //                 GROUP BY k.k;")
        //     .unwrap();
        b.extend_recipe(
            "VIEW test: SELECT a.x, prod(a.y)
                        FROM a
                        WHERE a.x = ?
                        GROUP By x;")
            .unwrap();

        let v = b.view("test").unwrap();

        // Here the code has already failed.

        let qr =
            v.lookup(&[1.into()], true).wait().unwrap().1;

        assert!(qr[0][1] == 24.0.into());

        b.table("a").unwrap().delete(vec![2.into()]).wait().unwrap();

        assert!(b.view("test").unwrap().lookup(&[1.into()],true).wait().unwrap().1[0][1] == 6.0.into());

    }
}
