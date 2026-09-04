use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;


/// One background service a Poller drives: the rows it owns, and what it does with
/// each one. The loop, the wake-ups and the error handling belong to the Poller, so an
/// implementor holds nothing but its own logic.
pub trait Service: Send + Sync + 'static {
    type Row: Send + Sync;

    fn name(&self) -> &'static str;

    /// Names one row in an error line, e.g. "task run attempt 5 of task run 3".
    fn row_context(&self, row: &Self::Row) -> String;

    fn select(&self) -> impl Future<Output = anyhow::Result<Vec<Self::Row>>> + Send;

    fn handle(&self, row: &Self::Row) -> impl Future<Output = anyhow::Result<()>> + Send;
}


/// Every poller shares this. It is the safety net rather than the driver — signals do the
/// waking — but it cannot be removed: `job submit` writes from another process and so
/// cannot publish, and this interval is the only thing that notices.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);


/// Drives one Service, waking on its signal or its interval, whichever comes first.
pub struct Poller<S: Service> {
    service: Arc<S>,
    wakeup: Arc<Notify>,
    interval: Duration,
}


impl<S: Service> Poller<S> {

    pub fn new(
        service: Arc<S>,
        wakeup: Arc<Notify>,
        interval: Duration,
    ) -> Self {
        Self {
            service,
            wakeup,
            interval,
        }
    }

    /// Spawns the polling loop and returns immediately, restarting it on error.
    pub fn start(self) {

        tokio::spawn(async move {
            loop {
                if let Err(e) = self.run().await {
                    eprintln!("{} error, restarting in 5s: {e:?}", self.service.name());
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every row the service selects, once per wake-up, until selecting fails.
    async fn run(&self) -> anyhow::Result<()> {

        // Burst, tokio's default MissedTickBehavior, is left as-is: it is what the loops
        // this replaced already did, so a handle pass longer than the interval is
        // followed immediately by another rather than by a delay to catch up.
        let mut timer = tokio::time::interval(self.interval);

        loop {

            // A Notified future dropped when the timer arm wins does not lose its permit
            // to tokio's internal bookkeeping: the permit is only consumed on a completed
            // await, so a signal published during the timer arm is still there next time.
            tokio::select! {
                _ = timer.tick() => {}
                _ = self.wakeup.notified() => {}
            }

            let rows = self.service.select().await?;

            // A row the service can never handle is logged and left for the next
            // wake-up: failing the whole loop over it would stop every other row from
            // being handled, since the restarted loop would select the same row again.
            for row in &rows {
                if let Err(e) = self.service.handle(row).await {
                    eprintln!(
                        "{} error on {}: {e:?}",
                        self.service.name(),
                        self.service.row_context(row),
                    );
                }
            }

        }

    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::Signals;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    struct CountingService {
        rows: Vec<i64>,
        select_fails: bool,
        fail_row: Option<i64>,
        selected: UnboundedSender<()>,
        handled: UnboundedSender<i64>,
    }

    impl Service for CountingService {
        type Row = i64;

        fn name(&self) -> &'static str {
            "Counting Service"
        }

        fn row_context(&self, row: &i64) -> String {
            format!("row {row}")
        }

        async fn select(&self) -> anyhow::Result<Vec<i64>> {

            self.selected.send(()).unwrap();

            if self.select_fails {
                anyhow::bail!("select failed");
            }

            Ok(self.rows.clone())
        }

        async fn handle(&self, row: &i64) -> anyhow::Result<()> {

            self.handled.send(*row).unwrap();

            if self.fail_row == Some(*row) {
                anyhow::bail!("handle failed");
            }

            Ok(())
        }
    }

    struct BlockingService {
        selected: UnboundedSender<()>,
        entered_handle: UnboundedSender<()>,
        release_handle: Arc<Notify>,
    }

    impl Service for BlockingService {
        type Row = i64;

        fn name(&self) -> &'static str {
            "Blocking Service"
        }

        fn row_context(&self, row: &i64) -> String {
            format!("row {row}")
        }

        async fn select(&self) -> anyhow::Result<Vec<i64>> {

            self.selected.send(()).unwrap();

            Ok(vec![1])
        }

        async fn handle(&self, _row: &i64) -> anyhow::Result<()> {

            self.entered_handle.send(()).unwrap();

            self.release_handle.notified().await;

            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_select_error_restarts_the_loop_after_five_seconds() {
        let (selected, mut selects) = unbounded_channel();
        let (handled, mut handles) = unbounded_channel();

        let service = Arc::new(CountingService {
            rows: vec![1],
            select_fails: true,
            fail_row: None,
            selected,
            handled,
        });

        Poller::new(service, Arc::new(Notify::new()), Duration::from_secs(1)).start();

        selects.recv().await.unwrap();
        let first_select = tokio::time::Instant::now();
        selects.recv().await.unwrap();

        assert_eq!(first_select.elapsed(), Duration::from_secs(5));
        assert!(handles.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_row_does_not_stop_the_next_row() {
        let (selected, mut selects) = unbounded_channel();
        let (handled, mut handles) = unbounded_channel();

        let service = Arc::new(CountingService {
            rows: vec![1, 2],
            select_fails: false,
            fail_row: Some(1),
            selected,
            handled,
        });

        Poller::new(service, Arc::new(Notify::new()), Duration::from_secs(60)).start();

        selects.recv().await.unwrap();

        assert_eq!(handles.recv().await.unwrap(), 1);
        assert_eq!(handles.recv().await.unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_up_selects_before_the_interval_elapses() {
        let (selected, mut selects) = unbounded_channel();
        let (handled, _handles) = unbounded_channel();

        let service = Arc::new(CountingService {
            rows: Vec::new(),
            select_fails: false,
            fail_row: None,
            selected,
            handled,
        });

        let wakeup = Arc::new(Notify::new());

        Poller::new(service, wakeup.clone(), Duration::from_secs(60)).start();

        selects.recv().await.unwrap();

        let woke_from = tokio::time::Instant::now();
        wakeup.notify_one();
        selects.recv().await.unwrap();

        assert_eq!(woke_from.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_up_during_a_handle_pass_is_not_lost() {
        let (selected, mut selects) = unbounded_channel();
        let (entered_handle, mut handle_entries) = unbounded_channel();

        let release_handle = Arc::new(Notify::new());

        let service = Arc::new(BlockingService {
            selected,
            entered_handle,
            release_handle: release_handle.clone(),
        });

        let wakeup = Arc::new(Notify::new());

        Poller::new(service, wakeup.clone(), Duration::from_secs(60)).start();

        selects.recv().await.unwrap();
        handle_entries.recv().await.unwrap();

        wakeup.notify_one();
        release_handle.notify_one();

        let woke_from = tokio::time::Instant::now();
        selects.recv().await.unwrap();

        assert_eq!(woke_from.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_publish_through_signals_wakes_a_registered_poller() {
        let (selected, mut selects) = unbounded_channel();
        let (handled, _handles) = unbounded_channel();

        let service = Arc::new(CountingService {
            rows: Vec::new(),
            select_fails: false,
            fail_row: None,
            selected,
            handled,
        });

        let signals = Signals::new();
        let wakeup = signals.register();

        Poller::new(service, wakeup, Duration::from_secs(60)).start();

        selects.recv().await.unwrap();

        let woke_from = tokio::time::Instant::now();
        signals.publish();
        selects.recv().await.unwrap();

        assert_eq!(woke_from.elapsed(), Duration::ZERO);
    }
}
