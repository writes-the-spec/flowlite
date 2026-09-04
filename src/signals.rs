use std::sync::{Arc, Mutex};
use tokio::sync::Notify;


/// Advisory wake-ups for the pollers. A publish means "a run status changed, look
/// again" and carries no data: losing one costs latency, never correctness, since
/// every poller re-derives its work from the database when it wakes and its interval
/// wakes it regardless.
pub struct Signals {
    wakeups: Mutex<Vec<Arc<Notify>>>,
}


impl Signals {

    pub fn new() -> Self {
        Self {
            wakeups: Mutex::new(Vec::new()),
        }
    }

    /// Hands out the wake-up one poller waits on, called once per poller at startup.
    pub fn register(&self) -> Arc<Notify> {

        let wakeup = Arc::new(Notify::new());

        self.wakeups.lock().unwrap().push(wakeup.clone());

        wakeup
    }

    /// notify_one stores a permit when nobody is waiting, so a status written while a
    /// poller is busy handling rows wakes it as soon as it looks again.
    pub fn publish(&self) {
        for wakeup in self.wakeups.lock().unwrap().iter() {
            wakeup.notify_one();
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn publish_wakes_every_registered_poller() {
        let signals = Signals::new();
        let first = signals.register();
        let second = signals.register();

        signals.publish();

        let first_woke = tokio::time::timeout(Duration::from_secs(60), first.notified()).await;
        let second_woke = tokio::time::timeout(Duration::from_secs(60), second.notified()).await;

        assert!(first_woke.is_ok());
        assert!(second_woke.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn a_publish_with_nobody_waiting_is_not_lost() {
        let signals = Signals::new();
        let wakeup = signals.register();

        signals.publish();

        let waited_from = tokio::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(60), wakeup.notified()).await.unwrap();

        assert_eq!(waited_from.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_publish_nothing_wakes() {
        let signals = Signals::new();
        let wakeup = signals.register();

        let woke = tokio::time::timeout(Duration::from_secs(60), wakeup.notified()).await;

        assert!(woke.is_err());
    }
}
