//! Abortable setup gate: placement and worker creation finish before timing.
use std::sync::{Condvar, Mutex, mpsc::Sender};

#[derive(Default)]
pub struct StartGate {
    state: Mutex<Option<bool>>,
    changed: Condvar,
}

impl StartGate {
    pub fn abort_on_drop(&self) -> AbortOnDrop<'_> {
        AbortOnDrop(self)
    }

    pub fn enter(
        &self,
        ready: Sender<Result<(), String>>,
        preparation: Result<(), String>,
    ) -> bool {
        let prepared = preparation.is_ok();
        let sent = ready.send(preparation);
        drop(ready); // A parked worker must not hide a missing startup result.
        if sent.is_err() || !prepared {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.is_none() {
            state = self.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        state.unwrap()
    }

    pub fn open(&self) {
        self.finish(true);
    }

    fn finish(&self, run: bool) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.is_none() {
            *state = Some(run);
            self.changed.notify_all();
        }
    }
}

pub struct AbortOnDrop<'a>(&'a StartGate);

impl Drop for AbortOnDrop<'_> {
    fn drop(&mut self) {
        self.0.finish(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn workers_start_only_after_release() {
        let gate = StartGate::default();
        std::thread::scope(|scope| {
            let _abort = gate.abort_on_drop();
            let (tx, rx) = mpsc::channel();
            let (ran_tx, ran_rx) = mpsc::channel();
            let worker_gate = &gate;
            scope.spawn(move || ran_tx.send(worker_gate.enter(tx, Ok(()))).unwrap());
            rx.recv().unwrap().unwrap();
            assert!(ran_rx.try_recv().is_err());
            gate.open();
            assert!(ran_rx.recv().unwrap());
        });
    }

    #[test]
    fn failed_preparation_and_partial_spawn_abort_waiters() {
        // One established worker must be released even if the next worker cannot
        // be created or cannot set affinity. The guard drops inside the scope,
        // before scoped threads are joined.
        for fail_preparation in [false, true] {
            let gate = StartGate::default();
            std::thread::scope(|scope| {
                let abort = gate.abort_on_drop();
                let (tx, rx) = mpsc::channel();
                let good_tx = tx.clone();
                let good = scope.spawn(|| gate.enter(good_tx, Ok(())));
                rx.recv().unwrap().unwrap();
                if fail_preparation {
                    let failed =
                        scope.spawn(|| gate.enter(tx, Err("injected affinity failure".into())));
                    assert!(rx.recv().unwrap().is_err());
                    assert!(!failed.join().unwrap());
                }
                drop(abort);
                assert!(!good.join().unwrap());
            });
        }
    }

    #[test]
    fn prepared_workers_do_not_hide_a_missing_startup_result() {
        let gate = StartGate::default();
        std::thread::scope(|scope| {
            let abort = gate.abort_on_drop();
            let (tx, rx) = mpsc::channel();
            let worker = scope.spawn(|| gate.enter(tx, Ok(())));
            rx.recv().unwrap().unwrap();
            assert_eq!(
                rx.recv_timeout(std::time::Duration::from_secs(1)),
                Err(mpsc::RecvTimeoutError::Disconnected),
            );
            drop(abort);
            assert!(!worker.join().unwrap());
        });
    }

    #[test]
    fn dropped_coordinator_is_not_a_hang() {
        let gate = StartGate::default();
        let (tx, rx) = mpsc::channel();
        drop(rx);
        assert!(!gate.enter(tx, Ok(())));
    }
}
