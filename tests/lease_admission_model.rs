//! Sequential ownership/admission model, not a weak-memory or CPU performance model.
//! Block IDs represent allocations, not message IDs. A single producer and one
//! held lease per consumer mirror exclusive endpoint borrows. Each transition
//! is indivisible; no claim is made about implementation-level atomic ordering.
//! Payload initialization, destructor panic, teardown and multiple producers are
//! outside this abstraction; public-API tests separately exercise real ownership.
//!
//! Two policy hypotheses are checked against returned-first acquisition:
//! a preferred outstanding window (strict, or with a ready-storage escape), and
//! no-wait consolidation of surplus returns. Small-state exploration checks
//! ownership and ready progress; scripted schedules demonstrate locality limits.
use std::collections::{BTreeSet, HashSet, VecDeque};

#[derive(Clone, Copy, Debug)]
enum Policy {
    ReturnedFirst,
    Preferred { limit: usize, escape: bool },
    Consolidate,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Pool {
    idle: Vec<usize>,
    filling: Option<usize>,
    queued: VecDeque<usize>,
    held: Vec<Option<usize>>,
    returned: Vec<VecDeque<usize>>,
    cursor: usize,
}

impl Pool {
    fn new(blocks: usize, consumers: usize) -> Self {
        assert!(blocks > 0 && consumers > 0);
        Self {
            idle: (0..blocks).collect(),
            filling: None,
            queued: VecDeque::new(),
            held: vec![None; consumers],
            returned: vec![VecDeque::new(); consumers],
            cursor: 0,
        }
    }

    fn assert_conserved(&self, blocks: usize) {
        let mut seen = vec![0; blocks];
        for &block in self
            .idle
            .iter()
            .chain(self.filling.iter())
            .chain(self.queued.iter())
            .chain(self.held.iter().flatten())
            .chain(self.returned.iter().flatten())
        {
            assert!(block < blocks);
            seen[block] += 1;
        }
        assert!(
            seen.iter().all(|&n| n == 1),
            "lost/duplicate owner: {self:?}"
        );
        assert!(self.cursor <= self.returned.len());
    }

    fn ready(&self) -> usize {
        self.idle.len() + self.returned.iter().map(VecDeque::len).sum::<usize>()
    }

    fn outstanding(&self) -> usize {
        self.queued.len() + self.held.iter().flatten().count()
    }

    fn take_return(&mut self) -> Option<usize> {
        for _ in 0..self.returned.len() {
            let lane = if self.cursor < self.returned.len() {
                self.cursor
            } else {
                0
            };
            self.cursor = lane + 1;
            if let Some(block) = self.returned[lane].pop_front() {
                return Some(block);
            }
        }
        None
    }

    fn reclaim(&mut self) -> usize {
        let mut count = 0;
        while let Some(block) = self.take_return() {
            self.idle.push(block);
            count += 1;
        }
        count
    }

    fn reserve(&mut self, policy: Policy) -> Option<usize> {
        assert!(self.filling.is_none(), "exclusive producer borrow");
        if let Policy::Preferred { limit, escape } = policy {
            // Optimistic oracle: excludes already-returned blocks. Production
            // in_flight includes them until collection; extra observation cost
            // would be required to implement this exact predicate.
            if self.outstanding() >= limit && (!escape || self.ready() == 0) {
                return None;
            }
        }
        let first = self.take_return();
        // IDs cannot unwind. A real payload kernel must put the selected block
        // under a rollback guard before reclaiming other values can call Drop.
        if first.is_some() && matches!(policy, Policy::Consolidate) {
            self.reclaim();
        }
        let block = first.or_else(|| self.idle.pop())?;
        self.filling = Some(block);
        Some(block)
    }

    fn commit(&mut self) {
        self.queued.push_back(self.filling.take().unwrap());
    }

    fn rollback(&mut self) {
        self.idle.push(self.filling.take().unwrap());
    }

    fn receive(&mut self, consumer: usize) -> bool {
        assert!(self.held[consumer].is_none(), "exclusive consumer borrow");
        self.held[consumer] = self.queued.pop_front();
        self.held[consumer].is_some()
    }

    fn release(&mut self, consumer: usize) {
        self.returned[consumer].push_back(self.held[consumer].take().unwrap());
    }

    fn publish(&mut self, policy: Policy) -> usize {
        let block = self
            .reserve(policy)
            .expect("ready storage must make progress");
        self.commit();
        block
    }

    fn complete_one(&mut self, consumer: usize) {
        assert!(self.receive(consumer));
        self.release(consumer);
    }
}

#[derive(Default, Debug)]
struct Coverage {
    states: usize,
    edges: usize,
    false_exhaustion: usize,
}

fn explore(blocks: usize, consumers: usize, policy: Policy) -> Coverage {
    let initial = Pool::new(blocks, consumers);
    let mut pending = VecDeque::from([initial.clone()]);
    let mut seen = HashSet::from([initial]);
    let mut coverage = Coverage::default();
    while let Some(state) = pending.pop_front() {
        state.assert_conserved(blocks);
        let mut successors = Vec::new();
        if state.filling.is_none() {
            let mut reserved = state.clone();
            let available = state.ready() != 0;
            let success = reserved.reserve(policy).is_some();
            if available && !success {
                coverage.false_exhaustion += 1;
            }
            assert!(!success || available);
            successors.push(reserved);
            let mut reclaimed = state.clone();
            reclaimed.reclaim();
            successors.push(reclaimed);
        } else {
            let mut committed = state.clone();
            committed.commit();
            successors.push(committed);
            let mut rolled_back = state.clone();
            rolled_back.rollback();
            successors.push(rolled_back);
        }
        for consumer in 0..consumers {
            let mut next = state.clone();
            if next.held[consumer].is_some() {
                next.release(consumer);
            } else if !next.receive(consumer) {
                continue;
            }
            successors.push(next);
        }
        for next in successors {
            next.assert_conserved(blocks);
            coverage.edges += 1;
            if seen.insert(next.clone()) {
                pending.push_back(next);
            }
        }
        // Catch accidental unbounded state in this deliberately finite model.
        assert!(seen.len() <= 1_000_000);
    }
    coverage.states = seen.len();
    coverage
}

#[test]
fn enumerate_small_ownership_and_ready_progress_states() {
    for blocks in 1..=4 {
        for consumers in 1..=3 {
            let mut policies = vec![Policy::ReturnedFirst, Policy::Consolidate];
            for limit in 1..=blocks {
                policies.push(Policy::Preferred {
                    limit,
                    escape: false,
                });
                policies.push(Policy::Preferred {
                    limit,
                    escape: true,
                });
            }
            for policy in policies {
                let coverage = explore(blocks, consumers, policy);
                match policy {
                    Policy::Preferred {
                        limit,
                        escape: false,
                    } if limit < blocks => {
                        assert!(coverage.false_exhaustion > 0);
                    }
                    _ => assert_eq!(coverage.false_exhaustion, 0),
                }
                eprintln!(
                    "admission_model;blocks={blocks};consumers={consumers};policy={policy:?};states={};edges={};false_exhaustion={}",
                    coverage.states, coverage.edges, coverage.false_exhaustion
                );
            }
        }
    }
}

#[test]
fn preferred_window_either_hides_capacity_or_allows_reexpansion() {
    for blocks in [2, 17, 64] {
        for limit in 1..blocks {
            let mut strict = Pool::new(blocks, 1);
            for _ in 0..limit {
                strict.publish(Policy::Preferred {
                    limit,
                    escape: false,
                });
            }
            // A consumer has not run; this is a legitimate configured burst.
            assert_eq!(strict.ready(), blocks - limit);
            assert!(
                strict
                    .reserve(Policy::Preferred {
                        limit,
                        escape: false
                    })
                    .is_none()
            );

            let mut escaped = Pool::new(blocks, 1);
            for _ in 0..blocks {
                escaped.publish(Policy::Preferred {
                    limit,
                    escape: true,
                });
            }
            assert_eq!(escaped.outstanding(), blocks);
            assert_eq!(escaped.ready(), 0);
            escaped.assert_conserved(blocks);
        }
    }
}

#[test]
fn preferred_window_cannot_wait_on_held_peers_when_storage_is_ready() {
    let mut pool = Pool::new(3, 2);
    pool.publish(Policy::ReturnedFirst);
    pool.publish(Policy::ReturnedFirst);
    assert!(pool.receive(0));
    assert!(pool.receive(1));
    // Both consumers may hold their leases indefinitely; one idle block remains.
    assert_eq!(pool.ready(), 1);
    assert!(
        pool.reserve(Policy::Preferred {
            limit: 2,
            escape: false
        })
        .is_none()
    );
    assert!(
        pool.reserve(Policy::Preferred {
            limit: 2,
            escape: true
        })
        .is_some()
    );
    pool.rollback();
    // Returning one held block must also remain independently usable.
    pool.release(1);
    assert_eq!(pool.ready(), 2);
    assert!(
        pool.reserve(Policy::Preferred {
            limit: 1,
            escape: false
        })
        .is_none()
    );
    assert!(
        pool.reserve(Policy::Preferred {
            limit: 1,
            escape: true
        })
        .is_some()
    );
    pool.assert_conserved(3);
}

#[test]
fn one_for_one_recycling_cannot_create_idle_surplus() {
    for blocks in [2, 17, 64] {
        for policy in [
            Policy::ReturnedFirst,
            Policy::Preferred {
                limit: 1,
                escape: true,
            },
            Policy::Consolidate,
        ] {
            let mut pool = Pool::new(blocks, 3);
            for _ in 0..blocks {
                pool.publish(policy);
            }
            let mut visited = BTreeSet::new();
            for step in 0..blocks * 4 {
                pool.complete_one(step % 3);
                assert_eq!(pool.ready(), 1);
                visited.insert(pool.publish(policy));
                assert!(pool.idle.is_empty());
                assert_eq!(pool.outstanding(), blocks);
                pool.assert_conserved(blocks);
            }
            assert_eq!(visited.len(), blocks);
            eprintln!(
                "admission_schedule;name=one_for_one;blocks={blocks};policy={policy:?};visited={};idle={}",
                visited.len(),
                pool.idle.len()
            );
        }
    }
}

#[test]
fn completion_surplus_contracts_but_the_next_burst_can_expand_again() {
    for blocks in [2, 17, 64] {
        for policy in [Policy::ReturnedFirst, Policy::Consolidate] {
            let mut pool = Pool::new(blocks, 1);
            for _ in 0..blocks {
                pool.publish(policy);
            }
            for _ in 0..blocks {
                pool.complete_one(0);
            }
            // Consumers got ahead. Consolidation can now park unused storage.
            let mut visited = BTreeSet::new();
            for _ in 0..blocks * 4 {
                visited.insert(pool.publish(policy));
                pool.complete_one(0);
            }
            match policy {
                Policy::Consolidate => assert_eq!(visited.len(), 1),
                _ => assert_eq!(visited.len(), blocks),
            }
            let parked = pool.idle.len();
            eprintln!(
                "admission_schedule;name=completion_surplus;blocks={blocks};policy={policy:?};visited={};idle={parked}",
                visited.len()
            );
            let mut burst = BTreeSet::new();
            for _ in 0..blocks {
                burst.insert(pool.publish(policy));
            }
            assert_eq!(burst.len(), blocks);
            assert!(pool.reserve(policy).is_none());
            assert_eq!(pool.outstanding(), blocks);
            pool.assert_conserved(blocks);
        }
    }
}

#[test]
fn servicing_a_return_does_not_require_rotating_its_allocation() {
    let mut pool = Pool::new(4, 2);
    for _ in 0..4 {
        pool.publish(Policy::ReturnedFirst);
    }
    pool.complete_one(0);
    pool.complete_one(1);
    let cold = *pool.returned[1].front().unwrap();
    pool.complete_one(0);
    pool.complete_one(0);
    let hot = pool.publish(Policy::Consolidate);
    assert_ne!(hot, cold);
    assert!(pool.returned.iter().all(VecDeque::is_empty));
    assert!(
        pool.idle.contains(&cold),
        "cold return serviced, storage parked"
    );
    for _ in 0..16 {
        pool.complete_one(0);
        assert_eq!(pool.publish(Policy::Consolidate), hot);
    }
    assert!(pool.idle.contains(&cold));
    pool.assert_conserved(4);
}

#[test]
fn net_completions_park_surplus_without_a_recovery_pause() {
    for blocks in [2, 17, 64] {
        for consumers in [1, 3] {
            let mut pool = Pool::new(blocks, consumers);
            for _ in 0..blocks {
                pool.publish(Policy::Consolidate);
            }
            // Two completions followed by one publication: consumers create
            // one unit of surplus; the policy collects it without waiting.
            for step in 0..blocks - 1 {
                pool.complete_one(step % consumers);
                pool.complete_one((step + 1) % consumers);
                pool.publish(Policy::Consolidate);
                assert_eq!(pool.idle.len(), step + 1);
                assert_eq!(pool.outstanding(), blocks - step - 1);
                assert!(pool.returned.iter().all(VecDeque::is_empty));
                pool.assert_conserved(blocks);
            }
            assert_eq!(pool.idle.len(), blocks - 1);
            let mut visited = BTreeSet::new();
            for step in 0..blocks * 2 {
                pool.complete_one(step % consumers);
                visited.insert(pool.publish(Policy::Consolidate));
            }
            assert_eq!(visited.len(), 1);
            eprintln!(
                "admission_schedule;name=net_completions;blocks={blocks};consumers={consumers};visited={};idle={}",
                visited.len(),
                pool.idle.len()
            );
        }
    }
}
