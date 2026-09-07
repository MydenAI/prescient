//! Repeatedly exercise the fixed tier's full produce/drain/disconnect cycle to
//! flush out any premature-disconnect or lost-message race.
use prescient::mpsc::fixed;

#[test]
fn fixed_no_loss_under_repetition() {
    const P: usize = 3;
    const M: u64 = 20_000;
    for iter in 0..200 {
        let (prods, mut rx) = fixed::channel::<u64>()
            .producers(P)
            .capacity(1024)
            .open()
            .unwrap();
        let n = std::thread::scope(|s| {
            for mut p in prods {
                s.spawn(move || {
                    for i in 0..M {
                        p.send(i).unwrap();
                    }
                });
            }
            let mut n = 0u64;
            while rx.recv().is_some() {
                n += 1;
            }
            n
        });
        assert_eq!(
            n,
            P as u64 * M,
            "iteration {iter}: lost or dropped messages"
        );
    }
}

#[test]
fn pool_no_loss_under_repetition() {
    use prescient::mpsc::pool;
    const P: usize = 3;
    const M: u64 = 20_000;
    for iter in 0..150 {
        let (h, mut rx) = pool::channel::<u64>()
            .max_producers(8)
            .capacity(1024)
            .open()
            .unwrap();
        let claimed: Vec<_> = (0..P).map(|_| h.claim().unwrap()).collect();
        drop(h);
        let n = std::thread::scope(|s| {
            for mut p in claimed {
                s.spawn(move || {
                    for i in 0..M {
                        p.send(i).unwrap();
                    }
                });
            }
            let mut n = 0u64;
            while rx.recv().is_some() {
                n += 1;
            }
            n
        });
        assert_eq!(n, P as u64 * M, "pool iter {iter}");
    }
}

#[test]
fn dynamic_no_loss_fast_register_drop() {
    use prescient::mpsc::dynamic;
    const P: usize = 4;
    const M: u64 = 10_000;
    for iter in 0..150 {
        let (reg, mut rx) = dynamic::channel::<u64>().capacity(1024).open().unwrap();
        let n = std::thread::scope(|s| {
            for _ in 0..P {
                let reg = reg.clone();
                s.spawn(move || {
                    // register, blast, drop as fast as possible
                    let mut p = reg.register();
                    for i in 0..M {
                        p.send(i).unwrap();
                    }
                });
            }
            drop(reg);
            let mut n = 0u64;
            while rx.recv().is_some() {
                n += 1;
            }
            n
        });
        assert_eq!(n, P as u64 * M, "dynamic iter {iter}");
    }
}
