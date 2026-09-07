#![cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]

use std::time::Duration;

use prescient::ipc;

fn endpoint() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "r-{:x}-{:x}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn open_pair() -> (ipc::Duplex, ipc::Duplex) {
    let endpoint = endpoint();
    let peer_endpoint = endpoint.clone();
    let peer = std::thread::spawn(move || {
        ipc::attach()
            .endpoint(peer_endpoint)
            .timeout(Duration::from_secs(2))
            .open()
            .unwrap()
    });
    let owner = ipc::channel()
        .endpoint(endpoint)
        .shape(64, 16, 2)
        .transfer_id(900)
        .timeout(Duration::from_secs(2))
        .open()
        .unwrap();
    (owner, peer.join().unwrap())
}

#[test]
fn duplex_exposes_distinct_direction_roles() {
    fn sender(_: &ipc::Sender) {}
    fn receiver(_: &ipc::Receiver) {}

    let (owner, peer) = open_pair();
    let (owner_sender, owner_receiver) = owner.split();
    let (peer_sender, peer_receiver) = peer.split();
    sender(&owner_sender);
    receiver(&owner_receiver);
    sender(&peer_sender);
    receiver(&peer_receiver);
}

#[test]
fn missing_peer_times_out_setup() {
    let error = ipc::channel()
        .endpoint(endpoint())
        .shape(64, 16, 2)
        .timeout(Duration::from_millis(100))
        .open()
        .err()
        .expect("setup without a peer unexpectedly succeeded");
    assert!(matches!(error, prescient::OpenError::Io(_)));
}
