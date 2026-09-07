//! Allocation and first-touch geometry, not a throughput benchmark.
//! Requested live bytes include payloads, descriptors and endpoint/shared state.
//! RSS is process-wide residency (including allocator/runtime/code), not exact
//! physical bytes attributable to one allocation. Run each shape in a fresh process.
use prescient::mpmc::brokerless::leased;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

struct Counted;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
// SAFETY: every operation delegates to System with the unchanged pointer/layout;
// atomics only record successful allocations and corresponding deallocations.
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Relaxed);
            CALLS.fetch_add(1, Relaxed);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Relaxed);
            CALLS.fetch_add(1, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(p, layout, size) };
        if !new.is_null() {
            LIVE.fetch_add(size, Relaxed);
            LIVE.fetch_sub(layout.size(), Relaxed);
            CALLS.fetch_add(1, Relaxed);
        }
        new
    }
}
#[global_allocator]
static ALLOCATOR: Counted = Counted;

#[cfg(target_os = "linux")]
fn resident_bytes() -> Result<usize, String> {
    let stat = std::fs::read_to_string("/proc/self/statm").map_err(|e| e.to_string())?;
    let pages: usize = stat
        .split_whitespace()
        .nth(1)
        .ok_or("missing RSS")?
        .parse()
        .map_err(|_| "invalid RSS")?;
    // SAFETY: sysconf has no pointer arguments.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err("cannot query page size".into());
    }
    pages
        .checked_mul(page_size as usize)
        .ok_or("RSS overflow".into())
}
#[cfg(not(target_os = "linux"))]
fn resident_bytes() -> Result<usize, String> {
    Err("RSS probe requires Linux".into())
}

fn run<const N: usize>(p: usize, c: usize, cap: usize, batch: usize) -> Result<(), String> {
    let payload_bytes = p
        .checked_mul(cap)
        .and_then(|v| v.checked_mul(N))
        .ok_or("payload overflow")?;
    let blocks = cap / batch;
    let descriptor_slots = blocks
        .checked_next_power_of_two()
        .ok_or("descriptor overflow")?;
    let before_rss = resident_bytes()?;
    let before_live = LIVE.load(Relaxed);
    let before_calls = CALLS.load(Relaxed);
    let (mut tx, mut rx) = leased::channel::<[u8; N]>()
        .producers(p)
        .consumers(c)
        .capacity(cap)
        .batch(batch)
        .open()
        .map_err(|e| format!("{e:?}"))?;
    let setup_live = LIVE.load(Relaxed) - before_live;
    let setup_calls = CALLS.load(Relaxed) - before_calls;
    let setup_rss = resident_bytes()?;
    // Fill the ENTIRE finite pool before recycling anything. Merely open()ing
    // MaybeUninit allocations does not fault in all payload pages.
    for producer in &mut tx {
        for _ in 0..blocks {
            let mut write = producer
                .try_reserve()
                .ok_or("pool exhausted before declared capacity")?;
            for _ in 0..batch {
                write.push([0xa5; N]).map_err(|_| "short block")?;
            }
            write
                .commit()
                .map_err(|_| "forward ring cannot hold the pool")?;
        }
        if producer.try_reserve().is_some() {
            return Err("pool exceeded its declared capacity".into());
        }
    }
    let touched_rss = resident_bytes()?;
    let mut received = 0;
    while received < p * cap {
        let before = received;
        for consumer in &mut rx {
            if let Some(read) = consumer.try_recv() {
                for value in read.as_slice() {
                    if std::hint::black_box(value) != &[0xa5; N] {
                        return Err("payload corrupt".into());
                    }
                    received += 1;
                }
            }
        }
        if before == received {
            return Err("filled pool made no receive progress".into());
        }
    }
    for producer in &mut tx {
        if !producer.flush() {
            return Err("pool did not return".into());
        }
    }
    let recycled_live = LIVE.load(Relaxed) - before_live;
    let recycled_rss = resident_bytes()?;
    drop(tx);
    drop(rx);
    let after_live = LIVE.load(Relaxed);
    if after_live != before_live {
        return Err(format!(
            "live allocation imbalance: {before_live}->{after_live}"
        ));
    }
    println!(
        "geometry_bytes={N};producers={p};consumers={c};capacity={cap};batch={batch};pool_blocks_per_producer={blocks};descriptor_slots_per_ring={descriptor_slots};payload_bytes={payload_bytes};requested_setup_bytes={setup_live};requested_nonpayload_bytes={};setup_allocations={setup_calls};requested_recycled_bytes={recycled_live};rss_before_bytes={before_rss};rss_setup_bytes={setup_rss};rss_touched_bytes={touched_rss};rss_recycled_bytes={recycled_rss};validated_messages={received};live_bytes_after_drop={after_live};live_bytes_before={before_live}",
        setup_live - payload_bytes
    );
    Ok(())
}
const USAGE: &str = "lease_pool_geometry BYTES PRODUCERS CONSUMERS CAPACITY BATCH";

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() == 1 && matches!(args[0].as_str(), "--help" | "-h") {
        println!("{USAGE}");
        return;
    }
    let result = (|| -> Result<(), String> {
        if args.len() != 5 {
            return Err(USAGE.into());
        }
        let dims: Vec<usize> = args
            .iter()
            .map(|s| s.parse().map_err(|_| "invalid dimension".to_string()))
            .collect::<Result<_, _>>()?;
        let (bytes, p, c, cap, batch) = (dims[0], dims[1], dims[2], dims[3], dims[4]);
        if p == 0 || c == 0 || batch == 0 || cap < batch || !cap.is_multiple_of(batch) {
            return Err("invalid pool geometry".into());
        }
        match bytes {
            8 => run::<8>(p, c, cap, batch),
            33 => run::<33>(p, c, cap, batch),
            64 => run::<64>(p, c, cap, batch),
            256 => run::<256>(p, c, cap, batch),
            257 => run::<257>(p, c, cap, batch),
            4096 => run::<4096>(p, c, cap, batch),
            _ => Err("unsupported payload size".into()),
        }
    })();
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(2);
    }
}
