//! Spawned-process IPC throughput and full-payload validation.
//! Usage: ipc_bench bytes|pod TOTAL_BYTES CHUNK_BYTES SLOTS ROUNDS SAMPLES CPUS|-
//! CPUS is parent,child. Counters include both processes and warmup; delivered is
//! bytes, not channel messages. Timings include receive validation and its pipe ACK.
//! Setup and liveness use the channel's private control plane; payload bytes do not.
use prescient::ipc::{self, TransferReport};
use std::io::{self, BufRead, Read, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[path = "support/affinity.rs"]
mod affinity;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const USAGE: &str = "ipc_bench bytes|pod TOTAL_BYTES CHUNK_BYTES SLOTS ROUNDS SAMPLES CPUS|-";

struct Config {
    mode: String,
    bytes: usize,
    chunk: usize,
    slots: usize,
    rounds: usize,
    samples: usize,
    placement: affinity::Placement,
}
impl Config {
    fn parse(args: &[String]) -> Result<Self> {
        if args.len() != 7 && !(args.len() == 9 && args[7] == "--child") {
            return Err(USAGE.into());
        }
        let n = |i: usize| -> Result<usize> { Ok(args[i].parse()?) };
        let c = Self {
            mode: args[0].clone(),
            bytes: n(1)?,
            chunk: n(2)?,
            slots: n(3)?,
            rounds: n(4)?,
            samples: n(5)?,
            placement: affinity::Placement::new((args[6] != "-").then_some(args[6].as_str()), 2)?,
        };
        if !matches!(c.mode.as_str(), "bytes" | "pod")
            || c.bytes == 0
            || c.chunk == 0
            || c.slots == 0
            || c.rounds == 0
            || c.samples == 0
            || (c.mode == "pod" && !c.bytes.is_multiple_of(8))
        {
            return Err(USAGE.into());
        }
        c.delivered()?;
        Ok(c)
    }
    fn transfers(&self) -> Result<usize> {
        Ok(self
            .samples
            .checked_add(1)
            .and_then(|n| n.checked_mul(self.rounds))
            .ok_or("transfer count overflow")?)
    }
    fn delivered(&self) -> Result<usize> {
        Ok(self
            .transfers()?
            .checked_mul(self.bytes)
            .ok_or("byte count overflow")?)
    }
}

trait Pipe: Sized {
    type Value: Copy + PartialEq;
    fn value(index: usize) -> Self::Value;
    fn create(c: &Config, endpoint: &str) -> Result<Self>;
    fn attach(endpoint: &str) -> Result<Self>;
    fn send(&mut self, values: &[Self::Value]) -> Result<TransferReport>;
    fn receive(&mut self) -> Result<(Vec<Self::Value>, TransferReport)>;
}
struct Bytes(ipc::Duplex);
impl Pipe for Bytes {
    type Value = u8;
    fn value(i: usize) -> u8 {
        (i.wrapping_mul(17) ^ (i >> 8)) as u8
    }
    fn create(c: &Config, endpoint: &str) -> Result<Self> {
        Ok(Self(
            ipc::channel()
                .endpoint(endpoint)
                .shape(c.bytes as u64, c.chunk, c.slots)
                .timeout(Duration::from_secs(10))
                .open()?,
        ))
    }
    fn attach(endpoint: &str) -> Result<Self> {
        Ok(Self(
            ipc::attach()
                .endpoint(endpoint)
                .timeout(Duration::from_secs(10))
                .open()?,
        ))
    }
    fn send(&mut self, mut values: &[u8]) -> Result<TransferReport> {
        Ok(self.0.send(&mut values)?)
    }
    fn receive(&mut self) -> Result<(Vec<u8>, TransferReport)> {
        let mut values = vec![0; usize::try_from(self.0.shape().total_bytes)?];
        let report = self.0.receive(&mut values.as_mut_slice())?;
        Ok((values, report))
    }
}
struct Pod(ipc::PodDuplex<u64>);
impl Pipe for Pod {
    type Value = u64;
    fn value(i: usize) -> u64 {
        (i as u64).wrapping_mul(0x9e3779b97f4a7c15) ^ 0xdeadbeef
    }
    fn create(c: &Config, endpoint: &str) -> Result<Self> {
        Ok(Self(
            ipc::pod::channel::<u64>()
                .endpoint(endpoint)
                .shape(c.bytes as u64, c.chunk, c.slots)
                .timeout(Duration::from_secs(10))
                .open()?,
        ))
    }
    fn attach(endpoint: &str) -> Result<Self> {
        Ok(Self(
            ipc::pod::attach::<u64>()
                .endpoint(endpoint)
                .timeout(Duration::from_secs(10))
                .open()?,
        ))
    }
    fn send(&mut self, values: &[u64]) -> Result<TransferReport> {
        Ok(self.0.send(values)?)
    }
    fn receive(&mut self) -> Result<(Vec<u64>, TransferReport)> {
        Ok(self.0.receive()?)
    }
}

fn line(input: &mut impl BufRead) -> Result<String> {
    let mut s = String::new();
    if input.read_line(&mut s)? == 0 {
        return Err("child output pipe closed".into());
    }
    Ok(s.trim_end().to_owned())
}

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        // Recover a failed benchmark without stranding a spawned worker.
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}
fn child<P: Pipe>(c: &Config, endpoint: &str) -> Result<()> {
    let mut pipe = P::attach(endpoint)?;
    c.placement.pin(1)?;
    let mut out = io::stdout().lock();
    writeln!(out, "READY")?;
    out.flush()?;
    for _ in 0..c.transfers()? {
        let (values, report) = pipe.receive()?;
        if report.total_bytes != c.bytes as u64
            || values.len() != c.bytes / size_of::<P::Value>()
            || !values.iter().enumerate().all(|(i, &v)| v == P::value(i))
        {
            return Err("received payload mismatch".into());
        }
        writeln!(out, "{}", report.checksum)?;
        out.flush()?;
    }
    Ok(())
}
fn parent<P: Pipe>(c: &Config, args: &[String]) -> Result<()> {
    let values: Vec<_> = (0..c.bytes / size_of::<P::Value>()).map(P::value).collect();
    let endpoint = format!("b-{:x}", std::process::id());
    let mut worker = Worker(
        Command::new(std::env::current_exe()?)
            .args(args)
            .arg("--child")
            .arg(&endpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()?,
    );
    let mut pipe = P::create(c, &endpoint)?;
    let mut output = io::BufReader::new(worker.0.stdout.take().ok_or("missing child stdout")?);
    if line(&mut output)? != "READY" {
        return Err("worker did not prepare".into());
    }
    c.placement.pin(0)?;
    let mut rates = Vec::with_capacity(c.samples);
    for sample in 0..=c.samples {
        let started = Instant::now();
        for _ in 0..c.rounds {
            let report = pipe.send(&values)?;
            if report.total_bytes != c.bytes as u64
                || line(&mut output)?.parse::<u64>()? != report.checksum
            {
                return Err("transfer report mismatch".into());
            }
        }
        let rate = (c.bytes * c.rounds) as f64 / started.elapsed().as_secs_f64() / 1e6;
        if sample != 0 {
            rates.push(rate);
        }
    }
    if !worker.0.wait()?.success() {
        return Err("worker failed".into());
    }
    // The child may not leave unvalidated output or unfinished transfers behind.
    let mut extra = String::new();
    output.read_to_string(&mut extra)?;
    if !extra.is_empty() {
        return Err("unexpected worker output".into());
    }
    rates.sort_unstable_by(f64::total_cmp);
    let median = (rates[(rates.len() - 1) / 2] + rates[rates.len() / 2]) / 2.0;
    println!(
        "impl=prescient-ipc-{};unit=byte;bytes={};chunk={};slots={};rounds={};samples={};cpus={};median_mps={median:.6};delivered={};validated=true",
        c.mode,
        c.bytes,
        c.chunk,
        c.slots,
        c.rounds,
        c.samples,
        c.placement.label(),
        c.delivered()?
    );
    Ok(())
}
fn run<P: Pipe>(c: &Config, args: &[String]) -> Result<()> {
    if args.len() == 9 {
        child::<P>(c, &args[8])
    } else {
        parent::<P>(c, args)
    }
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let c = Config::parse(&args)?;
    match c.mode.as_str() {
        "bytes" => run::<Bytes>(&c, &args),
        "pod" => run::<Pod>(&c, &args),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(s: &str) -> Result<Config> {
        Config::parse(&s.split_whitespace().map(str::to_owned).collect::<Vec<_>>())
    }
    #[test]
    fn rejects_invalid_shapes_and_counter_overflow() {
        for s in [
            "",
            "bad 8 1 1 1 1 -",
            "bytes 0 1 1 1 1 -",
            "bytes 8 0 1 1 1 -",
            "bytes 8 1 0 1 1 -",
            "bytes 8 1 1 0 1 -",
            "bytes 8 1 1 1 0 -",
            "pod 7 1 1 1 1 -",
            "bytes 8 1 1 1 1 - extra",
        ] {
            assert!(parse(s).is_err(), "{s}");
        }
        assert!(parse(&format!("bytes 8 1 1 1 {} -", usize::MAX)).is_err());
        assert!(parse(&format!("bytes {} 1 1 2 1 -", usize::MAX)).is_err());
        assert_eq!(parse("pod 8 3 1 2 3 -").unwrap().delivered().unwrap(), 64);
    }
}
