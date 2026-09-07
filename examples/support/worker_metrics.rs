//! Benchmark-only worker lifetime counters. No calls from message loops.
//!
//! A worker opens and enables its own pinned user-space perf group before
//! announcing readiness, and reads it before exiting. Thus counts include the
//! ready gate, useful work, retries, validation and endpoint teardown, but not
//! thread creation or channel allocation. CPU time includes user and kernel.
//! Neither CPU duty nor sampled instruction pointers are presented as IPC.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    Off,
    Duty,
    Perf,
}
impl Mode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "off" => Ok(Self::Off),
            "duty" => Ok(Self::Duty),
            "perf" => Ok(Self::Perf),
            _ => Err("worker metrics must be off, duty, or perf".into()),
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Duty => "duty",
            Self::Perf => "perf",
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub tid: i64,
    pub cpu_ns: u64,
    pub wall_ns: u64,
    pub minor_faults: i64,
    pub major_faults: i64,
    pub voluntary: i64,
    pub involuntary: i64,
    pub hardware: Option<Hardware>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Hardware {
    pub cycles: u64,
    pub instructions: u64,
    pub enabled_ns: u64,
    pub running_ns: u64,
}
impl Report {
    pub fn print(&self, sample: usize, role: &str, index: usize, messages: usize) {
        let (cycles, instructions, enabled, running) = self
            .hardware
            .as_ref()
            .map(|h| {
                (
                    h.cycles.to_string(),
                    h.instructions.to_string(),
                    h.enabled_ns.to_string(),
                    h.running_ns.to_string(),
                )
            })
            .unwrap_or_else(|| ("na".into(), "na".into(), "na".into(), "na".into()));
        println!(
            "worker_sample={sample};role={role};index={index};tid={};messages={messages};cpu_ns={};wall_ns={};minor_faults={};major_faults={};voluntary={};involuntary={};instructions={instructions};cycles={cycles};enabled_ns={enabled};running_ns={running}",
            self.tid,
            self.cpu_ns,
            self.wall_ns,
            self.minor_faults,
            self.major_faults,
            self.voluntary,
            self.involuntary
        );
    }
}

// Full group read: nr, enabled, running, (value, id) for each of two events.
// No extrapolation: incomplete, unscheduled and multiplexed groups fail closed.
#[cfg(any(test, all(target_os = "linux", target_arch = "x86_64")))]
fn decode_group(words: &[u64], cycles_id: u64, instructions_id: u64) -> Result<Hardware, String> {
    if words.len() != 7 || words[0] != 2 || cycles_id == instructions_id {
        return Err("incomplete perf group or invalid event identities".into());
    }
    if words[1] == 0 || words[2] != words[1] {
        return Err(format!(
            "perf group was not fully scheduled: enabled={} running={}",
            words[1], words[2]
        ));
    }
    let mut cycles = None;
    let mut instructions = None;
    for pair in words[3..].as_chunks::<2>().0 {
        let slot = if pair[1] == cycles_id {
            &mut cycles
        } else if pair[1] == instructions_id {
            &mut instructions
        } else {
            return Err("unexpected perf event id".into());
        };
        if slot.replace(pair[0]).is_some() {
            return Err("duplicate perf event id".into());
        }
    }
    Ok(Hardware {
        cycles: cycles.ok_or("missing cycles event")?,
        instructions: instructions.ok_or("missing instructions event")?,
        enabled_ns: words[1],
        running_ns: words[2],
    })
}

pub struct Meter {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    inner: Option<linux::Meter>,
}
impl Meter {
    pub fn prepare(mode: Mode) -> Result<Self, String> {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            Ok(Self {
                inner: if mode == Mode::Off {
                    None
                } else {
                    Some(linux::Meter::new(mode)?)
                },
            })
        }
        #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
        {
            if mode != Mode::Off {
                return Err("worker metrics currently require Linux x86_64".into());
            }
            Ok(Self {})
        }
    }
    pub fn finish(self) -> Result<Option<Report>, String> {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            self.inner.map(linux::Meter::finish).transpose()
        }
        #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
        {
            Ok(None)
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod linux {
    use super::{Hardware, Mode, Report, decode_group};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::time::Instant;

    // Linux UAPI perf_event_attr v0 (64 bytes). No sampling or inheritance.
    // The flags layout and ioctl encodings below are scoped to Linux x86_64.
    #[repr(C)]
    #[derive(Default)]
    struct Attr {
        kind: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        config1: u64,
    }
    const _: () = assert!(std::mem::size_of::<Attr>() == 64);
    const ENABLE: libc::c_ulong = 0x2400;
    const DISABLE: libc::c_ulong = 0x2401;
    const ID: libc::c_ulong = 0x80082407;
    const GROUP: libc::c_ulong = 1;

    struct Events {
        cycles: OwnedFd,
        _instructions: OwnedFd,
        cycles_id: u64,
        instructions_id: u64,
    }
    fn os_error(context: &str) -> String {
        format!("{context}: {}", io::Error::last_os_error())
    }
    fn open(config: u64, leader: Option<&OwnedFd>) -> Result<OwnedFd, String> {
        let attr = Attr {
            kind: 0,
            size: 64,
            config,
            read_format: 1 | 2 | 4 | 8,
            // Exclude kernel/hypervisor. Only the leader is disabled and pinned.
            flags: (1 << 5) | (1 << 6) | if leader.is_none() { 1 | (1 << 2) } else { 0 },
            ..Attr::default()
        };
        // SAFETY: attr is a fully initialized, correctly sized UAPI structure.
        // pid=0,cpu=-1 binds this calling thread; group fd remains live.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &attr,
                0,
                -1,
                leader.map_or(-1, AsRawFd::as_raw_fd),
                1u64 << 3,
            )
        };
        if fd < 0 {
            return Err(os_error("perf_event_open"));
        }
        // SAFETY: successful syscall returned a new exclusively owned fd.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }
    fn event_id(fd: &OwnedFd) -> Result<u64, String> {
        let mut id = 0u64;
        // SAFETY: ID writes one u64 to a valid aligned pointer.
        if unsafe { libc::ioctl(fd.as_raw_fd(), ID, &mut id) } != 0 {
            return Err(os_error("perf event id"));
        }
        Ok(id)
    }
    fn control(fd: &OwnedFd, op: libc::c_ulong) -> Result<(), String> {
        // SAFETY: ENABLE/DISABLE consume an integer group flag, not a pointer.
        if unsafe { libc::ioctl(fd.as_raw_fd(), op, GROUP) } != 0 {
            return Err(os_error("perf group control"));
        }
        Ok(())
    }
    impl Events {
        fn new() -> Result<Self, String> {
            let cycles = open(0, None)?;
            let instructions = open(1, Some(&cycles))?;
            Ok(Self {
                cycles_id: event_id(&cycles)?,
                instructions_id: event_id(&instructions)?,
                cycles,
                _instructions: instructions,
            })
        }
        fn finish(self) -> Result<Hardware, String> {
            control(&self.cycles, DISABLE)?;
            let mut words = [0u64; 7];
            let bytes = std::mem::size_of_val(&words);
            // SAFETY: words is writable for bytes; this is an ordinary fd read.
            let read =
                unsafe { libc::read(self.cycles.as_raw_fd(), words.as_mut_ptr().cast(), bytes) };
            if read < 0 {
                return Err(os_error("perf group read"));
            }
            if read as usize != bytes {
                return Err(format!("short perf group read: {read}/{bytes}"));
            }
            decode_group(&words, self.cycles_id, self.instructions_id)
        }
    }
    struct Snapshot {
        cpu_ns: u64,
        usage: libc::rusage,
    }
    fn snapshot() -> Result<Snapshot, String> {
        let mut ts = std::mem::MaybeUninit::<libc::timespec>::uninit();
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: both syscalls initialize the full output on success.
        if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, ts.as_mut_ptr()) } != 0 {
            return Err(os_error("thread CPU clock"));
        }
        if unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) } != 0 {
            return Err(os_error("thread rusage"));
        }
        // SAFETY: both calls succeeded.
        let (ts, usage) = unsafe { (ts.assume_init(), usage.assume_init()) };
        let cpu_ns = (ts.tv_sec as u64)
            .checked_mul(1_000_000_000)
            .and_then(|v| v.checked_add(ts.tv_nsec as u64))
            .ok_or("CPU clock overflow")?;
        Ok(Snapshot { cpu_ns, usage })
    }
    pub(super) struct Meter {
        events: Option<Events>,
        start: Snapshot,
        wall: Instant,
        tid: i64,
    }
    impl Meter {
        pub(super) fn new(mode: Mode) -> Result<Self, String> {
            let events = if mode == Mode::Perf {
                Some(Events::new()?)
            } else {
                None
            };
            let start = snapshot()?;
            // SAFETY: gettid has no arguments or pointer outputs.
            let tid = unsafe { libc::syscall(libc::SYS_gettid) };
            let wall = Instant::now();
            if let Some(events) = &events {
                control(&events.cycles, ENABLE)?;
            }
            Ok(Self {
                events,
                start,
                wall,
                tid,
            })
        }
        pub(super) fn finish(self) -> Result<Report, String> {
            let hardware = self.events.map(Events::finish).transpose()?;
            let end = snapshot()?;
            Ok(Report {
                tid: self.tid,
                cpu_ns: end
                    .cpu_ns
                    .checked_sub(self.start.cpu_ns)
                    .ok_or("CPU clock reversed")?,
                wall_ns: u64::try_from(self.wall.elapsed().as_nanos())
                    .map_err(|_| "wall clock overflow")?,
                minor_faults: end.usage.ru_minflt - self.start.usage.ru_minflt,
                major_faults: end.usage.ru_majflt - self.start.usage.ru_majflt,
                voluntary: end.usage.ru_nvcsw - self.start.usage.ru_nvcsw,
                involuntary: end.usage.ru_nivcsw - self.start.usage.ru_nivcsw,
                hardware,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn group_identity_and_full_coverage_are_required() {
        let valid = [2, 100, 100, 71, 9, 83, 11];
        assert_eq!(
            decode_group(&valid, 9, 11).unwrap(),
            Hardware {
                cycles: 71,
                instructions: 83,
                enabled_ns: 100,
                running_ns: 100
            }
        );
        assert_eq!(
            decode_group(&[2, 100, 100, 83, 11, 71, 9], 9, 11)
                .unwrap()
                .cycles,
            71
        );
        for bad in [
            vec![],
            vec![2, 100, 100, 71, 9],
            vec![1, 100, 100, 71, 9, 83, 11],
            vec![2, 0, 0, 71, 9, 83, 11],
            vec![2, 100, 99, 71, 9, 83, 11],
            vec![2, 100, 101, 71, 9, 83, 11],
            vec![2, 100, 100, 71, 9, 83, 9],
            vec![2, 100, 100, 71, 9, 83, 12],
        ] {
            assert!(decode_group(&bad, 9, 11).is_err(), "{bad:?}");
        }
        assert!(decode_group(&valid, 9, 9).is_err());
    }
    #[test]
    fn disabled_is_portable_and_unknown_mode_fails() {
        assert!(
            Meter::prepare(Mode::Off)
                .unwrap()
                .finish()
                .unwrap()
                .is_none()
        );
        assert_eq!(Mode::parse("duty").unwrap(), Mode::Duty);
        assert_eq!(Mode::parse("perf").unwrap(), Mode::Perf);
        assert!(Mode::parse("automatic").is_err());
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[test]
    fn duty_reads_the_current_worker_without_hardware_permission() {
        let report = std::thread::spawn(|| {
            let meter = Meter::prepare(Mode::Duty).unwrap();
            let mut x = 0u64;
            for i in 0..10_000 {
                x = std::hint::black_box(x.wrapping_add(i));
            }
            std::hint::black_box(x);
            meter.finish().unwrap().unwrap()
        })
        .join()
        .unwrap();
        assert!(report.cpu_ns > 0 && report.wall_ns > 0);
        assert!(report.tid > 0);
        assert!(report.hardware.is_none());
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    #[test]
    #[ignore = "requires accessible, fully scheduled hardware counters"]
    fn live_perf_group_counts_this_worker() {
        let meter = Meter::prepare(Mode::Perf).unwrap();
        let mut x = 1u64;
        for i in 0..100_000 {
            x = std::hint::black_box(x.wrapping_mul(3).wrapping_add(i));
        }
        std::hint::black_box(x);
        let hw = meter.finish().unwrap().unwrap().hardware.unwrap();
        assert!(hw.instructions >= 100_000 && hw.cycles > 0);
        assert_eq!(hw.enabled_ns, hw.running_ns);
    }
}
