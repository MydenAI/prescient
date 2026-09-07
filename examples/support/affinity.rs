//! Benchmark-only worker placement. Never called from a message loop.
#[derive(Debug)]
pub struct Placement {
    cpus: Option<Vec<usize>>,
}

impl Placement {
    pub fn new(spec: Option<&str>, workers: usize) -> Result<Self, String> {
        let cpus = spec.map(|s| parse(s, workers)).transpose()?;
        if let Some(cpus) = &cpus {
            let allowed = allowed_cpus()?;
            for cpu in cpus {
                if !allowed.contains(cpu) {
                    return Err(format!(
                        "CPU {cpu} is outside the allowed affinity {allowed:?}"
                    ));
                }
            }
        }
        Ok(Self { cpus })
    }

    pub fn pin(&self, worker: usize) -> Result<(), String> {
        if let Some(cpus) = &self.cpus {
            let cpu = cpus
                .get(worker)
                .ok_or("worker CPU map index out of range")?;
            pin_current(*cpu)?;
        }
        Ok(())
    }

    pub fn label(&self) -> String {
        match &self.cpus {
            Some(cpus) => cpus
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
            None => "inherited".into(),
        }
    }
}

fn parse(spec: &str, workers: usize) -> Result<Vec<usize>, String> {
    let cpus: Vec<usize> = spec
        .split(',')
        .map(|part| {
            if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
                return Err(format!(
                    "invalid CPU id {part:?}; use comma-separated integers"
                ));
            }
            part.parse()
                .map_err(|_| format!("CPU id {part:?} is too large"))
        })
        .collect::<Result<_, _>>()?;
    if cpus.len() != workers {
        return Err(format!(
            "CPU map needs {workers} entries, got {}",
            cpus.len()
        ));
    }
    Ok(cpus)
}

#[cfg(target_os = "linux")]
fn allowed_cpus() -> Result<Vec<usize>, String> {
    // SAFETY: cpu_set_t is a C integer bitset; all-zero bytes are valid.
    let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    // SAFETY: mask is writable for its full size. pid=0 queries this thread.
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } != 0 {
        return Err(format!(
            "cannot read CPU affinity: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok((0..libc::CPU_SETSIZE as usize)
        // SAFETY: every index is below CPU_SETSIZE and mask is initialized.
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &mask) })
        .collect())
}

#[cfg(target_os = "linux")]
fn pin_current(cpu: usize) -> Result<(), String> {
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err("CPU id exceeds the supported affinity mask".into());
    }
    // SAFETY: a zero-initialized C bitset is valid; cpu is in bounds.
    let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe { libc::CPU_SET(cpu, &mut mask) };
    // SAFETY: mask is readable for its full size; pid=0 changes only this thread.
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) } != 0 {
        return Err(format!(
            "cannot pin worker to CPU {cpu}: {}",
            std::io::Error::last_os_error()
        ));
    }
    if allowed_cpus()? != [cpu] {
        return Err(format!("worker affinity did not become CPU {cpu}"));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn allowed_cpus() -> Result<Vec<usize>, String> {
    Err("explicit worker CPU maps are supported only on Linux".into())
}

#[cfg(not(target_os = "linux"))]
fn pin_current(_: usize) -> Result<(), String> {
    Err("explicit worker CPU maps are supported only on Linux".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_cpu_map_and_explicit_oversubscription() {
        assert_eq!(parse("0,2,2,7", 4).unwrap(), [0, 2, 2, 7]);
        for bad in [
            "",
            "0,",
            ",0",
            "0,,1",
            "-1",
            "+1",
            " 1",
            "1-3",
            "999999999999999999999999999",
        ] {
            assert!(parse(bad, 1).is_err(), "{bad:?}");
        }
        assert!(parse("0,1", 3).is_err());
        assert!(parse("0,1", 1).is_err());
    }

    #[test]
    fn inherited_placement_is_portable() {
        let placement = Placement::new(None, 2).unwrap();
        assert_eq!(placement.label(), "inherited");
        placement.pin(0).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unavailable_cpu_is_rejected_before_startup() {
        let invalid = libc::CPU_SETSIZE as usize;
        assert!(Placement::new(Some(&invalid.to_string()), 1).is_err());
        assert!(pin_current(invalid).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_is_pinned_without_changing_the_coordinator() {
        let before = allowed_cpus().unwrap();
        let cpu = before[0];
        let placement = Placement::new(Some(&cpu.to_string()), 1).unwrap();
        assert_eq!(placement.label(), cpu.to_string());
        assert!(placement.pin(1).is_err());
        std::thread::spawn(move || {
            placement.pin(0).unwrap();
            assert_eq!(allowed_cpus().unwrap(), [cpu]);
        })
        .join()
        .unwrap();
        assert_eq!(allowed_cpus().unwrap(), before);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn explicit_map_is_not_silently_ignored() {
        assert!(Placement::new(Some("0"), 1).is_err());
    }
}
