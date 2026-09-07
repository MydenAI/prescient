use super::{Control, DirectionHandle, RegionHandle, StreamFlow, StreamShape};
use super::{DuplexHandle, PayloadContract, PayloadKind};
use std::io::{self, Read, Write};
use std::time::Duration;

const MAGIC: [u8; 8] = *b"PSCIPC\0\0";
const VERSION: u16 = 1;
const ACCEPTED: u8 = 0xa5;
const REJECTED: u8 = 0x5a;
const MAX_REGION_NAME: usize = 240;

pub(super) fn send(
    control: &mut Control,
    handle: &DuplexHandle,
    timeout: Duration,
) -> io::Result<()> {
    configure(control, timeout)?;
    control.write_all(&MAGIC)?;
    write_u16(control, VERSION)?;
    write_payload(control, handle.payload)?;
    write_direction(control, &handle.parent_to_worker)?;
    write_direction(control, &handle.worker_to_parent)?;
    control.flush()?;

    let mut acknowledgement = [0u8; 1];
    control.read_exact(&mut acknowledgement)?;
    match acknowledgement[0] {
        ACCEPTED => Ok(()),
        REJECTED => Err(invalid_data("peer rejected IPC setup")),
        _ => Err(invalid_data("invalid IPC setup acknowledgement")),
    }
}

pub(super) fn receive(control: &mut Control, timeout: Duration) -> io::Result<DuplexHandle> {
    configure(control, timeout)?;
    read_frame(control)
}

fn read_frame(input: &mut impl Read) -> io::Result<DuplexHandle> {
    let mut magic = [0u8; MAGIC.len()];
    input.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(invalid_data("invalid IPC setup magic"));
    }
    if read_u16(input)? != VERSION {
        return Err(invalid_data("unsupported IPC setup version"));
    }
    let payload = read_payload(input)?;
    let parent_to_worker = read_direction(input)?;
    let worker_to_parent = read_direction(input)?;
    Ok(DuplexHandle {
        parent_to_worker,
        worker_to_parent,
        payload,
    })
}

pub(super) fn accept(control: &mut Control) -> io::Result<()> {
    control.write_all(&[ACCEPTED])?;
    control.flush()
}

pub(super) fn reject(control: &mut Control) {
    let _ = control.write_all(&[REJECTED]);
    let _ = control.flush();
}

fn configure(control: &Control, timeout: Duration) -> io::Result<()> {
    control.set_read_timeout(Some(timeout))?;
    control.set_write_timeout(Some(timeout))
}

fn write_payload(output: &mut impl Write, payload: PayloadContract) -> io::Result<()> {
    output.write_all(&[payload.kind as u8])?;
    write_u64(output, payload.schema_id)?;
    write_u64(
        output,
        u64::try_from(payload.element_size)
            .map_err(|_| invalid_data("payload size is too large"))?,
    )?;
    write_u64(
        output,
        u64::try_from(payload.element_align)
            .map_err(|_| invalid_data("payload alignment is too large"))?,
    )
}

fn read_payload(input: &mut impl Read) -> io::Result<PayloadContract> {
    let kind = match read_u8(input)? {
        1 => PayloadKind::Bytes,
        2 => PayloadKind::Pod,
        3 => PayloadKind::Codec,
        _ => return Err(invalid_data("invalid IPC payload kind")),
    };
    let payload = PayloadContract {
        kind,
        schema_id: read_u64(input)?,
        element_size: read_usize(input, "payload size")?,
        element_align: read_usize(input, "payload alignment")?,
    };
    payload.validate()?;
    Ok(payload)
}

fn write_direction(output: &mut impl Write, handle: &DirectionHandle) -> io::Result<()> {
    write_region(output, handle.data_region())?;
    write_region(output, handle.release_region())?;
    let shape = handle.shape();
    write_u64(output, shape.total_bytes)?;
    write_u64(
        output,
        u64::try_from(shape.chunk_bytes).map_err(|_| invalid_data("chunk size is too large"))?,
    )?;
    write_u64(
        output,
        u64::try_from(shape.slots).map_err(|_| invalid_data("slot count is too large"))?,
    )?;
    write_u64(output, handle.transfer_id())?;
    output.write_all(&[handle.flow() as u8])
}

fn read_direction(input: &mut impl Read) -> io::Result<DirectionHandle> {
    let data = read_region(input)?;
    let releases = read_region(input)?;
    let shape = StreamShape {
        total_bytes: read_u64(input)?,
        chunk_bytes: read_usize(input, "chunk size")?,
        slots: read_usize(input, "slot count")?,
    };
    let transfer_id = read_u64(input)?;
    let flow = StreamFlow::try_from(read_u8(input)?)?;
    DirectionHandle::from_parts(data, releases, shape, transfer_id, flow)
}

fn write_region(output: &mut impl Write, handle: &RegionHandle) -> io::Result<()> {
    let name = handle.name().as_bytes();
    if name.is_empty() || name.len() > MAX_REGION_NAME {
        return Err(invalid_data("shared-region name length is invalid"));
    }
    write_u16(output, name.len() as u16)?;
    output.write_all(name)?;
    write_u64(
        output,
        u64::try_from(handle.len()).map_err(|_| invalid_data("region length is too large"))?,
    )
}

fn read_region(input: &mut impl Read) -> io::Result<RegionHandle> {
    let name_len = usize::from(read_u16(input)?);
    if name_len == 0 || name_len > MAX_REGION_NAME {
        return Err(invalid_data("shared-region name length is invalid"));
    }
    let mut name = vec![0u8; name_len];
    input.read_exact(&mut name)?;
    let name =
        String::from_utf8(name).map_err(|_| invalid_data("shared-region name is not UTF-8"))?;
    RegionHandle::from_parts(name, read_usize(input, "region length")?)
}

fn read_usize(input: &mut impl Read, field: &'static str) -> io::Result<usize> {
    usize::try_from(read_u64(input)?).map_err(|_| invalid_data(field))
}

fn write_u16(output: &mut impl Write, value: u16) -> io::Result<()> {
    output.write_all(&value.to_le_bytes())
}

fn write_u64(output: &mut impl Write, value: u64) -> io::Result<()> {
    output.write_all(&value.to_le_bytes())
}

fn read_u8(input: &mut impl Read) -> io::Result<u8> {
    let mut bytes = [0u8; 1];
    input.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn read_u16(input: &mut impl Read) -> io::Result<u16> {
    let mut bytes = [0u8; 2];
    input.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u64(input: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
#[cfg(test)]
mod tests {
    use super::*;

    fn direction(flow: StreamFlow, transfer_id: u64, prefix: &str) -> DirectionHandle {
        DirectionHandle::from_parts(
            RegionHandle::from_parts(format!("{prefix}-data"), 4096).unwrap(),
            RegionHandle::from_parts(format!("{prefix}-release"), 512).unwrap(),
            StreamShape {
                total_bytes: 1024,
                chunk_bytes: 256,
                slots: 4,
            },
            transfer_id,
            flow,
        )
        .unwrap()
    }

    fn handle() -> DuplexHandle {
        DuplexHandle {
            parent_to_worker: direction(StreamFlow::ParentToWorker, 41, "forward"),
            worker_to_parent: direction(StreamFlow::WorkerToParent, 42, "reverse"),
            payload: PayloadContract::bytes(),
        }
    }

    #[test]
    fn setup_round_trip_requires_acceptance() {
        let expected = handle();
        let outbound = expected.clone();
        let (mut creator, mut worker) = Control::pair().unwrap();
        let sender =
            std::thread::spawn(move || send(&mut creator, &outbound, Duration::from_secs(1)));

        let received = receive(&mut worker, Duration::from_secs(1)).unwrap();
        accept(&mut worker).unwrap();
        sender.join().unwrap().unwrap();

        assert_eq!(received.payload, expected.payload);
        assert_eq!(received.parent_to_worker, expected.parent_to_worker);
        assert_eq!(received.worker_to_parent, expected.worker_to_parent);
    }

    #[test]
    fn malformed_magic_and_truncated_frames_are_rejected() {
        let mut malformed = &b"NOT-IPC!"[..];
        let error = read_frame(&mut malformed).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut truncated = &MAGIC[..3];
        let error = read_frame(&mut truncated).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn oversized_region_name_is_rejected_before_allocation() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&MAGIC);
        write_u16(&mut frame, VERSION).unwrap();
        write_payload(&mut frame, PayloadContract::bytes()).unwrap();
        write_u16(&mut frame, (MAX_REGION_NAME + 1) as u16).unwrap();

        let mut input = frame.as_slice();
        let error = read_frame(&mut input).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejected_and_unknown_acknowledgements_fail_closed() {
        for acknowledgement in [REJECTED, 0xff] {
            let expected = handle();
            let (mut creator, mut worker) = Control::pair().unwrap();
            let peer = std::thread::spawn(move || {
                let _ = receive(&mut worker, Duration::from_secs(1)).unwrap();
                worker.write_all(&[acknowledgement]).unwrap();
            });
            let error = send(&mut creator, &expected, Duration::from_secs(1)).unwrap_err();
            peer.join().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }
}
