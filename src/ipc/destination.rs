//! A typed allocation whose initialized byte prefix is private until commit.
use super::IpcPod;
use std::io::{self, Write};

pub(super) struct PodDestination<P: IpcPod> {
    values: Vec<P>,
    bytes: usize,
    written: usize,
}

impl<P: IpcPod> PodDestination<P> {
    pub(super) fn new(bytes: usize) -> io::Result<Self> {
        if size_of::<P>() == 0 || !bytes.is_multiple_of(size_of::<P>()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "POD byte shape is not a whole number of non-zero-sized values",
            ));
        }
        Ok(Self {
            values: Vec::with_capacity(bytes / size_of::<P>()),
            bytes,
            written: 0,
        })
    }

    pub(super) fn finish(mut self) -> io::Result<Vec<P>> {
        if self.written != self.bytes {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "POD destination is not completely initialized",
            ));
        }
        // SAFETY: new reserved bytes/size_of<P> aligned elements. write initialized
        // every byte, without exposing partial P values; IpcPod permits all bit
        // patterns, has no padding/provenance and cannot own a destructor.
        unsafe { self.values.set_len(self.bytes / size_of::<P>()) };
        Ok(self.values)
    }
}

impl<P: IpcPod> Write for PodDestination<P> {
    #[inline]
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let n = input.len().min(self.bytes - self.written);
        // SAFETY: written <= bytes <= the typed allocation's byte capacity.
        // Only this writer accesses its allocation, input is initialized, and
        // no reference is formed to uninitialized storage. Nothing can unwind
        // between the copy and advancing the initialized-prefix count.
        unsafe {
            std::ptr::copy_nonoverlapping(
                input.as_ptr(),
                self.values.as_mut_ptr().cast::<u8>().add(self.written),
                n,
            );
        }
        self.written += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C, align(64))]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Aligned([u8; 64]);
    // SAFETY: exactly 64 initialized bytes, aligned without padding or provenance.
    unsafe impl IpcPod for Aligned {
        const SCHEMA_ID: u64 = 0x5053_5445_5354_0001;
    }

    #[test]
    fn fragmented_bytes_commit_the_original_aligned_allocation() {
        let input: Vec<_> = (0..128).map(|i| i as u8).collect();
        for chunk in [1, 3, 7, 63, 64, 65, 127, 128] {
            let mut out = PodDestination::<Aligned>::new(input.len()).unwrap();
            let allocation = out.values.as_ptr();
            assert_eq!(allocation.addr() % 64, 0);
            for part in input.chunks(chunk) {
                out.write_all(part).unwrap();
                assert_eq!(out.values.len(), 0); // Partial P values never escape.
            }
            out.flush().unwrap();
            let values = out.finish().unwrap();
            assert_eq!(values.as_ptr(), allocation);
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].0, input[..64]);
            assert_eq!(values[1].0, input[64..]);
        }
    }

    #[test]
    fn short_or_abandoned_prefix_never_exposes_uninitialized_values() {
        for written in 0..16 {
            let mut out = PodDestination::<u64>::new(16).unwrap();
            out.write_all(&[0xab; 16][..written]).unwrap();
            assert_eq!(out.values.len(), 0);
            assert_eq!(
                out.finish().unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            let mut abandoned = PodDestination::<Aligned>::new(64).unwrap();
            abandoned.write_all(&[0xff; 64][..written]).unwrap();
            drop(abandoned);
        }
    }

    #[test]
    fn write_all_matches_bounded_slice_prefix_semantics() {
        for bytes in [0, 1, 8, 16] {
            for input_len in 0..=24 {
                let input = vec![0xab; input_len];
                let mut reference = vec![0; bytes];
                let expected = reference.as_mut_slice().write_all(&input);
                let mut out = PodDestination::<u8>::new(bytes).unwrap();
                let actual = out.write_all(&input);
                assert_eq!(
                    actual.err().map(|e| e.kind()),
                    expected.err().map(|e| e.kind())
                );
                assert_eq!(out.written, bytes.min(input_len));
                assert_eq!(out.values.len(), 0);
                if input_len >= bytes {
                    assert_eq!(out.finish().unwrap(), reference);
                } else {
                    assert_eq!(
                        out.finish().unwrap_err().kind(),
                        io::ErrorKind::UnexpectedEof
                    );
                }
            }
        }
    }

    #[test]
    fn bounded_write_empty_destination_and_invalid_shape() {
        let mut out = PodDestination::<u64>::new(8).unwrap();
        assert_eq!(out.write(&[0xff; 9]).unwrap(), 8);
        assert_eq!(out.write(&[1]).unwrap(), 0);
        assert_eq!(
            out.write_all(&[1]).unwrap_err().kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(out.finish().unwrap(), [u64::MAX]);
        let mut empty = PodDestination::<Aligned>::new(0).unwrap();
        assert_eq!(empty.write(&[]).unwrap(), 0);
        assert!(empty.finish().unwrap().is_empty());
        assert!(PodDestination::<u64>::new(7).is_err());
    }
}
