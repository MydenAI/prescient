//! Consumer-local locality with bounded round-robin service.
//! The cursor is a hint, never ownership: every drain still acquires and releases
//! its claim.

/// Maximum productive drains before moving beyond the preferred ring.
const QUANTUM: u8 = 16;

/// Internal cursor, public only as a sealed membership associated type.
#[doc(hidden)]
pub struct Cursor {
    next: usize,
    credits: u8,
}

impl Default for Cursor {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Cursor {
    pub(super) fn new(seed: usize) -> Self {
        Self {
            next: seed,
            credits: QUANTUM - 1,
        }
    }

    /// Always advance before attempting a claim. Failure, emptiness and unwind
    /// therefore leave the full-scan/disconnect behavior intact.
    #[inline]
    pub(super) fn next(&mut self, len: usize) -> usize {
        crate::round_robin::next(&mut self.next, len)
    }

    /// Called only after a productive drain and after releasing its claim.
    /// Rewind the last selection while credit remains; periodically leave the
    /// next position intact so a permanently hot ring cannot hide its peers.
    #[inline]
    pub(super) fn prefer(&mut self) {
        debug_assert!(self.next > 0);
        self.next -= usize::from(self.credits != 0);
        self.credits = self.credits.wrapping_sub(1) & (QUANTUM - 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_rings_receive_bounded_service() {
        let mut cursor = Cursor::new(0);
        for i in 0..3 * usize::from(QUANTUM) * 4 {
            assert_eq!(cursor.next(3), i / usize::from(QUANTUM) % 3);
            cursor.prefer();
        }
    }

    #[test]
    fn empty_busy_and_unwinding_drains_advance_without_preference() {
        let mut cursor = Cursor::new(1);
        assert_eq!(cursor.next(3), 1);
        cursor.prefer();
        // A failed preferred attempt must not repeat itself in this scan.
        assert_eq!(cursor.next(3), 1);
        assert_eq!(cursor.next(3), 2);
        assert_eq!(cursor.next(3), 0);
        assert_eq!(cursor.next(3), 1);
    }

    #[test]
    fn growth_is_seen_after_bounded_preference_and_large_seeds_wrap() {
        let mut cursor = Cursor::new(usize::MAX);
        for _ in 0..QUANTUM {
            assert_eq!(cursor.next(1), 0);
            cursor.prefer();
        }
        assert_eq!(cursor.next(2), 1);
        assert_eq!(cursor.next(1), 0);
    }
}
