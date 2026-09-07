//! Owned FIFO bulk destination shared by storage backends and channel topologies.
use std::mem::MaybeUninit;

/// A reusable owned batch: append values, borrow its ready slice, or pop in FIFO
/// order. Call `clear()` to reuse consumed storage; it also drops unread values.
/// Backends can append safely without controlling initialization metadata.
///
/// Only `values[read..]` owns initialized values. The moved-out prefix is never
/// read or dropped as T. No pointers/initialization counts are exposed publicly.
pub struct Batch<T> {
    pub(super) values: Vec<MaybeUninit<T>>,
    pub(super) read: usize,
}

impl<T> Batch<T> {
    /// Create an empty batch without allocating.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            read: 0,
        }
    }

    /// Drop unread values and make the complete allocation reusable. If a
    /// destructor panics, slice drop glue still drops the remaining values.
    #[inline]
    pub fn clear(&mut self) {
        let remaining = self.values.len() - self.read;
        // SAFETY: this suffix alone owns initialized T values. Clear metadata
        // before running drop glue so unwinding never drops the suffix twice.
        unsafe {
            let first = self.values.as_mut_ptr().add(self.read).cast::<T>();
            self.values.set_len(0);
            self.read = 0;
            std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(first, remaining));
        }
    }

    /// Number of unread values.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len() - self.read
    }

    /// Whether every value has been consumed.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.read == self.values.len()
    }

    /// Borrow only initialized, unread values, without moving them.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: the private read/len invariant defines the initialized suffix;
        // Vec provides alignment even for zero-sized or empty allocations.
        unsafe {
            std::slice::from_raw_parts(self.values.as_ptr().add(self.read).cast::<T>(), self.len())
        }
    }

    /// Reserve space before a backend starts transferring ownership.
    #[inline]
    #[cfg(not(loom))]
    pub(super) fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }

    /// Append one owned value, preserving any unread prefix.
    #[inline]
    pub fn push_back(&mut self, value: T) {
        let len = self.values.len();
        if len == self.values.capacity() {
            // Reserve while value is still a T, so it is dropped if growth
            // panics. Wrapping it in MaybeUninit before reserve would leak it.
            self.grow();
        }
        // SAFETY: reserve ensures room for len+1 (also rejecting ZST length
        // overflow). The new slot is uninitialized and exclusively owned.
        unsafe {
            self.values
                .as_mut_ptr()
                .add(len)
                .write(MaybeUninit::new(value));
            self.values.set_len(len + 1);
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        self.values.reserve(1);
    }

    /// Move the oldest unread value out of the batch.
    #[inline]
    pub fn pop_front(&mut self) -> Option<T> {
        let read = self.read;
        if read == self.values.len() {
            return None;
        }
        self.read = read + 1;
        // SAFETY: read < len by the invariant and empty check. This slot owns
        // one initialized T. Advance before transferring ownership to the caller.
        Some(unsafe { self.values.get_unchecked(read).assume_init_read() })
    }
    /// Return a rejected value to the slot just vacated by pop_front.
    #[inline]
    pub(super) fn restore_front(&mut self, value: T) {
        let read = self
            .read
            .checked_sub(1)
            .expect("no moved-out slot to restore");
        self.values[read].write(value);
        self.read = read;
    }
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Batch<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn batch_borrowed_suffix_clear_and_reuse() {
        let mut batch = Batch::default();
        assert!(batch.is_empty());
        assert!(batch.as_slice().is_empty());
        for value in ["a", "b", "c"] {
            batch.push_back(String::from(value));
        }
        let allocation = batch.values.as_ptr();
        assert_eq!(batch.pop_front().unwrap(), "a");
        batch.push_back(String::from("d"));
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.as_slice(), ["b", "c", "d"]);
        batch.clear();
        assert!(batch.is_empty());
        assert!(batch.as_slice().is_empty());
        batch.push_back(String::from("reused"));
        assert_eq!(batch.values.as_ptr(), allocation);
        assert_eq!(batch.pop_front().unwrap(), "reused");
    }
    #[test]
    fn staging_fifo_and_allocation_reuse() {
        let mut staging = Batch::new();
        for n in [0, 1, 3, 64, 4096, 3, 1, 0] {
            staging.clear();
            for value in 0..n {
                staging.push_back(value);
            }
            let allocation = staging.values.as_ptr();
            let capacity = staging.values.capacity();
            for value in 0..n {
                assert_eq!(staging.pop_front(), Some(value));
            }
            assert_eq!(staging.pop_front(), None);
            staging.clear();
            for value in 0..n {
                staging.push_back(value);
            }
            assert_eq!(staging.values.as_ptr(), allocation);
            assert_eq!(staging.values.capacity(), capacity);
            for value in 0..n {
                assert_eq!(staging.pop_front(), Some(value));
            }
        }
    }

    #[test]
    fn staging_growth_panic_drops_the_incoming_value() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Zst;
        impl Drop for Zst {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut staging = Batch::<Zst>::new();
        // Model an exhausted ZST batch at maximum length without allocating.
        // SAFETY: MaybeUninit<Zst> needs no initialization and ZST capacity is
        // usize::MAX. Mark the entire synthetic prefix unowned before any T access.
        unsafe { staging.values.set_len(usize::MAX) };
        staging.read = usize::MAX;
        let result = catch_unwind(AssertUnwindSafe(|| staging.push_back(Zst)));
        assert!(result.is_err());
        assert_eq!(DROPS.load(Ordering::Relaxed), 1);
        drop(staging);
        assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    }
    struct Tracked {
        id: usize,
        drops: Arc<[AtomicUsize; 8]>,
        panic_on_drop: bool,
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops[self.id].fetch_add(1, Ordering::Relaxed);
            assert!(!self.panic_on_drop, "drop panic");
        }
    }

    #[test]
    fn staging_drops_only_unconsumed_values_even_when_a_destructor_panics() {
        for panic_on_drop in [false, true] {
            let drops = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
            let mut staging = Batch::new();
            for id in 0..8 {
                staging.push_back(Tracked {
                    id,
                    drops: Arc::clone(&drops),
                    panic_on_drop: panic_on_drop && id == 3,
                });
            }
            drop(staging.pop_front().unwrap());
            drop(staging.pop_front().unwrap());
            let result = catch_unwind(AssertUnwindSafe(|| drop(staging)));
            assert_eq!(result.is_err(), panic_on_drop);
            for count in drops.iter() {
                assert_eq!(count.load(Ordering::Relaxed), 1);
            }
        }
    }

    #[test]
    fn staging_partial_fill_survives_unwind_and_growth() {
        let mut staging = Batch::new();
        let result = catch_unwind(AssertUnwindSafe(|| {
            for value in 0..3 {
                staging.push_back(format!("value-{value}"));
            }
            panic!("refill interrupted");
        }));
        assert!(result.is_err());
        assert_eq!(staging.pop_front().unwrap(), "value-0");
        // Growing with a moved-out prefix must never read/drop it as T.
        for value in 3..100 {
            staging.push_back(format!("value-{value}"));
        }
        for value in 1..100 {
            assert_eq!(staging.pop_front().unwrap(), format!("value-{value}"));
        }
        assert!(staging.pop_front().is_none());
        staging.clear();
        staging.push_back(String::from("reused"));
        assert_eq!(staging.pop_front().unwrap(), "reused");
    }

    #[test]
    fn staging_zero_sized_values_are_dropped_once() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Zst;
        impl Drop for Zst {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut staging = Batch::new();
        for _ in 0..64 {
            staging.push_back(Zst);
        }
        for _ in 0..17 {
            drop(staging.pop_front().unwrap());
        }
        drop(staging);
        assert_eq!(DROPS.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn staging_overaligned_values_keep_alignment() {
        #[repr(align(256))]
        struct Aligned(u64);
        let mut staging = Batch::new();
        for n in 0..100 {
            staging.push_back(Aligned(n));
        }
        assert_eq!(staging.values.as_ptr().addr() % 256, 0);
        for n in 0..100 {
            assert_eq!(staging.pop_front().unwrap().0, n);
        }
    }
}
