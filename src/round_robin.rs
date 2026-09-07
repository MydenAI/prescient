//! Division-free round-robin positions, private to channel implementations.

/// Select the current slot. A cursor at the end, or beyond a shrunken membership,
/// restarts at zero. Callers must not select from an empty membership.
#[inline]
pub(crate) fn index(cursor: usize, len: usize) -> usize {
    debug_assert!(len > 0);
    if cursor < len { cursor } else { 0 }
}

/// Select and advance. Keep the end sentinel until the next selection so growth
/// can make newly appended slots visible without another pass over old slots.
#[inline]
pub(crate) fn next(cursor: &mut usize, len: usize) -> usize {
    let i = index(*cursor, len);
    *cursor = i + 1; // i < len, so this cannot overflow, even for len == usize::MAX.
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycles_growth_shrink_and_large_cursors() {
        for len in [1, 2, 3, 7, 4096] {
            let mut cursor = 0;
            for expected in (0..len).cycle().take(len * 3) {
                assert_eq!(next(&mut cursor, len), expected);
            }
            assert_eq!(cursor, len);
            assert_eq!(next(&mut cursor, len + 1), len);
            assert_eq!(next(&mut cursor, 1), 0);
        }
        let mut cursor = usize::MAX;
        assert_eq!(next(&mut cursor, 3), 0);
        cursor = usize::MAX - 1;
        assert_eq!(next(&mut cursor, usize::MAX), usize::MAX - 1);
        assert_eq!(next(&mut cursor, usize::MAX), 0);
    }

    #[test]
    fn selecting_without_advancing_rechecks_swapped_slot() {
        let cursor = 2;
        assert_eq!(index(cursor, 5), 2);
        assert_eq!(index(cursor, 4), 2);
        assert_eq!(index(cursor, 2), 0);
    }
}
