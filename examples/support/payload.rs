//! Shared, byte-complete payload construction and validation for benchmarks.
pub(super) fn fixtures<const N: usize>() -> [[u8; N]; 16] {
    std::array::from_fn(|pattern| {
        std::array::from_fn(|i| {
            (i.wrapping_mul(73) ^ (i >> 8).wrapping_mul(31) ^ (pattern * 19)) as u8
        })
    })
}
#[inline]
pub(super) fn packet<const N: usize>(id: u64, fixtures: &[[u8; N]; 16]) -> [u8; N] {
    let mut value = fixtures[id as usize & 15];
    value[..8].copy_from_slice(&id.to_le_bytes());
    value
}
#[inline]
pub(super) fn inspect<const N: usize>(
    value: &[u8; N],
    fixtures: &[[u8; N]; 16],
    total: usize,
) -> (u64, usize) {
    let id = u64::from_le_bytes(value[..8].try_into().unwrap());
    let bad = id >= total as u64 || value[8..] != fixtures[id as usize & 15][8..];
    (id, usize::from(bad))
}
pub(super) fn xor_prefix(last: u64) -> u64 {
    match last & 3 {
        0 => last,
        1 => 1,
        2 => last + 1,
        _ => 0,
    }
}
