//! A small seeded generator (SplitMix64): the same seed gives the same choices, so a run
//! is told by its seed, as far as the operating system's timing lets it repeat.

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `[0, n)`; 0 when `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    /// A number in `[low, high]`.
    pub fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high - low + 1)
    }

    /// Whether a chance of `percent` in a hundred came up.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    /// `length` bytes, the same for the same generator state.
    pub fn bytes(&mut self, length: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(length + 8);
        while bytes.len() < length {
            bytes.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        bytes.truncate(length);
        bytes
    }
}
