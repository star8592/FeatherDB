pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

pub fn hash_pair(a: u64, b: u64) -> u64 {
    mix64(a ^ mix64(b.wrapping_add(0x9e37_79b9_7f4a_7c15)))
}

pub fn unit_interval_open(hash: u64) -> f64 {
    // Use the top 53 bits so every representable value stays strictly in (0, 1).
    let numerator = (hash >> 12) + 1;
    let denominator = (1_u64 << 52) + 1;
    numerator as f64 / denominator as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_interval_is_open() {
        for hash in [0, 1, u64::MAX / 2, u64::MAX] {
            let value = unit_interval_open(hash);
            assert!(value > 0.0);
            assert!(value < 1.0);
        }
    }
}
