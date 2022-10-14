


fn encod_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}