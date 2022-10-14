use crate::error::{Error, Result};

pub fn encode_boolean(val: bool) -> u8 {
    match val {
        true => 0x01,
        false => 0x00,
    }
}

pub fn decode_boolean(byte: u8) -> Result<bool> {
    match byte {
        0x00 => Ok(false),
        0x01 => Ok(true),
        b => Err(Error::Internal(format!("Invalid boolean value {}", b))),
    }
}

pub fn take_boolean(bytes: &mut &[u8]) -> Result<bool> {
    take_byte(bytes).and_then(decode_boolean)
}

pub fn take_byte(bytes: &mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(Error::Internal(format!("empty bytes cannot take byte")));
    }
    let byte = bytes[0];
    *bytes = &bytes[1..];
    Ok(byte)
}

// pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {

// }

// pub fn take_bytes(bytes: &mut [u8]) {

// }

pub fn encod_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

#[cfg(test)]
mod test {
    #[test]
    fn coding_test() {
        let v: Vec<u8> = [0x00, 0x12, 0x13, 0x14].to_vec();
        let mut encode = Vec::with_capacity(v.len() + 2);
        encode.extend(
            v.iter().flat_map(|&val|
                match val {
                    0x00 => vec![0x00, 0xff],
                    b => vec![val],
                }
            )
            .chain(vec![0x00, 0x00])
        );
        println!("{:?}", encode);
     
    }
}