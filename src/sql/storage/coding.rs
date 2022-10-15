use crate::{error::{Error, Result}, util::coding};

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
        return Err(Error::Internal("empty bytes cannot take byte".to_string()));
    }
    let byte = bytes[0];
    *bytes = &bytes[1..];
    Ok(byte)
}

pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(bytes.len() + 2);
    encoded.extend(bytes.iter().flat_map(|&byte| 
        match  byte {
            0x00 => vec![0x00, 0xff],
            b => vec![b],
        }).chain(vec![0x00, 0x00]));

    encoded

}

pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>>{
    let mut decoded = Vec::new();
    let mut iter = bytes.iter().enumerate();
    let index = loop {
        match iter.next().map(|(_, &byte)| byte) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,
                Some((_ , 0xff)) => decoded.push(0x00),
                Some((_, b)) => return Err(Error::Value(format!("error decode bytes in {:?}", b))),
                None => return Err(Error::Internal(("encode error".to_string()))),
            }
            Some(b) => decoded.push(b),
            None => return Err(Error::Internal("encode error".to_string())),
        }
    };
    *bytes = &bytes[index..];
    Ok(decoded)
}

pub fn encode_f64(val: f64) -> [u8; 8] {
    let mut bytes = val.to_be_bytes();
    if bytes[0] & 1 << 7 == 0 {
        bytes[0] ^= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|byte| *byte = !*byte);
    }
    bytes
}

pub fn decode_f64(bytes: &mut &[u8]) -> f64 {
    if bytes[0] & 1 << 7 == 1 {
        bytes[0] ^= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|byte| *byte = !*byte);
    }
    f64::from_be_bytes(bytes)
}

pub fn take_f64(bytes: &mut &[u8]) -> Result<f64> {
    let mut val = take_bytes(bytes)?;
    decode_f64(val.as_mut_slice());
}

pub fn encode_i64(val: i64) -> [u8; 8] {
    let mut bytes = val.to_be_bytes();
    bytes[0] ^= 1 << 7;
    bytes
}

pub fn decode_i64(bytes)

pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

#[cfg(test)]
mod test {
    #[test]
    fn coding_test() {
        let v: Vec<u8> = [0x11, 0x12, 0x13, 0x14].to_vec();
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