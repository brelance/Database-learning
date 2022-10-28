use crate::{error::{Error, Result}};
use super::Value;

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
        match byte {
            0x00 => vec![0x00, 0xff],
            b => vec![b],
        }).chain(vec![0x00, 0x00]));

    encoded
}

pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty() {
        return Err(Error::Internal("take a empty bytes".into()));
    }
    let mut decoded = Vec::new();
    let mut iter = bytes.iter().enumerate();
    let index = loop {
        match iter.next().map(|(_, &byte)| byte) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,
                Some((_ , 0xff)) => decoded.push(0x00),
                Some((_, b)) => return Err(Error::Value(format!("error decode bytes in {:?}", b))),
                None => return Err(Error::Internal("encode error".to_string())),
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

pub fn decode_f64(mut bytes: [u8; 8]) -> f64 {
    if bytes[0] & 1 << 7 == 1 {
        bytes[0] ^= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|byte| *byte = !*byte);
    }
    f64::from_be_bytes(bytes)
}

pub fn take_f64(bytes: &mut &[u8]) -> Result<f64> {
    let mut val = take_bytes(bytes)?;
    Ok(decode_f64(bytes[0..8].try_into()?))
}

pub fn encode_i64(val: i64) -> [u8; 8] {
    let mut bytes = val.to_be_bytes();
    bytes[0] ^= 1 << 7;
    bytes
}

pub fn decode_i64(mut bytes: [u8; 8]) -> i64 {
    bytes[0] ^= 1 << 7;
    i64::from_be_bytes(bytes)
}

pub fn take_i64(bytes: &mut &[u8]) -> Result<i64> {
    if bytes.len() < 8 {
        return Err(Error::Internal(format!("Unable to decode i64 from {} bytes", bytes.len())));
    }
    let i = decode_i64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(i)
}

pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

pub fn decode_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

pub fn take_u64(bytes: &mut &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(Error::Internal("error take_u64".to_string()));
    }
    let n = u64::from_be_bytes(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}
pub fn encode_string(val: &str) -> Vec<u8> {
    encode_bytes(val.as_bytes())
}

pub fn take_string(bytes: &mut &[u8]) -> Result<String> {
    Ok(String::from_utf8(take_bytes(bytes)?)?)
}


//use val: &Value?
pub fn encode_value(val: Value) -> Vec<u8> {
    match val {
        Value::Null => vec![0x00],
        Value::Boolean(b) => vec![0x01, encode_boolean(b)],
        Value::Integer(i) => [&[0x02][..], &encode_i64(i)].concat(),
        Value::Float(f) => [&[0x03][..], &encode_f64(f)].concat(),
        Value::String(s) => [&[0x04][..], &encode_string(s.as_str())].concat(),
    }
}

pub fn take_value(bytes: &mut &[u8]) -> Result<Value> {
    match take_byte(bytes)? {
        0x00 => Ok(Value::Null),
        0x01 => Ok(Value::Boolean(take_boolean(bytes)?)),
        0x02 => Ok(Value::Integer(take_i64(bytes)?)),
        0x03 => Ok(Value::Float(take_f64(bytes)?)),
        0x04 => Ok(Value::String(take_string(bytes)?)),
        _ => return Err(Error::Internal("take error".to_string()))
    }
}


#[cfg(test)]
mod test {


    use super::*;
    #[test]
    fn encode_boolean() -> Result<()> {
        use super::encode_boolean;
        assert_eq!(encode_boolean(false), 0x00);
        assert_eq!(encode_boolean(true), 0x01);
        Ok(())
    }

    #[test]
    fn decode_boolean() -> Result<()> {
        use super::decode_boolean;
        assert_eq!(decode_boolean(0x00)?, false);
        assert_eq!(decode_boolean(0x01)?, true);
        Ok(())
    }

    #[test]
    fn take_boolean() -> Result<()> {
        use super::take_boolean;
        let mut bytes: &[u8] = &[0x00, 0xaf];
        take_boolean(&mut bytes)?;
        Ok(())
    }

    #[test]
        fn encode_bytes() {
        use super::encode_bytes;
        assert_eq!(encode_bytes(&[]), vec![0x00, 0x00]);
        assert_eq!(encode_bytes(&[0x01, 0x02, 0x03]), vec![0x01, 0x02, 0x03, 0x00, 0x00]);
        assert_eq!(encode_bytes(&[0x00, 0x01, 0x02]), vec![0x00, 0xff, 0x01, 0x02, 0x00, 0x00]);
    }

    #[test]
    fn take_bytes() -> Result<()> {
        use super::encode_bytes;
        use super::take_bytes;
        let mut bytes: &[u8] = &[];
        assert!(take_bytes(&mut bytes).is_err());
        
        let mut bytes: &[u8] = &[0x00, 0x00];
        assert_eq!(take_bytes(&mut bytes)?, Vec::<u8>::new());

        let mut bytes: &[u8] = &[0x01, 0x02, 0x00, 0x00];
        assert_eq!(take_bytes(&mut bytes)?, vec![0x01, 0x02]);

        let mut bytes: &[u8] = &[0x00, 0xff, 0x01, 0x02, 0x00, 0x00];
        assert_eq!(take_bytes(&mut bytes)?, &[0x00, 0x01, 0x02]);
        assert!(bytes.is_empty());

        assert!(take_bytes(&mut &[0x00][..]).is_err());
        assert!(take_bytes(&mut &[0x01][..]).is_err());
        assert!(take_bytes(&mut &[0x00, 0x01, 0x00, 0x00][..]).is_err());
        
        let mut bytes: &[u8] = &[&encode_u64(1), encode_bytes(&[0xff, 0x01, 0x02, 0x00, 0x00]).as_slice()].concat();
        
        assert_eq!(1, take_u64(&mut bytes)?);
        assert_eq!(vec![0xff, 0x01, 0x02, 0x00, 0x00], take_bytes(&mut bytes)?);

        
        Ok(())
        
    }


}