
mod kv;
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}


pub trait Store {
    fn set() {}

    fn get() {} 

    fn delete() {}

    fn flush() {}

    fn scan() {}
}

