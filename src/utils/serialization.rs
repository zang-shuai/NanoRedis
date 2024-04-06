use bytes::{Bytes, Buf, BytesMut, BufMut};
use serde::{Serialize, Deserialize};
use std::collections::{LinkedList, HashSet, HashMap};
use serde::de::DeserializeOwned;
use std::str::{self,Utf8Error};
use std::num::ParseIntError;
use crate::entity::Frame::Error;

pub fn string_to_bytes(s: &str) -> Bytes {
    Bytes::from(bincode::serialize(s).unwrap())
}

pub fn linked_list_to_bytes(list: LinkedList<String>) -> Bytes {
    Bytes::from(bincode::serialize(&list).unwrap())
}

pub fn hash_set_to_bytes<T: Serialize>(set: &HashSet<T>) -> Bytes {
    Bytes::from(bincode::serialize(set).unwrap())
}

pub fn hash_map_to_bytes<K: Serialize, V: Serialize>(map: &HashMap<K, V>) -> Bytes {
    Bytes::from(bincode::serialize(map).unwrap())
}


pub fn bytes_to_string(bytes: &Bytes) -> String {
    bincode::deserialize(bytes).unwrap()
}

pub fn bytes_to_linked_list<T: DeserializeOwned>(bytes: &Bytes) -> LinkedList<T> {
    bincode::deserialize(bytes).unwrap()
}

pub fn bytes_to_hash_set<T: DeserializeOwned + std::cmp::Eq + std::hash::Hash>(bytes: &Bytes) -> HashSet<T> {
    bincode::deserialize(bytes).unwrap()
}

pub fn bytes_to_hash_map<K: DeserializeOwned + std::cmp::Eq + std::hash::Hash, V: DeserializeOwned>(bytes: &Bytes) -> HashMap<K, V> {
    bincode::deserialize(bytes).unwrap()
}

// 将 i64 转换为 Bytes
pub fn i64_to_bytes(value: i64) -> Bytes {
    let mut bytes = BytesMut::with_capacity(8);
    bytes.put_i64(value);
    bytes.freeze()
}

pub fn f64_to_bytes(value: f64) -> Bytes {
    let mut bytes = BytesMut::with_capacity(8);
    bytes.put_f64(value);
    bytes.freeze()
}

use crate::entity::ParseError;

// 将 Bytes 转换回 i64
pub fn bytes_to_i64(bytes: Bytes) ->  Result<i64, Box<dyn std::error::Error>> {
    let num_str = str::from_utf8(&bytes)?;
    let num = num_str.parse::<i64>()?;
    Ok(num)
}


// 将 Bytes 转换回 f64
pub fn bytes_to_f64(bytes: Bytes) -> f64 {
    let mut buf = bytes;
    buf.get_f64()
}
