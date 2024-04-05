// 提供一个表示Redis协议帧的类型以及实用程序，解析字节数组中的帧。

use bytes::{Buf, Bytes};
pub use core::prelude::rust_2021::*;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

// redis 协议帧（字符串，错误，int，bytes，帧数组）
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    // 没有足够的信息转换
    Incomplete,

    // 无效编码
    // pub type Error = Box<dyn std::error::Error + Send + Sync>;
    Other(crate::Error),
}

impl Frame {
    // 返回一个空帧数组
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    // 如果这个self帧完成初始化，则在数组中 push 一个 bytes
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    // 如果这个self帧完成初始化，则在数组中 push 一个 int
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    // 检查是否可以从`src`解码整个消息（src 为一个光标指针）
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            // + 获取下一行
            // - 获取下一行
            // : 获取下一行（数字）
            // $ 如果下一个是 - 跳过 4 字节
            // $ 否则获取下一行（数字），然后跳过长度为：数字+2
            // * 获取下一个数字n，然后循环 n 次 check
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // Read the bulk string
                    let len: usize = get_decimal(src)?.try_into()?;

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    // 消息通过检查
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // 获取下一行，转为 string ，封装成Simple帧返回
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                // 获取下一行，转为 string ，封装成Error帧返回
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                // 获取下一行，转为 u64 ，封装成Integer帧返回
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                // 如果下一个为 - 则获取下一行，如果获取到的下一行为-1 则错误，否则返回 null
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }
                    Ok(Frame::Null)
                } else {
                    // 如果下一个为数组，则获取数字，数字+2 表示长度，将数据拷贝出来封装成 Bulk 并返回
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // 跳过 n 个字节 + 2 (\r\n).
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                // 获取数字，并 new 数组，并递归继续转换帧。
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    // 帧发生错误，转换
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into()
    }
}

// 判断字符串与帧是否等价（Simple，Bulk）才能对比
impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

// 为帧实现 Display 的方法（可以输出）
impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // use space as the array element display separator
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}

// 获取下一个 u8 字节，但是不改变指针位置
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    // 判断 src 是否结束
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    // src.chunk() 方法返回一个对当前游标位置之后的切片的引用，而不会移动游标
    Ok(src.chunk()[0])
}

// 获取下一个 u8
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

// 跳过 n 个字节
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}

/// 读取一行文本，将文本转为数字
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;
    let line = get_line(src)?;
    // 转为 u8
    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// 寻找相关行
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // 获取 Cursor 当前的位置作为起始点。
    let start = src.position() as usize;
    // 获取字节流的长度减一作为结束点。减 1 方便匹配\r\n
    let end = src.get_ref().len() - 1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // 重新设置指针位置
            src.set_position((i + 2) as u64);
            // 返回读取到的值
            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::Incomplete)
}


// 实现各种字符串错误返回情况（String，str，utf8，int，Error）
impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
