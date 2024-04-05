use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use crate::entity::Frame;
use crate::entity::Error;

// 从远程对等端发送和接收`Frame`值。当实现网络协议时，该协议上的消息通常由几个称为帧的较小消息组成。
// "Connection"的目的是在底层"TcpStream"上读写帧。
// 为了读取帧，"Connection"使用一个内部缓冲区，该缓冲区被填满，直到有足够的字节创建一个完整的帧。
// 一旦发生这种情况，`Connection`创建帧并将其返回给调用者。发送帧时，首先将帧编码到写入缓冲区。然后写入缓冲区的内容被写入套接字。
#[derive(Debug)]
pub struct Connection {
    // 可从中读写帧
    stream: BufWriter<TcpStream>,

    // 缓冲区，可将 stream中的帧写入缓冲区
    buffer: BytesMut,
}

impl Connection {
    // 通过TcpStream创建一个连接，连接包括写入流，和缓冲区
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 默认为4KB读缓冲区。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从stream中读取一个"Frame"值。
    /// 函数等待，直到检索到足够的数据来解析帧。在解析帧之后，读缓冲区中剩余的任何数据都将保留在那里，以备下次调用"read_frame"。
    ///
    /// # Returns
    ///
    /// 成功后，返回接收到的帧。如果`TcpStream`将一个帧分开，返回错误。
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 读取一个帧
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 将 stream 中的数据写入 buffer 中
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // buffer空时才关闭
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err("connection reset by peer".into())
                }
            }
        }
    }

    // 将 buffer 中的数据转为帧
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        // 检查是否缓冲了足够的数据来解析单个帧。（能否有一行数据）
        match Frame::check(&mut buf) {
            Ok(_) => {
                // 获取帧长度
                let len = buf.position() as usize;

                // 读指针设为 0
                buf.set_position(0);

                // 将 buf 内容转为帧（开头为一个符号，结尾为\r\n）
                let frame = Frame::parse(&mut buf)?;

                // 前进 n 个位置
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // 数据没有传送完成

            Err(Error::Incomplete) => Ok(None),
            // 其他错误
            Err(e) => Err(e.into()),
        }
    }

    /// 将帧写入 tcpstream 中
    /// 使用由`AsyncWrite`提供的各种`write_*`函数将`Frame`值写入套接字。
    /// 不建议直接在`TcpStream`上调用这些函数，因为这将导致大量的系统调用。
    /// 但是，在缓冲写流上调用这些函数是可以的。数据将被写入缓冲区。一旦缓冲区满了，它就会刷新到底层套接字。
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                // 编码帧类型前缀。数组为'*'。
                self.stream.write_u8(b'*').await?;
                // 编码数组的长度。
                self.write_decimal(val.len() as u64).await?;

                // 遍历数组内的值，写入帧，不同数据不同前缀
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 匹配不上直接写入
            _ => self.write_value(frame).await?,
        }

        // 用"flush"将缓冲区的剩余内容写入流中而不是留在缓冲区
        self.stream.flush().await
    }

    /// 将帧写入tcp流
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'#').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::USize(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b'=').await?;
                self.write_i64(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 不支持递归调用
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
    async fn write_i64(&mut self, val: i64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
