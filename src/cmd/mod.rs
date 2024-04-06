mod get;

pub use get::Get;


mod set;

pub use set::Set;


mod ping;

pub use ping::Ping;


mod lrange;

pub use lrange::Lrange;

mod incrby;

pub use incrby::Incrby;


mod unknown;
// mod mset;
pub mod push;

pub use push::Push;

pub mod pop;

pub use pop::Pop;

pub mod sadd;

pub use sadd::Sadd;

pub mod srem;

pub use srem::Srem;

pub mod scard;

pub use scard::Scard;

pub mod sismember;

pub use sismember::Sismember;


pub mod sismembers;

pub use sismembers::Sismembers;


pub mod sinter;

pub use sinter::Sinter;


pub mod sdiff;

pub use sdiff::Sdiff;

pub mod sunion;

pub use sunion::Sunion;


pub use unknown::Unknown;
use crate::entity::{Frame, Parse, Db};
use crate::connect::{Connection};

//共能接受 7 种命令，（最后一种为错误）
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Ping(Ping),
    Incrby(Incrby),
    Push(Push),
    Lrange(Lrange),
    Pop(Pop),
    Unknown(Unknown),
    Sadd(Sadd),
    Srem(Srem),
    Scard(Scard),
    Sismember(Sismember),
    Sismembers(Sismembers),
    Sinter(Sinter),
    Sdiff(Sdiff),
    Sunion(Sunion),
}

impl Command {
    /// 从接收到的帧中解析命令。并返回
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // 先转换帧为 parse
        let mut parse = Parse::new(frame)?;

        // 命令转小写
        let command_name = parse.next_string()?.to_lowercase();
        println!("---------------{:?}",command_name);

        // 匹配命令
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "pop" => Command::Pop(Pop::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "incrby" => Command::Incrby(Incrby::parse_frames(&mut parse)?),
            "lrange" => Command::Lrange(Lrange::parse_frames(&mut parse)?),
            "push" => Command::Push(Push::parse_frames(&mut parse)?),
            "sadd" => Command::Sadd(Sadd::parse_frames(&mut parse)?),
            "srem" => Command::Srem(Srem::parse_frames(&mut parse)?),
            "scard" => Command::Scard(Scard::parse_frames(&mut parse)?),
            "sismember" =>  Command::Sismember(Sismember::parse_frames(&mut parse)?) ,
            "sismembers" => Command::Sismembers(Sismembers::parse_frames(&mut parse)?),
            "sinter" => Command::Sinter(Sinter::parse_frames(&mut parse)?),
            "sdiff" => Command::Sdiff(Sdiff::parse_frames(&mut parse)?),
            "sunion" => Command::Sunion(Sunion::parse_frames(&mut parse)?),
            _ => {
                // 匹配到未知命令
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // 判断是否完成
        parse.finish()?;

        // 已成功解析命令
        Ok(command)
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        match self {
            Command::Get(cmd) => cmd.apply(db, dst).await,
            Command::Lrange(cmd) => cmd.apply(db, dst).await,
            Command::Pop(cmd) => cmd.apply(db, dst).await,
            Command::Set(cmd) => cmd.apply(db, dst).await,
            Command::Push(cmd) => cmd.apply(db, dst).await,
            Command::Ping(cmd) => cmd.apply(dst).await,
            Command::Unknown(cmd) => cmd.apply(dst).await,
            Command::Incrby(cmd) => cmd.apply(db, dst).await,
            Command::Sadd(cmd) => cmd.apply(db, dst).await,
            Command::Srem(cmd) => cmd.apply(db, dst).await,
            Command::Scard(cmd) => cmd.apply(db, dst).await,
            Command::Sismember(cmd) => {println!("xxx"); cmd.apply(db, dst).await },
            Command::Sismembers(cmd) => cmd.apply(db, dst).await,
            Command::Sinter(cmd) => cmd.apply(db, dst).await,
            Command::Sdiff(cmd) => cmd.apply(db, dst).await,
            Command::Sunion(cmd) => cmd.apply(db, dst).await,
        }
    }
}
