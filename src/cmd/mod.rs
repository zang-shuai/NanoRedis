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
}

impl Command {
    /// 从接收到的帧中解析命令。并返回
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // 先转换帧为 parse
        let mut parse = Parse::new(frame)?;

        // 命令转小写
        let command_name = parse.next_string()?.to_lowercase();

        // 匹配命令
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "pop" => Command::Pop(Pop::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "incrby" => Command::Incrby(Incrby::parse_frames(&mut parse)?),
            "lrange" => Command::Lrange(Lrange::parse_frames(&mut parse)?),
            "push" => Command::Push(Push::parse_frames(&mut parse)?),
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
            Command::Incrby(cmd) => cmd.apply(db, dst).await,
            Command::Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
