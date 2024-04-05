pub mod cmd;

pub use cmd::Command;

pub mod entity;

pub mod connect;

pub const DEFAULT_PORT: u16 = 6379;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
