use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// mrs-speaker CLI
#[derive(Parser, Debug)]
#[command(
    name = "mrs-speaker",
    author,
    version,
    about = "mrs-speaker CLI",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

/// sub-command selection
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 在安装 Magisk 模块后调用
    MagiskInstall(MagiskCommonArgs),

    /// 运行 Magisk Daemon 服务
    MagiskDaemon(MagiskCommonArgs),

    /// 执行 Magisk 操作
    MagiskAction(MagiskCommonArgs),

    /// 在卸载 Magisk 模块时调用
    MagiskUninstall(MagiskCommonArgs),

    /// 运行 Daemon 服务
    Daemon(DaemonArgs),
}

/// Magisk 公共选项
#[derive(Args, Debug)]
pub struct MagiskCommonArgs {
    /// 模块 ID
    #[arg(long, required = true)]
    pub module_id: String,

    /// 模块目录路径
    #[arg(long, required = true)]
    pub module_path: PathBuf,

    /// 临时目录路径
    #[arg(long, required = true)]
    pub temp_path: PathBuf,
}

/// daemon 选项
#[derive(Args, Debug)]
pub struct DaemonArgs {
    /// 配置目录路径
    #[arg(long, required = true)]
    pub conf_path: PathBuf,

    /// 临时目录路径
    #[arg(long, required = true)]
    pub temp_path: PathBuf,
}
