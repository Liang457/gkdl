use crate::download::config::DownloadConfig;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    pub host: String,
    pub port: u16,
    pub rpc_secret: String,
    pub no_tray: bool,
    /// 托盘「打开 AriaNG」指向的 Web UI 地址，留空则不显示该菜单项
    #[serde(alias = "aria2ng_url")]
    pub aria_ng_url: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 6800,
            rpc_secret: String::new(),
            no_tray: false,
            aria_ng_url: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
    pub file: String,
    pub max_size_mb: u64,
    pub backup_count: u32,
    pub archive_enabled: bool,
    pub retention_days: u32,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            file: "logs/gkdl.log".into(),
            max_size_mb: 10,
            backup_count: 5,
            archive_enabled: true,
            retention_days: 90,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HookConfigFile {
    /// 下载后命令配置文件（每行一条命令），相对路径基于配置目录解析；空则用默认 hooks.txt
    pub commands_file: String,
    pub timeout_sec: u64,
    /// 运行下载后命令时不弹出命令行窗口（Windows；仅影响控制台类子进程），默认 true
    pub hide_window: bool,
}

impl Default for HookConfigFile {
    fn default() -> Self {
        Self {
            commands_file: "hooks.txt".into(),
            timeout_sec: 60,
            hide_window: true,
        }
    }
}

/// 状态库配置（SQLite）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StateConfig {
    /// 状态数据库路径，空或相对路径基于配置目录解析；空则默认 state.db
    pub db_path: String,
    /// 已完成/失败记录保留天数（0 = 永久保留）
    pub retention_days: u32,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            db_path: "state.db".into(),
            retention_days: crate::app::db::DEFAULT_RETENTION_DAYS,
        }
    }
}

/// 运行时配置存储：内存中的 Config + 磁盘路径，RPC 修改设置后同步落盘。
#[derive(Clone)]
pub struct ConfigStore {
    pub path: PathBuf,
    pub inner: Arc<Mutex<Config>>,
}

impl ConfigStore {
    pub fn new(path: PathBuf, cfg: Config) -> Self {
        Self {
            path,
            inner: Arc::new(Mutex::new(cfg)),
        }
    }

    /// 把当前内存配置写回磁盘。
    pub async fn save(&self) -> Result<()> {
        let cfg = self.inner.lock().await;
        cfg.save(&self.path)
    }
}

/// 顶层 YAML 配置。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub daemon: DaemonConfig,
    pub download: DownloadConfig,
    pub log: LogConfig,
    pub hook: HookConfigFile,
    pub state: StateConfig,
}

/// 默认配置路径：%APPDATA%\gkdl\config.yaml
pub fn default_config_path() -> PathBuf {
    if let Some(appdata) = std::env::var_os("APPDATA") {
        PathBuf::from(appdata).join("gkdl").join("config.yaml")
    } else {
        PathBuf::from("gkdl").join("config.yaml")
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
        let cfg: Config = yaml_serde::from_str(&content).context("解析配置文件失败")?;
        Ok(cfg)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let yaml = yaml_serde::to_string(self).context("序列化配置失败")?;
        std::fs::write(path, yaml)
            .with_context(|| format!("写配置文件失败: {}", path.display()))?;
        Ok(())
    }

    /// 配置文件不存在则写入默认值。
    pub fn load_or_create(path: &Path) -> Result<Config> {
        if path.exists() {
            Self::load(path)
        } else {
            let cfg = Config::default();
            cfg.save(path)?;
            Ok(cfg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_roundtrip() {
        let cfg = Config::default();
        let p = std::env::temp_dir().join("gkdl_config_test.yaml");
        cfg.save(&p).unwrap();
        let loaded = Config::load(&p).unwrap();
        assert_eq!(loaded.daemon.port, 6800);
        assert_eq!(loaded.download.split, 8);
        assert_eq!(loaded.log.retention_days, 90);
        assert!(loaded.hook.hide_window);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn yaml_serde_parses() {
        let yaml = r#"
daemon:
  host: 0.0.0.0
  port: 6801
  rpc_secret: "abc"
download:
  split: 4
  max_speed: 0
log:
  level: debug
hook:
  commands_file: "C:/x/hooks.txt"
"#;
        let cfg: Config = yaml_serde::from_str(yaml).unwrap();
        assert_eq!(cfg.daemon.host, "0.0.0.0");
        assert_eq!(cfg.daemon.port, 6801);
        assert_eq!(cfg.daemon.rpc_secret, "abc");
        assert_eq!(cfg.download.split, 4);
        assert_eq!(cfg.log.level, "debug");
        assert_eq!(cfg.hook.commands_file, "C:/x/hooks.txt");
        assert!(cfg.hook.hide_window);
    }

    #[test]
    fn hook_hide_window_false_parses() {
        let yaml = r#"
hook:
  hide_window: false
"#;
        let cfg: Config = yaml_serde::from_str(yaml).unwrap();
        assert!(!cfg.hook.hide_window);
    }
}
