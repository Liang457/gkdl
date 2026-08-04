use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;

/// 下载后命令配置。
#[derive(Debug, Clone)]
pub struct HookConfig {
    /// 命令列表文件：每行一条命令，下载完成后逐条执行，下载文件路径作为最后一个参数。
    pub commands_file: Option<PathBuf>,
    /// 旧式单脚本方式（CLI --post-script 使用）。
    pub script: Option<PathBuf>,
    pub timeout_sec: u64,
}

impl Default for HookConfig {
    fn default() -> Self {
        Self {
            commands_file: None,
            script: None,
            timeout_sec: 60,
        }
    }
}

/// 传递给命令的上下文。
#[derive(Clone, Copy)]
pub struct HookContext<'a> {
    pub file_path: &'a Path,
    pub url: &'a str,
    pub gid: &'a str,
    pub size: u64,
    pub sha256: Option<&'a str>,
    pub timeout: u64,
}

/// 读取命令列表文件：每行一条命令，空行与 `#` 注释被忽略。
pub fn load_commands(path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("读取命令配置文件失败: {}", path.display()))?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect())
}

/// 简单的命令行切分：支持双引号包裹的空格，双引号内不切分。
fn split_command(line: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for c in line.chars() {
        match c {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

fn build_command(program: &str, args: &[String], file_str: &str) -> Command {
    let is_ps1 = Path::new(program)
        .extension()
        .map(|e| e.eq_ignore_ascii_case("ps1"))
        .unwrap_or(false);
    let mut cmd = if is_ps1 {
        // 使用 pwsh（PowerShell 7）优先，回退到 powershell；
        // 不使用 -ExecutionPolicy Bypass（易触发杀软启发式检测），
        // 用户应通过 Set-ExecutionPolicy 或签名策略管理执行策略。
        let mut c = Command::new("powershell");
        c.arg("-NoProfile")
            .arg("-NonInteractive")
            .arg("-File")
            .arg(program);
        c
    } else {
        Command::new(program)
    };
    cmd.args(args).arg(file_str);
    cmd
}

/// 逐行转发子进程 stdout/stderr 到日志，等待退出（超时则终止）。
async fn spawn_and_wait(mut cmd: Command, timeout_sec: u64) -> Result<()> {
    cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;

    // 逐行转发 stdout / stderr 到日志
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let out_task = stdout.map(|o| {
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut reader = BufReader::new(o).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                tracing::info!("[hook stdout] {}", line);
            }
        })
    });
    let err_task = stderr.map(|e| {
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut reader = BufReader::new(e).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                tracing::warn!("[hook stderr] {}", line);
            }
        })
    });

    let timeout = Duration::from_secs(timeout_sec);
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(st)) => st,
        Ok(Err(e)) => {
            tracing::warn!("命令执行出错: {e}");
            let _ = child.kill().await;
            return Ok(());
        }
        Err(_) => {
            tracing::warn!("命令执行超时（>{}s），已终止", timeout_sec);
            let _ = child.kill().await;
            return Ok(());
        }
    };

    if let Some(t) = out_task {
        t.await.ok();
    }
    if let Some(t) = err_task {
        t.await.ok();
    }

    tracing::info!("命令退出码: {}", status.code().unwrap_or(-1));
    Ok(())
}

fn apply_env(cmd: &mut Command, ctx: &HookContext<'_>) {
    cmd.env("GKDL_URL", ctx.url)
        .env("GKDL_GID", ctx.gid)
        .env("GKDL_SIZE", ctx.size.to_string());
    if let Some(sha) = ctx.sha256 {
        cmd.env("GKDL_SHA256", sha);
    }
}

/// 运行下载后命令（一条）：把下载文件路径作为最后一个参数传入。
pub async fn run_post_download_command(command: &str, ctx: HookContext<'_>) -> Result<()> {
    let parts = split_command(command);
    if parts.is_empty() {
        return Ok(());
    }
    let program = parts[0].clone();
    let args = &parts[1..];
    let file_str = ctx.file_path.display().to_string();

    tracing::info!("运行下载后命令: {} \"{}\"", command, file_str);

    let mut cmd = build_command(&program, args, &file_str);
    apply_env(&mut cmd, &ctx);
    spawn_and_wait(cmd, ctx.timeout).await
}

/// 依次运行下载后命令列表，单条失败不中断后续命令。
pub async fn run_post_download_commands(commands: &[String], ctx: HookContext<'_>) -> Result<()> {
    for command in commands {
        if let Err(e) = run_post_download_command(command, ctx).await {
            tracing::warn!("下载后命令执行失败: {e}");
        }
    }
    Ok(())
}

/// 运行下载后脚本（旧式单脚本方式）：把下载文件路径作为参数传入。
pub async fn run_post_download_script(script: &Path, ctx: HookContext<'_>) -> Result<()> {
    let script = script.to_path_buf();
    let file_str = ctx.file_path.display().to_string();

    tracing::info!("运行下载后脚本: {} \"{}\"", script.display(), file_str);

    let mut cmd = build_command(script.to_str().unwrap_or_default(), &[], &file_str);
    apply_env(&mut cmd, &ctx);
    spawn_and_wait(cmd, ctx.timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_default_config() {
        let cfg = HookConfig::default();
        assert!(cfg.commands_file.is_none());
        assert!(cfg.script.is_none());
        assert_eq!(cfg.timeout_sec, 60);
    }

    #[test]
    fn commands_file_skips_comments_and_blanks() {
        let p = std::env::temp_dir().join(format!("gkdl_cmds_{}.txt", std::process::id()));
        std::fs::write(
            &p,
            "# 注释\n\n  echo hello  \n\"C:/Program Files/x.exe\" --opt\n",
        )
        .unwrap();
        let cmds = load_commands(&p).unwrap();
        assert_eq!(
            cmds,
            vec![
                "echo hello".to_string(),
                "\"C:/Program Files/x.exe\" --opt".to_string()
            ]
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn split_command_respects_quotes() {
        assert_eq!(split_command("a b c"), vec!["a", "b", "c"]);
        assert_eq!(
            split_command("\"C:/Program Files/x.exe\" --opt"),
            vec!["C:/Program Files/x.exe", "--opt"]
        );
        assert_eq!(split_command("   "), Vec::<String>::new());
    }
}
