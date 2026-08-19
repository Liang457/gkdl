use anyhow::Result;
use muda::{Menu, MenuEvent, MenuItem};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};
use winit::application::ApplicationHandler;
use winit::event_loop::{ActiveEventLoop, ControlFlow, EventLoop};
#[cfg(target_os = "windows")]
use winit::platform::windows::EventLoopBuilderExtWindows;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayCommand {
    Quit,
}

/// 从内嵌的 assets/icon.ico 解码图像作为托盘图标（与 exe 图标同源，单一来源）。
/// 优先取 32x32 帧；缺失时回退到尺寸最大的图像，避免图标缺少 32x32 帧导致整个托盘启动失败。
fn build_icon() -> Result<Icon> {
    let bytes = include_bytes!("../../assets/icon.ico");
    let dir = ico::IconDir::read(std::io::Cursor::new(bytes))?;
    let entry = dir
        .entries()
        .iter()
        .filter(|e| e.width() == 32)
        .max_by_key(|e| e.bits_per_pixel())
        .or_else(|| dir.entries().iter().max_by_key(|e| e.width() * e.height()))
        .and_then(|e| e.decode().ok())
        .ok_or_else(|| anyhow::anyhow!("icon.ico 中没有可解码的图像"))?;
    let (w, h) = (entry.width(), entry.height());
    Ok(Icon::from_rgba(entry.rgba_data().to_vec(), w, h)?)
}

struct TrayApp {
    tx: mpsc::UnboundedSender<TrayCommand>,
    config_dir: PathBuf,
    log_dir: PathBuf,
    aria_ng_url: Option<String>,
    open_config_id: muda::MenuId,
    open_log_id: muda::MenuId,
    open_aria_ng_id: Option<muda::MenuId>,
    quit_id: muda::MenuId,
    _tray: TrayIcon,
}

impl TrayApp {
    fn poll_menu(&self) {
        let rx = MenuEvent::receiver();
        while let Ok(ev) = rx.try_recv() {
            if ev.id == self.open_config_id {
                open_folder(&self.config_dir);
            } else if ev.id == self.open_log_id {
                open_folder(&self.log_dir);
            } else if self.open_aria_ng_id.as_ref().is_some_and(|id| ev.id == *id) {
                if let Some(url) = &self.aria_ng_url {
                    open_browser(url);
                }
            } else if ev.id == self.quit_id {
                let _ = self.tx.send(TrayCommand::Quit);
            }
        }
    }
}

impl ApplicationHandler for TrayApp {
    fn resumed(&mut self, _el: &ActiveEventLoop) {}

    fn window_event(
        &mut self,
        _el: &ActiveEventLoop,
        _id: winit::window::WindowId,
        _event: winit::event::WindowEvent,
    ) {
    }

    fn about_to_wait(&mut self, el: &ActiveEventLoop) {
        self.poll_menu();
        el.set_control_flow(ControlFlow::WaitUntil(
            Instant::now() + Duration::from_millis(200),
        ));
    }

    fn user_event(&mut self, _el: &ActiveEventLoop, _event: ()) {
        self.poll_menu();
    }
}

/// 在独立线程启动托盘 + winit 事件循环。
/// 菜单动作通过 `tx` 发送给 daemon 主循环；打开文件夹/浏览器在托盘线程内完成。
pub fn spawn_tray(
    tx: mpsc::UnboundedSender<TrayCommand>,
    config_dir: PathBuf,
    log_dir: PathBuf,
    aria_ng_url: Option<String>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        if let Err(e) = run_tray(tx, config_dir, log_dir, aria_ng_url) {
            tracing::warn!("托盘启动失败: {e}");
        }
    })
}

fn run_tray(
    tx: mpsc::UnboundedSender<TrayCommand>,
    config_dir: PathBuf,
    log_dir: PathBuf,
    aria_ng_url: Option<String>,
) -> Result<()> {
    let mut builder = EventLoop::builder();
    #[cfg(target_os = "windows")]
    builder.with_any_thread(true);
    let event_loop = builder.build()?;

    let menu = Menu::new();
    let open_config = MenuItem::new("打开设置文件夹", true, None);
    let open_log = MenuItem::new("打开日志文件夹", true, None);
    let quit_item = MenuItem::new("退出", true, None);
    menu.append(&open_config)?;
    menu.append(&open_log)?;
    let open_aria_ng_id = if let Some(url) = &aria_ng_url {
        if url.is_empty() {
            None
        } else {
            let item = MenuItem::new("打开 AriaNG", true, None);
            let id = item.id().clone();
            menu.append(&item)?;
            Some(id)
        }
    } else {
        None
    };
    menu.append(&quit_item)?;

    let icon = build_icon()?;
    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("gkdl 下载器")
        .with_icon(icon)
        .build()?;

    let mut app = TrayApp {
        tx,
        config_dir,
        log_dir,
        aria_ng_url,
        open_config_id: open_config.id().clone(),
        open_log_id: open_log.id().clone(),
        open_aria_ng_id,
        quit_id: quit_item.id().clone(),
        _tray: tray,
    };
    event_loop.run_app(&mut app)?;
    Ok(())
}

fn open_browser(url: &str) {
    // 使用 ShellExecuteW 而非 cmd /c start，避免杀软启发式检测
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::UI::Shell::ShellExecuteW;
        let url_wide: Vec<u16> = std::ffi::OsStr::new(url)
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let op: Vec<u16> = "open".encode_utf16().chain(std::iter::once(0)).collect();
        unsafe {
            ShellExecuteW(
                std::ptr::null_mut(),
                op.as_ptr(),
                url_wide.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                1, // SW_SHOWNORMAL
            );
        }
    }
    #[cfg(not(windows))]
    {
        let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    }
}

fn open_folder(path: &Path) {
    let _ = std::fs::create_dir_all(path);
    let _ = std::process::Command::new("explorer").arg(path).spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icon_builds() {
        assert!(build_icon().is_ok());
    }
}
