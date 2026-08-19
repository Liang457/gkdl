//! Windows 构建脚本：为可执行文件嵌入版本信息资源（VERSIONINFO）与程序图标（icon.ico）。
//! 带有完整版本信息的 PE 文件可降低杀软启发式误报概率。

fn main() {
    #[cfg(windows)]
    {
        let mut res = winresource::WindowsResource::new();
        // 若需更换程序图标，直接替换 assets/icon.ico（合法多尺寸 ICO）即可。
        let icon_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("assets")
            .join("icon.ico");
        if icon_path.exists() {
            if let Some(p) = icon_path.to_str() {
                res.set_icon(p);
            } else {
                eprintln!("cargo:warning=assets/icon.ico 路径不是有效 UTF-8，跳过程序图标嵌入");
            }
        } else {
            eprintln!("cargo:warning=未找到 assets/icon.ico，跳过程序图标嵌入");
        }
        res.set("ProductName", "GKDL");
        res.set("FileDescription", "GKDL Multi-threaded Downloader");
        res.set("LegalCopyright", "Copyright (C) 2024-2026");
        res.set("OriginalFilename", "gkdl.exe");
        res.set(
            "ProductVersion",
            &format!(
                "{}.{}.{}",
                env!("CARGO_PKG_VERSION_MAJOR"),
                env!("CARGO_PKG_VERSION_MINOR"),
                env!("CARGO_PKG_VERSION_PATCH")
            ),
        );
        if let Err(e) = res.compile() {
            eprintln!("cargo:warning=Windows 资源编译失败（不影响构建）: {e}");
        }
    }
}
