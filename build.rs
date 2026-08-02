//! Windows 构建脚本：为可执行文件嵌入版本信息资源（VERSIONINFO）。
//! 带有完整版本信息的 PE 文件可降低杀软启发式误报概率。

fn main() {
    #[cfg(windows)]
    {
        let mut res = winresource::WindowsResource::new();
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
