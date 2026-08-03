# AGENTS.md

Rust async (tokio) multi-threaded downloader with an aria2/AriaNg-compatible JSON-RPC daemon and Windows system tray. Binary + library in one crate (`src/main.rs` + `src/lib.rs`).

## Commands

- Build release: `cargo build --release` → `target\release\gkdl.exe`
- Typecheck: `cargo check --all-targets`
- Test: `cargo test` (all pass in ~19s; the `tests/rpc_integration.rs` suite alone is ~16s, the rest is fast)
- Single test: `cargo test rate_limit_slows_download`
- Format check: `cargo fmt --check`
- Lint check: `cargo clippy --all-targets -- -D warnings`
- No lint/fmt config exists; keep to default `cargo fmt`/clippy.

## Architecture

- `src/download/` — engine internals. `config.rs` holds `DownloadConfig` (split/slow/steal tuning + UA/Referer/header, defaults `steal_ratio` 0.4, `slow_ratio` 0.3, `slow_confirm` 3); `engine.rs` probes (GET `Range: bytes=0-0`, never trust HEAD) and wires everything; `scheduler.rs` is the work-stealing scheduler; `worker.rs` per-connection loop; `detector.rs` slow-thread detection; `rate_limit.rs` global token bucket; `mmap_writer.rs` writes file via mmap; `resume.rs` `.gkdl` control file; `source.rs` multi-source failover; `segment.rs` segment bookkeeping.
- `src/rpc/` — aria2-compatible JSON-RPC. `server.rs` routes HTTP POST / GET JSONP / WebSocket on `/jsonrpc`, plus `/` status page; `methods.rs` dispatches `aria2.*`; `protocol.rs` request parsing.
- `src/task_manager.rs` — `TaskManager` bridges engine and RPC: GID registry, status FSM, `broadcast` events → WS notifications, post-download hook, error → aria2 code mapping (`map_error_code`: 24 checksum, 2 timeout, 3 404, 6 network, else 1).
- `src/logging.rs` — rotating file + stdout dual writer; archives stale logs at startup (tar.xz → gz → tar fallback).
- `src/config.rs` (YAML at `%APPDATA%\gkdl\config.yaml`, priority: defaults < file < CLI), `src/hooks.rs`, `src/hash.rs`, `src/tray.rs`, `src/cli.rs`.

## Conventions & gotchas

- **All comments, doc strings, and error messages are in Chinese.** Match this in new code.
- Windows-only in practice (tray via `tray-icon`/`muda`/`winit`). `reqwest` 0.13 is built without default features with the `rustls` feature — no OpenSSL/libcurl. Note that rustls now pulls in `aws-lc-rs` via the platform verifier, so building requires a C toolchain + cmake on PATH (works on this machine).
- Config YAML is parsed with **`yaml_serde`** (0.10), not `serde_yaml`. (Historically was `serde_yml`; the switch happened in commit `cf8817a`.)
- `README.md` (Chinese) mirrors this architecture and lists every CLI/daemon flag — prefer it over `src/cli.rs` for flag semantics.
- aria2 RPC quirks (locked in by `tests/rpc_integration.rs`): missing `params` treated as empty array; wrong token → 1s delay then HTTP 400; `system.multicall` carries token per inner call, not top-level; `getVersion`/`getGlobalStat` are unauthenticated.
- Resume: control file `<file>.gkdl` (JSON) saved ~1s by the monitor via a `.gkdl.tmp` temp file; resume only applies when `total` and `urls` match the probe, else it restarts; deleted on success.
- **Work stealing** is the core algorithm: idle worker steals the slowest segment's tail 40%; slow thread (speed < 30% of median, confirmed 3×) is killed and its segment returned. Design docs in `doc/` (Chinese) — read `doc/多线程下载算法设计.md` and `doc/慢线程拖累问题的处理方式.md` before touching `scheduler.rs`/`detector.rs`.
- `scheduler.rs` deliberately uses `tokio::sync::Mutex` (not std) to avoid blocking runtime threads. Keep that.
- Integration tests (`tests/`) use a hand-rolled HTTP server in `tests/common/mod.rs` (Range support, `/fail` and `/broken` 404 paths, ignore-range mode) and write artifacts to `%TEMP%\gkdl_it`. Don't expect real network.
- `file.bin` (256 KiB) at repo root is a test artifact, **regenerated every `cargo test`**: `tests/rpc_integration.rs` `rpc_server_serves_aria2_methods` calls `aria2.addUri` with no options (no `dir`), so `TaskManager::add_download` resolves the relative filename against CWD (crate root). It's gitignored — don't commit it. (`libcurl-x64.dll` at root is a stale leftover, also gitignored, unused.)
