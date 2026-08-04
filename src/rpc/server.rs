use super::methods::{check_token, dispatch, MethodCtx};
use super::protocol::{self, RpcOut};
use super::{GlobalOptionsRef, ShutdownHandle};
use crate::app::config::ConfigStore;
use crate::app::task_manager::{TaskEvent, TaskManager};
use anyhow::{Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::FromRequestParts;
use axum::extract::{Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

pub struct AppState {
    pub mgr: Arc<TaskManager>,
    pub global: GlobalOptionsRef,
    pub session_id: String,
    pub shutdown: ShutdownHandle,
    pub secret: String,
    pub config: Option<Arc<ConfigStore>>,
}

pub struct RpcHttpResponse {
    pub status: StatusCode,
    pub body: String,
}

impl IntoResponse for RpcHttpResponse {
    fn into_response(self) -> Response {
        (
            self.status,
            [
                ("Content-Type", "application/json-rpc; charset=utf-8"),
                ("Access-Control-Allow-Origin", "*"),
            ],
            self.body,
        )
            .into_response()
    }
}

fn make_out(id: Option<Value>, result: Option<Value>, code: i32, message: &str) -> RpcOut {
    RpcOut::Response {
        id,
        result,
        error: Some(super::protocol::RpcErrorBody {
            code,
            message: message.into(),
        }),
    }
}

/// 需要顶层 token 鉴权的方法。与 aria2 一致：system.multicall / listMethods /
/// listNotifications 由各自实现鉴权或免鉴权，getVersion / getGlobalStat 本实现放行。
fn needs_auth(method: &str) -> bool {
    !matches!(
        method,
        "aria2.getVersion"
            | "aria2.getGlobalStat"
            | "system.multicall"
            | "system.listMethods"
            | "system.listNotifications"
    )
}

/// 处理单个 JSON-RPC 请求文本。鉴权失败：延迟 1s 后返回 400。
pub async fn handle_jsonrpc(state: &AppState, body: &str) -> RpcHttpResponse {
    let req = match protocol::parse_request(body) {
        Ok(r) => r,
        Err(e) => {
            return RpcHttpResponse {
                status: StatusCode::BAD_REQUEST,
                body: make_out(None, None, e.code, &e.message).to_text(),
            };
        }
    };

    let id = req.id.clone();
    if id.is_none() {
        return RpcHttpResponse {
            status: StatusCode::OK,
            body: make_out(None, None, -32600, "缺少 id").to_text(),
        };
    }

    let params: Vec<Value> = req.params.as_array().cloned().unwrap_or_default();

    if needs_auth(&req.method) && !check_token(&state.secret, &params) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return RpcHttpResponse {
            status: StatusCode::BAD_REQUEST,
            body: make_out(id, None, 1, "身份认证失败").to_text(),
        };
    }

    // 剥离 params[0] 中的 token（aria2 约定）
    let params: Vec<Value> = params
        .into_iter()
        .filter(|p| !p.as_str().map(|s| s.starts_with("token:")).unwrap_or(false))
        .collect();

    let ctx = MethodCtx {
        mgr: &state.mgr,
        global: &state.global,
        session_id: &state.session_id,
        shutdown: &state.shutdown,
        secret: &state.secret,
        config: state.config.as_deref(),
    };

    let out = match dispatch(ctx, &req.method, &params).await {
        Ok(v) => RpcOut::Response {
            id: id.clone(),
            result: Some(v),
            error: None,
        },
        Err((code, message)) => RpcOut::Response {
            id,
            result: None,
            error: Some(super::protocol::RpcErrorBody { code, message }),
        },
    };

    RpcHttpResponse {
        status: StatusCode::OK,
        body: out.to_text(),
    }
}

/// 可选的 WebSocket 升级提取器：非升级 GET 请求回退到 JSONP。
struct MaybeWs(Option<WebSocketUpgrade>);

impl<S: Send + Sync> FromRequestParts<S> for MaybeWs {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        const UPGRADE: HeaderName = HeaderName::from_static("upgrade");
        const CONNECTION: HeaderName = HeaderName::from_static("connection");
        let is_upgrade = parts
            .headers
            .get(UPGRADE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false)
            && parts
                .headers
                .get(CONNECTION)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_ascii_lowercase().contains("upgrade"))
                .unwrap_or(false);
        if !is_upgrade {
            return Ok(MaybeWs(None));
        }
        let ws = WebSocketUpgrade::from_request_parts(parts, state)
            .await
            .map_err(|_| (StatusCode::BAD_REQUEST, "WebSocket 升级失败"))?;
        Ok(MaybeWs(Some(ws)))
    }
}

#[derive(serde::Deserialize)]
struct JsonpQuery {
    method: String,
    #[serde(default)]
    id: Option<Value>,
    #[serde(default)]
    params: Option<String>,
    #[serde(default)]
    jsoncallback: Option<String>,
}

fn b64decode(s: &str) -> Option<Vec<u8>> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0u32;
    for &c in s.as_bytes() {
        if c == b'=' {
            break;
        }
        let v = TABLE.iter().position(|&t| t == c)?;
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

async fn get_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<JsonpQuery>,
    ws: MaybeWs,
) -> Response {
    if let Some(ws) = ws.0 {
        // WS 鉴权在每条消息中完成（token 位于请求 params[0]，与 aria2 一致）
        return ws
            .on_upgrade(move |socket| ws_loop(socket, state))
            .into_response();
    }

    // JSONP：仅当显式提供 jsoncallback 时才包裹回调，否则返回纯 JSON（与 aria2 一致）
    let callback = q.jsoncallback.clone();
    let wrap = |body: String| -> String {
        match &callback {
            Some(cb) => format!("{cb}({body});"),
            None => body,
        }
    };
    let params: Vec<Value> = match &q.params {
        Some(b64) => b64decode(b64)
            .and_then(|bytes| serde_json::from_slice::<Vec<Value>>(&bytes).ok())
            .unwrap_or_default(),
        None => Vec::new(),
    };
    let id = q.id.clone();

    if needs_auth(&q.method) && !check_token(&state.secret, &params) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let out = make_out(id, None, 1, "身份认证失败");
        return RpcHttpResponse {
            status: StatusCode::BAD_REQUEST,
            body: wrap(out.to_text()),
        }
        .into_response();
    }

    // 剥离 token
    let params: Vec<Value> = params
        .into_iter()
        .filter(|p| !p.as_str().map(|s| s.starts_with("token:")).unwrap_or(false))
        .collect();

    let ctx = MethodCtx {
        mgr: &state.mgr,
        global: &state.global,
        session_id: &state.session_id,
        shutdown: &state.shutdown,
        secret: &state.secret,
        config: state.config.as_deref(),
    };
    let out = match dispatch(ctx, &q.method, &params).await {
        Ok(v) => RpcOut::Response {
            id,
            result: Some(v),
            error: None,
        },
        Err((code, message)) => RpcOut::Response {
            id,
            result: None,
            error: Some(super::protocol::RpcErrorBody { code, message }),
        },
    };
    RpcHttpResponse {
        status: StatusCode::OK,
        body: wrap(out.to_text()),
    }
    .into_response()
}

async fn ws_loop(socket: WebSocket, state: Arc<AppState>) {
    let mut events = state.mgr.subscribe();
    let (mut sender, mut receiver) = socket.split();

    loop {
        tokio::select! {
            msg = receiver.next() => {
                let Some(Ok(msg)) = msg else { break };
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Close(_) => break,
                    _ => continue,
                };
                let resp = handle_jsonrpc(&state, &text).await;
                if sender.send(Message::Text(resp.body.into())).await.is_err() {
                    break;
                }
            }
            ev = events.recv() => {
                let Ok(ev) = ev else { break };
                let (method, gid) = match ev {
                    TaskEvent::Started { gid } => ("aria2.onDownloadStart", gid),
                    TaskEvent::Paused { gid } => ("aria2.onDownloadPause", gid),
                    TaskEvent::Stopped { gid } => ("aria2.onDownloadStop", gid),
                    TaskEvent::Complete { gid } => ("aria2.onDownloadComplete", gid),
                    TaskEvent::Error { gid, .. } => ("aria2.onDownloadError", gid),
                };
                let msg = RpcOut::Notification {
                    method: method.into(),
                    params: vec![Value::String(gid)],
                }
                .to_text();
                if sender.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// 启动 RPC 服务（HTTP POST + GET JSONP + WS）。返回实际监听地址。
pub async fn serve(state: Arc<AppState>, host: &str, port: u16) -> Result<String> {
    let cors = tower_http::cors::CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/jsonrpc", get(get_handler).post(jsonrpc_post_handler))
        .with_state(state)
        .layer(cors);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("监听 {addr} 失败"))?;
    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or(addr.clone());
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("RPC 服务异常: {e}");
        }
    });
    Ok(local)
}

async fn jsonrpc_post_handler(State(state): State<Arc<AppState>>, body: String) -> RpcHttpResponse {
    handle_jsonrpc(&state, &body).await
}
