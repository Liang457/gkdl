use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// JSON-RPC 2.0 请求（aria2 风格：params 数组，可省略）。
#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    #[allow(dead_code)]
    #[serde(rename = "jsonrpc")]
    pub jsonrpc: Option<String>,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcErrorBody {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum RpcOut {
    Response {
        id: Option<Value>,
        result: Option<Value>,
        error: Option<RpcErrorBody>,
    },
    Notification {
        method: String,
        params: Vec<Value>,
    },
}

impl RpcOut {
    /// 序列化为单个 JSON 文本（HTTP POST / WS frame）。
    pub fn to_text(&self) -> String {
        match self {
            RpcOut::Response { id, result, error } => {
                let mut obj = serde_json::Map::new();
                obj.insert("id".into(), id.clone().unwrap_or(serde_json::Value::Null));
                obj.insert("jsonrpc".into(), serde_json::json!("2.0"));
                if let Some(r) = result {
                    obj.insert("result".into(), r.clone());
                }
                if let Some(e) = error {
                    obj.insert(
                        "error".into(),
                        serde_json::json!({
                            "code": e.code,
                            "message": e.message,
                        }),
                    );
                }
                serde_json::Value::Object(obj).to_string()
            }
            RpcOut::Notification { method, params } => json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            })
            .to_string(),
        }
    }
}

/// 解析请求体。params 可选：缺失/Null 视为空数组（与 aria2 一致）。
pub fn parse_request(body: &str) -> Result<RpcRequest, RpcErrorBody> {
    let req: RpcRequest = serde_json::from_str(body).map_err(|e| RpcErrorBody {
        code: -32700,
        message: format!("解析 JSON 失败: {e}"),
    })?;
    if !req.params.is_null() && !req.params.is_array() {
        return Err(RpcErrorBody {
            code: -32602,
            message: "params 必须是数组".into(),
        });
    }
    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_may_be_omitted_or_null() {
        // AriaNg 对无参方法（getVersion/getGlobalOption/getGlobalStat…）省略 params 字段
        let req =
            parse_request(r#"{"jsonrpc":"2.0","id":"1","method":"aria2.getVersion"}"#).unwrap();
        assert!(req.params.is_null());
        let req = parse_request(
            r#"{"jsonrpc":"2.0","id":"1","method":"aria2.getGlobalOption","params":null}"#,
        )
        .unwrap();
        assert!(req.params.is_null());
    }

    #[test]
    fn params_non_array_rejected() {
        let e = parse_request(
            r#"{"jsonrpc":"2.0","id":"1","method":"aria2.addUri","params":{"dir":"/x"}}"#,
        )
        .unwrap_err();
        assert_eq!(e.code, -32602);
    }
}
