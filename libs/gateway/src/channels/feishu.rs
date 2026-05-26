use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::{
    channels::{ApprovalButton, ButtonStyle, Channel, ChannelTestResult, parse_approval_callback},
    chunking::chunk_text,
    types::{ChannelId, ChatType, InboundMessage, OutboundReply, PeerId},
};

const DEFAULT_DOMAIN: &str = "https://open.feishu.cn";
const FEISHU_TEXT_LIMIT: usize = 30_000;
const TOKEN_REFRESH_SKEW_SECS: i64 = 60;
const PARTIAL_TTL: Duration = Duration::from_secs(300);

pub struct FeishuChannel {
    id: ChannelId,
    app_id: String,
    app_secret: String,
    domain: String,
    http: reqwest::Client,
    token_cache: Mutex<Option<TokenCache>>,
    bot_open_id: Mutex<Option<String>>,
    partials: Mutex<HashMap<String, PartialPayload>>,
    active_threads: Mutex<HashSet<(String, String)>>,
}

#[derive(Debug, Clone)]
struct TokenCache {
    token: String,
    expires_at: i64,
}

#[derive(Debug)]
struct PartialPayload {
    parts: Vec<Option<Vec<u8>>>,
    updated_at: Instant,
}

#[derive(Clone, PartialEq, Message)]
struct WsHeader {
    #[prost(map = "string, string", tag = "1")]
    key_values: HashMap<String, String>,
}

#[derive(Clone, PartialEq, Message)]
struct WsFrame {
    #[prost(uint64, tag = "1")]
    seq_id: u64,
    #[prost(string, tag = "2")]
    log_id: String,
    #[prost(uint32, tag = "3")]
    service: u32,
    #[prost(uint32, tag = "4")]
    method: u32,
    #[prost(message, repeated, tag = "5")]
    headers: Vec<WsHeader>,
    #[prost(string, tag = "6")]
    payload_encoding: String,
    #[prost(string, tag = "7")]
    payload_type: String,
    #[prost(bytes = "vec", tag = "8")]
    payload: Vec<u8>,
    #[prost(string, tag = "9")]
    log_id_new: String,
}

#[derive(Debug, Deserialize)]
struct EndpointResponse {
    code: i64,
    #[serde(default)]
    msg: Option<String>,
    data: Option<EndpointData>,
}

#[derive(Debug, Deserialize)]
struct EndpointData {
    #[serde(rename = "URL")]
    url: String,
    #[serde(default, rename = "ClientConfig")]
    client_config: Option<EndpointClientConfig>,
}

#[derive(Debug, Deserialize)]
struct EndpointClientConfig {
    #[serde(default, rename = "PingInterval")]
    ping_interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FeishuApiResponse<T> {
    code: i64,
    #[serde(default)]
    msg: Option<String>,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct TokenResponseData {
    tenant_access_token: String,
    expire: i64,
}

#[derive(Debug, Deserialize)]
struct SendMessageData {
    message_id: String,
}

#[derive(Debug, Deserialize)]
struct BotInfoData {
    #[serde(default)]
    open_id: Option<String>,
    #[serde(default)]
    app_name: Option<String>,
}

impl FeishuChannel {
    pub fn new(app_id: String, app_secret: String, domain: Option<String>) -> Self {
        let domain = domain
            .and_then(|value| {
                let trimmed = value.trim().trim_end_matches('/').to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            })
            .unwrap_or_else(|| DEFAULT_DOMAIN.to_string());

        Self {
            id: "feishu".into(),
            app_id,
            app_secret,
            domain,
            http: reqwest::Client::new(),
            token_cache: Mutex::new(None),
            bot_open_id: Mutex::new(None),
            partials: Mutex::new(HashMap::new()),
            active_threads: Mutex::new(HashSet::new()),
        }
    }

    async fn open_endpoint(&self) -> Result<EndpointData> {
        let response = self
            .http
            .post(format!("{}/callback/ws/endpoint", self.domain))
            .json(&serde_json::json!({
                "AppID": self.app_id,
                "AppSecret": self.app_secret,
            }))
            .send()
            .await
            .context("feishu websocket endpoint request failed")?;

        let payload: EndpointResponse = response
            .json()
            .await
            .context("feishu websocket endpoint decode failed")?;

        if payload.code != 0 {
            return Err(anyhow!(
                "feishu websocket endpoint failed: {}",
                payload.msg.unwrap_or_else(|| payload.code.to_string())
            ));
        }

        payload
            .data
            .ok_or_else(|| anyhow!("feishu websocket endpoint missing data"))
    }

    async fn tenant_access_token(&self) -> Result<String> {
        let now = Utc::now().timestamp();
        if let Some(cache) = self.token_cache.lock().ok().and_then(|guard| guard.clone())
            && cache.expires_at - TOKEN_REFRESH_SKEW_SECS > now
        {
            return Ok(cache.token);
        }

        let response = self
            .http
            .post(format!(
                "{}/open-apis/auth/v3/tenant_access_token/internal",
                self.domain
            ))
            .json(&serde_json::json!({
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            }))
            .send()
            .await
            .context("feishu tenant_access_token request failed")?;

        let payload: FeishuApiResponse<TokenResponseData> = response
            .json()
            .await
            .context("feishu tenant_access_token decode failed")?;

        if payload.code != 0 {
            return Err(anyhow!(
                "feishu tenant_access_token failed: {}",
                payload.msg.unwrap_or_else(|| payload.code.to_string())
            ));
        }

        let data = payload
            .data
            .ok_or_else(|| anyhow!("feishu tenant_access_token missing data"))?;
        let cache = TokenCache {
            token: data.tenant_access_token,
            expires_at: now + data.expire,
        };
        let token = cache.token.clone();
        if let Ok(mut guard) = self.token_cache.lock() {
            *guard = Some(cache);
        }

        Ok(token)
    }

    async fn bot_info(&self) -> Result<BotInfoData> {
        let token = self.tenant_access_token().await?;
        let response = self
            .http
            .get(format!("{}/open-apis/bot/v3/info", self.domain))
            .bearer_auth(token)
            .send()
            .await
            .context("feishu bot info request failed")?;

        let payload: FeishuApiResponse<BotInfoData> = response
            .json()
            .await
            .context("feishu bot info decode failed")?;

        if payload.code != 0 {
            return Err(anyhow!(
                "feishu bot info failed: {}",
                payload.msg.unwrap_or_else(|| payload.code.to_string())
            ));
        }

        payload
            .data
            .ok_or_else(|| anyhow!("feishu bot info missing data"))
    }

    async fn send_text_payload(
        &self,
        chat_id: &str,
        reply_to_message_id: Option<&str>,
        text: &str,
    ) -> Result<String> {
        let token = self.tenant_access_token().await?;
        let content = serde_json::to_string(&serde_json::json!({ "text": text }))
            .context("feishu text content encode failed")?;

        let request = if let Some(message_id) = reply_to_message_id {
            self.http
                .post(format!(
                    "{}/open-apis/im/v1/messages/{message_id}/reply",
                    self.domain
                ))
                .bearer_auth(token)
                .json(&serde_json::json!({
                    "msg_type": "text",
                    "content": content,
                }))
        } else {
            self.http
                .post(format!(
                    "{}/open-apis/im/v1/messages?receive_id_type=chat_id",
                    self.domain
                ))
                .bearer_auth(token)
                .json(&serde_json::json!({
                    "receive_id": chat_id,
                    "msg_type": "text",
                    "content": content,
                }))
        };

        let payload: FeishuApiResponse<SendMessageData> = request
            .send()
            .await
            .context("feishu send text request failed")?
            .json()
            .await
            .context("feishu send text decode failed")?;

        if payload.code != 0 {
            return Err(anyhow!(
                "feishu send text failed: {}",
                payload.msg.unwrap_or_else(|| payload.code.to_string())
            ));
        }

        payload
            .data
            .map(|data| data.message_id)
            .ok_or_else(|| anyhow!("feishu send text missing message_id"))
    }

    async fn send_card_payload(
        &self,
        chat_id: &str,
        reply_to_message_id: Option<&str>,
        card: serde_json::Value,
    ) -> Result<String> {
        let token = self.tenant_access_token().await?;
        let content = serde_json::to_string(&card).context("feishu card content encode failed")?;

        let request = if let Some(message_id) = reply_to_message_id {
            self.http
                .post(format!(
                    "{}/open-apis/im/v1/messages/{message_id}/reply",
                    self.domain
                ))
                .bearer_auth(token)
                .json(&serde_json::json!({
                    "msg_type": "interactive",
                    "content": content,
                }))
        } else {
            self.http
                .post(format!(
                    "{}/open-apis/im/v1/messages?receive_id_type=chat_id",
                    self.domain
                ))
                .bearer_auth(token)
                .json(&serde_json::json!({
                    "receive_id": chat_id,
                    "msg_type": "interactive",
                    "content": content,
                }))
        };

        let payload: FeishuApiResponse<SendMessageData> = request
            .send()
            .await
            .context("feishu send card request failed")?
            .json()
            .await
            .context("feishu send card decode failed")?;

        if payload.code != 0 {
            return Err(anyhow!(
                "feishu send card failed: {}",
                payload.msg.unwrap_or_else(|| payload.code.to_string())
            ));
        }

        payload
            .data
            .map(|data| data.message_id)
            .ok_or_else(|| anyhow!("feishu send card missing message_id"))
    }

    async fn patch_card_message(&self, message_id: &str, card: serde_json::Value) -> Result<()> {
        let token = self.tenant_access_token().await?;
        let content = serde_json::to_string(&card).context("feishu card content encode failed")?;

        let payload: FeishuApiResponse<serde_json::Value> = self
            .http
            .patch(format!(
                "{}/open-apis/im/v1/messages/{message_id}",
                self.domain
            ))
            .bearer_auth(token)
            .json(&serde_json::json!({
                "msg_type": "interactive",
                "content": content,
            }))
            .send()
            .await
            .context("feishu update card request failed")?
            .json()
            .await
            .context("feishu update card decode failed")?;

        if payload.code == 0 {
            return Ok(());
        }

        Err(anyhow!(
            "feishu update card failed: {}",
            payload.msg.unwrap_or_else(|| payload.code.to_string())
        ))
    }

    fn extract_target(reply: &OutboundReply) -> Result<(String, Option<String>)> {
        let chat_id = reply
            .metadata
            .get("chat_id")
            .and_then(value_as_string)
            .or_else(|| {
                if reply.peer_id.0.starts_with("oc_") {
                    Some(reply.peer_id.0.clone())
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow!("feishu reply missing chat_id in metadata/peer_id"))?;

        let reply_to_message_id = reply.metadata.get("message_id").and_then(value_as_string);
        Ok((chat_id, reply_to_message_id))
    }

    async fn connect_once(
        &self,
        inbound_tx: &mpsc::Sender<InboundMessage>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let endpoint = self.open_endpoint().await?;
        let ping_interval = endpoint
            .client_config
            .and_then(|config| config.ping_interval)
            .filter(|seconds| *seconds > 0)
            .unwrap_or(120);
        let service_id = parse_query_u32(&endpoint.url, "service_id").unwrap_or_default();

        let (ws, _response) = tokio_tungstenite::connect_async(&endpoint.url)
            .await
            .context("feishu websocket connect failed")?;
        let (mut writer, mut reader) = ws.split();
        let mut keepalive = tokio::time::interval(Duration::from_secs(ping_interval));
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let _ = keepalive.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = writer.send(WsMessage::Close(None)).await;
                    return Ok(());
                }
                _ = keepalive.tick() => {
                    let ping = build_ping_frame(service_id)?;
                    if writer.send(WsMessage::Binary(ping)).await.is_err() {
                        break;
                    }
                }
                next = reader.next() => {
                    let Some(next) = next else {
                        break;
                    };

                    match next {
                        Ok(WsMessage::Binary(bytes)) => {
                            match self.handle_binary_frame(&bytes, inbound_tx).await {
                                Ok(Some(response)) => {
                                    if writer.send(WsMessage::Binary(response)).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => {}
                                Err(error) => warn!(error = %error, "feishu websocket frame handler failed"),
                            }
                        }
                        Ok(WsMessage::Ping(payload)) => {
                            if writer.send(WsMessage::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(WsMessage::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            warn!(error = %error, "feishu websocket read failed");
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_binary_frame(
        &self,
        bytes: &[u8],
        inbound_tx: &mpsc::Sender<InboundMessage>,
    ) -> Result<Option<Vec<u8>>> {
        let frame = WsFrame::decode(bytes).context("feishu websocket frame decode failed")?;
        let headers = frame_headers(&frame);
        let message_type = headers.get("type").map(String::as_str).unwrap_or_default();

        if matches!(message_type, "ping" | "pong") {
            return Ok(None);
        }

        let Some(payload) = self.complete_payload(&frame, &headers)? else {
            return Ok(None);
        };

        let result = match message_type {
            "event" => self.handle_event_payload(&payload, inbound_tx).await,
            "card" => self.handle_card_payload(&payload, inbound_tx).await,
            _ => Ok(()),
        };

        let status_code = if result.is_ok() { 200 } else { 500 };
        if let Err(error) = result {
            warn!(error = %error, "feishu websocket payload handling failed");
        }

        Ok(Some(build_response_frame(&frame, status_code)?))
    }

    fn complete_payload(
        &self,
        frame: &WsFrame,
        headers: &HashMap<String, String>,
    ) -> Result<Option<Vec<u8>>> {
        let sum = headers
            .get("sum")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1);
        if sum <= 1 {
            return Ok(Some(frame.payload.clone()));
        }

        let message_id = headers
            .get("message_id")
            .cloned()
            .ok_or_else(|| anyhow!("feishu partial frame missing message_id"))?;
        let seq = headers
            .get("seq")
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| anyhow!("feishu partial frame missing seq"))?;
        if seq == 0 || seq > sum {
            return Err(anyhow!("feishu partial frame has invalid seq {seq}/{sum}"));
        }

        let mut guard = self
            .partials
            .lock()
            .map_err(|_| anyhow!("failed to lock feishu partials"))?;
        let now = Instant::now();
        guard.retain(|_, value| now.duration_since(value.updated_at) < PARTIAL_TTL);

        let entry = guard
            .entry(message_id.clone())
            .or_insert_with(|| PartialPayload {
                parts: vec![None; sum],
                updated_at: now,
            });
        if entry.parts.len() != sum {
            *entry = PartialPayload {
                parts: vec![None; sum],
                updated_at: now,
            };
        }
        entry.parts[seq - 1] = Some(frame.payload.clone());
        entry.updated_at = now;

        if entry.parts.iter().any(Option::is_none) {
            return Ok(None);
        }

        let parts = guard
            .remove(&message_id)
            .map(|value| value.parts)
            .unwrap_or_default();
        let mut payload = Vec::new();
        for part in parts.into_iter().flatten() {
            payload.extend(part);
        }

        Ok(Some(payload))
    }

    async fn handle_event_payload(
        &self,
        payload: &[u8],
        inbound_tx: &mpsc::Sender<InboundMessage>,
    ) -> Result<()> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).context("feishu event payload decode failed")?;

        if let Some(inbound) = self.map_message_event(&value)
            && inbound_tx.send(inbound).await.is_err()
        {
            return Err(anyhow!("feishu inbound receiver closed"));
        }

        Ok(())
    }

    async fn handle_card_payload(
        &self,
        payload: &[u8],
        inbound_tx: &mpsc::Sender<InboundMessage>,
    ) -> Result<()> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).context("feishu card payload decode failed")?;

        if let Some(inbound) = self.map_card_callback(&value)
            && inbound_tx.send(inbound).await.is_err()
        {
            return Err(anyhow!("feishu inbound receiver closed"));
        }

        Ok(())
    }

    fn map_message_event(&self, value: &serde_json::Value) -> Option<InboundMessage> {
        let event_type = value
            .pointer("/header/event_type")
            .and_then(serde_json::Value::as_str)?;
        if event_type != "im.message.receive_v1" {
            return None;
        }

        let event = value.get("event")?;
        let sender_id = first_string(&[
            event.pointer("/sender/sender_id/open_id"),
            event.pointer("/sender/sender_id/user_id"),
            event.pointer("/sender/sender_id/union_id"),
        ])?;
        let message = event.get("message")?;
        let message_id = message.get("message_id").and_then(value_as_string)?;
        let chat_id = message.get("chat_id").and_then(value_as_string)?;
        let chat_type_raw = message
            .get("chat_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let message_type = message
            .get("message_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        let mut text = match message_type {
            "text" => parse_text_content(message.get("content")?.as_str()?),
            _ => return None,
        };

        let bot_open_id = self
            .bot_open_id
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_default();
        if !bot_open_id.is_empty() && sender_id == bot_open_id {
            return None;
        }

        let mentions = message
            .get("mentions")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mentioned = mention_targets_bot(&mentions, &bot_open_id);
        text = strip_mention_text(text, &mentions);

        let root_id = message.get("root_id").and_then(value_as_string);
        let parent_id = message.get("parent_id").and_then(value_as_string);
        let is_direct = chat_type_raw == "p2p";
        let thread_id = root_id
            .as_ref()
            .filter(|root| !root.is_empty() && root.as_str() != message_id.as_str())
            .cloned();

        if !is_direct {
            if mentioned {
                let active_thread = thread_id.clone().unwrap_or_else(|| message_id.clone());
                self.activate_thread(&chat_id, &active_thread);
            } else if let Some(thread_id) = thread_id.as_deref() {
                if !self.is_thread_active(&chat_id, thread_id) {
                    return None;
                }
            } else {
                return None;
            }
        }

        let timestamp = message
            .get("create_time")
            .and_then(value_as_string)
            .and_then(|value| value.parse::<i64>().ok())
            .and_then(timestamp_millis_to_datetime)
            .unwrap_or_else(Utc::now);

        let chat_type = if is_direct {
            ChatType::Direct
        } else if let Some(thread_id) = thread_id {
            ChatType::Thread {
                group_id: chat_id.clone(),
                thread_id,
            }
        } else {
            ChatType::Group {
                id: chat_id.clone(),
            }
        };

        Some(InboundMessage {
            channel: self.id.clone(),
            peer_id: PeerId(sender_id.clone()),
            chat_type,
            text: text.trim().to_string(),
            media: Vec::new(),
            metadata: serde_json::json!({
                "chat_id": chat_id,
                "message_id": message_id,
                "root_id": root_id,
                "parent_id": parent_id,
                "chat_type": chat_type_raw,
                "message_type": message_type,
                "mentioned": mentioned,
                "sender_id": sender_id,
            }),
            timestamp,
        })
    }

    fn map_card_callback(&self, value: &serde_json::Value) -> Option<InboundMessage> {
        let callback = first_string(&[
            value.pointer("/event/action/value/callback"),
            value.pointer("/event/action/value/callback_data"),
            value.pointer("/event/action/value/action"),
            value.pointer("/action/value/callback"),
            value.pointer("/action/value/callback_data"),
            value.pointer("/action/value/action"),
        ])?;
        let (approval_id, decision) = parse_approval_callback(&callback)?;

        let operator = first_string(&[
            value.pointer("/event/operator/operator_id/open_id"),
            value.pointer("/event/operator/open_id"),
            value.pointer("/operator/operator_id/open_id"),
            value.pointer("/operator/open_id"),
            value.get("open_id"),
            value.get("user_id"),
        ])
        .unwrap_or_else(|| "unknown".to_string());

        let chat_id = first_string(&[
            value.pointer("/event/context/open_chat_id"),
            value.pointer("/event/context/chat_id"),
            value.pointer("/context/open_chat_id"),
            value.pointer("/context/chat_id"),
            value.get("open_chat_id"),
            value.get("chat_id"),
        ]);
        let message_id = first_string(&[
            value.pointer("/event/context/open_message_id"),
            value.pointer("/event/context/message_id"),
            value.pointer("/context/open_message_id"),
            value.pointer("/context/message_id"),
            value.get("open_message_id"),
            value.get("message_id"),
        ]);

        Some(InboundMessage {
            channel: self.id.clone(),
            peer_id: PeerId(operator),
            chat_type: chat_id
                .as_ref()
                .map(|id| ChatType::Group { id: id.clone() })
                .unwrap_or(ChatType::Direct),
            text: String::new(),
            media: Vec::new(),
            metadata: serde_json::json!({
                "type": "approval_response",
                "approval_id": approval_id,
                "decision": decision,
                "chat_id": chat_id,
                "message_id": message_id,
            }),
            timestamp: Utc::now(),
        })
    }

    fn activate_thread(&self, chat_id: &str, thread_id: &str) {
        if let Ok(mut guard) = self.active_threads.lock() {
            guard.insert((chat_id.to_string(), thread_id.to_string()));
        }
    }

    fn is_thread_active(&self, chat_id: &str, thread_id: &str) -> bool {
        self.active_threads
            .lock()
            .map(|guard| guard.contains(&(chat_id.to_string(), thread_id.to_string())))
            .unwrap_or(false)
    }
}

#[async_trait]
impl Channel for FeishuChannel {
    fn id(&self) -> &ChannelId {
        &self.id
    }

    fn display_name(&self) -> &str {
        "Feishu"
    }

    async fn start(
        &self,
        inbound_tx: mpsc::Sender<InboundMessage>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let info = self.bot_info().await?;
        if let Ok(mut guard) = self.bot_open_id.lock() {
            *guard = info.open_id;
        }

        let mut reconnect_backoff_secs = 1_u64;
        loop {
            if cancel.is_cancelled() {
                break;
            }

            match self.connect_once(&inbound_tx, &cancel).await {
                Ok(()) if cancel.is_cancelled() => break,
                Ok(()) => {}
                Err(error) => error!(error = %error, "feishu websocket connection failed"),
            }

            tokio::time::sleep(Duration::from_secs(reconnect_backoff_secs)).await;
            reconnect_backoff_secs = (reconnect_backoff_secs * 2).min(30);
        }

        Ok(())
    }

    async fn send(&self, reply: OutboundReply) -> Result<()> {
        self.send_with_receipt(reply).await.map(|_| ())
    }

    async fn send_with_receipt(
        &self,
        reply: OutboundReply,
    ) -> Result<crate::channels::DeliveryReceipt> {
        let (chat_id, reply_to_message_id) = Self::extract_target(&reply)?;
        let chunks = chunk_text(&reply.text, FEISHU_TEXT_LIMIT);
        let mut first_message_id: Option<String> = None;

        for chunk in chunks {
            let message_id = self
                .send_text_payload(&chat_id, reply_to_message_id.as_deref(), &chunk)
                .await?;
            if first_message_id.is_none() {
                first_message_id = Some(message_id);
            }
        }

        Ok(crate::channels::DeliveryReceipt {
            message_id: first_message_id,
            thread_id: reply_to_message_id,
        })
    }

    async fn send_with_buttons(
        &self,
        reply: OutboundReply,
        buttons: Vec<ApprovalButton>,
    ) -> Result<String> {
        let (chat_id, reply_to_message_id) = Self::extract_target(&reply)?;
        let card = build_approval_card(&reply.text, &buttons);
        self.send_card_payload(&chat_id, reply_to_message_id.as_deref(), card)
            .await
    }

    async fn edit_message(&self, message_id: &str, new_text: &str) -> Result<()> {
        let card = build_status_card(new_text);
        self.patch_card_message(message_id, card).await
    }

    async fn test(&self) -> Result<ChannelTestResult> {
        let info = self.bot_info().await?;
        Ok(ChannelTestResult {
            channel: self.id.0.clone(),
            identity: info
                .app_name
                .unwrap_or_else(|| info.open_id.clone().unwrap_or_else(|| self.app_id.clone())),
            details: info
                .open_id
                .map(|open_id| format!("open_id={open_id}"))
                .unwrap_or_else(|| format!("app_id={}", self.app_id)),
        })
    }
}

fn build_ping_frame(service_id: u32) -> Result<Vec<u8>> {
    let frame = WsFrame {
        seq_id: Utc::now().timestamp_millis().max(0) as u64,
        service: service_id,
        method: 0,
        headers: vec![header_from_pairs([("type", "ping")])],
        ..WsFrame::default()
    };
    let mut bytes = Vec::new();
    frame
        .encode(&mut bytes)
        .context("feishu ping encode failed")?;
    Ok(bytes)
}

fn build_response_frame(frame: &WsFrame, status_code: u16) -> Result<Vec<u8>> {
    let mut response = frame.clone();
    response.payload = serde_json::to_vec(&serde_json::json!({
        "code": status_code,
        "data": {}
    }))
    .context("feishu response payload encode failed")?;
    response.headers.push(header_from_pairs([
        ("biz_rt", "0"),
        ("response-code", &status_code.to_string()),
    ]));

    let mut bytes = Vec::new();
    response
        .encode(&mut bytes)
        .context("feishu response frame encode failed")?;
    Ok(bytes)
}

fn header_from_pairs<const N: usize>(pairs: [(&str, &str); N]) -> WsHeader {
    WsHeader {
        key_values: pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    }
}

fn frame_headers(frame: &WsFrame) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    for header in &frame.headers {
        for (key, value) in &header.key_values {
            headers.insert(key.clone(), value.clone());
        }
    }
    headers
}

fn build_approval_card(text: &str, buttons: &[ApprovalButton]) -> serde_json::Value {
    let actions = buttons
        .iter()
        .map(|button| {
            let kind = match button.style {
                ButtonStyle::Success => "primary",
                ButtonStyle::Danger => "danger",
            };
            serde_json::json!({
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": button.label,
                },
                "type": kind,
                "value": {
                    "callback": button.callback_data,
                },
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "config": {
            "wide_screen_mode": true,
        },
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "Stakpak approval",
            },
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": text,
                },
            },
            {
                "tag": "action",
                "actions": actions,
            },
        ],
    })
}

fn build_status_card(text: &str) -> serde_json::Value {
    serde_json::json!({
        "config": {
            "wide_screen_mode": true,
        },
        "elements": [{
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": text,
            },
        }],
    })
}

fn parse_text_content(content: &str) -> String {
    serde_json::from_str::<serde_json::Value>(content)
        .ok()
        .and_then(|value| {
            value
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| content.to_string())
}

fn mention_targets_bot(mentions: &[serde_json::Value], bot_open_id: &str) -> bool {
    if bot_open_id.is_empty() {
        return !mentions.is_empty();
    }

    mentions.iter().any(|mention| {
        mention
            .pointer("/id/open_id")
            .and_then(serde_json::Value::as_str)
            == Some(bot_open_id)
    })
}

fn strip_mention_text(mut text: String, mentions: &[serde_json::Value]) -> String {
    for mention in mentions {
        if let Some(key) = mention.get("key").and_then(serde_json::Value::as_str) {
            text = text.replace(key, "");
        }
        if let Some(name) = mention.get("name").and_then(serde_json::Value::as_str) {
            text = text.replace(&format!("@{name}"), "");
        }
    }

    text.trim().to_string()
}

fn first_string(values: &[Option<&serde_json::Value>]) -> Option<String> {
    values
        .iter()
        .find_map(|value| value.and_then(value_as_string))
}

fn value_as_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn timestamp_millis_to_datetime(value: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
}

fn parse_query_u32(url: &str, key: &str) -> Option<u32> {
    let query = url.split_once('?')?.1;
    query.split('&').find_map(|part| {
        let (part_key, part_value) = part.split_once('=')?;
        if part_key == key {
            part_value.parse::<u32>().ok()
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        FeishuChannel, build_approval_card, parse_query_u32, parse_text_content, strip_mention_text,
    };
    use crate::channels::{ApprovalButton, ButtonStyle};

    #[test]
    fn text_content_extracts_text_field() {
        assert_eq!(
            parse_text_content(r#"{"text":"hello"}"#),
            "hello".to_string()
        );
        assert_eq!(parse_text_content("plain"), "plain".to_string());
    }

    #[test]
    fn query_parser_reads_service_id() {
        assert_eq!(
            parse_query_u32(
                "wss://example.test/ws?device_id=x&service_id=42",
                "service_id"
            ),
            Some(42)
        );
    }

    #[test]
    fn mention_text_strips_keys_and_names() {
        let mentions = vec![serde_json::json!({
            "key": "@_user_1",
            "name": "OpsBot",
        })];
        assert_eq!(
            strip_mention_text("@_user_1 check nginx".to_string(), &mentions),
            "check nginx"
        );
        assert_eq!(
            strip_mention_text("@OpsBot check nginx".to_string(), &mentions),
            "check nginx"
        );
    }

    #[test]
    fn card_button_callback_is_embedded() {
        let card = build_approval_card(
            "Approve?",
            &[ApprovalButton {
                label: "Allow".to_string(),
                callback_data: "a:approval-1:allow".to_string(),
                style: ButtonStyle::Success,
            }],
        );
        assert!(card.to_string().contains("a:approval-1:allow"));
    }

    #[test]
    fn group_messages_require_mentions_until_thread_is_active() {
        let channel = FeishuChannel::new(
            "cli_x".to_string(),
            "secret".to_string(),
            Some("https://open.feishu.cn".to_string()),
        );
        if let Ok(mut guard) = channel.bot_open_id.lock() {
            *guard = Some("ou_bot".to_string());
        }

        let unmentioned = serde_json::json!({
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user"}},
                "message": {
                    "message_id": "om_1",
                    "chat_id": "oc_1",
                    "chat_type": "group",
                    "message_type": "text",
                    "content": "{\"text\":\"hello\"}",
                    "mentions": []
                }
            }
        });
        assert!(channel.map_message_event(&unmentioned).is_none());

        let mentioned = serde_json::json!({
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user"}},
                "message": {
                    "message_id": "om_2",
                    "chat_id": "oc_1",
                    "chat_type": "group",
                    "message_type": "text",
                    "content": "{\"text\":\"@_user_1 check\"}",
                    "mentions": [{"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "OpsBot"}]
                }
            }
        });
        assert!(channel.map_message_event(&mentioned).is_some());

        let thread_reply = serde_json::json!({
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user"}},
                "message": {
                    "message_id": "om_3",
                    "root_id": "om_2",
                    "chat_id": "oc_1",
                    "chat_type": "group",
                    "message_type": "text",
                    "content": "{\"text\":\"more\"}",
                    "mentions": []
                }
            }
        });
        assert!(channel.map_message_event(&thread_reply).is_some());
    }
}
