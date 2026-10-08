//! Persistent Redis command and bounded live agent sessions.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use voidb_core::{
    AGENT_LIVE_SESSION_PROTOCOL_VERSION, AgentLiveSessionAuditIdentity,
    AgentLiveSessionBackpressureMode, AgentLiveSessionBufferOverflow, AgentLiveSessionBufferPolicy,
    AgentLiveSessionCallCancellation, AgentLiveSessionCancelBehavior, AgentLiveSessionCloseEffect,
    AgentLiveSessionContract, AgentLiveSessionControlPolicy, AgentLiveSessionCursor,
    AgentLiveSessionCursorKind, AgentLiveSessionCursorScopePolicy, AgentLiveSessionDeliveryPolicy,
    AgentLiveSessionEventBuffer, AgentLiveSessionEventInput, AgentLiveSessionEventKind,
    AgentLiveSessionHeartbeatPolicy, AgentLiveSessionKind, AgentLiveSessionOperations,
    AgentLiveSessionReadRequest, AgentLiveSessionReconnectMode, AgentLiveSessionReconnectPolicy,
    AgentLiveSessionResourceDescriptor, AgentLiveSessionResumeMode,
    AgentLiveSessionSourcePacedState, AgentLiveSessionStartRequest, AgentSessionCallRequest,
    AgentSessionCallResult, AgentSessionOpenContext, CapabilityRiskLevel, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionStatus, RedactionTarget, agent_live_session_cursor_scope,
    collect_redaction_targets, redact_text_with_targets,
};

use crate::RedisConfig;
use crate::service::PersistentRedisConnection;
use crate::service::agent_live::{
    RedisMonitorSource, RedisPubSubSource, RedisStreamEntry, RedisStreamSource, truncate_utf8,
};

pub(crate) const PUBSUB_READ_CAPABILITY: &str = "redis.pubsub_read";
pub(crate) const MONITOR_READ_CAPABILITY: &str = "redis.monitor_read";
pub(crate) const STREAM_READ_CAPABILITY: &str = "redis.stream_read";

const MAX_SUBSCRIPTIONS: usize = 32;
const MAX_SUBSCRIPTION_BYTES: usize = 256;
const MAX_PUBSUB_PAYLOAD_BYTES: usize = 16 * 1024;
const DEFAULT_STREAM_COUNT: usize = 100;
const MAX_STREAM_COUNT: usize = 500;
const DEFAULT_STREAM_BLOCK_MS: u64 = 5_000;
const MAX_STREAM_BLOCK_MS: u64 = 30_000;
const HEARTBEAT_INTERVAL_MS: u64 = 15_000;
const IDLE_TIMEOUT_MS: u64 = 60_000;

pub struct RedisAgentSessionFactory {
    config: RedisConfig,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

impl RedisAgentSessionFactory {
    pub fn new(config: RedisConfig) -> Self {
        let redaction_targets = serde_json::to_value(&config)
            .map(|value| collect_redaction_targets(&value))
            .unwrap_or_default();
        Self {
            config,
            redaction_targets: Arc::new(redaction_targets),
        }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for RedisAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "redis"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        let family = RedisSessionFamily::from_binding(&context)?;
        if family == RedisSessionFamily::Exec {
            let connection = PersistentRedisConnection::open(&self.config)
                .await
                .map_err(|_| owner_error("Redis session connection could not be opened."))?;
            return Ok(Arc::new(RedisCommandSession {
                connection: Mutex::new(Some(connection)),
                redaction_targets: Arc::clone(&self.redaction_targets),
            }));
        }

        let capability = family.capability();
        let (purpose, contract) = redis_live_session_contract(capability)
            .expect("Redis live-session family has a contract");
        if context.binding.purpose != purpose {
            return Err(error(
                PluginSessionErrorCode::BindingMismatch,
                "The Redis live-session purpose does not match its capability family.",
            ));
        }
        contract.validate(&context.binding.allowed_capabilities)?;
        contract.validate_start(&context.request.input)?;
        if contract.start_requires_acknowledgement() && !context.request.destructive_acknowledged {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Redis MONITOR requires an explicit per-open acknowledgement.",
            ));
        }
        let start: AgentLiveSessionStartRequest =
            serde_json::from_value(context.request.input.clone()).map_err(|_| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The Redis live-session start envelope is invalid.",
                )
            })?;
        let policy = start
            .buffer
            .clone()
            .unwrap_or_else(|| contract.buffer.clone());
        let cancellations = AgentLiveSessionCallCancellation::default();

        let source = match family {
            RedisSessionFamily::PubSub => {
                let (channels, patterns) = subscriptions(&start.parameters)?;
                let initial = RedisPubSubSource::open(&self.config, &channels, &patterns)
                    .await
                    .map_err(|_| owner_error("Redis Pub/Sub could not be opened."))?;
                let buffer = AgentLiveSessionEventBuffer::new(contract.clone(), policy)?;
                let producer_buffer = buffer.clone();
                let config = self.config.clone();
                let targets = Arc::clone(&self.redaction_targets);
                let task = tokio::spawn(async move {
                    run_pubsub(
                        config,
                        channels,
                        patterns,
                        initial,
                        producer_buffer.clone(),
                        targets,
                    )
                    .await;
                    producer_buffer.close_source().await;
                });
                RedisLiveSource::Buffered {
                    buffer,
                    task: Mutex::new(Some(task)),
                }
            }
            RedisSessionFamily::Monitor => {
                let initial = RedisMonitorSource::open(&self.config)
                    .await
                    .map_err(|_| owner_error("Redis MONITOR could not be opened or authorized."))?;
                let buffer = AgentLiveSessionEventBuffer::new(contract.clone(), policy)?;
                let producer_buffer = buffer.clone();
                let config = self.config.clone();
                let task = tokio::spawn(async move {
                    run_monitor(config, initial, producer_buffer.clone()).await;
                    producer_buffer.close_source().await;
                });
                RedisLiveSource::Buffered {
                    buffer,
                    task: Mutex::new(Some(task)),
                }
            }
            RedisSessionFamily::Stream => {
                let scope =
                    agent_live_session_cursor_scope(STREAM_READ_CAPABILITY, &start.resource)?;
                if start
                    .resume_from
                    .as_ref()
                    .and_then(|cursor| cursor.scope.as_deref())
                    .is_some_and(|supplied| supplied != scope)
                {
                    return Err(error(
                        PluginSessionErrorCode::BindingMismatch,
                        "The Redis stream resume cursor belongs to a different resource.",
                    ));
                }
                let stream = open_stream_source(&self.config, &start).await?;
                let count = parameter_usize(
                    &start.parameters,
                    "count",
                    DEFAULT_STREAM_COUNT,
                    1,
                    MAX_STREAM_COUNT,
                )?;
                let block_ms = parameter_u64(
                    &start.parameters,
                    "block_ms",
                    DEFAULT_STREAM_BLOCK_MS,
                    1,
                    MAX_STREAM_BLOCK_MS,
                )?;
                RedisLiveSource::Stream(Box::new(Mutex::new(Some(RedisStreamSession {
                    stream,
                    pending: VecDeque::new(),
                    delivery: AgentLiveSessionSourcePacedState::default(),
                    scope,
                    count,
                    block_ms,
                    last_heartbeat: Instant::now(),
                    redaction_targets: Arc::clone(&self.redaction_targets),
                }))))
            }
            RedisSessionFamily::Exec => unreachable!(),
        };

        Ok(Arc::new(RedisAgentLiveSession {
            family,
            contract,
            source,
            cancellations,
            closed: AtomicBool::new(false),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedisSessionFamily {
    Exec,
    PubSub,
    Monitor,
    Stream,
}

impl RedisSessionFamily {
    fn from_binding(context: &AgentSessionOpenContext) -> Result<Self, PluginSessionError> {
        let actual = context
            .binding
            .allowed_capabilities
            .iter()
            .map(|capability| capability.strip_prefix("redis.").unwrap_or(capability))
            .collect::<HashSet<_>>();
        let family = match actual.iter().copied().collect::<Vec<_>>().as_slice() {
            ["exec"] => Self::Exec,
            ["pubsub_read"] => Self::PubSub,
            ["monitor_read"] => Self::Monitor,
            ["stream_read"] => Self::Stream,
            _ => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "A Redis session requires one complete, unmixed capability family.",
                ));
            }
        };
        let purpose_valid = match family {
            Self::Exec => matches!(
                context.binding.purpose,
                PluginSessionPurpose::CacheCommand | PluginSessionPurpose::DatabaseTransaction
            ),
            Self::PubSub | Self::Monitor | Self::Stream => {
                context.binding.purpose == PluginSessionPurpose::WatchStream
            }
        };
        if !purpose_valid {
            return Err(error(
                PluginSessionErrorCode::BindingMismatch,
                "The Redis session purpose does not match its capability family.",
            ));
        }
        Ok(family)
    }

    fn capability(self) -> &'static str {
        match self {
            Self::Exec => "redis.exec",
            Self::PubSub => PUBSUB_READ_CAPABILITY,
            Self::Monitor => MONITOR_READ_CAPABILITY,
            Self::Stream => STREAM_READ_CAPABILITY,
        }
    }
}

struct RedisCommandSession {
    connection: Mutex<Option<PersistentRedisConnection>>,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

#[async_trait]
impl PluginAgentSession for RedisCommandSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if request.capability != "redis.exec" {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Redis command sessions accept redis.exec.",
            ));
        }
        if !request.destructive_acknowledged {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "redis.exec requires destructive acknowledgement.",
            ));
        }
        let command = request
            .input
            .get("command")
            .and_then(Value::as_str)
            .filter(|command| !command.trim().is_empty())
            .ok_or_else(|| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "Redis command is required.",
                )
            })?;
        let mut guard = self.connection.lock().await;
        let connection = guard
            .as_mut()
            .ok_or_else(|| owner_error("Redis session is closed."))?;
        let output = connection
            .execute(command)
            .await
            .map_err(|_| owner_error("Redis session command failed."))?;
        let (output, _) = redact_text_with_targets(&output, &self.redaction_targets);
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "output": output }),
            request.output_limit_bytes,
        )
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.connection.lock().await.is_some() {
            PluginSessionHealth::Ready
        } else {
            PluginSessionHealth::Closed
        })
    }

    async fn cancel(&self, _call_id: &str) -> Result<(), PluginSessionError> {
        close_connection(&self.connection).await;
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        close_connection(&self.connection).await;
        Ok(())
    }
}

enum RedisLiveSource {
    Buffered {
        buffer: AgentLiveSessionEventBuffer,
        task: Mutex<Option<JoinHandle<()>>>,
    },
    Stream(Box<Mutex<Option<RedisStreamSession>>>),
}

struct RedisStreamSession {
    stream: RedisStreamSource,
    pending: VecDeque<RedisStreamEntry>,
    delivery: AgentLiveSessionSourcePacedState,
    scope: String,
    count: usize,
    block_ms: u64,
    last_heartbeat: Instant,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

struct RedisAgentLiveSession {
    family: RedisSessionFamily,
    contract: AgentLiveSessionContract,
    source: RedisLiveSource,
    cancellations: AgentLiveSessionCallCancellation,
    closed: AtomicBool,
}

#[async_trait]
impl PluginAgentSession for RedisAgentLiveSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if request.capability != self.family.capability() {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "The Redis call is outside this live-session binding.",
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(owner_error("The Redis live session is closed."));
        }
        match &self.source {
            RedisLiveSource::Buffered { buffer, .. } => {
                let mut read = read_request(&request)?;
                read.max_bytes = read.max_bytes.min(request.output_limit_bytes);
                let call_id = request.call_id.clone();
                let batch = self.cancellations.run(&call_id, buffer.read(&read)).await?;
                bounded_batch(request, batch)
            }
            RedisLiveSource::Stream(state) => {
                let read = read_request(&request)?;
                let call_id = request.call_id.clone();
                let batch = self
                    .cancellations
                    .run(
                        &call_id,
                        self.read_stream(state, read, request.output_limit_bytes),
                    )
                    .await?;
                bounded_batch(request, batch)
            }
        }
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.closed.load(Ordering::Acquire) {
            PluginSessionHealth::Closed
        } else {
            PluginSessionHealth::Ready
        })
    }

    async fn cancel(&self, call_id: &str) -> Result<(), PluginSessionError> {
        self.cancellations.cancel(call_id).await;
        if let RedisLiveSource::Stream(state) = &self.source {
            state.lock().await.take();
            self.closed.store(true, Ordering::Release);
        }
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cancellations.close().await;
        match &self.source {
            RedisLiveSource::Buffered { buffer, task } => {
                if let Some(task) = task.lock().await.take() {
                    task.abort();
                    let _ = task.await;
                }
                buffer.close_source().await;
            }
            RedisLiveSource::Stream(state) => {
                state.lock().await.take();
            }
        }
        Ok(())
    }
}

impl RedisAgentLiveSession {
    async fn read_stream(
        &self,
        state: &Mutex<Option<RedisStreamSession>>,
        mut read: AgentLiveSessionReadRequest,
        output_limit_bytes: usize,
    ) -> Result<voidb_core::AgentLiveSessionEventBatch, PluginSessionError> {
        read.max_bytes = read.max_bytes.min(output_limit_bytes);
        self.contract.validate_read(&read)?;
        let mut guard = state.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| owner_error("The Redis stream source is closed."))?;
        let wait_ms = self.contract.effective_read_wait_ms(&read)?;
        if state.pending.is_empty() {
            let block_ms = if wait_ms == 0 {
                0
            } else {
                state.block_ms.min(wait_ms)
            };
            let count = state.count.min(read.max_events);
            let entries = match state.stream.read(count, block_ms).await {
                Ok(entries) => entries,
                Err(_) => {
                    state.delivery.record_reconnect(&self.contract)?;
                    state
                        .stream
                        .reconnect()
                        .await
                        .map_err(|_| owner_error("Redis stream reconnect failed."))?;
                    state
                        .stream
                        .read(count, block_ms)
                        .await
                        .map_err(|_| owner_error("Redis stream read failed after reconnect."))?
                }
            };
            state.pending.extend(entries);
        }

        let mut candidates = state
            .pending
            .iter()
            .map(|entry| stream_event(entry, &state.scope, &state.redaction_targets))
            .collect::<Vec<_>>();
        let wait_expired = candidates.is_empty();
        let heartbeat_due =
            state.last_heartbeat.elapsed() >= Duration::from_millis(HEARTBEAT_INTERVAL_MS);
        if candidates.is_empty() && read.wait_timeout_ms > 0 && heartbeat_due {
            candidates.push(AgentLiveSessionEventInput {
                observed_at: Utc::now(),
                kind: AgentLiveSessionEventKind::Heartbeat,
                data: Value::Null,
                cursor: None,
                redaction: RedactionStatus::NotRequired,
                terminal: false,
            });
        }
        let (batch, consumed) = state.delivery.build_batch(
            &read,
            &self.contract,
            &candidates,
            wait_expired && candidates.is_empty(),
            false,
        )?;
        let stream_events_consumed = consumed.min(state.pending.len());
        state.pending.drain(..stream_events_consumed);
        if heartbeat_due && consumed > stream_events_consumed {
            state.last_heartbeat = Instant::now();
        }
        Ok(batch)
    }
}

pub(crate) fn redis_live_session_contract(
    capability: &str,
) -> Option<(PluginSessionPurpose, AgentLiveSessionContract)> {
    let (
        kind,
        resource_type,
        identity_schema,
        identity_fields,
        parameters,
        event_schema,
        buffer,
        reconnect,
        delivery,
        cancel,
        start_risk,
    ) = match capability {
        PUBSUB_READ_CAPABILITY => (
            AgentLiveSessionKind::Subscription,
            "redis_subscription",
            json!({
                "type": "object",
                "required": ["scope"],
                "properties": { "scope": { "const": "subscriptions" } },
                "additionalProperties": false
            }),
            vec!["/scope".into()],
            json!({
                "type": "object",
                "properties": {
                    "channels": subscription_array_schema(),
                    "patterns": subscription_array_schema()
                },
                "anyOf": [
                    { "required": ["channels"] },
                    { "required": ["patterns"] }
                ],
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["channel", "payload", "payload_bytes", "truncated"],
                "properties": {
                    "channel": { "type": "string" },
                    "pattern": { "type": ["string", "null"] },
                    "payload": { "type": "string" },
                    "payload_bytes": { "type": "integer", "minimum": 0 },
                    "truncated": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            observation_buffer(),
            restart_reconnect_policy(),
            bounded_delivery(),
            AgentLiveSessionCancelBehavior::CallOnly,
            CapabilityRiskLevel::ReadOnly,
        ),
        MONITOR_READ_CAPABILITY => (
            AgentLiveSessionKind::Events,
            "redis_server",
            json!({
                "type": "object",
                "required": ["scope"],
                "properties": { "scope": { "const": "server" } },
                "additionalProperties": false
            }),
            vec!["/scope".into()],
            empty_object_schema(),
            json!({
                "type": "object",
                "required": ["timestamp", "database", "command_name", "argument_count", "raw_omitted"],
                "properties": {
                    "timestamp": { "type": ["number", "null"] },
                    "database": { "type": ["integer", "null"] },
                    "command_name": { "type": "string" },
                    "argument_count": { "type": "integer", "minimum": 0 },
                    "raw_omitted": { "const": true }
                },
                "additionalProperties": false
            }),
            observation_buffer(),
            restart_reconnect_policy(),
            bounded_delivery(),
            AgentLiveSessionCancelBehavior::CallOnly,
            CapabilityRiskLevel::ExternalSideEffect,
        ),
        STREAM_READ_CAPABILITY => (
            AgentLiveSessionKind::Cursor,
            "redis_stream",
            json!({
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": { "type": "string", "minLength": 1, "maxLength": 1024 },
                    "group": { "type": "string", "minLength": 1, "maxLength": 256 },
                    "consumer": { "type": "string", "minLength": 1, "maxLength": 256 }
                },
                "dependentRequired": {
                    "group": ["consumer"],
                    "consumer": ["group"]
                },
                "additionalProperties": false
            }),
            vec!["/key".into()],
            json!({
                "type": "object",
                "properties": {
                    "start_id": { "type": "string", "minLength": 1, "maxLength": 128 },
                    "count": { "type": "integer", "minimum": 1, "maximum": MAX_STREAM_COUNT, "default": DEFAULT_STREAM_COUNT },
                    "block_ms": { "type": "integer", "minimum": 1, "maximum": MAX_STREAM_BLOCK_MS, "default": DEFAULT_STREAM_BLOCK_MS },
                    "noack": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["id", "fields", "fields_truncated"],
                "properties": {
                    "id": { "type": "string" },
                    "fields": { "type": "object", "additionalProperties": { "type": "string" } },
                    "fields_truncated": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            AgentLiveSessionBufferPolicy {
                max_events: MAX_STREAM_COUNT,
                max_bytes: 1024 * 1024,
                overflow: AgentLiveSessionBufferOverflow::DropOldest,
            },
            AgentLiveSessionReconnectPolicy {
                mode: AgentLiveSessionReconnectMode::Transient,
                max_attempts: 3,
                initial_backoff_ms: 250,
                max_backoff_ms: 2_000,
                resume: AgentLiveSessionResumeMode::BestEffortCursor,
                cursor_kind: Some(AgentLiveSessionCursorKind::EventId),
            },
            AgentLiveSessionDeliveryPolicy {
                backpressure: AgentLiveSessionBackpressureMode::SourcePaced,
                cursor_scope: AgentLiveSessionCursorScopePolicy::Required,
                heartbeat: heartbeat_policy(),
                max_read_wait_ms: MAX_STREAM_BLOCK_MS,
            },
            AgentLiveSessionCancelBehavior::CallAndSource,
            CapabilityRiskLevel::ReadOnly,
        ),
        _ => return None,
    };
    Some((
        PluginSessionPurpose::WatchStream,
        AgentLiveSessionContract {
            protocol_version: AGENT_LIVE_SESSION_PROTOCOL_VERSION,
            kind,
            resource: AgentLiveSessionResourceDescriptor {
                resource_type: resource_type.into(),
                identity_schema,
                identity_fields,
                audit_identity: AgentLiveSessionAuditIdentity::Fingerprint,
            },
            start_parameters_schema: parameters,
            event_schema,
            operations: AgentLiveSessionOperations {
                events: capability.into(),
                input: None,
                resize: None,
                signal: None,
            },
            buffer,
            reconnect,
            delivery,
            control: AgentLiveSessionControlPolicy {
                cancel,
                close: AgentLiveSessionCloseEffect::StopObservation,
            },
            start_risk,
        },
    ))
}

async fn run_pubsub(
    config: RedisConfig,
    channels: Vec<String>,
    patterns: Vec<String>,
    mut source: RedisPubSubSource,
    buffer: AgentLiveSessionEventBuffer,
    redaction_targets: Arc<Vec<RedactionTarget>>,
) {
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        Duration::from_millis(HEARTBEAT_INTERVAL_MS),
    );
    loop {
        tokio::select! {
            message = source.next() => {
                if let Some(message) = message {
                    let payload_bytes = message.payload.len();
                    let payload = String::from_utf8_lossy(&message.payload);
                    let (payload, truncated) = truncate_utf8(&payload, MAX_PUBSUB_PAYLOAD_BYTES);
                    let (payload, payload_redaction) = redact_text_with_targets(&payload, &redaction_targets);
                    let (channel, channel_redaction) = redact_text_with_targets(&message.channel, &redaction_targets);
                    let (pattern, pattern_redaction) = match message.pattern {
                        Some(pattern) => {
                            let (pattern, status) =
                                redact_text_with_targets(&pattern, &redaction_targets);
                            (Some(pattern), status)
                        }
                        None => (None, RedactionStatus::NotRequired),
                    };
                    let redaction = if payload_redaction == RedactionStatus::Applied
                        || channel_redaction == RedactionStatus::Applied
                        || pattern_redaction == RedactionStatus::Applied
                    {
                        RedactionStatus::Applied
                    } else {
                        RedactionStatus::NotRequired
                    };
                    if buffer.push(
                        Utc::now(),
                        AgentLiveSessionEventKind::Data,
                        json!({
                            "channel": channel,
                            "pattern": pattern,
                            "payload": payload,
                            "payload_bytes": payload_bytes,
                            "truncated": truncated
                        }),
                        None,
                        redaction,
                        false,
                    ).await.is_err() {
                        return;
                    }
                } else if !reconnect_pubsub(&config, &channels, &patterns, &buffer, &mut source).await {
                    return;
                }
            }
            _ = heartbeat.tick() => {
                if push_heartbeat(&buffer).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn reconnect_pubsub(
    config: &RedisConfig,
    channels: &[String],
    patterns: &[String],
    buffer: &AgentLiveSessionEventBuffer,
    source: &mut RedisPubSubSource,
) -> bool {
    for attempt in 0..5u32 {
        if buffer.record_reconnect().await.is_err() {
            return false;
        }
        tokio::time::sleep(reconnect_delay(attempt)).await;
        if let Ok(next) = RedisPubSubSource::open(config, channels, patterns).await {
            *source = next;
            return true;
        }
    }
    false
}

async fn run_monitor(
    config: RedisConfig,
    mut source: RedisMonitorSource,
    buffer: AgentLiveSessionEventBuffer,
) {
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        Duration::from_millis(HEARTBEAT_INTERVAL_MS),
    );
    loop {
        tokio::select! {
            line = source.next() => {
                if let Some(line) = line {
                    if buffer.push(
                        Utc::now(),
                        AgentLiveSessionEventKind::Data,
                        monitor_summary(&line),
                        None,
                        RedactionStatus::NotRequired,
                        false,
                    ).await.is_err() {
                        return;
                    }
                } else if !reconnect_monitor(&config, &buffer, &mut source).await {
                    return;
                }
            }
            _ = heartbeat.tick() => {
                if push_heartbeat(&buffer).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn reconnect_monitor(
    config: &RedisConfig,
    buffer: &AgentLiveSessionEventBuffer,
    source: &mut RedisMonitorSource,
) -> bool {
    for attempt in 0..5u32 {
        if buffer.record_reconnect().await.is_err() {
            return false;
        }
        tokio::time::sleep(reconnect_delay(attempt)).await;
        if let Ok(next) = RedisMonitorSource::open(config).await {
            *source = next;
            return true;
        }
    }
    false
}

async fn push_heartbeat(buffer: &AgentLiveSessionEventBuffer) -> Result<(), PluginSessionError> {
    buffer
        .push(
            Utc::now(),
            AgentLiveSessionEventKind::Heartbeat,
            Value::Null,
            None,
            RedactionStatus::NotRequired,
            false,
        )
        .await
        .map(|_| ())
}

fn reconnect_delay(attempt: u32) -> Duration {
    Duration::from_millis((250u64.saturating_mul(1u64 << attempt.min(4))).min(5_000))
}

async fn open_stream_source(
    config: &RedisConfig,
    start: &AgentLiveSessionStartRequest,
) -> Result<RedisStreamSource, PluginSessionError> {
    let key = required_resource_string(&start.resource, "key")?;
    let group = optional_resource_string(&start.resource, "group")?;
    let consumer = optional_resource_string(&start.resource, "consumer")?;
    if group.is_some() != consumer.is_some() {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis stream group and consumer must be supplied together.",
        ));
    }
    let noack = start
        .parameters
        .get("noack")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if noack && group.is_none() {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis stream noack is valid only with a consumer group.",
        ));
    }
    let next_id = start
        .resume_from
        .as_ref()
        .map(|cursor| cursor.value.clone())
        .or_else(|| {
            start
                .parameters
                .get("start_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            if group.is_some() {
                ">".into()
            } else {
                "$".into()
            }
        });
    validate_stream_id(&next_id, group.is_some())?;
    RedisStreamSource::open(config, key, group, consumer, noack, next_id)
        .await
        .map_err(|_| owner_error("Redis stream connection could not be opened."))
}

fn stream_event(
    entry: &RedisStreamEntry,
    scope: &str,
    redaction_targets: &[RedactionTarget],
) -> AgentLiveSessionEventInput {
    let mut redaction = RedactionStatus::NotRequired;
    let fields = entry
        .fields
        .iter()
        .map(|(name, value)| {
            let (name, name_status) = redact_text_with_targets(name, redaction_targets);
            let (value, value_status) = redact_text_with_targets(value, redaction_targets);
            if name_status == RedactionStatus::Applied || value_status == RedactionStatus::Applied {
                redaction = RedactionStatus::Applied;
            }
            (name, Value::String(value))
        })
        .collect::<serde_json::Map<_, _>>();
    AgentLiveSessionEventInput {
        observed_at: Utc::now(),
        kind: AgentLiveSessionEventKind::Data,
        data: json!({
            "id": entry.id,
            "fields": fields,
            "fields_truncated": entry.fields_truncated
        }),
        cursor: Some(AgentLiveSessionCursor {
            kind: AgentLiveSessionCursorKind::EventId,
            value: entry.id.clone(),
            scope: Some(scope.to_string()),
        }),
        redaction,
        terminal: false,
    }
}

fn monitor_summary(line: &str) -> Value {
    let timestamp = line
        .split_whitespace()
        .next()
        .and_then(|value| value.parse::<f64>().ok());
    let database = line
        .split_once('[')
        .and_then(|(_, tail)| tail.split_once(']'))
        .and_then(|(context, _)| context.split_whitespace().next())
        .and_then(|value| value.parse::<i64>().ok());
    let command_tail = line
        .split_once(']')
        .map(|(_, tail)| tail)
        .unwrap_or_default();
    let (command_name, argument_count) = monitor_command_and_count(command_tail);
    json!({
        "timestamp": timestamp,
        "database": database,
        "command_name": command_name,
        "argument_count": argument_count,
        "raw_omitted": true
    })
}

fn monitor_command_and_count(value: &str) -> (String, usize) {
    const MAX_COMMAND_BYTES: usize = 64;

    let mut command = String::new();
    let mut completed = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for character in value.chars() {
        if !quoted {
            if character == '"' {
                quoted = true;
            }
            continue;
        }
        if escaped {
            if completed == 0
                && command.len().saturating_add(character.len_utf8()) <= MAX_COMMAND_BYTES
            {
                command.push(character);
            }
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '"' {
            completed = completed.saturating_add(1);
            quoted = false;
        } else if completed == 0
            && command.len().saturating_add(character.len_utf8()) <= MAX_COMMAND_BYTES
        {
            command.push(character);
        }
    }
    let command = if completed == 0 || command.is_empty() {
        "UNKNOWN".into()
    } else {
        command.to_ascii_uppercase()
    };
    (command, completed.saturating_sub(1))
}

fn subscriptions(parameters: &Value) -> Result<(Vec<String>, Vec<String>), PluginSessionError> {
    let channels = subscription_values(parameters, "channels", false)?;
    let patterns = subscription_values(parameters, "patterns", true)?;
    if channels.is_empty() && patterns.is_empty() {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis Pub/Sub requires at least one channel or safe pattern.",
        ));
    }
    if channels.len().saturating_add(patterns.len()) > MAX_SUBSCRIPTIONS {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis Pub/Sub subscription count exceeds the bounded limit.",
        ));
    }
    Ok((channels, patterns))
}

fn subscription_values(
    parameters: &Value,
    field: &str,
    pattern: bool,
) -> Result<Vec<String>, PluginSessionError> {
    let Some(values) = parameters.get(field) else {
        return Ok(Vec::new());
    };
    let values = values.as_array().ok_or_else(|| {
        error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis subscriptions must be arrays of strings.",
        )
    })?;
    let mut unique = HashSet::new();
    let mut output = Vec::new();
    for value in values {
        let value = value.as_str().filter(|value| {
            !value.is_empty()
                && value.len() <= MAX_SUBSCRIPTION_BYTES
                && !value.chars().any(char::is_control)
        });
        let Some(value) = value else {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Redis subscription names must be bounded printable strings.",
            ));
        };
        if pattern && !safe_subscription_pattern(value) {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Redis Pub/Sub patterns must have a literal prefix and bounded wildcard use.",
            ));
        }
        if unique.insert(value.to_string()) {
            output.push(value.to_string());
        }
    }
    Ok(output)
}

fn safe_subscription_pattern(pattern: &str) -> bool {
    let wildcard_count = pattern
        .chars()
        .filter(|character| matches!(character, '*' | '?' | '['))
        .count();
    !matches!(pattern.chars().next(), Some('*' | '?' | '[')) && wildcard_count <= 4
}

fn read_request(
    request: &AgentSessionCallRequest,
) -> Result<AgentLiveSessionReadRequest, PluginSessionError> {
    if request.input.is_null() {
        Ok(AgentLiveSessionReadRequest::default())
    } else {
        serde_json::from_value(request.input.clone()).map_err(|_| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                "The Redis live-session read request is invalid.",
            )
        })
    }
}

fn bounded_batch(
    request: AgentSessionCallRequest,
    batch: voidb_core::AgentLiveSessionEventBatch,
) -> Result<AgentSessionCallResult, PluginSessionError> {
    let output = serde_json::to_value(batch).map_err(|_| {
        error(
            PluginSessionErrorCode::RedactionFailed,
            "The Redis live-session batch could not be serialized.",
        )
    })?;
    AgentSessionCallResult::bounded(request.call_id, output, request.output_limit_bytes)
}

fn subscription_array_schema() -> Value {
    json!({
        "type": "array",
        "minItems": 1,
        "maxItems": MAX_SUBSCRIPTIONS,
        "uniqueItems": true,
        "items": { "type": "string", "minLength": 1, "maxLength": MAX_SUBSCRIPTION_BYTES }
    })
}

fn empty_object_schema() -> Value {
    json!({ "type": "object", "additionalProperties": false })
}

fn observation_buffer() -> AgentLiveSessionBufferPolicy {
    AgentLiveSessionBufferPolicy {
        max_events: 2_000,
        max_bytes: 2 * 1024 * 1024,
        overflow: AgentLiveSessionBufferOverflow::DropOldest,
    }
}

fn restart_reconnect_policy() -> AgentLiveSessionReconnectPolicy {
    AgentLiveSessionReconnectPolicy {
        mode: AgentLiveSessionReconnectMode::Transient,
        max_attempts: 5,
        initial_backoff_ms: 250,
        max_backoff_ms: 5_000,
        resume: AgentLiveSessionResumeMode::Restart,
        cursor_kind: None,
    }
}

fn heartbeat_policy() -> AgentLiveSessionHeartbeatPolicy {
    AgentLiveSessionHeartbeatPolicy {
        interval_ms: Some(HEARTBEAT_INTERVAL_MS),
        idle_timeout_ms: Some(IDLE_TIMEOUT_MS),
    }
}

fn bounded_delivery() -> AgentLiveSessionDeliveryPolicy {
    AgentLiveSessionDeliveryPolicy {
        backpressure: AgentLiveSessionBackpressureMode::BoundedBuffer,
        cursor_scope: AgentLiveSessionCursorScopePolicy::Optional,
        heartbeat: heartbeat_policy(),
        max_read_wait_ms: 30_000,
    }
}

fn required_resource_string(resource: &Value, field: &str) -> Result<String, PluginSessionError> {
    resource
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_string)
        .ok_or_else(|| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                "Redis stream resource identity is invalid.",
            )
        })
}

fn optional_resource_string(
    resource: &Value,
    field: &str,
) -> Result<Option<String>, PluginSessionError> {
    match resource.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "Redis stream resource identity is invalid.",
                )
            }),
    }
}

fn parameter_usize(
    parameters: &Value,
    field: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, PluginSessionError> {
    let value = parameters
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis live-session numeric parameter is outside its bound.",
        ));
    }
    Ok(value)
}

fn parameter_u64(
    parameters: &Value,
    field: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, PluginSessionError> {
    let value = parameters
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis live-session numeric parameter is outside its bound.",
        ));
    }
    Ok(value)
}

fn validate_stream_id(value: &str, grouped: bool) -> Result<(), PluginSessionError> {
    let numeric = value.split_once('-').is_some_and(|(left, right)| {
        !left.is_empty()
            && !right.is_empty()
            && left.bytes().all(|byte| byte.is_ascii_digit())
            && right.bytes().all(|byte| byte.is_ascii_digit())
    });
    let special = if grouped { value == ">" } else { value == "$" };
    if value.len() > 128 || value.chars().any(char::is_control) || !(numeric || special) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Redis stream start or resume ID is invalid.",
        ));
    }
    Ok(())
}

async fn close_connection(connection: &Mutex<Option<PersistentRedisConnection>>) {
    if let Some(connection) = connection.lock().await.take() {
        connection.close().await;
    }
}

fn owner_error(message: &str) -> PluginSessionError {
    error(PluginSessionErrorCode::OwnerUnavailable, message)
}

fn error(code: PluginSessionErrorCode, message: &str) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_context(capability: &str, input: Value, acknowledged: bool) -> AgentSessionOpenContext {
        AgentSessionOpenContext {
            binding: voidb_core::AgentSessionBinding {
                grant_id: "grant".into(),
                profile_id: "profile".into(),
                plugin_id: "redis".into(),
                purpose: PluginSessionPurpose::WatchStream,
                allowed_capabilities: vec![capability.into()],
                host_generation: 1,
            },
            request: voidb_core::AgentSessionOpenRequest {
                purpose: PluginSessionPurpose::WatchStream,
                capabilities: vec![capability.into()],
                lease_seconds: 60,
                concurrency: voidb_core::AgentSessionConcurrency::Serialized,
                destructive_acknowledged: acknowledged,
                input,
            },
            lease_expires_at: Utc::now() + chrono::Duration::minutes(1),
        }
    }

    fn unavailable_factory() -> RedisAgentSessionFactory {
        RedisAgentSessionFactory::new(RedisConfig {
            host: "127.0.0.1".into(),
            port: 0,
            password: Some("fixture-secret".into()),
            username: None,
            db: 0,
            tls: false,
        })
    }

    #[test]
    fn pubsub_patterns_need_a_literal_prefix() {
        assert!(!safe_subscription_pattern("*"));
        assert!(!safe_subscription_pattern("?events"));
        assert!(safe_subscription_pattern("tenant:*:events"));
    }

    #[test]
    fn monitor_summary_never_returns_arguments() {
        let summary =
            monitor_summary("1721812000.500000 [4 127.0.0.1:55000] \"AUTH\" \"very-secret\"");
        assert_eq!(summary["database"], 4);
        assert_eq!(summary["command_name"], "AUTH");
        assert_eq!(summary["argument_count"], 1);
        assert_eq!(summary["raw_omitted"], true);
        assert!(!summary.to_string().contains("very-secret"));
    }

    #[test]
    fn stream_contract_requires_scoped_event_cursors() {
        let (_, contract) = redis_live_session_contract(STREAM_READ_CAPABILITY).unwrap();
        contract.validate(&[STREAM_READ_CAPABILITY.into()]).unwrap();
        assert_eq!(
            contract.delivery.backpressure,
            AgentLiveSessionBackpressureMode::SourcePaced
        );
        assert_eq!(
            contract.delivery.cursor_scope,
            AgentLiveSessionCursorScopePolicy::Required
        );
    }

    #[test]
    fn stream_ids_keep_plain_and_consumer_group_sentinels_separate() {
        assert!(validate_stream_id("$", false).is_ok());
        assert!(validate_stream_id(">", true).is_ok());
        assert!(validate_stream_id("1712345678901-0", false).is_ok());
        assert!(validate_stream_id("1712345678901-0", true).is_ok());
        assert!(validate_stream_id(">", false).is_err());
        assert!(validate_stream_id("$", true).is_err());
    }

    #[tokio::test]
    async fn monitor_acknowledgement_is_checked_before_connecting() {
        let error = unavailable_factory()
            .open(live_context(
                MONITOR_READ_CAPABILITY,
                json!({
                    "resource": { "scope": "server" },
                    "parameters": {}
                }),
                false,
            ))
            .await
            .err()
            .expect("MONITOR without acknowledgement must fail closed");
        assert_eq!(error.code, PluginSessionErrorCode::PolicyDenied);
    }

    #[tokio::test]
    async fn stream_scope_mismatch_is_rejected_before_connecting() {
        let error = unavailable_factory()
            .open(live_context(
                STREAM_READ_CAPABILITY,
                json!({
                    "resource": { "key": "fixture:stream" },
                    "parameters": { "start_id": "0-0", "block_ms": 250 },
                    "resume_from": {
                        "kind": "event_id",
                        "value": "1-0",
                        "scope": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    }
                }),
                false,
            ))
            .await
            .err()
            .expect("scope mismatch must fail before target I/O");
        assert_eq!(error.code, PluginSessionErrorCode::BindingMismatch);
    }

    #[test]
    fn subscriptions_are_deduplicated_and_globs_stay_bounded() {
        let (channels, patterns) = subscriptions(&json!({
            "channels": ["fixture.events", "fixture.events"],
            "patterns": ["fixture.*"]
        }))
        .expect("bounded subscriptions");
        assert_eq!(channels, vec!["fixture.events"]);
        assert_eq!(patterns, vec!["fixture.*"]);
        assert!(subscriptions(&json!({ "patterns": ["*"] })).is_err());
        assert!(
            subscriptions(&json!({
                "channels": (0..=MAX_SUBSCRIPTIONS)
                    .map(|index| format!("fixture.{index}"))
                    .collect::<Vec<_>>()
            }))
            .is_err()
        );
    }
}
