#![allow(clippy::result_large_err)]

use serde_json::{Value, json};
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, RedactionStatus, TargetSystemFailure,
};

use crate::agent_session::{
    MONITOR_READ_CAPABILITY, PUBSUB_READ_CAPABILITY, STREAM_READ_CAPABILITY,
    redis_live_session_contract,
};
use crate::config::RedisConfig;
use crate::redis_ops::{InfoSection, KeyEditData, KeyInfo, RedisKeyType};
use crate::service::RedisService;

const PLUGIN_ID: &str = "redis";
const DEFAULT_KEY_LIMIT: usize = 100;
const MAX_KEY_LIMIT: usize = 200;
const MAX_STRING_VALUE_BYTES: usize = 64 * 1024;
const MAX_EXEC_OUTPUT_BYTES: usize = 64 * 1024;
const COLLECTION_ITEM_LIMIT: usize = 1_000;

pub fn redis_capabilities() -> Vec<CapabilityDefinition> {
    vec![
        capability(
            "keys",
            "Scan Redis keys by pattern.",
            json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "default": "*",
                        "description": "Redis SCAN pattern. Use InvocationControls.page for cursor and limit."
                    }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["pattern", "keys", "limit", "key_count", "truncated"],
                "properties": {
                    "pattern": { "type": "string" },
                    "cursor": { "type": ["string", "null"] },
                    "next_cursor": { "type": ["string", "null"] },
                    "limit": { "type": "integer", "minimum": 1, "maximum": MAX_KEY_LIMIT },
                    "key_count": { "type": "integer", "minimum": 0 },
                    "scan_count": { "type": "integer", "minimum": 0 },
                    "truncated": { "type": "boolean" },
                    "keys": { "type": "array", "items": key_info_schema() }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "redis.keys"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "get",
            "Fetch one Redis key with type-aware data.",
            json!({
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["key", "key_type", "ttl", "size", "collection_has_more"],
                "properties": {
                    "key": { "type": "string" },
                    "key_type": key_type_schema(),
                    "ttl": { "type": "integer" },
                    "size": { "type": "integer", "minimum": 0 },
                    "string_value": { "type": ["string", "null"] },
                    "string_truncated": { "type": "boolean" },
                    "string_byte_limit": { "type": "integer", "minimum": 1 },
                    "hash_fields": { "type": "array" },
                    "list_items": { "type": "array" },
                    "set_members": { "type": "array" },
                    "zset_members": { "type": "array" },
                    "stream_messages": { "type": "array" },
                    "collection_item_limit": { "type": "integer", "minimum": 1 },
                    "collection_has_more": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "redis.get"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "set",
            "Set a Redis string value.",
            json!({
                "type": "object",
                "required": ["key", "value"],
                "properties": {
                    "key": { "type": "string", "minLength": 1 },
                    "value": { "type": "string" }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "properties": write_output_properties(),
                "additionalProperties": false
            }),
            vec!["connection.write", "redis.set"],
            true,
            true,
            Some(30_000),
        ),
        capability(
            "del",
            "Delete one Redis key.",
            json!({
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "properties": write_output_properties(),
                "additionalProperties": false
            }),
            vec!["connection.write", "redis.del"],
            true,
            true,
            Some(30_000),
        ),
        capability(
            "ttl",
            "Read one Redis key TTL without mutating it.",
            json!({
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["key", "ttl"],
                "properties": {
                    "key": { "type": "string" },
                    "ttl": { "type": "integer" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "redis.ttl"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "expire",
            "Set or remove one Redis key TTL.",
            json!({
                "type": "object",
                "required": ["key", "seconds"],
                "properties": {
                    "key": { "type": "string", "minLength": 1 },
                    "seconds": { "type": "integer", "minimum": -1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "properties": write_output_properties(),
                "additionalProperties": false
            }),
            vec!["connection.write", "redis.expire"],
            true,
            true,
            Some(30_000),
        ),
        capability(
            "info",
            "Fetch Redis INFO sections.",
            json!({
                "type": "object",
                "properties": {
                    "section": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["sections"],
                "properties": {
                    "sections": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "entries"],
                            "properties": {
                                "name": { "type": "string" },
                                "entries": { "type": "array" }
                            },
                            "additionalProperties": false
                        }
                    }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "redis.info"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "exec",
            "Execute a raw Redis command string.",
            json!({
                "type": "object",
                "required": ["command"],
                "properties": {
                    "command": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "properties": {
                    "command_name": { "type": "string" },
                    "output": { "type": "string" },
                    "output_truncated": { "type": "boolean" },
                    "output_byte_limit": { "type": "integer", "minimum": 1 },
                    "duration_ms": { "type": "integer", "minimum": 0 },
                    "dry_run": { "type": "boolean" },
                    "would_execute": { "type": "boolean" },
                    "destructive": { "type": "boolean" },
                    "operation": { "type": "string" },
                    "details": { "type": "object" }
                },
                "additionalProperties": false
            }),
            vec!["connection.write", "redis.exec"],
            true,
            true,
            Some(30_000),
        ),
        live_capability(
            PUBSUB_READ_CAPABILITY,
            "Read bounded Redis Pub/Sub messages from explicitly scoped channels or safe patterns.",
            vec!["connection.read", "redis.pubsub"],
        ),
        live_capability(
            MONITOR_READ_CAPABILITY,
            "Observe bounded Redis MONITOR command metadata with arguments always omitted.",
            vec!["connection.read", "redis.monitor"],
        ),
        live_capability(
            STREAM_READ_CAPABILITY,
            "Read Redis Stream entries through a bounded blocking, resumable session.",
            vec!["connection.read", "redis.streams.read"],
        ),
    ]
}

fn key_type_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["string", "hash", "list", "set", "zset", "stream", "unknown"]
    })
}

fn key_info_schema() -> Value {
    json!({
        "type": "object",
        "required": ["key", "key_type", "ttl"],
        "properties": {
            "key": { "type": "string" },
            "key_type": key_type_schema(),
            "ttl": { "type": "integer" }
        },
        "additionalProperties": false
    })
}

fn write_output_properties() -> Value {
    json!({
        "ok": { "type": "boolean" },
        "message": { "type": "string" },
        "dry_run": { "type": "boolean" },
        "would_execute": { "type": "boolean" },
        "destructive": { "type": "boolean" },
        "operation": { "type": "string" },
        "details": { "type": "object" }
    })
}

pub async fn invoke_redis_capability(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match Redis.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "keys" => invoke_keys(config, invocation).await,
        "get" => invoke_get(config, invocation).await,
        "set" => invoke_set(config, invocation).await,
        "del" => invoke_del(config, invocation).await,
        "ttl" => invoke_ttl(config, invocation).await,
        "expire" => invoke_expire(config, invocation).await,
        "info" => invoke_info(config, invocation).await,
        "exec" => invoke_exec(config, invocation).await,
        "pubsub_read" | "monitor_read" | "stream_read" => Err(unavailable_error(
            "unavailable.session_required",
            "This Redis live workflow requires a persistent agent session.",
            json!({ "capability_id": invocation.capability_id }),
        )),
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "Redis capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

fn live_capability(
    qualified_id: &str,
    description: &str,
    permissions: Vec<&str>,
) -> CapabilityDefinition {
    let id = qualified_id
        .strip_prefix("redis.")
        .expect("Redis live capability ID");
    let (purpose, contract) =
        redis_live_session_contract(qualified_id).expect("Redis live-session contract");
    let handoff_capabilities = contract
        .operations
        .capabilities()
        .cloned()
        .collect::<Vec<_>>();
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema: live_read_schema(),
        output_schema: live_batch_schema(),
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: redis_live_authorization(qualified_id, purpose.clone()),
        risk: contract.start_risk,
        destructive: false,
        streaming: true,
        execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
        session_handoff: Some(
            voidb_core::CapabilitySessionHandoff::new(purpose, handoff_capabilities)
                .with_live_session(contract),
        ),
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run: false,
        default_timeout_ms: Some(30_000),
    }
}

fn redis_live_authorization(
    capability: &str,
    purpose: voidb_core::PluginSessionPurpose,
) -> voidb_core::CapabilityAuthorizationMetadata {
    let fields = match capability {
        PUBSUB_READ_CAPABILITY => vec![
            voidb_core::CapabilityApprovalField::new(
                "/parameters/channels",
                "Channels",
                voidb_core::CapabilityApprovalValueType::StringList,
            )
            .with_constraint(voidb_core::CapabilityConstraintKind::Subset),
            voidb_core::CapabilityApprovalField::new(
                "/parameters/patterns",
                "Safe channel patterns",
                voidb_core::CapabilityApprovalValueType::StringList,
            )
            .with_constraint(voidb_core::CapabilityConstraintKind::Subset),
        ],
        STREAM_READ_CAPABILITY => vec![
            voidb_core::CapabilityApprovalField::new(
                "/resource/key",
                "Stream key",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Prefix),
            voidb_core::CapabilityApprovalField::new(
                "/resource/group",
                "Consumer group",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            ),
        ],
        MONITOR_READ_CAPABILITY => vec![
            voidb_core::CapabilityApprovalField::new(
                "/resource/scope",
                "Server scope",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        ],
        _ => Vec::new(),
    };
    let note = if capability == MONITOR_READ_CAPABILITY {
        "MONITOR is excluded from broad read presets, requires explicit session-open acknowledgement, and emits command metadata without arguments."
    } else {
        "Redis live-session resource and subscription scope are revalidated when the session opens."
    };
    voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_session_purposes(vec![purpose])
        .with_note(note)
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
}

fn live_read_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "after_sequence": { "type": "integer", "minimum": 0 },
            "max_events": { "type": "integer", "minimum": 1, "maximum": 1000 },
            "max_bytes": { "type": "integer", "minimum": 1, "maximum": 1048576 },
            "wait_timeout_ms": { "type": "integer", "minimum": 0, "maximum": 30000 }
        },
        "additionalProperties": false
    })
}

fn live_batch_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "protocol_version", "events", "next_sequence", "timed_out", "source_closed",
            "dropped_events", "dropped_bytes", "coalesced_events", "reconnect_attempts"
        ],
        "properties": {
            "protocol_version": { "type": "integer", "const": 1 },
            "events": { "type": "array", "maxItems": 1000 },
            "next_sequence": { "type": "integer", "minimum": 1 },
            "resume_cursor": { "type": "object" },
            "checkpoint": { "type": "object" },
            "oldest_available_sequence": { "type": "integer", "minimum": 1 },
            "truncated": { "type": "boolean" },
            "timed_out": { "type": "boolean" },
            "source_closed": { "type": "boolean" },
            "dropped_events": { "type": "integer", "minimum": 0 },
            "dropped_bytes": { "type": "integer", "minimum": 0 },
            "coalesced_events": { "type": "integer", "minimum": 0 },
            "reconnect_attempts": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    supports_dry_run: bool,
    default_timeout_ms: Option<u64>,
) -> CapabilityDefinition {
    let session_handoff = (id == "exec").then(|| {
        voidb_core::CapabilitySessionHandoff::new(
            voidb_core::PluginSessionPurpose::CacheCommand,
            ["redis.exec"],
        )
    });
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: redis_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming: false,
        execution_mode: if session_handoff.is_some() {
            voidb_core::CapabilityExecutionMode::Both
        } else {
            voidb_core::CapabilityExecutionMode::Stateless
        },
        session_handoff,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms,
    }
}

fn redis_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let metadata = voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_session_purposes(vec![voidb_core::PluginSessionPurpose::CacheCommand]);
    match id {
        "keys" => metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
            voidb_core::CapabilityApprovalField::new(
                "/pattern",
                "Key pattern",
                voidb_core::CapabilityApprovalValueType::String,
            ),
        ])),
        "get" | "set" | "del" | "ttl" | "expire" => metadata
            .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
                voidb_core::CapabilityApprovalField::new(
                    "/key",
                    "Key or key prefix",
                    voidb_core::CapabilityApprovalValueType::ResourceId,
                )
                .required()
                .with_constraint(voidb_core::CapabilityConstraintKind::Prefix)
                .with_risk_emphasis(if matches!(id, "set" | "del" | "expire") {
                    voidb_core::CapabilityApprovalRiskEmphasis::Destructive
                } else {
                    voidb_core::CapabilityApprovalRiskEmphasis::Normal
                }),
            ]))
            .with_note(if id == "ttl" {
                "TTL inspection is read-only; mutation is isolated in redis.expire."
            } else if id == "expire" {
                "TTL mutation is isolated from redis.ttl and requires destructive acknowledgement."
            } else {
                "The declared key prefix is revalidated against the normalized operation."
            }),
        "exec" => metadata
            .with_session_purposes(vec![
                voidb_core::PluginSessionPurpose::CacheCommand,
                voidb_core::PluginSessionPurpose::DatabaseTransaction,
            ])
            .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
                voidb_core::CapabilityApprovalField::new(
                    "/command",
                    "Raw Redis command",
                    voidb_core::CapabilityApprovalValueType::String,
                )
                .required()
                .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
            ]))
            .with_note(
                "Raw commands are exact-string constrained and remain outside the recommended Interactive/Execute preset.",
            ),
        _ => metadata,
    }
}

async fn invoke_keys(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let pattern = optional_string(&invocation.input, "pattern")?.unwrap_or_else(|| "*".to_string());
    let page_request = key_page_request(&invocation)?;
    let service = RedisService::new_direct(config.clone());
    let (keys, next_cursor) = service
        .scan_keys(page_request.scan_cursor, Some(&pattern))
        .await
        .map_err(|e| target_error("redis.keys_failed", e))?;
    let scanned_key_count = keys.len();
    let skipped = page_request.offset.min(scanned_key_count);
    let output_keys = keys
        .into_iter()
        .skip(skipped)
        .take(page_request.limit)
        .map(key_info_output)
        .collect::<Vec<_>>();
    let key_count = output_keys.len();
    let local_offset = skipped + key_count;
    let next_page_cursor = if local_offset < scanned_key_count {
        Some(format!("{}:{}", page_request.scan_cursor, local_offset))
    } else if next_cursor != 0 {
        Some(next_cursor.to_string())
    } else {
        None
    };
    let truncated = next_page_cursor.is_some();
    let output = json!({
        "pattern": pattern,
        "cursor": page_request.output_cursor(),
        "next_cursor": next_page_cursor,
        "limit": page_request.limit,
        "key_count": key_count,
        "scan_count": scanned_key_count,
        "truncated": truncated,
        "keys": output_keys
    });
    let page = output["next_cursor"]
        .as_str()
        .map(|next_cursor| InvocationOutputPage {
            next_cursor: Some(next_cursor.to_string()),
        });
    let summary = json!({
        "key_count": key_count,
        "scan_count": scanned_key_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"]
    });

    Ok(result(invocation.id, output, summary, page))
}

async fn invoke_get(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let key = required_string(&invocation.input, "key")?;
    let service = RedisService::new_direct(config.clone());
    let data = service
        .fetch_key_data(&key)
        .await
        .map_err(|e| target_error("redis.get_failed", e))?;
    let summary = json!({
        "key": data.key.clone(),
        "key_type": key_type_label(data.key_type),
        "ttl": data.ttl,
        "size": data.size,
        "collection_has_more": data.collection_has_more
    });

    Ok(result(invocation.id, key_data_output(data), summary, None))
}

async fn invoke_set(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let key = required_string(&invocation.input, "key")?;
    let value = required_string_allow_empty(&invocation.input, "value")?;
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "set", json!({ "key": key })));
    }

    let service = RedisService::new_direct(config.clone());
    let message = service
        .set_string(&key, &value)
        .await
        .map_err(|e| target_error("redis.set_failed", e))?;

    Ok(result(
        invocation.id,
        json!({ "ok": true, "message": message }),
        json!({ "ok": true, "operation": "set" }),
        None,
    ))
}

async fn invoke_del(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let key = required_string(&invocation.input, "key")?;
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "del", json!({ "key": key })));
    }

    let service = RedisService::new_direct(config.clone());
    let message = service
        .delete_key(&key)
        .await
        .map_err(|e| target_error("redis.del_failed", e))?;

    Ok(result(
        invocation.id,
        json!({ "ok": true, "message": message }),
        json!({ "ok": true, "operation": "del" }),
        None,
    ))
}

async fn invoke_ttl(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let key = required_string(&invocation.input, "key")?;
    let service = RedisService::new_direct(config.clone());
    let preview = service
        .fetch_key_preview(&key)
        .await
        .map_err(|e| target_error("redis.ttl_failed", e))?;
    Ok(result(
        invocation.id,
        json!({ "key": key, "ttl": preview.ttl }),
        json!({ "ttl": preview.ttl }),
        None,
    ))
}

async fn invoke_expire(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let key = required_string(&invocation.input, "key")?;
    let seconds = required_i64(&invocation.input, "seconds")?;
    if invocation.controls.dry_run {
        return Ok(dry_run_result(
            invocation.id,
            "expire",
            json!({ "key": key, "seconds": seconds }),
        ));
    }
    let service = RedisService::new_direct(config.clone());
    let message = service
        .set_ttl(&key, seconds)
        .await
        .map_err(|e| target_error("redis.expire_failed", e))?;
    Ok(result(
        invocation.id,
        json!({ "ok": true, "message": message }),
        json!({ "ok": true, "operation": "expire" }),
        None,
    ))
}

async fn invoke_info(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let section = optional_string(&invocation.input, "section")?;
    let service = RedisService::new_direct(config.clone());
    let mut sections = service
        .fetch_server_info()
        .await
        .map_err(|e| target_error("redis.info_failed", e))?;
    if let Some(section) = section {
        sections.retain(|info_section| info_section.name.eq_ignore_ascii_case(&section));
    }
    let section_count = sections.len();
    Ok(result(
        invocation.id,
        json!({ "sections": info_sections_output(sections) }),
        json!({ "section_count": section_count }),
        None,
    ))
}

async fn invoke_exec(
    config: &RedisConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let command = required_string(&invocation.input, "command")?;
    let command_name = command_name(&command);
    if invocation.controls.dry_run {
        return Ok(dry_run_result(
            invocation.id,
            "exec",
            json!({ "command_name": command_name }),
        ));
    }

    let service = RedisService::new_direct(config.clone());
    let command_result = service
        .execute_command(&command)
        .await
        .map_err(|e| target_error("redis.exec_failed", e))?;
    if let Some(error) = command_result.error {
        return Err(target_error("redis.exec_failed", error));
    }
    let (output, output_truncated) = truncate_string(command_result.output, MAX_EXEC_OUTPUT_BYTES);

    Ok(result(
        invocation.id,
        json!({
            "command_name": command_name.clone(),
            "output": output,
            "output_truncated": output_truncated,
            "output_byte_limit": MAX_EXEC_OUTPUT_BYTES,
            "duration_ms": command_result.duration_ms
        }),
        json!({
            "command_name": command_name,
            "output_truncated": output_truncated,
            "duration_ms": command_result.duration_ms
        }),
        None,
    ))
}

fn key_info_output(info: KeyInfo) -> Value {
    json!({
        "key": info.key,
        "key_type": key_type_label(info.key_type),
        "ttl": info.ttl
    })
}

fn key_data_output(data: KeyEditData) -> Value {
    let (string_value, value_truncated) = match data.string_value {
        Some(value) => {
            let (value, truncated) = truncate_string(value, MAX_STRING_VALUE_BYTES);
            (Some(value), truncated)
        }
        None => (None, false),
    };
    let string_truncated = data.string_truncated || value_truncated;

    json!({
        "key": data.key,
        "key_type": key_type_label(data.key_type),
        "ttl": data.ttl,
        "size": data.size,
        "string_value": string_value,
        "string_truncated": string_truncated,
        "string_byte_limit": MAX_STRING_VALUE_BYTES,
        "hash_fields": data.hash_fields.unwrap_or_default().into_iter().map(|(field, value)| {
            json!({ "field": field, "value": value })
        }).collect::<Vec<_>>(),
        "list_items": data.list_items.unwrap_or_default(),
        "set_members": data.set_members.unwrap_or_default(),
        "zset_members": data.zset_members.unwrap_or_default().into_iter().map(|(member, score)| {
            json!({ "member": member, "score": score })
        }).collect::<Vec<_>>(),
        "stream_messages": data.stream_messages.unwrap_or_default().into_iter().map(|(id, fields)| {
            json!({
                "id": id,
                "fields": fields.into_iter().map(|(field, value)| {
                    json!({ "field": field, "value": value })
                }).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>(),
        "collection_item_limit": COLLECTION_ITEM_LIMIT,
        "collection_has_more": data.collection_has_more
    })
}

fn info_sections_output(sections: Vec<InfoSection>) -> Vec<Value> {
    sections
        .into_iter()
        .map(|section| {
            json!({
                "name": section.name,
                "entries": section.entries.into_iter().map(|(key, value)| {
                    json!({ "key": key, "value": value })
                }).collect::<Vec<_>>()
            })
        })
        .collect()
}

fn key_type_label(key_type: RedisKeyType) -> &'static str {
    match key_type {
        RedisKeyType::String => "string",
        RedisKeyType::Hash => "hash",
        RedisKeyType::List => "list",
        RedisKeyType::Set => "set",
        RedisKeyType::ZSet => "zset",
        RedisKeyType::Stream => "stream",
        RedisKeyType::Unknown => "unknown",
    }
}

fn command_name(command: &str) -> String {
    command
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyPageRequest {
    scan_cursor: u64,
    offset: usize,
    limit: usize,
}

impl KeyPageRequest {
    fn output_cursor(self) -> Value {
        if self.scan_cursor == 0 && self.offset == 0 {
            Value::Null
        } else if self.offset == 0 {
            json!(self.scan_cursor.to_string())
        } else {
            json!(format!("{}:{}", self.scan_cursor, self.offset))
        }
    }
}

fn key_page_request(invocation: &CapabilityInvocation) -> Result<KeyPageRequest, CapabilityError> {
    let limit = invocation
        .controls
        .page
        .as_ref()
        .map(|page| page.limit as usize)
        .unwrap_or(DEFAULT_KEY_LIMIT);
    if limit == 0 || limit > MAX_KEY_LIMIT {
        return Err(validation_error(
            "validation.page_limit_invalid",
            "Redis keys page limit must be between 1 and the maximum key limit.",
            json!({ "limit": limit, "maximum": MAX_KEY_LIMIT }),
        ));
    }

    let (scan_cursor, offset) = invocation
        .controls
        .page
        .as_ref()
        .and_then(|page| page.cursor.as_deref())
        .map(parse_key_cursor)
        .transpose()?
        .unwrap_or((0, 0));

    Ok(KeyPageRequest {
        scan_cursor,
        offset,
        limit,
    })
}

fn parse_key_cursor(cursor: &str) -> Result<(u64, usize), CapabilityError> {
    let Some((scan_cursor, offset)) = cursor.split_once(':') else {
        return cursor
            .parse::<u64>()
            .map(|scan_cursor| (scan_cursor, 0))
            .map_err(|_| invalid_key_cursor(cursor));
    };

    let scan_cursor = scan_cursor
        .parse::<u64>()
        .map_err(|_| invalid_key_cursor(cursor))?;
    let offset = offset
        .parse::<usize>()
        .map_err(|_| invalid_key_cursor(cursor))?;
    Ok((scan_cursor, offset))
}

fn invalid_key_cursor(cursor: &str) -> CapabilityError {
    validation_error(
        "validation.invalid_cursor",
        "Redis keys cursor must be an unsigned integer or scan-cursor:offset pair.",
        json!({ "cursor": cursor }),
    )
}

fn truncate_string(value: String, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value, false);
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Required input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "Required string input field is missing.",
                    json!({ "field": field }),
                )
            }),
        None => Err(validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )),
    }
}

fn required_string_allow_empty(input: &Value, field: &str) -> Result<String, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Required input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value.as_str().map(str::to_string).ok_or_else(|| {
            validation_error(
                "validation.input_field_required",
                "Required string input field is missing.",
                json!({ "field": field }),
            )
        }),
        None => Err(validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )),
    }
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    input
        .get(field)
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                validation_error(
                    "validation.input_field_invalid",
                    "Optional string input field must be a string.",
                    json!({ "field": field }),
                )
            })
        })
        .transpose()
}

fn required_i64(input: &Value, field: &str) -> Result<i64, CapabilityError> {
    input.get(field).and_then(Value::as_i64).ok_or_else(|| {
        validation_error(
            "validation.input_field_required",
            "Required integer input field is missing or invalid.",
            json!({ "field": field }),
        )
    })
}

fn dry_run_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    result(
        invocation_id,
        json!({
            "dry_run": true,
            "would_execute": true,
            "destructive": true,
            "operation": operation,
            "details": details
        }),
        json!({ "dry_run": true, "operation": operation }),
        None,
    )
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
    )
}

fn target_error(code: &str, message: String) -> CapabilityError {
    let (message, redaction) = redact_redis_target_message(message);
    CapabilityError {
        category: CapabilityErrorCategory::TargetSystem,
        code: code.to_string(),
        message: "Redis target operation failed.".to_string(),
        details: Value::Null,
        target: Some(TargetSystemFailure {
            system: Some(PLUGIN_ID.to_string()),
            code: None,
            message: Some(message),
        }),
        retryable: false,
        redaction,
    }
}

fn redact_redis_target_message(message: String) -> (String, RedactionStatus) {
    let original = message.clone();
    let redacted = redact_redis_url_auth(redact_redis_url_auth(message, "redis://"), "rediss://");
    let redaction = if redacted != original {
        RedactionStatus::Applied
    } else {
        RedactionStatus::NotRequired
    };
    (redacted, redaction)
}

fn redact_redis_url_auth(mut message: String, scheme: &str) -> String {
    let mut search_from = 0;
    while let Some(relative_start) = message[search_from..].find(scheme) {
        let scheme_start = search_from + relative_start;
        let auth_start = scheme_start + scheme.len();
        let tail = &message[auth_start..];
        let slash = tail.find('/');
        let Some(at) = tail.find('@') else {
            search_from = auth_start;
            continue;
        };

        if slash.is_some_and(|slash| slash < at) {
            search_from = auth_start;
            continue;
        }

        let auth_end = auth_start + at;
        message.replace_range(auth_start..auth_end, "<redacted>");
        search_from = auth_start + "<redacted>@".len();
    }
    message
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction: RedactionStatus::NotRequired,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;
    use voidb_core::{
        ActorRef, ActorType, CapabilityErrorCategory, InvocationConnectionTarget,
        InvocationControls, InvocationStatus, Pagination, RedactionStatus,
    };

    #[test]
    fn exposes_redis_capability_metadata() {
        let capabilities = redis_capabilities();

        assert_eq!(capabilities.len(), 11);
        assert!(
            capabilities
                .iter()
                .any(|cap| cap.qualified_id() == "redis.keys")
        );
        assert!(
            capabilities
                .iter()
                .any(|cap| cap.id == "set" && cap.destructive)
        );
        assert!(
            capabilities
                .iter()
                .any(|cap| cap.id == "set" && cap.supports_dry_run)
        );
        assert!(capabilities.iter().any(|cap| {
            cap.id == "ttl"
                && !cap.destructive
                && cap.permissions.contains(&"connection.read".to_string())
        }));
        assert!(capabilities.iter().any(|cap| {
            cap.id == "expire"
                && cap.destructive
                && cap.permissions.contains(&"connection.write".to_string())
        }));
    }

    #[test]
    fn keys_schema_discovery_is_agent_safe_and_bounded() {
        let capabilities = redis_capabilities();
        let keys = capabilities.iter().find(|cap| cap.id == "keys").unwrap();

        assert_eq!(
            keys.output_schema["properties"]["limit"]["maximum"],
            json!(MAX_KEY_LIMIT)
        );
        assert_eq!(keys.output_schema["required"][0], "pattern");
        assert_eq!(
            keys.output_schema["properties"]["keys"]["items"]["properties"]["key_type"]["enum"][0],
            "string"
        );
        assert!(keys.required_secret_classes.is_empty());
        assert!(!serde_json::to_string(keys).unwrap().contains("password"));
    }

    #[test]
    fn get_schema_exposes_bounded_collection_shape() {
        let capabilities = redis_capabilities();
        let get = capabilities.iter().find(|cap| cap.id == "get").unwrap();

        assert_eq!(get.output_schema["required"][0], "key");
        assert_eq!(
            get.output_schema["properties"]["string_byte_limit"]["type"],
            "integer"
        );
        assert_eq!(
            get.output_schema["properties"]["collection_item_limit"]["type"],
            "integer"
        );
        assert_eq!(
            get.output_schema["properties"]["collection_has_more"]["type"],
            "boolean"
        );
    }

    #[test]
    fn set_schema_discovery_is_agent_safe() {
        let capabilities = redis_capabilities();
        let set = capabilities.iter().find(|cap| cap.id == "set").unwrap();

        assert_eq!(set.input_schema["required"][0], "key");
        assert_eq!(set.input_schema["required"][1], "value");
        assert!(set.destructive);
        assert!(set.supports_dry_run);
        assert_eq!(
            set.output_schema["properties"]["dry_run"]["type"],
            "boolean"
        );
        assert!(!serde_json::to_string(set).unwrap().contains("password"));
    }

    #[tokio::test]
    async fn set_dry_run_does_not_open_redis_connection() {
        let mut invocation = invocation(
            "set",
            json!({
                "key": "agent:test",
                "value": "redacted-safe"
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_redis_capability(&RedisConfig::default(), invocation)
            .await
            .unwrap();

        assert_eq!(result.status, InvocationStatus::Succeeded);
        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output["details"]["key"], "agent:test");
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("redacted-safe")
        );
    }

    #[tokio::test]
    async fn write_dry_runs_do_not_open_redis_connection() {
        let config = unavailable_config();

        let mut del = invocation("del", json!({ "key": "agent:test" }));
        del.controls.dry_run = true;
        let del_result = invoke_redis_capability(&config, del).await.unwrap();
        assert_eq!(del_result.output["operation"], "del");

        let mut expire = invocation("expire", json!({ "key": "agent:test", "seconds": 60 }));
        expire.controls.dry_run = true;
        let expire_result = invoke_redis_capability(&config, expire).await.unwrap();
        assert_eq!(expire_result.output["operation"], "expire");

        let mut exec = invocation("exec", json!({ "command": "DEL agent:test" }));
        exec.controls.dry_run = true;
        let exec_result = invoke_redis_capability(&config, exec).await.unwrap();
        assert_eq!(exec_result.output["operation"], "exec");
        assert_eq!(exec_result.output["details"]["command_name"], "DEL");
    }

    #[tokio::test]
    async fn invalid_keys_cursor_returns_structured_validation_error() {
        let mut invocation = invocation("keys", json!({ "pattern": "agent:*" }));
        invocation.controls.page = Some(Pagination {
            limit: 50,
            cursor: Some("not-a-cursor".into()),
        });

        let error = invoke_redis_capability(&RedisConfig::default(), invocation)
            .await
            .unwrap_err();

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.invalid_cursor");
        assert_eq!(error.details["cursor"], "not-a-cursor");
        assert_eq!(error.redaction, RedactionStatus::NotRequired);
        assert!(error.target.is_none());
    }

    #[tokio::test]
    async fn invalid_keys_page_limit_returns_structured_validation_error() {
        let mut invocation = invocation("keys", json!({ "pattern": "agent:*" }));
        invocation.controls.page = Some(Pagination {
            limit: 0,
            cursor: None,
        });

        let error = invoke_redis_capability(&RedisConfig::default(), invocation)
            .await
            .unwrap_err();

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.page_limit_invalid");
        assert_eq!(error.details["maximum"], MAX_KEY_LIMIT);
        assert_eq!(error.redaction, RedactionStatus::NotRequired);
        assert!(error.target.is_none());
    }

    #[test]
    fn key_page_request_accepts_legacy_and_composite_cursors() {
        let default_request = key_page_request(&invocation("keys", json!({}))).unwrap();
        assert_eq!(
            default_request,
            KeyPageRequest {
                scan_cursor: 0,
                offset: 0,
                limit: DEFAULT_KEY_LIMIT
            }
        );

        let mut legacy = invocation("keys", json!({}));
        legacy.controls.page = Some(Pagination {
            limit: 25,
            cursor: Some("42".into()),
        });
        assert_eq!(
            key_page_request(&legacy).unwrap(),
            KeyPageRequest {
                scan_cursor: 42,
                offset: 0,
                limit: 25
            }
        );

        let mut composite = invocation("keys", json!({}));
        composite.controls.page = Some(Pagination {
            limit: 25,
            cursor: Some("42:10".into()),
        });
        assert_eq!(
            key_page_request(&composite).unwrap(),
            KeyPageRequest {
                scan_cursor: 42,
                offset: 10,
                limit: 25
            }
        );
    }

    #[tokio::test]
    async fn invalid_required_string_type_returns_validation_error() {
        let invocation = invocation("get", json!({ "key": 42 }));
        let error = invoke_redis_capability(&RedisConfig::default(), invocation)
            .await
            .unwrap_err();

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.input_field_invalid");
        assert_eq!(error.details["field"], "key");
        assert!(error.target.is_none());
    }

    #[tokio::test]
    async fn unavailable_service_returns_structured_target_error() {
        let invocation = invocation("get", json!({ "key": "agent:test" }));
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            invoke_redis_capability(&unavailable_config(), invocation),
        )
        .await
        .expect("redis target failure should be fast")
        .unwrap_err();

        assert_eq!(error.category, CapabilityErrorCategory::TargetSystem);
        assert_eq!(error.code, "redis.get_failed");
        assert_eq!(error.details, Value::Null);
        assert_eq!(error.redaction, RedactionStatus::NotRequired);
        let target = error.target.unwrap();
        assert_eq!(target.system.as_deref(), Some(PLUGIN_ID));
        assert!(target.message.unwrap().contains("Connection failed"));
    }

    #[test]
    fn target_error_redacts_redis_url_auth() {
        let error = target_error(
            "redis.test_failed",
            "failed rediss://user:secret@127.0.0.1:6379/0".into(),
        );

        assert_eq!(error.redaction, RedactionStatus::Applied);
        let message = error.target.unwrap().message.unwrap();
        assert!(message.contains("rediss://<redacted>@127.0.0.1:6379/0"));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn string_truncation_preserves_utf8_boundaries() {
        let (value, truncated) = truncate_string("ab猫cd".to_string(), 4);

        assert_eq!(value, "ab");
        assert!(truncated);
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: "invoke-test".into(),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls::default(),
            actor: Some(ActorRef {
                id: "agent:test".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        }
    }

    fn unavailable_config() -> RedisConfig {
        RedisConfig {
            host: "127.0.0.1".into(),
            port: 0,
            password: None,
            username: None,
            db: 0,
            tls: false,
        }
    }
}
