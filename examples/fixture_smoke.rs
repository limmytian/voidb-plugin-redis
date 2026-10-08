//! Redis fixture-backed capability smoke driver.
//!
//! This example is script-facing. It exercises the Redis plugin capability
//! surface against a disposable local fixture using only scratch keys from the
//! generated fixture environment.

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde_json::{Value, json};
use voidb_core::{
    ActorRef, ActorType, AgentSessionBinding, AgentSessionCallRequest, AgentSessionOpenContext,
    AgentSessionOpenRequest, AgentSessionRef, CapabilityError, CapabilityInvocation,
    CapabilityInvocationResult, InvocationConnectionTarget, InvocationControls, InvocationStatus,
    Pagination, PluginAgentSession, PluginAgentSessionFactory, PluginSessionErrorCode,
    PluginSessionHealth, PluginSessionPurpose, RedactionStatus,
};
use voidb_plugin_redis::{RedisAgentSessionFactory, RedisConfig, invoke_redis_capability};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = config_from_env()?;
    let run_id =
        std::env::var("VOIDB_FIXTURE_RUN_ID").unwrap_or_else(|_| "redis-fixture-smoke".into());
    let key_prefix = required_env("VOIDB_REDIS_SMOKE_KEY_PREFIX")?;
    ensure!(
        key_prefix.starts_with("voidb-fixture:"),
        "refusing Redis smoke outside a generated fixture key prefix: {key_prefix}"
    );

    let probe_key = format!("{key_prefix}:probe");
    let extra_key = format!("{key_prefix}:extra");
    let missing_key = format!("{key_prefix}:missing");
    let value = format!("fixture-value:{run_id}");
    let dry_run_secret = "voidb-redis-fixture-dry-run-secret";
    let password_secret = "voidb-redis-fixture-password-secret";

    let info = invoke_checked(
        &config,
        "info",
        json!({ "section": "server" }),
        false,
        None,
        "redis.info should succeed",
    )
    .await?;
    ensure_succeeded(&info, "redis.info")?;
    ensure!(
        info.output["sections"]
            .as_array()
            .is_some_and(|items| !items.is_empty()),
        "redis.info returned no server sections"
    );

    let missing = invoke_checked(
        &config,
        "get",
        json!({ "key": missing_key }),
        false,
        None,
        "redis.get missing key should succeed",
    )
    .await?;
    ensure_succeeded(&missing, "redis.get missing")?;
    ensure!(
        missing.output["key_type"] == "unknown",
        "missing key should report unknown key_type: {}",
        missing.output
    );

    let dry_set = invoke_checked(
        &unavailable_config(),
        "set",
        json!({ "key": probe_key, "value": dry_run_secret }),
        true,
        None,
        "redis.set dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_set, "redis.set dry-run")?;
    ensure_output_excludes(&dry_set, dry_run_secret, "redis.set dry-run")?;
    ensure!(
        dry_set.output["details"]["key"] == probe_key,
        "dry-run set should report target key only"
    );

    let dry_expire = invoke_checked(
        &unavailable_config(),
        "expire",
        json!({ "key": probe_key, "seconds": 60 }),
        true,
        None,
        "redis.expire dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_expire, "redis.expire dry-run")?;

    let dry_exec = invoke_checked(
        &unavailable_config(),
        "exec",
        json!({ "command": format!("DEL {probe_key}") }),
        true,
        None,
        "redis.exec dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_exec, "redis.exec dry-run")?;
    ensure!(
        dry_exec.output["details"]["command_name"] == "DEL",
        "exec dry-run should expose only the command name"
    );
    ensure_output_excludes(&dry_exec, &probe_key, "redis.exec dry-run")?;

    invoke_checked(
        &config,
        "set",
        json!({ "key": probe_key, "value": value }),
        false,
        None,
        "redis.set probe key",
    )
    .await?;
    invoke_checked(
        &config,
        "set",
        json!({ "key": extra_key, "value": "fixture-extra" }),
        false,
        None,
        "redis.set extra key",
    )
    .await?;

    let get_probe = invoke_checked(
        &config,
        "get",
        json!({ "key": probe_key }),
        false,
        None,
        "redis.get probe key",
    )
    .await?;
    ensure_succeeded(&get_probe, "redis.get probe")?;
    ensure!(
        get_probe.output["key_type"] == "string"
            && get_probe.output["string_value"] == value
            && get_probe.output["string_truncated"] == false,
        "probe key should return an untruncated string preview: {}",
        get_probe.output
    );

    invoke_checked(
        &config,
        "expire",
        json!({ "key": probe_key, "seconds": 60 }),
        false,
        None,
        "redis.expire set probe key",
    )
    .await?;
    let ttl = invoke_checked(
        &config,
        "ttl",
        json!({ "key": probe_key }),
        false,
        None,
        "redis.ttl read probe key",
    )
    .await?;
    ensure_succeeded(&ttl, "redis.ttl read")?;
    let ttl_seconds = ttl.output["ttl"]
        .as_i64()
        .context("redis.ttl output should include ttl")?;
    ensure!(
        (0..=60).contains(&ttl_seconds),
        "ttl should be bounded by the smoke value: {ttl_seconds}"
    );

    let paged_keys = invoke_checked(
        &config,
        "keys",
        json!({ "pattern": format!("{key_prefix}:*") }),
        false,
        Some(Pagination {
            limit: 1,
            cursor: None,
        }),
        "redis.keys paged list",
    )
    .await?;
    ensure_succeeded(&paged_keys, "redis.keys paged")?;
    ensure!(
        paged_keys.output["key_count"].as_u64().unwrap_or_default() <= 1,
        "redis.keys should honor the page limit: {}",
        paged_keys.output
    );

    let keys = invoke_checked(
        &config,
        "keys",
        json!({ "pattern": format!("{key_prefix}:*") }),
        false,
        Some(Pagination {
            limit: 20,
            cursor: None,
        }),
        "redis.keys scratch list",
    )
    .await?;
    ensure_succeeded(&keys, "redis.keys scratch")?;
    ensure_key_present(&keys.output, &probe_key)?;

    let del_dry_run = invoke_checked(
        &unavailable_config(),
        "del",
        json!({ "key": probe_key }),
        true,
        None,
        "redis.del dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&del_dry_run, "redis.del dry-run")?;

    invoke_checked(
        &config,
        "del",
        json!({ "key": probe_key }),
        false,
        None,
        "redis.del probe key",
    )
    .await?;
    invoke_checked(
        &config,
        "del",
        json!({ "key": extra_key }),
        false,
        None,
        "redis.del extra key",
    )
    .await?;
    let after_delete = invoke_checked(
        &config,
        "get",
        json!({ "key": probe_key }),
        false,
        None,
        "redis.get after delete",
    )
    .await?;
    ensure!(
        after_delete.output["key_type"] == "unknown",
        "deleted key should report unknown key_type: {}",
        after_delete.output
    );

    ensure_target_error_redacts_auth(password_secret).await?;
    run_persistent_session_smoke(&config, &key_prefix).await?;
    run_live_session_smoke(&config, &key_prefix).await?;

    println!("redis fixture capability smoke passed");
    println!(
        "capabilities: info, keys, get, set, expire, ttl, del, exec, pubsub_read, monitor_read, stream_read"
    );
    println!("scratch_prefix: {key_prefix}");
    Ok(())
}

async fn run_live_session_smoke(config: &RedisConfig, key_prefix: &str) -> Result<()> {
    run_pubsub_live_smoke(config, key_prefix).await?;
    run_monitor_live_smoke(config, key_prefix).await?;
    run_stream_live_smoke(config, key_prefix).await?;
    Ok(())
}

async fn run_pubsub_live_smoke(config: &RedisConfig, key_prefix: &str) -> Result<()> {
    let channel = format!("{key_prefix}:live-events");
    let factory = RedisAgentSessionFactory::new(config.clone());
    let session = factory
        .open(redis_live_context(
            "redis.pubsub_read",
            json!({
                "resource": { "scope": "subscriptions" },
                "parameters": { "channels": [channel] },
                "buffer": { "max_events": 2, "max_bytes": 65536, "overflow": "drop_oldest" }
            }),
            false,
        ))
        .await
        .map_err(|error| anyhow::anyhow!("open Redis Pub/Sub session: {error}"))?;

    for index in 0..5 {
        redis_exec(config, &format!("PUBLISH {channel} fixture-{index}")).await?;
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let batch = redis_live_call(
        session.as_ref(),
        "redis.pubsub_read",
        "redis-pubsub-slow-consumer",
        json!({ "max_events": 10, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        batch["events"]
            .as_array()
            .is_some_and(|events| !events.is_empty() && events.len() <= 2),
        "Redis Pub/Sub bounded buffer did not retain the expected suffix: {batch}"
    );
    ensure!(
        batch["dropped_events"].as_u64().unwrap_or_default() >= 3,
        "Redis Pub/Sub slow-consumer loss was not disclosed: {batch}"
    );

    redis_exec(config, "CLIENT KILL TYPE pubsub SKIPME yes").await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    redis_exec(config, &format!("PUBLISH {channel} after-reconnect")).await?;
    let reconnected = redis_live_call(
        session.as_ref(),
        "redis.pubsub_read",
        "redis-pubsub-reconnect",
        json!({
            "after_sequence": batch["next_sequence"].as_u64().unwrap_or(1).saturating_sub(1),
            "max_events": 10,
            "max_bytes": 65536,
            "wait_timeout_ms": 2000
        }),
    )
    .await?;
    ensure!(
        reconnected["reconnect_attempts"]
            .as_u64()
            .unwrap_or_default()
            >= 1,
        "Redis Pub/Sub reconnect was not disclosed: {reconnected}"
    );

    let waiting = session.clone();
    let wait_task = tokio::spawn(async move {
        waiting
            .call(AgentSessionCallRequest {
                session: AgentSessionRef::new("redis-fixture-pubsub", 1),
                call_id: "redis-pubsub-cancel".into(),
                capability: "redis.pubsub_read".into(),
                input: json!({
                    "after_sequence": reconnected["next_sequence"].as_u64().unwrap_or(1).saturating_sub(1),
                    "max_events": 1,
                    "max_bytes": 65536,
                    "wait_timeout_ms": 30000
                }),
                destructive_acknowledged: false,
                timeout_ms: Some(30_000),
                output_limit_bytes: 65536,
            })
            .await
    });
    tokio::task::yield_now().await;
    session.cancel("redis-pubsub-cancel").await?;
    let cancelled = wait_task
        .await
        .context("join Redis Pub/Sub cancellation")?
        .expect_err("Redis Pub/Sub wait should be cancelled");
    ensure!(cancelled.code == PluginSessionErrorCode::Cancelled);

    session.close("fixture cleanup".into()).await?;
    session.close("fixture cleanup repeated".into()).await?;
    ensure!(session.health().await? == PluginSessionHealth::Closed);
    let subscribers = redis_exec(config, &format!("PUBSUB NUMSUB {channel}")).await?;
    ensure!(
        subscribers["output"]
            .as_str()
            .is_some_and(|output| output.contains('0')),
        "Redis Pub/Sub close did not release its subscription: {subscribers}"
    );
    Ok(())
}

async fn run_monitor_live_smoke(config: &RedisConfig, key_prefix: &str) -> Result<()> {
    let monitored_key = format!("{key_prefix}:monitor-probe");
    let monitor_secret = "voidb-monitor-argument-secret";
    let factory = RedisAgentSessionFactory::new(config.clone());
    let session = factory
        .open(redis_live_context(
            "redis.monitor_read",
            json!({ "resource": { "scope": "server" }, "parameters": {} }),
            true,
        ))
        .await
        .map_err(|error| anyhow::anyhow!("open Redis MONITOR session: {error}"))?;
    redis_exec(config, &format!("SET {monitored_key} {monitor_secret}")).await?;

    let mut after_sequence = None;
    let mut observed = None;
    for attempt in 0..5 {
        let mut input = json!({
            "max_events": 20,
            "max_bytes": 65536,
            "wait_timeout_ms": 1000
        });
        if let Some(after_sequence) = after_sequence {
            input["after_sequence"] = json!(after_sequence);
        }
        let batch = redis_live_call(
            session.as_ref(),
            "redis.monitor_read",
            &format!("redis-monitor-{attempt}"),
            input,
        )
        .await?;
        after_sequence = batch["next_sequence"]
            .as_u64()
            .map(|sequence| sequence.saturating_sub(1));
        if batch["events"].as_array().is_some_and(|events| {
            events
                .iter()
                .any(|event| event["data"]["command_name"] == "SET")
        }) {
            observed = Some(batch);
            break;
        }
    }
    let observed = observed.context("Redis MONITOR did not observe the fixture SET")?;
    let encoded = serde_json::to_string(&observed)?;
    ensure!(
        !encoded.contains(monitor_secret) && !encoded.contains(&monitored_key),
        "Redis MONITOR exposed command arguments"
    );
    ensure!(
        observed["events"].as_array().is_some_and(|events| events
            .iter()
            .all(|event| { event["data"].is_null() || event["data"]["raw_omitted"] == true })),
        "Redis MONITOR did not mark raw commands omitted: {observed}"
    );
    session.close("fixture cleanup".into()).await?;
    ensure!(session.health().await? == PluginSessionHealth::Closed);
    redis_exec(config, &format!("DEL {monitored_key}")).await?;
    Ok(())
}

async fn run_stream_live_smoke(config: &RedisConfig, key_prefix: &str) -> Result<()> {
    let stream_key = format!("{key_prefix}:stream");
    redis_exec(config, &format!("DEL {stream_key}")).await?;
    redis_exec(
        config,
        &format!("XADD {stream_key} * kind first value fixture"),
    )
    .await?;

    let factory = RedisAgentSessionFactory::new(config.clone());
    let first = factory
        .open(redis_live_context(
            "redis.stream_read",
            json!({
                "resource": { "key": stream_key },
                "parameters": { "start_id": "0-0", "count": 1, "block_ms": 500 }
            }),
            false,
        ))
        .await
        .map_err(|error| anyhow::anyhow!("open Redis Stream session: {error}"))?;
    let first_batch = redis_live_call(
        first.as_ref(),
        "redis.stream_read",
        "redis-stream-first",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        first_batch["events"]
            .as_array()
            .is_some_and(|events| events.len() == 1),
        "Redis Stream did not return the first entry: {first_batch}"
    );
    let resume = first_batch["checkpoint"]["cursor"].clone();
    ensure!(
        resume["scope"]
            .as_str()
            .is_some_and(|scope| scope.starts_with("sha256:"))
    );
    let first_id = first_batch["events"][0]["data"]["id"]
        .as_str()
        .context("Redis Stream first event ID")?
        .to_string();
    first.close("resume fixture".into()).await?;

    redis_exec(
        config,
        &format!("XADD {stream_key} * kind second value fixture"),
    )
    .await?;
    let resumed = factory
        .open(redis_live_context(
            "redis.stream_read",
            json!({
                "resource": { "key": stream_key },
                "parameters": { "count": 1, "block_ms": 500 },
                "resume_from": resume
            }),
            false,
        ))
        .await
        .map_err(|error| anyhow::anyhow!("resume Redis Stream session: {error}"))?;
    let resumed_batch = redis_live_call(
        resumed.as_ref(),
        "redis.stream_read",
        "redis-stream-resumed",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        resumed_batch["events"][0]["data"]["id"].as_str() != Some(first_id.as_str()),
        "Redis Stream resume repeated the accepted entry: {resumed_batch}"
    );

    let wait_after = resumed_batch["next_sequence"]
        .as_u64()
        .unwrap_or(1)
        .saturating_sub(1);
    let waiting = resumed.clone();
    let wait_task = tokio::spawn(async move {
        waiting
            .call(AgentSessionCallRequest {
                session: AgentSessionRef::new("redis-fixture-stream", 1),
                call_id: "redis-stream-cancel".into(),
                capability: "redis.stream_read".into(),
                input: json!({
                    "after_sequence": wait_after,
                    "max_events": 1,
                    "max_bytes": 65536,
                    "wait_timeout_ms": 30000
                }),
                destructive_acknowledged: false,
                timeout_ms: Some(30_000),
                output_limit_bytes: 65536,
            })
            .await
    });
    tokio::task::yield_now().await;
    resumed.cancel("redis-stream-cancel").await?;
    let cancelled = wait_task
        .await
        .context("join Redis Stream cancellation")?
        .expect_err("Redis Stream wait should be cancelled");
    ensure!(cancelled.code == PluginSessionErrorCode::Cancelled);
    ensure!(resumed.health().await? == PluginSessionHealth::Closed);
    resumed.close("fixture cleanup".into()).await?;
    redis_exec(config, &format!("DEL {stream_key}")).await?;
    Ok(())
}

fn redis_live_context(
    capability: &str,
    input: Value,
    acknowledged: bool,
) -> AgentSessionOpenContext {
    let purpose = PluginSessionPurpose::WatchStream;
    AgentSessionOpenContext {
        binding: AgentSessionBinding {
            grant_id: "redis-fixture-live-grant".into(),
            profile_id: "redis-fixture-profile".into(),
            plugin_id: "redis".into(),
            purpose: purpose.clone(),
            allowed_capabilities: vec![capability.into()],
            host_generation: 1,
        },
        request: AgentSessionOpenRequest {
            purpose,
            capabilities: vec![capability.into()],
            lease_seconds: 60,
            concurrency: Default::default(),
            destructive_acknowledged: acknowledged,
            input,
        },
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

async fn redis_live_call(
    session: &dyn PluginAgentSession,
    capability: &str,
    call_id: &str,
    input: Value,
) -> Result<Value> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("redis-fixture-live-session", 1),
            call_id: call_id.into(),
            capability: capability.into(),
            input,
            destructive_acknowledged: false,
            timeout_ms: Some(30_000),
            output_limit_bytes: 65536,
        })
        .await
        .map(|result| result.output)
        .map_err(|error| anyhow::anyhow!("Redis live-session call failed: {error}"))
}

async fn redis_exec(config: &RedisConfig, command: &str) -> Result<Value> {
    invoke_checked(
        config,
        "exec",
        json!({ "command": command }),
        false,
        None,
        "Redis fixture command",
    )
    .await
    .map(|result| result.output)
}

async fn run_persistent_session_smoke(config: &RedisConfig, key_prefix: &str) -> Result<()> {
    let committed_key = format!("{key_prefix}:session-committed");
    let discarded_key = format!("{key_prefix}:session-discarded");
    let factory = RedisAgentSessionFactory::new(config.clone());
    let session = factory
        .open(redis_session_context())
        .await
        .map_err(|error| anyhow::anyhow!("open Redis session: {error}"))?;
    for command in [
        format!("WATCH {committed_key}"),
        "MULTI".into(),
        format!("SET {committed_key} kept"),
        "EXEC".into(),
    ] {
        redis_session_call(session.as_ref(), &command).await?;
    }
    let value = redis_session_call(session.as_ref(), &format!("GET {committed_key}")).await?;
    ensure!(
        value["output"]
            .as_str()
            .unwrap_or_default()
            .contains("kept"),
        "Redis session did not retain WATCH/MULTI state: {value}"
    );
    redis_session_call(session.as_ref(), "MULTI").await?;
    redis_session_call(session.as_ref(), &format!("SET {discarded_key} no")).await?;
    session.close("fixture close".into()).await?;

    let discarded = invoke_checked(
        config,
        "get",
        json!({ "key": discarded_key }),
        false,
        None,
        "redis session close should discard queued transaction",
    )
    .await?;
    ensure!(discarded.output["key_type"] == "unknown");
    let _ = invoke_checked(
        config,
        "del",
        json!({ "key": committed_key }),
        false,
        None,
        "redis session cleanup",
    )
    .await?;
    Ok(())
}

fn redis_session_context() -> AgentSessionOpenContext {
    let purpose = PluginSessionPurpose::DatabaseTransaction;
    AgentSessionOpenContext {
        binding: AgentSessionBinding {
            grant_id: "redis-fixture-grant".into(),
            profile_id: "redis-fixture-profile".into(),
            plugin_id: "redis".into(),
            purpose: purpose.clone(),
            allowed_capabilities: vec!["redis.exec".into()],
            host_generation: 1,
        },
        request: AgentSessionOpenRequest {
            purpose,
            capabilities: vec!["redis.exec".into()],
            lease_seconds: 60,
            concurrency: Default::default(),
            destructive_acknowledged: false,
            input: Value::Null,
        },
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

async fn redis_session_call(session: &dyn PluginAgentSession, command: &str) -> Result<Value> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("redis-fixture-session", 1),
            call_id: format!("redis-session-{command}"),
            capability: "redis.exec".into(),
            input: json!({ "command": command }),
            destructive_acknowledged: true,
            timeout_ms: Some(5_000),
            output_limit_bytes: 64 * 1024,
        })
        .await
        .map(|result| result.output)
        .map_err(|error| anyhow::anyhow!("Redis session call failed: {error}"))
}

fn config_from_env() -> Result<RedisConfig> {
    Ok(RedisConfig {
        host: required_env("VOIDB_REDIS_SMOKE_HOST")?,
        port: required_env("VOIDB_REDIS_SMOKE_PORT")?
            .parse()
            .context("VOIDB_REDIS_SMOKE_PORT must be a u16")?,
        password: None,
        username: None,
        db: required_env("VOIDB_REDIS_SMOKE_DB")?
            .parse()
            .context("VOIDB_REDIS_SMOKE_DB must be a u8")?,
        tls: false,
    })
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
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

async fn invoke(
    config: &RedisConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    page: Option<Pagination>,
) -> std::result::Result<CapabilityInvocationResult, CapabilityError> {
    invoke_redis_capability(
        config,
        CapabilityInvocation {
            id: format!("redis-fixture-smoke-{capability_id}"),
            plugin_id: "redis".into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls {
                dry_run,
                page,
                ..InvocationControls::default()
            },
            actor: Some(ActorRef {
                id: "agent:redis-fixture-smoke".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        },
    )
    .await
}

async fn invoke_checked(
    config: &RedisConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    page: Option<Pagination>,
    label: &str,
) -> Result<CapabilityInvocationResult> {
    invoke(config, capability_id, input, dry_run, page)
        .await
        .map_err(|error| {
            let error_json = serde_json::to_string(&error).unwrap_or_else(|_| format!("{error:?}"));
            anyhow::anyhow!("{label}: {error_json}")
        })
}

fn ensure_succeeded(result: &CapabilityInvocationResult, label: &str) -> Result<()> {
    ensure!(
        result.status == InvocationStatus::Succeeded,
        "{label} returned non-success status: {:?}",
        result.status
    );
    Ok(())
}

fn ensure_output_excludes(
    result: &CapabilityInvocationResult,
    sample: &str,
    label: &str,
) -> Result<()> {
    let output = serde_json::to_string(&result.output)?;
    let summary = serde_json::to_string(&result.output_summary)?;
    ensure!(
        !output.contains(sample) && !summary.contains(sample),
        "{label} output exposed protected sample"
    );
    Ok(())
}

fn ensure_key_present(output: &Value, key: &str) -> Result<()> {
    let keys = output["keys"]
        .as_array()
        .context("redis.keys output should include keys array")?;
    ensure!(
        keys.iter().any(|item| item["key"] == key),
        "redis.keys did not include the probe key: {}",
        output
    );
    Ok(())
}

async fn ensure_target_error_redacts_auth(password_secret: &str) -> Result<()> {
    let config = RedisConfig {
        host: "127.0.0.1".into(),
        port: 0,
        password: Some(password_secret.into()),
        username: Some("fixture-user".into()),
        db: 0,
        tls: false,
    };
    match invoke(&config, "info", json!({}), false, None).await {
        Ok(result) => bail!(
            "expected redis.info target error, got output: {}",
            result.output
        ),
        Err(error) => {
            let text = serde_json::to_string(&error)?;
            ensure!(
                !text.contains(password_secret) && !text.contains("fixture-user"),
                "target error exposed Redis auth material: {text}"
            );
            ensure!(
                matches!(
                    error.redaction,
                    RedactionStatus::Applied | RedactionStatus::NotRequired
                ),
                "target error should report a non-failed redaction state: {:?}",
                error.redaction
            );
        }
    }
    Ok(())
}
