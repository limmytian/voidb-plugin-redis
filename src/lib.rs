//! VoidB Redis Plugin - Key-value service and capability surface.

mod agent_session;
mod capabilities;
mod config;
pub mod redis_ops;
pub mod service;
mod cli_plugin;

pub use capabilities::{invoke_redis_capability, redis_capabilities};
pub use agent_session::RedisAgentSessionFactory;
pub use config::RedisConfig;
pub use cli_plugin::create_redis_cli_plugin;

/// Test a Redis connection by sending PING.
/// Used by ConnectionManager for the "Test Connection" feature.
pub async fn test_connection(conn: &voidb_core::connection::ConnectionConfig) -> anyhow::Result<String> {
    let redis_config: RedisConfig = conn
        .plugin_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing plugin_config"))
        .and_then(|pc| serde_json::from_value(pc.clone()).map_err(Into::into))?;

    let url = redis_config.to_url();
    let client = redis::Client::open(url.as_str())
        .map_err(|e| anyhow::anyhow!("Client error: {}", e))?;
    let mut conn = client.get_multiplexed_async_connection().await
        .map_err(|e| anyhow::anyhow!("Connection failed: {}", e))?;
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .map_err(|e| anyhow::anyhow!("PING failed: {}", e))?;
    Ok(format!("OK: {} ({}:{})", pong, redis_config.host, redis_config.port))
}
