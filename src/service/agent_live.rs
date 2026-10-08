//! Driver-owned transports for bounded Redis agent live sessions.

use std::collections::BTreeMap;
use std::pin::Pin;

use futures_util::{Stream, StreamExt};
use redis::streams::StreamReadReply;

use crate::config::RedisConfig;
use crate::redis_ops::format_redis_value;

const MAX_STREAM_FIELDS: usize = 64;
const MAX_STREAM_FIELD_BYTES: usize = 256;
const MAX_STREAM_VALUE_BYTES: usize = 4 * 1024;
const MAX_STREAM_ENTRY_BYTES: usize = 32 * 1024;

type AgentStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPubSubMessage {
    pub channel: String,
    pub pattern: Option<String>,
    pub payload: Vec<u8>,
}

pub struct RedisPubSubSource {
    stream: AgentStream<RedisPubSubMessage>,
}

impl RedisPubSubSource {
    pub async fn open(
        config: &RedisConfig,
        channels: &[String],
        patterns: &[String],
    ) -> Result<Self, String> {
        let client = redis::Client::open(config.to_url())
            .map_err(|_| "Redis Pub/Sub client configuration is invalid".to_string())?;
        let mut pubsub = client
            .get_async_pubsub()
            .await
            .map_err(|_| "Redis Pub/Sub connection failed".to_string())?;
        for channel in channels {
            pubsub
                .subscribe(channel)
                .await
                .map_err(|_| "Redis channel subscription failed".to_string())?;
        }
        for pattern in patterns {
            pubsub
                .psubscribe(pattern)
                .await
                .map_err(|_| "Redis pattern subscription failed".to_string())?;
        }
        let stream = pubsub.into_on_message().map(|message| RedisPubSubMessage {
            channel: message.get_channel_name().to_string(),
            pattern: message.get_pattern::<Option<String>>().ok().flatten(),
            payload: message.get_payload_bytes().to_vec(),
        });
        Ok(Self {
            stream: Box::pin(stream),
        })
    }

    pub async fn next(&mut self) -> Option<RedisPubSubMessage> {
        self.stream.next().await
    }
}

pub struct RedisMonitorSource {
    stream: AgentStream<String>,
}

impl RedisMonitorSource {
    pub async fn open(config: &RedisConfig) -> Result<Self, String> {
        let client = redis::Client::open(config.to_url())
            .map_err(|_| "Redis MONITOR client configuration is invalid".to_string())?;
        let mut monitor = client
            .get_async_monitor()
            .await
            .map_err(|_| "Redis MONITOR connection failed".to_string())?;
        monitor
            .monitor()
            .await
            .map_err(|_| "Redis MONITOR authorization failed".to_string())?;
        Ok(Self {
            stream: Box::pin(monitor.into_on_message::<String>()),
        })
    }

    pub async fn next(&mut self) -> Option<String> {
        self.stream.next().await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisStreamEntry {
    pub id: String,
    pub fields: BTreeMap<String, String>,
    pub fields_truncated: bool,
}

pub struct RedisStreamSource {
    config: RedisConfig,
    connection: redis::aio::MultiplexedConnection,
    key: String,
    group: Option<String>,
    consumer: Option<String>,
    noack: bool,
    next_id: String,
}

impl RedisStreamSource {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &RedisConfig,
        key: String,
        group: Option<String>,
        consumer: Option<String>,
        noack: bool,
        next_id: String,
    ) -> Result<Self, String> {
        let connection = open_connection(config).await?;
        Ok(Self {
            config: config.clone(),
            connection,
            key,
            group,
            consumer,
            noack,
            next_id,
        })
    }

    pub async fn reconnect(&mut self) -> Result<(), String> {
        self.connection = open_connection(&self.config).await?;
        Ok(())
    }

    pub async fn read(
        &mut self,
        count: usize,
        block_ms: u64,
    ) -> Result<Vec<RedisStreamEntry>, String> {
        let reading_group_pending = self.group.is_some() && self.next_id != ">";
        let mut command = if let (Some(group), Some(consumer)) = (&self.group, &self.consumer) {
            let mut command = redis::cmd("XREADGROUP");
            command.arg("GROUP").arg(group).arg(consumer);
            command
        } else {
            redis::cmd("XREAD")
        };
        command.arg("COUNT").arg(count);
        if block_ms > 0 {
            command.arg("BLOCK").arg(block_ms);
        }
        if self.group.is_some() && self.noack {
            command.arg("NOACK");
        }
        command.arg("STREAMS").arg(&self.key).arg(&self.next_id);

        let reply: StreamReadReply = command
            .query_async(&mut self.connection)
            .await
            .map_err(|_| "Redis stream read failed".to_string())?;
        let mut entries = Vec::new();
        for stream in reply.keys {
            for entry in stream.ids {
                if self.group.is_none() || reading_group_pending {
                    self.next_id.clone_from(&entry.id);
                }
                entries.push(bounded_stream_entry(entry));
            }
        }
        if reading_group_pending && entries.is_empty() {
            self.next_id = ">".into();
        }
        Ok(entries)
    }

    pub fn last_id(&self) -> &str {
        &self.next_id
    }
}

async fn open_connection(
    config: &RedisConfig,
) -> Result<redis::aio::MultiplexedConnection, String> {
    let client = redis::Client::open(config.to_url())
        .map_err(|_| "Redis stream client configuration is invalid".to_string())?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|_| "Redis stream connection failed".to_string())?;
    redis::cmd("PING")
        .query_async::<String>(&mut connection)
        .await
        .map_err(|_| "Redis stream connection check failed".to_string())?;
    Ok(connection)
}

fn bounded_stream_entry(entry: redis::streams::StreamId) -> RedisStreamEntry {
    let mut fields = BTreeMap::new();
    let mut total_bytes = 0usize;
    let mut fields_truncated = false;
    let mut source_fields = entry.map.into_iter().collect::<Vec<_>>();
    source_fields.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, value) in source_fields {
        if fields.len() >= MAX_STREAM_FIELDS {
            fields_truncated = true;
            break;
        }
        let (name, name_truncated) = truncate_utf8(&name, MAX_STREAM_FIELD_BYTES);
        let rendered = format_redis_value(&value, 0);
        let (rendered, value_truncated) = truncate_utf8(&rendered, MAX_STREAM_VALUE_BYTES);
        let next_bytes = name.len().saturating_add(rendered.len());
        if total_bytes.saturating_add(next_bytes) > MAX_STREAM_ENTRY_BYTES {
            fields_truncated = true;
            break;
        }
        total_bytes = total_bytes.saturating_add(next_bytes);
        fields_truncated |= name_truncated || value_truncated;
        fields.insert(name, rendered);
    }
    RedisStreamEntry {
        id: entry.id,
        fields,
        fields_truncated,
    }
}

pub(crate) fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

#[cfg(test)]
mod tests {
    use super::truncate_utf8;

    #[test]
    fn truncation_preserves_utf8_boundaries() {
        assert_eq!(truncate_utf8("hello", 8), ("hello".into(), false));
        assert_eq!(truncate_utf8("ab界", 4), ("ab".into(), true));
    }
}
