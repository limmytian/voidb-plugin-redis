use serde::{Deserialize, Serialize};

/// Redis connection configuration, stored as JSON in ConnectionConfig.plugin_config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub db: u8,
    #[serde(default)]
    pub tls: bool,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            password: None,
            username: None,
            db: 0,
            tls: false,
        }
    }
}

impl RedisConfig {
    /// Build a Redis URL for the `redis` crate client.
    pub fn to_url(&self) -> String {
        let scheme = if self.tls { "rediss" } else { "redis" };
        let auth = match (&self.username, &self.password) {
            (Some(u), Some(p)) => format!("{}:{}@", u, p),
            (None, Some(p)) => format!(":{}@", p),
            _ => String::new(),
        };
        format!("{}://{}{}:{}/{}", scheme, auth, self.host, self.port, self.db)
    }
}
