use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("environment variable {name}: {source}")]
    Env {
        name: &'static str,
        #[source]
        source: std::env::VarError,
    },

    #[error("environment variable {name}={value:?} is not a valid {expected}")]
    Parse {
        name: &'static str,
        value: String,
        expected: &'static str,
    },

    #[error("required environment variable {0} is unset")]
    Missing(&'static str),
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("database operation failed: {0}")]
    Db(#[from] sqlx::Error),

    #[error("json decode failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error("upstream {upstream} returned non-success status {status}")]
    Upstream { upstream: &'static str, status: u16 },
}
