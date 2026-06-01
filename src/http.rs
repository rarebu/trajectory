use reqwest::{Client, ClientBuilder};
use std::time::Duration;

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
const USER_AGENT: &str = concat!("trajectory/", env!("CARGO_PKG_VERSION"));

pub fn create_client() -> Result<Client, reqwest::Error> {
    create_client_with_timeout(DEFAULT_TIMEOUT_SECS)
}

pub fn create_client_with_timeout(timeout_secs: u64) -> Result<Client, reqwest::Error> {
    ClientBuilder::new()
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
        .user_agent(USER_AGENT)
        .gzip(true)
        .build()
}
