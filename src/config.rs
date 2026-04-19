use anyhow::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,

    pub ais_api_key: Option<String>,
    pub opensky_username: Option<String>,
    pub opensky_password: Option<String>,
    pub bratwurst_username: Option<String>,
    pub bratwurst_password: Option<String>,
    pub bratwurst_login_url: Option<String>,
    pub bratwurst_api_url: Option<String>,

    pub data_dir: String,

    pub flights_interval_secs: u64,
    pub trains_interval_secs: u64,
    pub gtfs_rt_interval_secs: u64,
    pub gtfs_static_interval_secs: u64,
    pub satellites_interval_secs: u64,
    pub bratwurst_interval_secs: u64,

    pub ais_dedup_enabled: bool,
    pub ais_enabled: bool,
    pub satellites_enabled: bool,
    pub trains_enabled: bool,
    pub gtfs_rt_enabled: bool,
    pub gtfs_static_enabled: bool,

    pub adsb_opensky_enabled: bool,
    pub adsb_lol_enabled: bool,
    pub adsb_fi_enabled: bool,
    pub adsb_one_enabled: bool,
    pub adsb_airplaneslive_enabled: bool,
    pub adsb_bratwurst_enabled: bool,

    pub adsb_one_delay_ms: u64,

    pub retention_ships_days: Option<i64>,
    pub retention_flights_days: Option<i64>,
    pub retention_service_alerts_days: Option<i64>,
    pub retention_vehicle_positions_days: Option<i64>,
    pub retention_departures_days: Option<i64>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/trajectory".to_string());

        let ais_api_key = non_empty_var("AIS_API_KEY").filter(|s| s != "disabled");
        let opensky_username = non_empty_var("OPENSKY_USERNAME");
        let opensky_password = non_empty_var("OPENSKY_PASSWORD");
        let bratwurst_username = non_empty_var("BRATWURST_USERNAME");
        let bratwurst_password = non_empty_var("BRATWURST_PASSWORD");
        let bratwurst_login_url = non_empty_var("BRATWURST_LOGIN_URL");
        let bratwurst_api_url = non_empty_var("BRATWURST_API_URL");

        let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

        Ok(Self {
            database_url,
            ais_api_key,
            opensky_username,
            opensky_password,
            bratwurst_username,
            bratwurst_password,
            bratwurst_login_url,
            bratwurst_api_url,
            data_dir,

            flights_interval_secs: parse_u64("FLIGHTS_INTERVAL_SECS", 300),
            trains_interval_secs: parse_u64("TRAINS_INTERVAL_SECS", 900),
            gtfs_rt_interval_secs: parse_u64("GTFS_RT_INTERVAL_SECS", 120),
            gtfs_static_interval_secs: parse_u64("GTFS_STATIC_INTERVAL_SECS", 24 * 3600),
            satellites_interval_secs: parse_u64("SATELLITES_INTERVAL_SECS", 6 * 3600),
            bratwurst_interval_secs: parse_u64("BRATWURST_INTERVAL_SECS", 60),

            ais_dedup_enabled: parse_bool("AIS_DEDUP_ENABLED", true),
            ais_enabled: parse_bool("AIS_ENABLED", true),
            satellites_enabled: parse_bool("SATELLITES_ENABLED", true),
            trains_enabled: parse_bool("TRAINS_ENABLED", true),
            gtfs_rt_enabled: parse_bool("GTFS_RT_ENABLED", true),
            gtfs_static_enabled: parse_bool("GTFS_STATIC_ENABLED", true),

            adsb_opensky_enabled: parse_bool("ADSB_OPENSKY_ENABLED", true),
            adsb_lol_enabled: parse_bool("ADSB_LOL_ENABLED", true),
            adsb_fi_enabled: parse_bool("ADSB_FI_ENABLED", true),
            adsb_one_enabled: parse_bool("ADSB_ONE_ENABLED", true),
            adsb_airplaneslive_enabled: parse_bool("ADSB_AIRPLANESLIVE_ENABLED", true),
            adsb_bratwurst_enabled: parse_bool("ADSB_BRATWURST_ENABLED", true),

            adsb_one_delay_ms: parse_u64("ADSB_ONE_DELAY_MS", 1500),

            retention_ships_days: parse_i64_opt("RETENTION_SHIPS_DAYS"),
            retention_flights_days: parse_i64_opt("RETENTION_FLIGHTS_DAYS"),
            retention_service_alerts_days: parse_i64_opt("RETENTION_SERVICE_ALERTS_DAYS"),
            retention_vehicle_positions_days: parse_i64_opt("RETENTION_VEHICLE_POSITIONS_DAYS"),
            retention_departures_days: parse_i64_opt("RETENTION_DEPARTURES_DAYS"),
        })
    }
}

fn non_empty_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|s| !s.is_empty())
}

fn parse_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_i64_opt(key: &str) -> Option<i64> {
    env::var(key).ok().and_then(|v| v.parse::<i64>().ok())
}

fn parse_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}
