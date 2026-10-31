//! Logging estruturado (tracing) em stderr. JSON por padrão; `RUST_LOG` tem precedência sobre a config.

use tracing_subscriber::EnvFilter;

use crate::config::LoggingConfig;
use crate::error::{KavoError, Result};

pub fn init(cfg: &LoggingConfig) -> Result<()> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::try_new(&cfg.level)
            .map_err(|e| KavoError::Config(format!("logging.level '{}': {e}", cfg.level)))?,
    };

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true);

    let result = if cfg.json {
        builder.json().flatten_event(true).try_init()
    } else {
        builder.try_init()
    };
    result.map_err(|e| KavoError::Config(format!("inicialização de logging: {e}")))
}
