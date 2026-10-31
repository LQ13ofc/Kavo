//! CLI do Kavo: `run` (núcleo), `ping` (verifica núcleo via socket), `check-config`.

use std::path::PathBuf;
use std::process::ExitCode;

use kavo::config::KavoConfig;
use kavo::verification::Verification;
use kavo::{ipc, logging, runtime};

const USAGE: &str = "uso: kavo <run|ping|check-config> [--config CAMINHO]";

struct Args {
    command: String,
    config: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(|| USAGE.to_owned())?;
    let mut config = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args.next().ok_or("--config exige um caminho")?;
                config = Some(PathBuf::from(value));
            }
            other => return Err(format!("argumento desconhecido '{other}'\n{USAGE}")),
        }
    }
    Ok(Args { command, config })
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };

    let config = match KavoConfig::load(args.config.as_deref()) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("erro: {e}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = logging::init(&config.logging) {
        eprintln!("erro: {e}");
        return ExitCode::from(2);
    }

    match args.command.as_str() {
        "run" => match runtime::run(config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "núcleo terminou com erro");
                ExitCode::FAILURE
            }
        },
        "ping" => {
            let socket = match config.socket_path() {
                Ok(path) => path,
                Err(e) => {
                    eprintln!("erro: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let settings = ipc::IpcSettings::from_config(&config);
            let status: Verification = ipc::ping(&socket, settings).await;
            match serde_json::to_string(&status) {
                Ok(line) => println!("{line}"),
                Err(e) => {
                    eprintln!("erro ao serializar status: {e}");
                    return ExitCode::FAILURE;
                }
            }
            if status.is_success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        "check-config" => match serde_json::to_string_pretty(&config) {
            Ok(text) => {
                println!("{text}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("erro ao serializar configuração: {e}");
                ExitCode::FAILURE
            }
        },
        other => {
            eprintln!("comando desconhecido '{other}'\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
