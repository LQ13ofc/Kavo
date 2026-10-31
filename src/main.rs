//! CLI do Kavo: `run` (núcleo), `ping` (verifica núcleo via socket), `check-config`, `harness` (cenários de teste).

use std::path::PathBuf;
use std::process::ExitCode;

use kavo::config::KavoConfig;
use kavo::harness::{self, Options, SCENARIOS};
use kavo::verification::Verification;
use kavo::{ipc, logging, menu, runtime};

const USAGE: &str = "uso: kavo <run|ping|check-config> [--config CAMINHO]\n\
     uso: kavo harness <CENÁRIO...|all|list> [--dir DIR] [--socket SOCKET --store BANCO]";

#[derive(Default)]
struct Args {
    command: String,
    config: Option<PathBuf>,
    scenarios: Vec<String>,
    dir: Option<PathBuf>,
    socket: Option<PathBuf>,
    store: Option<PathBuf>,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    // Sem argumentos: `command` vazio, que abre o menu em TTY ou mostra o uso.
    let command = args.next().unwrap_or_default();
    let mut parsed = Args {
        command,
        ..Args::default()
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => parsed.config = Some(path_value(&mut args, "--config")?),
            "--dir" => parsed.dir = Some(path_value(&mut args, "--dir")?),
            "--socket" => parsed.socket = Some(path_value(&mut args, "--socket")?),
            "--store" => parsed.store = Some(path_value(&mut args, "--store")?),
            other if parsed.command == "harness" && !other.starts_with("--") => {
                parsed.scenarios.push(other.to_owned());
            }
            other => return Err(format!("argumento desconhecido '{other}'\n{USAGE}")),
        }
    }
    Ok(parsed)
}

fn path_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<PathBuf, String> {
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| format!("{flag} exige um caminho"))
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

    if args.command.is_empty() {
        if let Some(code) = menu::open_if_tty(&config).await {
            return code;
        }
        eprintln!("{USAGE}");
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
        "harness" => run_harness(&args, &config).await,
        other => {
            eprintln!("comando desconhecido '{other}'\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

/// Executa cenários do harness. Saída: uma linha JSON por verificação e uma por cenário.
/// Código: 0 tudo ok; 1 alguma verificação falhou ou erro de infraestrutura; 2 uso inválido.
async fn run_harness(args: &Args, config: &KavoConfig) -> ExitCode {
    let names: Vec<String> = match args.scenarios.as_slice() {
        [] => {
            eprintln!("cenário obrigatório\n{USAGE}");
            return ExitCode::from(2);
        }
        [one] if one == "list" => {
            for (name, description) in SCENARIOS {
                println!(
                    "{}",
                    serde_json::json!({"cenario": name, "descricao": description})
                );
            }
            return ExitCode::SUCCESS;
        }
        [one] if one == "all" => SCENARIOS
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .filter(|name| !name.starts_with("ipc-"))
            .collect(),
        many => many.to_vec(),
    };
    if let Some(unknown) = names
        .iter()
        .find(|name| !SCENARIOS.iter().any(|(known, _)| known == name))
    {
        eprintln!("cenário desconhecido '{unknown}'\n{USAGE}");
        return ExitCode::from(2);
    }

    // Diretório temporário próprio por execução, removido ao final. Nunca usa o banco do usuário.
    let (dir, temporary) = match &args.dir {
        Some(dir) => (dir.clone(), false),
        None => (
            std::env::temp_dir().join(format!("kavo-harness-{}", uuid::Uuid::new_v4())),
            true,
        ),
    };
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("erro: não foi possível criar {}: {e}", dir.display());
        return ExitCode::FAILURE;
    }

    let opts = Options {
        dir: dir.clone(),
        socket: args.socket.clone(),
        store: args.store.clone(),
        ipc: ipc::IpcSettings::from_config(config),
    };

    let mut all_ok = true;
    for name in &names {
        match harness::run(name, &opts).await {
            Ok(report) => {
                for check in &report.checks {
                    println!(
                        "{}",
                        serde_json::json!({
                            "cenario": report.scenario,
                            "verificacao": check.name,
                            "ok": check.ok,
                            "detalhe": check.detail,
                        })
                    );
                }
                if !report.metrics.is_empty() {
                    let metrics: serde_json::Map<String, serde_json::Value> = report
                        .metrics
                        .iter()
                        .map(|(name, value)| (name.clone(), serde_json::json!(value)))
                        .collect();
                    println!(
                        "{}",
                        serde_json::json!({"cenario": report.scenario, "metricas": metrics})
                    );
                }
                let passed = report.passed();
                all_ok &= passed;
                println!(
                    "{}",
                    serde_json::json!({
                        "cenario": report.scenario,
                        "resultado": if passed { "APROVADO" } else { "REPROVADO" },
                        "verificacoes": report.checks.len(),
                    })
                );
            }
            Err(e) => {
                all_ok = false;
                println!(
                    "{}",
                    serde_json::json!({"cenario": name, "resultado": "ERRO", "erro": e.to_string()})
                );
            }
        }
    }

    if temporary {
        let _ = std::fs::remove_dir_all(&dir);
    }
    if all_ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
