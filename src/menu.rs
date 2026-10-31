//! Menu interativo de desenvolvimento: `kavo` sem argumentos, somente em TTY.
//!
//! Não contém regra de negócio: chama as mesmas operações da CLI. A interface de terminal não é
//! dependência do núcleo; o núcleo continua executável apenas com `kavo run`.

use std::io::{self, BufRead, IsTerminal, Write};
use std::process::ExitCode;

use crate::config::KavoConfig;
use crate::harness::SCENARIOS;
use crate::ipc::{self, IpcSettings};

/// Abre o menu se stdin e stdout forem terminais. Caso contrário, `None` (quem chama mostra o uso).
pub async fn open_if_tty(config: &KavoConfig) -> Option<ExitCode> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return None;
    }
    Some(run(config).await)
}

async fn run(config: &KavoConfig) -> ExitCode {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    loop {
        println!();
        println!("Kavo — menu de desenvolvimento");
        println!("  1) Configuração efetiva");
        println!("  2) Verificar núcleo (ping)");
        println!("  3) Listar cenários de teste");
        println!("  0) Sair");
        print!("> ");
        let _ = io::stdout().flush();

        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) | Err(_) => return ExitCode::SUCCESS,
            Ok(_) => {}
        }
        match line.trim() {
            "1" => show_config(config),
            "2" => ping(config).await,
            "3" => list_scenarios(),
            "0" | "q" | "sair" => return ExitCode::SUCCESS,
            "" => {}
            _ => println!("opção inválida"),
        }
    }
}

fn show_config(config: &KavoConfig) {
    match serde_json::to_string_pretty(config) {
        Ok(text) => println!("{text}"),
        Err(e) => println!("erro ao serializar configuração: {e}"),
    }
}

async fn ping(config: &KavoConfig) {
    let socket = match config.socket_path() {
        Ok(path) => path,
        Err(e) => {
            println!("erro: {e}");
            return;
        }
    };
    let settings = IpcSettings::from_config(config);
    let status = ipc::ping(&socket, settings).await;
    match serde_json::to_string(&status) {
        Ok(line) => println!("{line}"),
        Err(e) => println!("erro ao serializar status: {e}"),
    }
}

fn list_scenarios() {
    for (name, description) in SCENARIOS {
        println!("  {name:<22} {description}");
    }
}
