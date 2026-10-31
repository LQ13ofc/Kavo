//! Configuração do Kavo: arquivo TOML + overrides por variável de ambiente, validados em tempo de execução.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{KavoError, Result};

const APP_DIR: &str = "kavo";
const CONFIG_FILE: &str = "kavo.toml";
const SOCKET_FILE: &str = "kavo.sock";
const LOCK_FILE: &str = "kavo.lock";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KavoConfig {
    pub runtime: RuntimeConfig,
    pub logging: LoggingConfig,
    pub ipc: IpcConfig,
    pub speech: SpeechConfig,
    pub store: StoreConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreConfig {
    /// Caminho do banco SQLite de admissões. `None` usa `$XDG_DATA_HOME/kavo/kavo.db`.
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpeechConfig {
    /// Sem atividade por este tempo, um stream de STT é marcado como expirado.
    /// Valor provisório: deve ser calibrado por medição.
    pub stream_idle_timeout_ms: u64,
    /// Streams simultâneos rastreados pelo monitor.
    pub max_streams: usize,
    /// Confiança mínima para admissão de intenção. `None` = sem limite (sem valor padrão medido).
    pub min_confidence: Option<f32>,
}

impl Default for SpeechConfig {
    fn default() -> Self {
        Self {
            stream_idle_timeout_ms: 10_000,
            max_streams: 16,
            min_confidence: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Tempo máximo para componentes encerrarem após o sinal de parada.
    pub shutdown_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// Filtro no formato do `EnvFilter`, ex.: `info` ou `kavo=debug,info`.
    pub level: String,
    /// `true` emite uma linha JSON por evento.
    pub json: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IpcConfig {
    /// Caminho do socket Unix. `None` usa `$XDG_RUNTIME_DIR/kavo/kavo.sock`.
    pub socket_path: Option<PathBuf>,
    /// Tamanho máximo de uma mensagem do Kavo Protocol (uma linha JSON), em bytes.
    pub max_frame_bytes: usize,
    /// Tempo máximo para uma operação de leitura/escrita de mensagem.
    pub request_timeout_ms: u64,
    /// Tempo máximo sem nenhuma mensagem antes de fechar a conexão.
    pub idle_timeout_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout_ms: 5_000,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            json: true,
        }
    }
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            socket_path: None,
            max_frame_bytes: 64 * 1024,
            request_timeout_ms: 2_000,
            idle_timeout_ms: 30_000,
        }
    }
}

impl KavoConfig {
    /// Carrega a configuração.
    ///
    /// Com `explicit`, o arquivo deve existir. Sem ele, usa `$XDG_CONFIG_HOME/kavo/kavo.toml`
    /// e cai nos padrões se o arquivo não existir. Variáveis `KAVO_*` sobrescrevem o arquivo.
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let (path, required) = match explicit {
            Some(path) => (path.to_path_buf(), true),
            None => (config_dir()?.join(CONFIG_FILE), false),
        };
        Self::load_from(&path, required, |key| std::env::var(key).ok())
    }

    /// Versão testável de [`KavoConfig::load`], com fonte de ambiente injetada.
    pub fn load_from(
        path: &Path,
        required: bool,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let mut cfg = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str::<Self>(&text)
                .map_err(|e| KavoError::Config(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => Self::default(),
            Err(e) => return Err(KavoError::io(format!("ler {}", path.display()), e)),
        };
        cfg.apply_env(env)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Aplica overrides `KAVO_*`. Valor inválido é erro, nunca é ignorado em silêncio.
    pub fn apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()> {
        if let Some(v) = env("KAVO_LOG_LEVEL") {
            self.logging.level = v;
        }
        if let Some(v) = env("KAVO_LOG_JSON") {
            self.logging.json = parse_bool("KAVO_LOG_JSON", &v)?;
        }
        if let Some(v) = env("KAVO_IPC_SOCKET_PATH") {
            self.ipc.socket_path = Some(PathBuf::from(v));
        }
        if let Some(v) = env("KAVO_STORE_PATH") {
            self.store.path = Some(PathBuf::from(v));
        }
        if let Some(v) = env("KAVO_SHUTDOWN_TIMEOUT_MS") {
            self.runtime.shutdown_timeout_ms = parse_u64("KAVO_SHUTDOWN_TIMEOUT_MS", &v)?;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.runtime.shutdown_timeout_ms == 0 {
            return Err(invalid(
                "runtime.shutdown_timeout_ms",
                "deve ser maior que zero",
            ));
        }
        if self.ipc.request_timeout_ms == 0 {
            return Err(invalid("ipc.request_timeout_ms", "deve ser maior que zero"));
        }
        if let Some(path) = &self.store.path {
            if !path.is_absolute() {
                return Err(invalid("store.path", "deve ser um caminho absoluto"));
            }
        }
        if self.speech.stream_idle_timeout_ms == 0 {
            return Err(invalid(
                "speech.stream_idle_timeout_ms",
                "deve ser maior que zero",
            ));
        }
        if self.speech.max_streams == 0 {
            return Err(invalid("speech.max_streams", "deve ser maior que zero"));
        }
        if let Some(c) = self.speech.min_confidence {
            if !c.is_finite() || !(0.0..=1.0).contains(&c) {
                return Err(invalid("speech.min_confidence", "deve estar em [0.0, 1.0]"));
            }
        }
        if self.ipc.idle_timeout_ms == 0 {
            return Err(invalid("ipc.idle_timeout_ms", "deve ser maior que zero"));
        }
        if !(1024..=16 * 1024 * 1024).contains(&self.ipc.max_frame_bytes) {
            return Err(invalid(
                "ipc.max_frame_bytes",
                "deve estar entre 1024 e 16777216",
            ));
        }
        if let Some(path) = &self.ipc.socket_path {
            if !path.is_absolute() {
                return Err(invalid("ipc.socket_path", "deve ser um caminho absoluto"));
            }
        }
        tracing_subscriber::EnvFilter::try_new(&self.logging.level)
            .map_err(|e| invalid("logging.level", &e.to_string()))?;
        Ok(())
    }

    /// Caminho efetivo do socket IPC.
    pub fn socket_path(&self) -> Result<PathBuf> {
        match &self.ipc.socket_path {
            Some(path) => Ok(path.clone()),
            None => Ok(runtime_dir()?.join(SOCKET_FILE)),
        }
    }

    /// Caminho do banco de admissões. O diretório pai é criado com modo 0700.
    pub fn store_path(&self) -> Result<PathBuf> {
        match &self.store.path {
            Some(path) => {
                // Caminho explícito: o diretório pai precisa ser privado, como o socket.
                let parent = path.parent().ok_or_else(|| {
                    KavoError::Config(format!("store.path '{}' sem diretório pai", path.display()))
                })?;
                let meta = std::fs::symlink_metadata(parent)
                    .map_err(|e| KavoError::io(format!("verificar {}", parent.display()), e))?;
                use std::os::unix::fs::PermissionsExt;
                if !meta.file_type().is_dir() || meta.permissions().mode() & 0o077 != 0 {
                    return Err(KavoError::Config(format!(
                        "diretório '{}' do banco deve ser um diretório privado (0700)",
                        parent.display()
                    )));
                }
                Ok(path.clone())
            }
            None => {
                let dir = xdg_dir("XDG_DATA_HOME", ".local/share")?.join(APP_DIR);
                ensure_private_dir(&dir)?;
                Ok(dir.join("kavo.db"))
            }
        }
    }

    /// Caminho do arquivo de lock que impede duas instâncias.
    pub fn lock_path(&self) -> Result<PathBuf> {
        Ok(runtime_dir()?.join(LOCK_FILE))
    }
}

fn invalid(key: &str, reason: &str) -> KavoError {
    KavoError::Config(format!("{key}: {reason}"))
}

fn parse_bool(key: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(KavoError::Config(format!(
            "{key}: valor booleano inválido '{value}'"
        ))),
    }
}

fn parse_u64(key: &str, value: &str) -> Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| KavoError::Config(format!("{key}: número inválido '{value}'")))
}

fn xdg_dir(var: &str, fallback: &str) -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        return Ok(dir);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .ok_or_else(|| KavoError::Config("HOME não definido ou não absoluto".to_owned()))?;
    Ok(home.join(fallback))
}

/// Diretório de configuração do Kavo (`$XDG_CONFIG_HOME/kavo`).
pub fn config_dir() -> Result<PathBuf> {
    Ok(xdg_dir("XDG_CONFIG_HOME", ".config")?.join(APP_DIR))
}

fn ensure_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)
        .map_err(|e| KavoError::io(format!("criar {}", dir.display()), e))?;
    let meta = std::fs::symlink_metadata(dir)
        .map_err(|e| KavoError::io(format!("verificar {}", dir.display()), e))?;
    if !meta.file_type().is_dir() {
        return Err(KavoError::Config(format!(
            "{} não é um diretório real",
            dir.display()
        )));
    }
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| KavoError::io(format!("permissões de {}", dir.display()), e))
}

/// Diretório de runtime do Kavo (`$XDG_RUNTIME_DIR/kavo`), criado com permissão 0700.
pub fn runtime_dir() -> Result<PathBuf> {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .ok_or_else(|| {
            KavoError::Config("XDG_RUNTIME_DIR não definido ou não absoluto".to_owned())
        })?;
    let dir = base.join(APP_DIR);
    std::fs::create_dir_all(&dir)
        .map_err(|e| KavoError::io(format!("criar {}", dir.display()), e))?;
    // symlink_metadata: não seguir link, senão o chmod altera o alvo do link.
    let meta = std::fs::symlink_metadata(&dir)
        .map_err(|e| KavoError::io(format!("verificar {}", dir.display()), e))?;
    if !meta.file_type().is_dir() {
        return Err(KavoError::Config(format!(
            "{} não é um diretório real",
            dir.display()
        )));
    }
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| KavoError::io(format!("definir permissões de {}", dir.display()), e))?;
    Ok(dir)
}
