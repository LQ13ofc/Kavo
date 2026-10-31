//! Kavo Protocol v1.0-draft (ver `docs/KAVO-PROTOCOL.md`). Hermes JSON é só referência externa.
//! Uma mensagem = um objeto JSON em uma linha (NDJSON). Validação ocorre na entrada e antes do envio.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KavoError, Result};

pub const PROTOCOL_NAME: &str = "Kavo Protocol";
pub const SCHEMA_VERSION: &str = "1.0";
pub const SENDER_CORE: &str = "kavo-core";
pub const UNKNOWN_SESSION: &str = "unknown";
const MAX_SESSION_ID_LEN: usize = 128;
const MAX_SENDER_LEN: usize = 64;
const MAX_ERROR_MESSAGE_LEN: usize = 512;

/// Nomes de wire dos tipos implementados em 1.0-draft (ver `MessageType`).
pub const IMPLEMENTED_TYPES: &[&str] = &["HEARTBEAT", "ERROR", "CANCEL"];

/// Tipos reservados pela decisão 6.6, ainda não implementados. Recebê-los gera erro explícito.
pub const RESERVED_TYPES: &[&str] = &[
    "STT_RESULT",
    "WAKE_WORD_DETECTED",
    "INTENT_DETECTED",
    "COMMAND_EXECUTED",
    "MEMORY_UPDATE",
    "SYSTEM_STATUS",
    "ASSISTANT_SPEAK",
];

/// Códigos de erro do fio. Apenas `GENERIC_ERROR` é usado pela fundação (ver `ERROR-TAXONOMY.md`).
pub mod error_code {
    pub const STT_AUDIO_ERROR: u16 = 1001;
    pub const STT_MODEL_ERROR: u16 = 1002;
    pub const INTENT_PARSING_ERROR: u16 = 1003;
    pub const COMMAND_NOT_ALLOWED: u16 = 1004;
    pub const COMMAND_TIMEOUT: u16 = 1005;
    pub const MEMORY_STORAGE_ERROR: u16 = 1006;
    pub const WAKE_WORD_MODEL_ERROR: u16 = 1007;
    /// Provisório: usado para todo erro de protocolo, transporte e interno até a taxonomia ser aprovada.
    pub const GENERIC_ERROR: u16 = 1100;
}

/// Tipos implementados em 1.0-draft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    Heartbeat,
    Error,
    Cancel,
}

/// Classe de entrega no barramento interno (ver `KAVO-PROTOCOL.md`, seção 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Nunca descartada em silêncio: falha explícita se um assinante não aceitar no prazo.
    Guaranteed,
    /// Descartada e contada se o assinante estiver cheio.
    BestEffort,
}

impl MessageType {
    pub fn delivery(self) -> Delivery {
        match self {
            Self::Error | Self::Cancel => Delivery::Guaranteed,
            Self::Heartbeat => Delivery::BestEffort,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KavoMessage {
    #[serde(rename = "type")]
    pub kind: MessageType,
    pub schema_version: String,
    /// UUID v4 desta mensagem.
    pub message_id: String,
    /// `message_id` da mensagem à qual esta responde ou que cancela.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    pub session_id: String,
    /// Segundos Unix com precisão sub-segundo.
    pub timestamp: f64,
    pub sender: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl KavoMessage {
    pub fn new(
        kind: MessageType,
        session_id: impl Into<String>,
        sender: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            schema_version: SCHEMA_VERSION.to_owned(),
            message_id: Uuid::new_v4().to_string(),
            correlation_id: None,
            session_id: session_id.into(),
            timestamp: now_epoch_seconds(),
            sender: sender.into(),
            error_code: None,
            error_message: None,
        }
    }

    pub fn with_correlation_id(mut self, message_id: impl Into<String>) -> Self {
        self.correlation_id = Some(message_id.into());
        self
    }

    /// Resposta a `self`: mesma sessão, `correlation_id` = `message_id` de `self`, emissor = núcleo.
    pub fn reply(&self, kind: MessageType) -> Self {
        Self::new(kind, self.session_id.clone(), SENDER_CORE)
            .with_correlation_id(self.message_id.clone())
    }

    /// `ERROR` em resposta a `self`. A mensagem recebe a classe como prefixo (`[CLASSE] texto`).
    pub fn error_reply(&self, code: u16, message: impl AsRef<str>) -> Self {
        let mut reply = self.reply(MessageType::Error);
        reply.error_code = Some(code);
        reply.error_message = Some(truncate(message.as_ref(), MAX_ERROR_MESSAGE_LEN));
        reply
    }

    /// `ERROR` para uma entrada que não pôde ser lida (sem requisição identificável).
    pub fn unparseable_error(code: u16, message: impl AsRef<str>) -> Self {
        let mut msg = Self::new(MessageType::Error, UNKNOWN_SESSION, SENDER_CORE);
        msg.error_code = Some(code);
        msg.error_message = Some(truncate(message.as_ref(), MAX_ERROR_MESSAGE_LEN));
        msg
    }

    /// Verifica o contrato. Nunca presume campos válidos vindos de outro processo.
    pub fn validate(&self) -> Result<()> {
        let version_ok = self
            .schema_version
            .split_once('.')
            .is_some_and(|(major, minor)| {
                major == "1" && !minor.is_empty() && minor.bytes().all(|b| b.is_ascii_digit())
            });
        if !version_ok {
            return Err(KavoError::UnsupportedVersion(format!(
                "schema_version '{}' incompatível (suportado: 1.x)",
                self.schema_version
            )));
        }
        check_uuid("message_id", &self.message_id)?;
        if let Some(id) = &self.correlation_id {
            check_uuid("correlation_id", id)?;
        }
        check_text("session_id", &self.session_id, MAX_SESSION_ID_LEN)?;
        check_text("sender", &self.sender, MAX_SENDER_LEN)?;
        if !self.timestamp.is_finite() || self.timestamp < 0.0 {
            return Err(KavoError::Protocol("timestamp inválido".to_owned()));
        }
        match self.kind {
            MessageType::Error if self.error_code.is_none() => {
                Err(KavoError::Protocol("ERROR sem error_code".to_owned()))
            }
            MessageType::Cancel if self.correlation_id.is_none() => Err(KavoError::Protocol(
                "CANCEL sem correlation_id do alvo".to_owned(),
            )),
            MessageType::Heartbeat | MessageType::Cancel if self.error_code.is_some() => Err(
                KavoError::Protocol("error_code só é permitido em ERROR".to_owned()),
            ),
            _ => Ok(()),
        }
    }

    /// Decodifica e valida uma linha JSON (sem o `\n` final).
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self> {
        // Checa o tipo antes do serde: tipo fora dos implementados vira UNSUPPORTED_MESSAGE_TYPE, não INVALID_MESSAGE.
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_slice::<serde_json::Value>(bytes)
        {
            if let Some(serde_json::Value::String(kind)) = map.get("type") {
                if !IMPLEMENTED_TYPES.contains(&kind.as_str()) {
                    let reason = if RESERVED_TYPES.contains(&kind.as_str()) {
                        "reservado, ainda não implementado"
                    } else {
                        "desconhecido"
                    };
                    return Err(KavoError::UnsupportedType(format!(
                        "tipo '{kind}' {reason}"
                    )));
                }
            }
        }
        let msg: Self = serde_json::from_slice(bytes).map_err(|e| {
            KavoError::Protocol(format!("JSON inválido ou campo obrigatório ausente: {e}"))
        })?;
        msg.validate()?;
        Ok(msg)
    }

    /// Serializa em uma linha JSON terminada por `\n`.
    pub fn to_json_line(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut line = serde_json::to_vec(self)
            .map_err(|e| KavoError::Protocol(format!("falha ao serializar: {e}")))?;
        line.push(b'\n');
        Ok(line)
    }
}

fn check_uuid(field: &str, value: &str) -> Result<()> {
    match Uuid::parse_str(value) {
        Ok(uuid) if uuid.get_version() == Some(uuid::Version::Random) => Ok(()),
        _ => Err(KavoError::Protocol(format!("{field} deve ser UUID v4"))),
    }
}

fn check_text(field: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty() || value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(KavoError::Protocol(format!(
            "{field} vazio, longo demais (máx. {max}) ou com caracteres de controle"
        )));
    }
    Ok(())
}

fn truncate(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

fn now_epoch_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
