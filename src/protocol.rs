//! Kavo Protocol v1.1-draft (ver `docs/KAVO-PROTOCOL.md`). Hermes JSON é só referência externa.
//! Uma mensagem = um objeto JSON em uma linha (NDJSON). Validação ocorre na entrada e antes do envio.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::{Uuid, Version};

use crate::error::{KavoError, Result};
use crate::intent::InterpretationPayload;
use crate::speech::{CancelPayload, SpeechPayload};

pub const PROTOCOL_NAME: &str = "Kavo Protocol";
/// `schema_version` padrão das mensagens criadas por `KavoMessage::new`. Mantido em 1.1 para que clientes
/// 1.1 continuem com o comportamento legado. Tipos 1.2 definem `schema_version` explicitamente.
pub const SCHEMA_VERSION: &str = "1.1";
pub const SENDER_CORE: &str = "kavo-core";
pub const UNKNOWN_SESSION: &str = "unknown";
const MAX_SESSION_ID_LEN: usize = 128;
const MAX_SENDER_LEN: usize = 64;
const MAX_ERROR_MESSAGE_LEN: usize = 512;

/// Nomes de wire dos tipos implementados em 1.1-draft (ver `MessageType`).
pub const IMPLEMENTED_TYPES: &[&str] = &[
    "HEARTBEAT",
    "ERROR",
    "CANCEL",
    "STT_RESULT",
    "INTENT_DETECTED",
    "SESSION_CONTROL",
    "CONTROL_RESULT",
    "ADMISSION_ACK",
];

/// Tipos reservados pela decisão 6.6, ainda não implementados. Recebê-los gera erro explícito.
pub const RESERVED_TYPES: &[&str] = &[
    "WAKE_WORD_DETECTED",
    "COMMAND_EXECUTED",
    "MEMORY_UPDATE",
    "SYSTEM_STATUS",
    "ASSISTANT_SPEAK",
];

/// Códigos de erro do fio. Apenas `GENERIC_ERROR` e os de STT são usados (ver `ERROR-TAXONOMY.md`).
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

    /// Códigos aprovados ou provisórios. Códigos propostos (2xxx/3xxx) não entram aqui até aprovação.
    pub const KNOWN: &[u16] = &[
        STT_AUDIO_ERROR,
        STT_MODEL_ERROR,
        INTENT_PARSING_ERROR,
        COMMAND_NOT_ALLOWED,
        COMMAND_TIMEOUT,
        MEMORY_STORAGE_ERROR,
        WAKE_WORD_MODEL_ERROR,
        GENERIC_ERROR,
    ];

    pub fn is_known(code: u16) -> bool {
        KNOWN.contains(&code)
    }
}

/// Tipos implementados em 1.1-draft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    Heartbeat,
    Error,
    Cancel,
    SttResult,
    IntentDetected,
    SessionControl,
    ControlResult,
    AdmissionAck,
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
            Self::Heartbeat => Delivery::BestEffort,
            Self::Error
            | Self::Cancel
            | Self::SttResult
            | Self::IntentDetected
            | Self::SessionControl
            | Self::ControlResult
            | Self::AdmissionAck => Delivery::Guaranteed,
        }
    }

    /// Tipos introduzidos na 1.1.
    pub fn requires_minor_1_1(self) -> bool {
        matches!(self, Self::SttResult | Self::IntentDetected)
    }

    /// Tipos introduzidos na 1.2 (controle de sessões e requisições).
    pub fn requires_minor_1_2(self) -> bool {
        matches!(
            self,
            Self::SessionControl | Self::ControlResult | Self::AdmissionAck
        )
    }

    /// Tipos que carregam `payload`.
    pub fn takes_payload(self) -> bool {
        matches!(
            self,
            Self::Cancel
                | Self::SttResult
                | Self::IntentDetected
                | Self::SessionControl
                | Self::ControlResult
                | Self::AdmissionAck
        )
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
    /// Conteúdo tipado por `type` (1.1). Validado conforme o tipo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
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
            payload: None,
        }
    }

    pub fn with_correlation_id(mut self, message_id: impl Into<String>) -> Self {
        self.correlation_id = Some(message_id.into());
        self
    }

    /// Anexa payload tipado. A validação completa acontece em `validate`.
    pub fn with_payload<T: Serialize>(mut self, payload: &T) -> Result<Self> {
        let value = serde_json::to_value(payload)
            .map_err(|e| KavoError::Protocol(format!("payload não serializável: {e}")))?;
        self.payload = Some(value);
        Ok(self)
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
            return Err(KavoError::UnsupportedVersion(
                "schema_version incompatível (suportado: 1.x)".to_owned(),
            ));
        }
        check_uuid("message_id", &self.message_id)?;
        if let Some(id) = &self.correlation_id {
            check_uuid("correlation_id", id)?;
        }
        check_text("session_id", &self.session_id, MAX_SESSION_ID_LEN)?;
        check_text("sender", &self.sender, MAX_SENDER_LEN)?;
        if let Some(text) = &self.error_message {
            check_text("error_message", text, MAX_ERROR_MESSAGE_LEN)?;
        }
        if !self.timestamp.is_finite() || self.timestamp < 0.0 {
            return Err(KavoError::Protocol("timestamp inválido".to_owned()));
        }

        // Tipos 1.1 e payload exigem schema 1.1. Mensagem 1.0 com eles é recusada, não interpretada.
        if (self.kind.requires_minor_1_1() || self.payload.is_some()) && self.minor() < 1 {
            return Err(KavoError::UnsupportedVersion(format!(
                "{:?} com payload exige schema_version 1.1",
                self.kind
            )));
        }

        if self.kind.requires_minor_1_2() && self.minor() < 2 {
            return Err(KavoError::UnsupportedVersion(format!(
                "{:?} exige schema_version 1.2",
                self.kind
            )));
        }

        match self.kind {
            MessageType::Error if self.error_code.is_none() => {
                return Err(KavoError::Protocol("ERROR sem error_code".to_owned()));
            }
            MessageType::Error if !error_code::is_known(self.error_code.unwrap_or_default()) => {
                return Err(KavoError::Protocol(
                    "error_code fora da taxonomia aprovada".to_owned(),
                ));
            }
            MessageType::Heartbeat
            | MessageType::Cancel
            | MessageType::SttResult
            | MessageType::IntentDetected
            | MessageType::SessionControl
            | MessageType::ControlResult
            | MessageType::AdmissionAck
                if self.error_code.is_some() =>
            {
                return Err(KavoError::Protocol(
                    "error_code só é permitido em ERROR".to_owned(),
                ));
            }
            _ => {}
        }
        self.validate_payload()
    }

    /// Número menor de `schema_version` (`1.x` → `x`). Só válido depois de `validate` ter aceitado a versão.
    pub fn minor(&self) -> u32 {
        self.schema_version
            .split_once('.')
            .and_then(|(_, minor)| minor.parse().ok())
            .unwrap_or(0)
    }

    fn validate_payload(&self) -> Result<()> {
        match (self.kind, &self.payload) {
            (MessageType::Heartbeat | MessageType::Error, Some(_)) => Err(KavoError::Protocol(
                format!("{:?} não aceita payload", self.kind),
            )),
            (MessageType::SttResult, Some(p)) => SpeechPayload::from_value(p).map(|_| ()),
            (MessageType::IntentDetected, Some(p)) => {
                InterpretationPayload::from_value(p).map(|_| ())
            }
            (MessageType::SttResult | MessageType::IntentDetected, None) => Err(
                KavoError::Protocol(format!("{:?} exige payload", self.kind)),
            ),
            (MessageType::SessionControl, Some(p)) => {
                crate::control::SessionControl::from_value(p).map(|_| ())
            }
            (MessageType::AdmissionAck, Some(p)) => {
                crate::control::AdmissionAck::from_value(p).map(|_| ())
            }
            (MessageType::AdmissionAck, None) => Err(KavoError::Protocol(
                "ADMISSION_ACK exige payload".to_owned(),
            )),
            (MessageType::ControlResult, Some(p)) => {
                crate::control::ControlResult::from_value(p).map(|_| ())
            }
            (MessageType::SessionControl | MessageType::ControlResult, None) => Err(
                KavoError::Protocol(format!("{:?} exige payload", self.kind)),
            ),
            (MessageType::Cancel, Some(p)) => CancelPayload::from_value(p).map(|_| ()),
            (MessageType::Cancel, None) if self.correlation_id.is_none() => {
                Err(KavoError::Protocol(
                    "CANCEL exige correlation_id do alvo ou payload.stream_id".to_owned(),
                ))
            }
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
                    // O nome do tipo vem do cliente: não é ecoado em erro nem em log.
                    return Err(KavoError::UnsupportedType(format!("tipo {reason}")));
                }
            }
        }
        let msg: Self = serde_json::from_slice(bytes).map_err(|e| {
            KavoError::Protocol(format!(
                "JSON inválido ou campo obrigatório ausente ({})",
                describe_serde(&e)
            ))
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
        Ok(uuid) if uuid.get_version() == Some(Version::Random) => Ok(()),
        _ => Err(KavoError::Protocol(format!("{field} deve ser UUID v4"))),
    }
}

/// Descrição segura de erro do serde: categoria e coluna. Nunca inclui o valor recebido do cliente.
pub(crate) fn describe_serde(e: &serde_json::Error) -> String {
    format!("{:?} na coluna {}", e.classify(), e.column())
}

pub(crate) fn check_text(field: &str, value: &str, max: usize) -> Result<()> {
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
