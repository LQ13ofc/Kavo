//! Controle de sessões e requisições (Kavo Protocol 1.2, decisão D-09).
//!
//! - `SESSION_CONTROL` (cliente → núcleo): abre, encerra, consulta sessão; registra e encerra requisições.
//! - `CANCEL` 1.2 com `correlation_id` (cancela requisição) ou com `payload.stream_id` (cancela stream).
//! - `CONTROL_RESULT` (núcleo → cliente): resultado de cada operação de controle, com `correlation_id`
//!   igual ao `message_id` da mensagem recebida.
//!
//! O protocolo só transporta pedidos e resultados. Não concede permissões.

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use uuid::{Uuid, Version};

use crate::error::{KavoError, Result};
use crate::protocol::{KavoMessage, MessageType};

/// Versão de `schema_version` exigida por `SESSION_CONTROL`, `CONTROL_RESULT` e `CANCEL` com controle.
pub const CONTROL_SCHEMA_VERSION: &str = "1.2";
/// Capacidade do canal de controle entre IPC e monitor.
pub const CONTROL_CHANNEL_CAPACITY: usize = 64;

/// Tipo de requisição. Só STT streaming existe; novos tipos exigem decisão própria.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RequestKind {
    SttStream,
}

/// Payload de `SESSION_CONTROL`. O `session_id` vem do envelope; `request_id` de `REQUEST_BEGIN` é o
/// `message_id` desta mensagem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SessionControl {
    Open,
    Close,
    Status,
    RequestBegin { kind: RequestKind, stream_id: Uuid },
    RequestEnd { request_id: Uuid },
}

impl SessionControl {
    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        let payload: Self = serde_json::from_value(value.clone()).map_err(|e| {
            KavoError::Protocol(format!(
                "payload SESSION_CONTROL inválido ({})",
                crate::protocol::describe_serde(&e)
            ))
        })?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::RequestBegin { stream_id, .. } => check_v4("stream_id", stream_id),
            Self::RequestEnd { request_id } => check_v4("request_id", request_id),
            _ => Ok(()),
        }
    }
}

/// Operação a que um `CONTROL_RESULT` se refere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlOp {
    Open,
    Close,
    Status,
    RequestBegin,
    RequestEnd,
    RequestCancel,
    StreamCancel,
}

/// `APPLIED`: estado mudou. `NOOP`: nenhuma mudança (repetição idempotente ou consulta).
/// `REJECTED`: operação recusada; `reason` diz o motivo. Nada foi alterado.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Outcome {
    Applied,
    Noop,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Reason {
    SessionUnknown,
    SessionClosed,
    RequestUnknown,
    RequestDuplicate,
    RequestFinished,
    RequestCancelled,
    StreamBound,
    InvalidPayload,
    StoreUnavailable,
    /// Stream encerrado, cancelado ou aposentado: evento tardio não é aceito.
    StreamClosed,
    /// Limite de streams ativos atingido.
    StreamLimit,
    /// Sequência já usada ou fora de ordem.
    SequenceInvalid,
    /// Lacuna de sequência: o produtor deve reenviar as anteriores primeiro.
    SequenceGap,
    /// Revisão de interpretação já superada.
    StaleRevision,
    /// Mesma sequência ou revisão com conteúdo diferente.
    Conflict,
    /// Conteúdo ou transição inválida para o estado atual.
    InvalidContent,
    /// Fila de admissão cheia. Repetir mais tarde com a mesma identidade.
    Busy,
}

/// Estado de sessão no fio. `UNKNOWN`: nunca aberta nesta execução do núcleo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WireState {
    Active,
    Closed,
    Unknown,
}

/// Payload de `CONTROL_RESULT`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlResult {
    pub op: ControlOp,
    pub outcome: Outcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,
    pub state: WireState,
    #[serde(default)]
    pub cancelled_requests: u64,
    #[serde(default)]
    pub cancelled_streams: u64,
}

impl ControlResult {
    pub fn new(op: ControlOp, outcome: Outcome, state: WireState) -> Self {
        Self {
            op,
            outcome,
            reason: None,
            state,
            cancelled_requests: 0,
            cancelled_streams: 0,
        }
    }

    pub fn rejected(op: ControlOp, reason: Reason, state: WireState) -> Self {
        Self {
            reason: Some(reason),
            ..Self::new(op, Outcome::Rejected, state)
        }
    }

    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone()).map_err(|e| {
            KavoError::Protocol(format!(
                "payload CONTROL_RESULT inválido ({})",
                crate::protocol::describe_serde(&e)
            ))
        })
    }

    /// Monta a resposta a `request` com `correlation_id` = `message_id` dela e versão 1.2.
    pub fn reply_to(&self, request: &KavoMessage) -> Result<KavoMessage> {
        let mut msg = request.reply(MessageType::ControlResult);
        msg.schema_version = CONTROL_SCHEMA_VERSION.to_owned();
        msg.with_payload(self)
    }
}

/// Pedido de controle entregue ao monitor, com canal de resposta.
pub struct ControlCommand {
    pub msg: KavoMessage,
    pub reply: oneshot::Sender<KavoMessage>,
}

pub type ControlSender = mpsc::Sender<ControlCommand>;
pub type ControlReceiver = mpsc::Receiver<ControlCommand>;

pub fn channel() -> (ControlSender, ControlReceiver) {
    channel_with(CONTROL_CHANNEL_CAPACITY)
}

pub fn channel_with(capacity: usize) -> (ControlSender, ControlReceiver) {
    mpsc::channel(capacity.max(1))
}

/// Evento que recebe admissão. Ver `ADMISSION_ACK` em `KAVO-PROTOCOL.md`, seção 11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AckEvent {
    SttResult,
    IntentDetected,
}

/// Semântica do ACK. Não é conclusão de processamento nem execução de ação.
/// `ACCEPTED`: validado e admitido (com gravação durável quando exigida).
/// `DUPLICATE`: mesma identidade e mesmo conteúdo já admitidos; nada foi reaplicado.
/// `REJECTED`: não admitido por motivo permanente (`reason`).
/// `NOT_READY`: não admitido por indisponibilidade transitória; repetir com a mesma identidade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AdmissionStatus {
    Accepted,
    Duplicate,
    Rejected,
    NotReady,
}

/// Payload de `ADMISSION_ACK`. Identifica o evento original sem ecoar conteúdo. O `correlation_id`
/// do envelope é o `message_id` do evento.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionAck {
    pub event: AckEvent,
    pub status: AdmissionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interpretation_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u32>,
}

impl AdmissionAck {
    /// ACK para `msg` (evento STT ou INTENT), copiando só os identificadores do payload.
    pub fn for_event(msg: &KavoMessage, status: AdmissionStatus, reason: Option<Reason>) -> Self {
        let event = if msg.kind == MessageType::IntentDetected {
            AckEvent::IntentDetected
        } else {
            AckEvent::SttResult
        };
        let field = |name: &str| msg.payload.as_ref().and_then(|p| p.get(name)).cloned();
        let uuid = |name: &str| {
            field(name)
                .and_then(|v| v.as_str().map(str::to_owned))
                .and_then(|v| Uuid::parse_str(&v).ok())
        };
        Self {
            event,
            status,
            reason,
            stream_id: uuid("stream_id"),
            sequence: field("sequence").and_then(|v| v.as_u64()),
            interpretation_id: uuid("interpretation_id"),
            revision: field("revision")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
        }
    }

    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone()).map_err(|e| {
            KavoError::Protocol(format!(
                "payload ADMISSION_ACK inválido ({})",
                crate::protocol::describe_serde(&e)
            ))
        })
    }

    /// Monta `ADMISSION_ACK` em resposta a `request` (`correlation_id` = `message_id`), versão 1.2.
    pub fn reply_to(&self, request: &KavoMessage) -> Result<KavoMessage> {
        let mut msg = request.reply(MessageType::AdmissionAck);
        msg.schema_version = CONTROL_SCHEMA_VERSION.to_owned();
        msg.with_payload(self)
    }
}

/// Classifica a recusa de um stream pelo texto produzido por `speech.rs`. Os textos são as
/// constantes `REJECT_*`, testadas em `tests/control.rs` para evitar divergência.
pub fn stt_reject_reason(text: &str) -> Reason {
    use crate::speech::{REJECT_CONFLICT, REJECT_STREAM_CLOSED, REJECT_STREAM_LIMIT};
    if text.starts_with(REJECT_STREAM_CLOSED) {
        Reason::StreamClosed
    } else if text == REJECT_STREAM_LIMIT {
        Reason::StreamLimit
    } else if text == REJECT_CONFLICT {
        Reason::Conflict
    } else {
        Reason::InvalidContent
    }
}

/// Classifica a recusa de uma interpretação pelo texto produzido por `intent.rs`.
pub fn intent_reject_reason(text: &str) -> Reason {
    use crate::intent::{
        REJECT_CONFLICT as I_CONFLICT, REJECT_STALE as I_STALE, REJECT_STREAM_CLOSED as I_CLOSED,
    };
    if text.starts_with(I_CLOSED) {
        Reason::StreamClosed
    } else if text == I_STALE {
        Reason::StaleRevision
    } else if text == I_CONFLICT {
        Reason::Conflict
    } else {
        Reason::InvalidContent
    }
}

fn check_v4(field: &str, id: &Uuid) -> Result<()> {
    if id.get_version() == Some(Version::Random) {
        Ok(())
    } else {
        Err(KavoError::Protocol(format!("{field} deve ser UUID v4")))
    }
}
