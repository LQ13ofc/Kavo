//! Erro único do Kavo. Cada variante tem uma mensagem legível em PT-BR para logs e diagnóstico.

use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum KavoError {
    #[error("configuração inválida: {0}")]
    Config(String),

    #[error("falha de I/O ao {context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },

    #[error("mensagem do Kavo Protocol inválida: {0}")]
    Protocol(String),

    #[error("{0}")]
    UnsupportedVersion(String),

    #[error("{0}")]
    UnsupportedType(String),

    #[error("entrega garantida falhou: {0}")]
    DeliveryFailed(String),

    #[error("mensagem excede o limite de {limit} bytes")]
    FrameTooLarge { limit: usize },

    #[error("já existe uma instância do Kavo em execução (lock: {path})")]
    AlreadyRunning { path: String },

    #[error("tempo esgotado após {0:?}")]
    Timeout(Duration),

    #[error("componente '{component}' falhou: {reason}")]
    Component { component: String, reason: String },
}

impl KavoError {
    /// Adiciona contexto a um erro de I/O sem perder a causa original.
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

pub type Result<T> = std::result::Result<T, KavoError>;

/// Classe estável de erro. Aparece como prefixo `[CLASSE]` em `ERROR.error_message`.
/// Códigos numéricos propostos estão em `docs/ERROR-TAXONOMY.md`; o fio usa 1100 por enquanto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidMessage,
    UnsupportedVersion,
    UnsupportedMessageType,
    FrameTooLarge,
    DeliveryFailed,
    Timeout,
    ConfigInvalid,
    AlreadyRunning,
    ComponentFailed,
    Cancelled,
    Io,
    /// Reservado para falhas não classificadas. Nenhum erro atual cai aqui ainda.
    Internal,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidMessage => "INVALID_MESSAGE",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::UnsupportedMessageType => "UNSUPPORTED_MESSAGE_TYPE",
            Self::FrameTooLarge => "FRAME_TOO_LARGE",
            Self::DeliveryFailed => "DELIVERY_FAILED",
            Self::Timeout => "TIMEOUT",
            Self::ConfigInvalid => "CONFIG_INVALID",
            Self::AlreadyRunning => "ALREADY_RUNNING",
            Self::ComponentFailed => "COMPONENT_FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Io => "IO",
            Self::Internal => "INTERNAL",
        }
    }
}

impl KavoError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Config(_) => ErrorKind::ConfigInvalid,
            Self::Io { .. } => ErrorKind::Io,
            Self::Protocol(_) => ErrorKind::InvalidMessage,
            Self::UnsupportedVersion(_) => ErrorKind::UnsupportedVersion,
            Self::UnsupportedType(_) => ErrorKind::UnsupportedMessageType,
            Self::FrameTooLarge { .. } => ErrorKind::FrameTooLarge,
            Self::DeliveryFailed(_) => ErrorKind::DeliveryFailed,
            Self::AlreadyRunning { .. } => ErrorKind::AlreadyRunning,
            Self::Timeout(_) => ErrorKind::Timeout,
            Self::Component { .. } => ErrorKind::ComponentFailed,
        }
    }
}
