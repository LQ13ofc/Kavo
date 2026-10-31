//! Session Manager (A0): ciclo de vida de sessões de interação e cancelamento por requisição.
//!
//! Responsabilidades:
//! - abrir, encerrar e consultar sessões; uma sessão encerrada não volta a abrir;
//! - registrar requisições (`request_id` = `message_id` aguardado) e entregar um token de cancelamento;
//! - cancelar uma requisição sem afetar outras sessões ou requisições;
//! - encerrar uma sessão cancela só as requisições dela ainda em execução;
//! - recusar eventos de sessões desconhecidas ou encerradas.
//!
//! Não concede permissões (isso é do Policy Engine). Estado só em memória: sessões não sobrevivem
//! ao reinício do núcleo. Nada aqui é gravado; o que precisa ser durável continua no `Store`.

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Limite igual ao do envelope do Kavo Protocol (`session_id`, 1–128 caracteres Unicode).
pub const MAX_SESSION_ID_CHARS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Active,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestState {
    Running,
    Cancelled,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    /// `session_id` vazio, longo demais ou com caractere de controle. Mensagem sem o valor recebido.
    InvalidId(&'static str),
    /// Sessão nunca aberta nesta execução do núcleo.
    UnknownSession,
    /// Sessão já encerrada. Não reabre: evita que eventos atrasados a ressuscitem.
    ClosedSession,
    /// `request_id` desconhecido na sessão.
    UnknownRequest,
    /// `request_id` já registrado na sessão.
    DuplicateRequest,
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidId(reason) => write!(f, "session_id inválido: {reason}"),
            Self::UnknownSession => f.write_str("sessão desconhecida"),
            Self::ClosedSession => f.write_str("sessão já encerrada"),
            Self::UnknownRequest => f.write_str("requisição desconhecida na sessão"),
            Self::DuplicateRequest => f.write_str("requisição já registrada na sessão"),
        }
    }
}

impl std::error::Error for SessionError {}

/// Resultado de `open`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenOutcome {
    Opened,
    AlreadyActive,
}

/// Resultado de `close`. Idempotente: a segunda chamada devolve `AlreadyClosed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseOutcome {
    Closed { cancelled_requests: usize },
    AlreadyClosed,
}

/// Resultado de `cancel_request`. Idempotente.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    AlreadyCancelled,
    AlreadyFinished,
}

/// Classificação de evento recebido para uma sessão.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDisposition {
    Accept,
    RejectUnknownSession,
    RejectClosedSession,
}

struct Request {
    state: RequestState,
    token: CancellationToken,
}

struct Session {
    state: SessionState,
    requests: HashMap<Uuid, Request>,
}

/// Dono único do estado das sessões. Para uso concorrente, envolver em `Mutex` no ponto de uso.
#[derive(Default)]
pub struct SessionManager {
    sessions: HashMap<String, Session>,
}

/// Valida `session_id` com as mesmas regras do envelope: 1–128 caracteres, sem controle.
pub fn validate_session_id(id: &str) -> Result<(), SessionError> {
    if id.is_empty() {
        return Err(SessionError::InvalidId("vazio"));
    }
    if id.chars().count() > MAX_SESSION_ID_CHARS {
        return Err(SessionError::InvalidId("longo demais"));
    }
    if id.chars().any(char::is_control) {
        return Err(SessionError::InvalidId("caractere de controle"));
    }
    Ok(())
}

impl SessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Abre uma sessão. Reabrir uma ativa é no-op; reabrir uma encerrada é erro.
    pub fn open(&mut self, id: &str) -> Result<OpenOutcome, SessionError> {
        validate_session_id(id)?;
        match self.sessions.get(id) {
            Some(session) if session.state == SessionState::Active => {
                Ok(OpenOutcome::AlreadyActive)
            }
            Some(_) => Err(SessionError::ClosedSession),
            None => {
                self.sessions.insert(
                    id.to_owned(),
                    Session {
                        state: SessionState::Active,
                        requests: HashMap::new(),
                    },
                );
                Ok(OpenOutcome::Opened)
            }
        }
    }

    /// Encerra a sessão e cancela suas requisições em execução. Não toca outras sessões.
    pub fn close(&mut self, id: &str) -> Result<CloseOutcome, SessionError> {
        let session = self
            .sessions
            .get_mut(id)
            .ok_or(SessionError::UnknownSession)?;
        if session.state == SessionState::Closed {
            return Ok(CloseOutcome::AlreadyClosed);
        }
        session.state = SessionState::Closed;
        let mut cancelled = 0;
        for request in session.requests.values_mut() {
            if request.state == RequestState::Running {
                request.state = RequestState::Cancelled;
                request.token.cancel();
                cancelled += 1;
            }
        }
        Ok(CloseOutcome::Closed {
            cancelled_requests: cancelled,
        })
    }

    pub fn state(&self, id: &str) -> Option<SessionState> {
        self.sessions.get(id).map(|s| s.state)
    }

    /// Registra uma requisição em sessão ativa e devolve o token de cancelamento dela.
    pub fn begin_request(
        &mut self,
        id: &str,
        request_id: Uuid,
    ) -> Result<CancellationToken, SessionError> {
        let session = self.active_session_mut(id)?;
        if session.requests.contains_key(&request_id) {
            return Err(SessionError::DuplicateRequest);
        }
        let token = CancellationToken::new();
        session.requests.insert(
            request_id,
            Request {
                state: RequestState::Running,
                token: token.clone(),
            },
        );
        Ok(token)
    }

    /// Marca a requisição como concluída. Se já estava cancelada, o estado cancelado prevalece.
    pub fn finish_request(&mut self, id: &str, request_id: Uuid) -> Result<(), SessionError> {
        let request = self.request_mut(id, request_id)?;
        if request.state == RequestState::Running {
            request.state = RequestState::Finished;
        }
        Ok(())
    }

    /// Cancela uma requisição. Idempotente; não afeta outras requisições nem sessões.
    pub fn cancel_request(
        &mut self,
        id: &str,
        request_id: Uuid,
    ) -> Result<CancelOutcome, SessionError> {
        let request = self.request_mut(id, request_id)?;
        match request.state {
            RequestState::Running => {
                request.state = RequestState::Cancelled;
                request.token.cancel();
                Ok(CancelOutcome::Cancelled)
            }
            RequestState::Cancelled => Ok(CancelOutcome::AlreadyCancelled),
            RequestState::Finished => Ok(CancelOutcome::AlreadyFinished),
        }
    }

    pub fn request_state(&self, id: &str, request_id: Uuid) -> Option<RequestState> {
        self.sessions
            .get(id)?
            .requests
            .get(&request_id)
            .map(|r| r.state)
    }

    /// Decide se um evento de `id` pode ser processado. Não cria sessão.
    pub fn check_event(&self, id: &str) -> EventDisposition {
        match self.sessions.get(id).map(|s| s.state) {
            Some(SessionState::Active) => EventDisposition::Accept,
            Some(SessionState::Closed) => EventDisposition::RejectClosedSession,
            None => EventDisposition::RejectUnknownSession,
        }
    }

    fn active_session_mut(&mut self, id: &str) -> Result<&mut Session, SessionError> {
        let session = self
            .sessions
            .get_mut(id)
            .ok_or(SessionError::UnknownSession)?;
        match session.state {
            SessionState::Active => Ok(session),
            SessionState::Closed => Err(SessionError::ClosedSession),
        }
    }

    fn request_mut(&mut self, id: &str, request_id: Uuid) -> Result<&mut Request, SessionError> {
        let session = self
            .sessions
            .get_mut(id)
            .ok_or(SessionError::UnknownSession)?;
        session
            .requests
            .get_mut(&request_id)
            .ok_or(SessionError::UnknownRequest)
    }
}
