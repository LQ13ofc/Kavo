//! Monitor de contratos: assina o barramento, aplica as regras de STT e intenção e persiste o estado durável.
//!
//! Não executa ações nem autoriza nada. Regras de durabilidade:
//! - Stream é gravado antes de ser aceito (`OPEN`) e fechado ao terminar.
//! - Admissão só conta depois do `COMMIT` da transação que consome a chave.
//! - Falha de gravação de admissão = `NOT_READY(STORE_UNAVAILABLE)`. Nunca `ADMIT`.
//! - Logs sem texto transcrito e sem valores de parâmetro.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::SpeechConfig;
use crate::control::{
    self, AdmissionAck, AdmissionStatus, ControlCommand, ControlOp, ControlReceiver, ControlResult,
    Outcome, Reason, RequestKind, SessionControl, WireState,
};
use crate::error::Result;
use crate::intent::{
    Decision, InterpretationPayload, InterpretationTracker, NotReadyReason, ReadinessGate,
    RiskClass, SourceSnapshot,
};
use crate::protocol::{KavoMessage, MessageType, error_code};
use crate::runtime::{Component, ComponentContext};
use crate::session::{EventDisposition, OpenOutcome, RequestState, SessionManager, SessionState};
use crate::speech::{
    Acceptance, CancelPayload, SpeechPayload, StreamStatus, StreamTracker, TerminalTransition,
};
use crate::store::{
    AdmissionRow, CancelOutcome, CommitOutcome, InterpWrite, Store, StreamDbStatus,
};

const IDLE_CHECK_PERIOD: Duration = Duration::from_millis(250);

/// Contadores observáveis (testes e diagnóstico). Sem conteúdo de transcrição.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MonitorStats {
    pub speech_applied: u64,
    pub speech_duplicate: u64,
    pub speech_stale: u64,
    pub speech_gap: u64,
    pub speech_rejected: u64,
    pub interpretation_applied: u64,
    pub interpretation_duplicate: u64,
    pub interpretation_rejected: u64,
    pub admitted: u64,
    pub duplicate_admissions: u64,
    pub superseded: u64,
    pub not_ready: u64,
    pub already_admitted: u64,
    pub store_errors: u64,
    pub streams_timed_out: u64,
    pub cancels: u64,
    pub session_rejected: u64,
    pub session_implicit_open: u64,
    pub requests_cancelled: u64,
    pub control_handled: u64,
    pub admission_accepted: u64,
    pub admission_duplicate: u64,
    pub admission_rejected: u64,
    pub admission_not_ready: u64,
    /// Aberturas gravadas e ainda não admitidas em memória (gauge). Deve voltar a zero ao fim dos streams.
    pub pending_open: u64,
}

pub type SharedStats = Arc<Mutex<MonitorStats>>;

pub struct ContractMonitor {
    rx: mpsc::Receiver<KavoMessage>,
    /// Autoridade única de sessões e requisições. O monitor é o único dono desse estado.
    sessions: SessionManager,
    /// Requisição (sessão, `request_id`) → stream de STT vinculado.
    bindings: HashMap<(String, Uuid), Uuid>,
    control: ControlReceiver,
    /// Streams já gravados como `OPEN` cuja admissão em memória ainda não aconteceu (falha transitória).
    pending_open: HashSet<(String, Uuid)>,
    /// Teto de `pending_open` (= `speech.max_streams`). Acima disso, novas aberturas recebem BUSY.
    pending_limit: usize,
    speech: StreamTracker,
    interpretations: InterpretationTracker,
    gate: ReadinessGate,
    store: Store,
    idle_timeout_ms: u64,
    started: Instant,
    stats: SharedStats,
}

fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

impl ContractMonitor {
    /// Carrega o estado durável e cria o monitor. Falha de carga = erro (fail-closed).
    /// `rx` deve vir de `EventBus::subscribe` antes do início do IPC.
    pub fn open(
        rx: mpsc::Receiver<KavoMessage>,
        cfg: &SpeechConfig,
        stats: SharedStats,
        store: Store,
    ) -> Result<Self> {
        // Sem canal de controle: o emissor é descartado e o ramo de controle fica desativado.
        let (_unused, control) = control::channel();
        Self::open_with_control(rx, cfg, stats, store, control)
    }

    /// Como `open`, com o canal de controle que o IPC usa para pedidos com resposta.
    pub fn open_with_control(
        rx: mpsc::Receiver<KavoMessage>,
        cfg: &SpeechConfig,
        stats: SharedStats,
        mut store: Store,
        control: ControlReceiver,
    ) -> Result<Self> {
        let snap = store.load()?;
        let mut speech = StreamTracker::new(cfg.max_streams);
        // Todo stream já gravado é aposentado: nenhum pode voltar a aceitar eventos.
        speech.restore_retired(snap.streams.iter().cloned());
        let mut interpretations = InterpretationTracker::default();
        interpretations.restore(snap.interpretations);
        let mut gate = ReadinessGate::new(cfg.min_confidence);
        gate.restore(snap.consumed, snap.current);
        Ok(Self {
            rx,
            sessions: SessionManager::new(),
            bindings: HashMap::new(),
            control,
            pending_open: HashSet::new(),
            pending_limit: cfg.max_streams.max(1),
            speech,
            interpretations,
            gate,
            store,
            idle_timeout_ms: cfg.stream_idle_timeout_ms,
            started: Instant::now(),
            stats,
        })
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Publica o tamanho atual de `pending_open` nas estatísticas.
    fn sync_pending(&self) {
        let len = self.pending_open.len() as u64;
        self.bump(|s| s.pending_open = len);
    }

    fn bump(&self, f: impl FnOnce(&mut MonitorStats)) {
        let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut stats);
    }

    fn handle(&mut self, msg: &KavoMessage) {
        match msg.kind {
            // Caminho legado do barramento (1.1 e testes): mesma admissão, sem ACK.
            MessageType::SttResult | MessageType::IntentDetected => {
                let (_, pending) = self.process_admission(msg);
                if let Some(payload) = pending {
                    self.evaluate_readiness(msg.session_id.as_str(), &payload);
                }
            }
            // CANCEL 1.1 chega pelo barramento e não tem resposta. CANCEL 1.2 usa o canal de controle.
            MessageType::Cancel => {
                let _ = self.apply_cancel(msg);
            }
            // Controle só pelo canal de controle, nunca pelo barramento.
            MessageType::SessionControl
            | MessageType::ControlResult
            | MessageType::AdmissionAck
            | MessageType::Heartbeat
            | MessageType::Error => {}
        }
    }

    /// Gate de sessão para STT e intenção. Devolve o motivo da recusa, se houver.
    /// Mensagem 1.1 em sessão desconhecida abre a sessão (legado). Mensagem 1.2 é recusada.
    /// Sessão encerrada recusa sempre, inclusive 1.1.
    fn session_gate(&mut self, msg: &KavoMessage) -> Option<Reason> {
        let session = msg.session_id.as_str();
        match self.sessions.check_event(session) {
            EventDisposition::Accept => None,
            EventDisposition::RejectUnknownSession if msg.minor() < 2 => {
                match self.sessions.open(session) {
                    Ok(_) => {
                        self.bump(|s| s.session_implicit_open += 1);
                        None
                    }
                    Err(_) => Some(Reason::InvalidPayload),
                }
            }
            EventDisposition::RejectUnknownSession => Some(Reason::SessionUnknown),
            EventDisposition::RejectClosedSession => Some(Reason::SessionClosed),
        }
    }

    /// Admissão de um evento STT ou INTENT. Devolve o ACK e, se a interpretação foi gravada agora,
    /// a interpretação para avaliação de prontidão (feita depois do ACK, fora da semântica do ACK).
    fn process_admission(
        &mut self,
        msg: &KavoMessage,
    ) -> (AdmissionAck, Option<InterpretationPayload>) {
        if let Some(reason) = self.session_gate(msg) {
            self.bump(|s| s.session_rejected += 1);
            warn!(kind = ?msg.kind, motivo = ?reason, "evento rejeitado: sessão não ativa");
            let ack = AdmissionAck::for_event(msg, AdmissionStatus::Rejected, Some(reason));
            self.count_admission(ack.status);
            return (ack, None);
        }
        let (status, reason, pending) = if msg.kind == MessageType::IntentDetected {
            let (status, reason, pending) = self.apply_intent(msg);
            (status, reason, pending)
        } else {
            let (status, reason) = self.apply_speech(msg);
            (status, reason, None)
        };
        let ack = AdmissionAck::for_event(msg, status, reason);
        self.count_admission(status);
        (ack, pending)
    }

    fn count_admission(&self, status: AdmissionStatus) {
        self.bump(|s| match status {
            AdmissionStatus::Accepted => s.admission_accepted += 1,
            AdmissionStatus::Duplicate => s.admission_duplicate += 1,
            AdmissionStatus::Rejected => s.admission_rejected += 1,
            AdmissionStatus::NotReady => s.admission_not_ready += 1,
        });
    }

    /// Aplica um pedido de controle e devolve o resultado (sempre, mesmo quando recusado).
    fn apply_control(&mut self, msg: &KavoMessage) -> ControlResult {
        self.bump(|s| s.control_handled += 1);
        match msg.kind {
            MessageType::SessionControl => self.apply_session_control(msg),
            MessageType::Cancel => self.apply_cancel(msg),
            _ => ControlResult::rejected(
                ControlOp::Status,
                Reason::InvalidPayload,
                WireState::Unknown,
            ),
        }
    }

    fn apply_session_control(&mut self, msg: &KavoMessage) -> ControlResult {
        let session = msg.session_id.as_str();
        let state = wire_state(self.sessions.state(session));
        let payload = match msg.payload.as_ref().map(SessionControl::from_value) {
            Some(Ok(payload)) => payload,
            // A IPC já valida o payload; esta recusa é defesa em profundidade.
            _ => return ControlResult::rejected(ControlOp::Status, Reason::InvalidPayload, state),
        };
        match payload {
            SessionControl::Open => match self.sessions.open(session) {
                Ok(OpenOutcome::Opened) => {
                    ControlResult::new(ControlOp::Open, Outcome::Applied, WireState::Active)
                }
                Ok(OpenOutcome::AlreadyActive) => {
                    ControlResult::new(ControlOp::Open, Outcome::Noop, WireState::Active)
                }
                Err(crate::session::SessionError::ClosedSession) => ControlResult::rejected(
                    ControlOp::Open,
                    Reason::SessionClosed,
                    WireState::Closed,
                ),
                Err(_) => ControlResult::rejected(ControlOp::Open, Reason::InvalidPayload, state),
            },
            SessionControl::Status => ControlResult::new(ControlOp::Status, Outcome::Noop, state),
            SessionControl::Close => self.close_session(session),
            SessionControl::RequestBegin { kind, stream_id } => {
                self.begin_request(msg, session, kind, stream_id)
            }
            SessionControl::RequestEnd { request_id } => self.end_request(session, request_id),
        }
    }

    /// Encerra a sessão. Antes, cancela os streams abertos dela e os vinculados às requisições em
    /// andamento, com gravação durável. Sessões de outras chaves não são tocadas.
    fn close_session(&mut self, session: &str) -> ControlResult {
        let state = wire_state(self.sessions.state(session));
        match self.sessions.state(session) {
            None => {
                return ControlResult::rejected(ControlOp::Close, Reason::SessionUnknown, state);
            }
            Some(SessionState::Closed) => {
                return ControlResult::new(ControlOp::Close, Outcome::Noop, WireState::Closed);
            }
            Some(SessionState::Active) => {}
        }
        let mut streams = self.speech.active_streams(session);
        for ((s, request), stream) in &self.bindings {
            if s == session
                && self.sessions.request_state(session, *request) == Some(RequestState::Running)
                && !streams.contains(stream)
            {
                streams.push(*stream);
            }
        }
        let mut cancelled_streams = 0;
        for stream in &streams {
            match self.cancel_stream(session, stream) {
                Ok(crate::store::CancelOutcome::CancelledOpen) => cancelled_streams += 1,
                Ok(_) => {}
                Err(e) => {
                    // Fail-closed: a sessão continua ativa; streams já gravados seguem cancelados e
                    // um novo CLOSE conclui a operação.
                    warn!(
                        classe = e.kind().as_str(),
                        "encerramento de sessão não gravado"
                    );
                    self.bump(|s| s.store_errors += 1);
                    return ControlResult::rejected(
                        ControlOp::Close,
                        Reason::StoreUnavailable,
                        WireState::Active,
                    );
                }
            }
        }
        match self.sessions.close(session) {
            Ok(crate::session::CloseOutcome::Closed { cancelled_requests }) => {
                // Sessão encerrada recusa todo evento (gate): as pendências dela não são mais necessárias.
                self.pending_open.retain(|(s, _)| s != session);
                self.sync_pending();
                self.bump(|s| s.requests_cancelled += cancelled_requests as u64);
                let mut result =
                    ControlResult::new(ControlOp::Close, Outcome::Applied, WireState::Closed);
                result.cancelled_requests = cancelled_requests as u64;
                result.cancelled_streams = cancelled_streams;
                result
            }
            _ => ControlResult::rejected(ControlOp::Close, Reason::SessionUnknown, state),
        }
    }

    fn begin_request(
        &mut self,
        msg: &KavoMessage,
        session: &str,
        kind: RequestKind,
        stream: Uuid,
    ) -> ControlResult {
        let op = ControlOp::RequestBegin;
        let RequestKind::SttStream = kind;
        let state = wire_state(self.sessions.state(session));
        match self.sessions.state(session) {
            None => return ControlResult::rejected(op, Reason::SessionUnknown, state),
            Some(SessionState::Closed) => {
                return ControlResult::rejected(op, Reason::SessionClosed, state);
            }
            Some(SessionState::Active) => {}
        }
        let Ok(request) = Uuid::parse_str(&msg.message_id) else {
            return ControlResult::rejected(op, Reason::InvalidPayload, state);
        };
        if self.sessions.request_state(session, request).is_some() {
            return ControlResult::rejected(op, Reason::RequestDuplicate, state);
        }
        // Um stream tem no máximo uma requisição em andamento na sessão: cancelar uma não pode atingir outra.
        let stream_taken = self.bindings.iter().any(|((s, request), bound)| {
            s == session
                && *bound == stream
                && self.sessions.request_state(session, *request) == Some(RequestState::Running)
        });
        if stream_taken {
            return ControlResult::rejected(op, Reason::StreamBound, state);
        }
        match self.sessions.begin_request(session, request) {
            Ok(_) => {
                self.bindings.insert((session.to_owned(), request), stream);
                ControlResult::new(op, Outcome::Applied, state)
            }
            Err(crate::session::SessionError::DuplicateRequest) => {
                ControlResult::rejected(op, Reason::RequestDuplicate, state)
            }
            Err(_) => ControlResult::rejected(op, Reason::InvalidPayload, state),
        }
    }

    fn end_request(&mut self, session: &str, request: Uuid) -> ControlResult {
        let op = ControlOp::RequestEnd;
        let state = wire_state(self.sessions.state(session));
        if self.sessions.state(session).is_none() {
            return ControlResult::rejected(op, Reason::SessionUnknown, state);
        }
        match self.sessions.request_state(session, request) {
            None => ControlResult::rejected(op, Reason::RequestUnknown, state),
            Some(RequestState::Cancelled) => {
                ControlResult::rejected(op, Reason::RequestCancelled, state)
            }
            Some(RequestState::Finished) => ControlResult::new(op, Outcome::Noop, state),
            Some(RequestState::Running) => {
                let _ = self.sessions.finish_request(session, request);
                ControlResult::new(op, Outcome::Applied, state)
            }
        }
    }

    /// CANCEL. Com `payload.stream_id`: cancela o stream. Sem payload: cancela a requisição indicada
    /// por `correlation_id`, dentro da própria sessão, e o stream vinculado a ela.
    fn apply_cancel(&mut self, msg: &KavoMessage) -> ControlResult {
        self.bump(|s| s.cancels += 1);
        let session = msg.session_id.as_str();
        let state = wire_state(self.sessions.state(session));
        // Mensagem 1.2 em sessão desconhecida é recusada. A 1.1 segue o comportamento legado.
        if msg.minor() >= 2 && self.sessions.state(session).is_none() {
            let op = if msg.payload.is_some() {
                ControlOp::StreamCancel
            } else {
                ControlOp::RequestCancel
            };
            return ControlResult::rejected(op, Reason::SessionUnknown, state);
        }
        match msg.payload.as_ref() {
            Some(value) => {
                let op = ControlOp::StreamCancel;
                let Ok(cancel) = CancelPayload::from_value(value) else {
                    return ControlResult::rejected(op, Reason::InvalidPayload, state);
                };
                match self.cancel_stream(session, &cancel.stream_id) {
                    Ok(crate::store::CancelOutcome::CancelledOpen) => {
                        let mut result = ControlResult::new(op, Outcome::Applied, state);
                        result.cancelled_streams = 1;
                        result
                    }
                    Ok(crate::store::CancelOutcome::Tombstoned) => {
                        ControlResult::new(op, Outcome::Applied, state)
                    }
                    Ok(crate::store::CancelOutcome::AlreadyTerminal(_)) => {
                        ControlResult::new(op, Outcome::Noop, state)
                    }
                    Err(e) => {
                        warn!(
                            classe = e.kind().as_str(),
                            "cancelamento não gravado; nada foi alterado"
                        );
                        self.bump(|s| s.store_errors += 1);
                        ControlResult::rejected(op, Reason::StoreUnavailable, state)
                    }
                }
            }
            None => self.cancel_request(session, msg),
        }
    }

    fn cancel_request(&mut self, session: &str, msg: &KavoMessage) -> ControlResult {
        let op = ControlOp::RequestCancel;
        let state = wire_state(self.sessions.state(session));
        let Some(request) = msg
            .correlation_id
            .as_deref()
            .and_then(|id| Uuid::parse_str(id).ok())
        else {
            return ControlResult::rejected(op, Reason::InvalidPayload, state);
        };
        match self.sessions.request_state(session, request) {
            None => return ControlResult::rejected(op, Reason::RequestUnknown, state),
            Some(RequestState::Finished) => {
                return ControlResult::rejected(op, Reason::RequestFinished, state);
            }
            Some(RequestState::Cancelled) => return ControlResult::new(op, Outcome::Noop, state),
            Some(RequestState::Running) => {}
        }
        // Durável primeiro: se o stream não puder ser cancelado, a requisição continua em andamento.
        let mut cancelled_streams = 0;
        if let Some(stream) = self.bindings.get(&(session.to_owned(), request)).copied() {
            match self.cancel_stream(session, &stream) {
                Ok(crate::store::CancelOutcome::CancelledOpen) => cancelled_streams = 1,
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        classe = e.kind().as_str(),
                        "cancelamento de requisição não gravado"
                    );
                    self.bump(|s| s.store_errors += 1);
                    return ControlResult::rejected(op, Reason::StoreUnavailable, state);
                }
            }
        }
        let _ = self.sessions.cancel_request(session, request);
        self.bump(|s| s.requests_cancelled += 1);
        let mut result = ControlResult::new(op, Outcome::Applied, state);
        result.cancelled_streams = cancelled_streams;
        result
    }

    /// Cancela um stream: grava primeiro (SQLite) e só então muda a memória.
    fn cancel_stream(
        &mut self,
        session: &str,
        stream: &Uuid,
    ) -> Result<crate::store::CancelOutcome> {
        let outcome = self.store.cancel_stream(session, stream, wall_ms())?;
        // Depois da gravação durável: o stream fica aposentado na memória, então a pendência não serve mais.
        if self.pending_open.remove(&(session.to_owned(), *stream)) {
            self.sync_pending();
        }
        info!(stream_id = %stream, resultado = outcome.label(), "CANCEL de stream");
        match outcome {
            CancelOutcome::CancelledOpen => {
                self.speech.cancel(session, stream);
                self.interpretations.cancel_stream(session, stream);
                self.interpretations.forget_stream(session, stream);
            }
            CancelOutcome::Tombstoned => {
                // Id desconhecido: aposentado também na memória, para não abrir nesta execução.
                self.speech.cancel(session, stream);
            }
            CancelOutcome::AlreadyTerminal(_) => {
                // No-op completo: nem a memória do stream nem as interpretações são tocadas.
            }
        }
        Ok(outcome)
    }

    /// Aplica um STT. `ACCEPTED` só depois da gravação durável exigida (abertura e terminal).
    ///
    /// Abertura: o SQLite grava `OPEN` antes de a memória aceitar o stream. Se a memória recusar por
    /// capacidade, o status `REJECTED` é gravado antes do ACK. Se essa gravação falhar, o stream fica em
    /// `pending_open` e a próxima tentativa não reabre no banco: ela só refaz a decisão de memória.
    fn apply_speech(&mut self, msg: &KavoMessage) -> (AdmissionStatus, Option<Reason>) {
        let Some(value) = msg.payload.as_ref() else {
            return (AdmissionStatus::Rejected, Some(Reason::InvalidPayload));
        };
        let payload = match SpeechPayload::from_value(value) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    classe = e.kind().as_str(),
                    "STT_RESULT inválido ignorado pelo monitor"
                );
                return (AdmissionStatus::Rejected, Some(Reason::InvalidPayload));
            }
        };
        let session = msg.session_id.as_str();
        let stream = payload.stream_id;
        let key: (String, Uuid) = (session.to_owned(), stream);

        // Identidade primeiro: um stream novo só começa depois de gravado (ou de já estar gravado, pendente).
        let opening = payload.sequence == 1 && self.speech.is_unknown(session, &stream);
        if opening && !self.pending_open.contains(&key) {
            // Teto de pendências: sem ele, clientes que abandonam streams acumulariam entradas sem limite.
            if self.pending_open.len() >= self.pending_limit {
                warn!("aberturas pendentes no teto; nova abertura adiada");
                return (AdmissionStatus::NotReady, Some(Reason::Busy));
            }
            match self.store.open_stream(session, &stream, wall_ms()) {
                Ok(true) => {
                    self.pending_open.insert(key.clone());
                    self.sync_pending();
                }
                Ok(false) => {
                    // Já gravado e não está na memória: o status durável diz o motivo da recusa anterior.
                    let reason = match self.store.stream_status(session, &stream) {
                        Ok(Some(status)) if status == StreamDbStatus::Rejected.as_str() => {
                            Reason::StreamLimit
                        }
                        Ok(_) => Reason::StreamClosed,
                        Err(e) => {
                            warn!(classe = e.kind().as_str(), "estado do stream não lido");
                            self.bump(|s| s.store_errors += 1);
                            return (AdmissionStatus::NotReady, Some(Reason::StoreUnavailable));
                        }
                    };
                    warn!(stream_id = %stream, motivo = ?reason, "início de stream já gravado; recusado");
                    self.bump(|s| s.speech_rejected += 1);
                    return (AdmissionStatus::Rejected, Some(reason));
                }
                Err(e) => {
                    warn!(
                        classe = e.kind().as_str(),
                        "não foi possível gravar início de stream"
                    );
                    self.bump(|s| {
                        s.speech_rejected += 1;
                        s.store_errors += 1;
                    });
                    return (AdmissionStatus::NotReady, Some(Reason::StoreUnavailable));
                }
            }
        }
        let now = self.now_ms();
        // Transição terminal: o SQLite confirma primeiro; só então a memória muda (ver `apply_with`).
        let applied = {
            let store = &mut self.store;
            let mut persist = |t: &TerminalTransition| -> Result<()> {
                let status = match t {
                    TerminalTransition::Ended => StreamDbStatus::Ended,
                    TerminalTransition::Failed(_) => StreamDbStatus::Failed,
                };
                store.close_stream(session, &stream, status, wall_ms())
            };
            self.speech.apply_with(session, &payload, now, &mut persist)
        };
        let outcome = match applied {
            Ok(outcome) => outcome,
            Err(e) => {
                // Nada mudou: nem memória, nem sequência. O produtor pode reenviar o mesmo evento.
                warn!(
                    classe = e.kind().as_str(),
                    "transição terminal não gravada; estado anterior preservado"
                );
                self.bump(|s| s.store_errors += 1);
                return (AdmissionStatus::NotReady, Some(Reason::StoreUnavailable));
            }
        };
        if self.speech.status(session, &stream).is_some() {
            // A memória agora tem o stream: a gravação de abertura não está mais pendente.
            if self.pending_open.remove(&key) {
                self.sync_pending();
            }
        } else if opening {
            // Recusa por capacidade: grava REJECTED antes do ACK. Se falhar, NOT_READY e segue pendente.
            if let Err(e) =
                self.store
                    .close_stream(session, &stream, StreamDbStatus::Rejected, wall_ms())
            {
                warn!(classe = e.kind().as_str(), "recusa de stream não gravada");
                self.bump(|s| s.store_errors += 1);
                return (AdmissionStatus::NotReady, Some(Reason::StoreUnavailable));
            }
            if self.pending_open.remove(&key) {
                self.sync_pending();
            }
        }
        if self
            .speech
            .status(session, &stream)
            .is_some_and(StreamStatus::is_terminal)
        {
            self.interpretations.forget_stream(session, &stream);
        }
        self.bump(|s| match &outcome {
            Acceptance::Applied => s.speech_applied += 1,
            Acceptance::Duplicate => s.speech_duplicate += 1,
            Acceptance::Stale { .. } => s.speech_stale += 1,
            Acceptance::Gap { .. } => s.speech_gap += 1,
            Acceptance::Rejected(_) => s.speech_rejected += 1,
        });
        match &outcome {
            Acceptance::Applied => {
                debug!(stream_id = %stream, sequence = payload.sequence, "evento STT aplicado")
            }
            Acceptance::Duplicate => {
                debug!(stream_id = %stream, sequence = payload.sequence, "evento STT duplicado ignorado")
            }
            other => {
                warn!(stream_id = %stream, sequence = payload.sequence, resultado = other.label(), "evento STT não aplicado")
            }
        }
        match outcome {
            Acceptance::Applied => (AdmissionStatus::Accepted, None),
            Acceptance::Duplicate => (AdmissionStatus::Duplicate, None),
            Acceptance::Stale { .. } => (AdmissionStatus::Rejected, Some(Reason::SequenceInvalid)),
            Acceptance::Gap { .. } => (AdmissionStatus::Rejected, Some(Reason::SequenceGap)),
            Acceptance::Rejected(text) => (
                AdmissionStatus::Rejected,
                Some(control::stt_reject_reason(&text)),
            ),
        }
    }

    /// Registra uma interpretação. `ACCEPTED` só depois do `upsert` durável. Em falha, a memória
    /// volta ao estado anterior: um retry com a mesma identidade não vira `DUPLICATE` indevido.
    fn apply_intent(
        &mut self,
        msg: &KavoMessage,
    ) -> (
        AdmissionStatus,
        Option<Reason>,
        Option<InterpretationPayload>,
    ) {
        let Some(value) = msg.payload.as_ref() else {
            return (
                AdmissionStatus::Rejected,
                Some(Reason::InvalidPayload),
                None,
            );
        };
        let payload = match InterpretationPayload::from_value(value) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    classe = e.kind().as_str(),
                    "INTENT_DETECTED inválido ignorado pelo monitor"
                );
                return (
                    AdmissionStatus::Rejected,
                    Some(Reason::InvalidPayload),
                    None,
                );
            }
        };
        let session = msg.session_id.as_str();
        // Memória sem o registro (fim de stream, reinício) não decide sozinha: a durável é consultada.
        if self
            .interpretations
            .snapshot(session, &payload.interpretation_id)
            .is_none()
        {
            match self
                .store
                .interpretation(session, &payload.interpretation_id)
            {
                Ok(Some(row)) => self.interpretations.restore([row]),
                Ok(None) => {}
                Err(e) => {
                    warn!(classe = e.kind().as_str(), "interpretação durável não lida");
                    self.bump(|s| s.store_errors += 1);
                    return (
                        AdmissionStatus::NotReady,
                        Some(Reason::StoreUnavailable),
                        None,
                    );
                }
            }
        }
        let previous = self
            .interpretations
            .snapshot(session, &payload.interpretation_id);
        match self.interpretations.apply(session, &payload) {
            Acceptance::Applied => {
                self.bump(|s| s.interpretation_applied += 1);
                let row = InterpretationTracker::row_for(session, &payload);
                // A durável decide. A memória pode estar esquecida (stream terminal), então não basta.
                match self.store.upsert_interpretation(&row, wall_ms()) {
                    Ok(InterpWrite::Inserted | InterpWrite::Updated) => {
                        (AdmissionStatus::Accepted, None, Some(payload))
                    }
                    Ok(InterpWrite::Unchanged) => {
                        self.bump(|s| s.interpretation_duplicate += 1);
                        debug!(interpretation_id = %payload.interpretation_id, "reenvio idêntico da interpretação");
                        (AdmissionStatus::Duplicate, None, None)
                    }
                    Ok(InterpWrite::Stale(stored)) => {
                        self.interpretations.restore([stored]);
                        self.bump(|s| s.interpretation_rejected += 1);
                        warn!(interpretation_id = %payload.interpretation_id, resultado = "STALE", "revisão recusada pela durável");
                        (AdmissionStatus::Rejected, Some(Reason::StaleRevision), None)
                    }
                    Ok(InterpWrite::Conflict(stored)) => {
                        self.interpretations.restore([stored]);
                        self.bump(|s| s.interpretation_rejected += 1);
                        warn!(interpretation_id = %payload.interpretation_id, resultado = "CONFLICT", "revisão recusada pela durável");
                        (AdmissionStatus::Rejected, Some(Reason::Conflict), None)
                    }
                    Err(e) => {
                        // Fail-closed: a memória volta ao que estava antes desta tentativa.
                        self.interpretations.rollback(
                            session,
                            &payload.interpretation_id,
                            previous,
                        );
                        warn!(
                            classe = e.kind().as_str(),
                            "interpretação não gravada; memória desfeita"
                        );
                        self.bump(|s| s.store_errors += 1);
                        (
                            AdmissionStatus::NotReady,
                            Some(Reason::StoreUnavailable),
                            None,
                        )
                    }
                }
            }
            Acceptance::Duplicate => {
                self.bump(|s| s.interpretation_duplicate += 1);
                debug!(interpretation_id = %payload.interpretation_id, "interpretação duplicada ignorada");
                (AdmissionStatus::Duplicate, None, None)
            }
            Acceptance::Stale { .. } => {
                self.bump(|s| s.interpretation_rejected += 1);
                (AdmissionStatus::Rejected, Some(Reason::StaleRevision), None)
            }
            Acceptance::Gap { .. } => {
                self.bump(|s| s.interpretation_rejected += 1);
                (AdmissionStatus::Rejected, Some(Reason::SequenceGap), None)
            }
            Acceptance::Rejected(text) => {
                self.bump(|s| s.interpretation_rejected += 1);
                warn!(interpretation_id = %payload.interpretation_id, "interpretação não aplicada");
                (
                    AdmissionStatus::Rejected,
                    Some(control::intent_reject_reason(&text)),
                    None,
                )
            }
        }
    }

    /// Prontidão para execução. Não altera o ACK: falha aqui não desfaz a admissão da interpretação.
    fn evaluate_readiness(&mut self, session: &str, payload: &InterpretationPayload) {
        let source = SourceSnapshot {
            stream_active: matches!(
                self.speech.status(session, &payload.stream_id),
                Some(StreamStatus::Active)
            ),
            segment: self
                .speech
                .segment(session, &payload.stream_id, &payload.segment_id),
        };
        // Sem Capability Registry/Policy Engine ainda: risco `Read`, confirmação de política falsa.
        let decision = self
            .gate
            .evaluate(session, payload, source, RiskClass::Read, false);
        let decision = match decision.admitted_key() {
            None => decision,
            Some(key) => {
                let row = InterpretationTracker::row_for(session, payload);
                let adm = AdmissionRow {
                    session_id: session.to_owned(),
                    segment_id: payload.segment_id,
                    stream_id: payload.stream_id,
                    execution_key: key.as_stored().to_owned(),
                    intent: payload
                        .intent
                        .as_ref()
                        .map(|i| i.name.clone())
                        .unwrap_or_default(),
                    interpretation: row,
                };
                match self.store.commit_admission(&adm, wall_ms()) {
                    Ok(CommitOutcome::Committed) => {
                        self.gate.record(session, payload.segment_id, &decision);
                        decision
                    }
                    Ok(CommitOutcome::AlreadyConsumed) => {
                        Decision::NotReady(NotReadyReason::AlreadyAdmitted)
                    }
                    Ok(CommitOutcome::InterpretationRejected(stored)) => {
                        self.interpretations.restore([stored]);
                        Decision::NotReady(NotReadyReason::StaleRevision)
                    }
                    Err(e) => {
                        warn!(
                            classe = e.kind().as_str(),
                            "admissão não gravada; nada foi admitido"
                        );
                        self.bump(|s| s.store_errors += 1);
                        Decision::NotReady(NotReadyReason::StoreUnavailable)
                    }
                }
            }
        };
        match &decision {
            Decision::Admit(_) => self.bump(|s| s.admitted += 1),
            Decision::Duplicate(_) => self.bump(|s| s.duplicate_admissions += 1),
            Decision::Supersede { .. } => self.bump(|s| s.superseded += 1),
            Decision::NotReady(reason) => self.bump(|s| {
                s.not_ready += 1;
                if *reason == NotReadyReason::AlreadyAdmitted {
                    s.already_admitted += 1;
                }
            }),
        }
        // Logs sem parâmetros: só rótulo, motivo e impressão curta da chave.
        info!(
            interpretation_id = %payload.interpretation_id,
            decisao = decision.label(),
            motivo = ?match &decision {
                Decision::NotReady(reason) => Some(*reason),
                _ => None,
            },
            chave = ?decision.fingerprint(),
            "decisão de prontidão"
        );
    }

    /// Responde a um pedido de controle ou de admissão pelo canal de resposta do IPC.
    /// Admissão: o ACK sai antes da avaliação de prontidão.
    fn answer(&mut self, cmd: ControlCommand) {
        let (reply, pending) = match cmd.msg.kind {
            MessageType::SttResult | MessageType::IntentDetected => {
                let (ack, pending) = self.process_admission(&cmd.msg);
                let reply = ack.reply_to(&cmd.msg);
                (reply, pending)
            }
            _ => (self.apply_control(&cmd.msg).reply_to(&cmd.msg), None),
        };
        let reply = match reply {
            Ok(reply) => reply,
            Err(e) => {
                warn!(
                    classe = e.kind().as_str(),
                    "resposta de controle não montada"
                );
                cmd.msg.error_reply(
                    error_code::GENERIC_ERROR,
                    format!("[{}] {e}", e.kind().as_str()),
                )
            }
        };
        // Quem pediu pode ter desistido (prazo do IPC): nada a fazer além de descartar.
        let _ = cmd.reply.send(reply);
        if let Some(payload) = pending {
            self.evaluate_readiness(cmd.msg.session_id.as_str(), &payload);
        }
    }

    fn expire_idle(&mut self) {
        let now = self.now_ms();
        for key in self.speech.idle_candidates(now, self.idle_timeout_ms) {
            let (session, stream) = (key.0.clone(), key.1);
            // Grava TIMED_OUT antes de marcar a memória. Falha: segue ativo nos dois lados e tenta de novo.
            if let Err(e) =
                self.store
                    .close_stream(&session, &stream, StreamDbStatus::TimedOut, wall_ms())
            {
                warn!(
                    classe = e.kind().as_str(),
                    "expiração não gravada; stream segue ativo"
                );
                self.bump(|s| s.store_errors += 1);
                continue;
            }
            self.speech.mark_timed_out(&key);
            self.bump(|s| s.streams_timed_out += 1);
            warn!(stream_id = %stream, "stream expirado por inatividade");
            self.interpretations.forget_stream(&session, &stream);
        }
    }
}

impl Component for ContractMonitor {
    fn name(&self) -> &'static str {
        "contract-monitor"
    }

    async fn run(mut self, ctx: ComponentContext) -> Result<()> {
        let mut tick = tokio::time::interval(IDLE_CHECK_PERIOD);
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    // Drena o que já chegou antes de sair: eventos e pedidos aceitos não são descartados.
                    while let Ok(msg) = self.rx.try_recv() {
                        self.handle(&msg);
                    }
                    while let Ok(cmd) = self.control.try_recv() {
                        self.answer(cmd);
                    }
                    break;
                }
                _ = tick.tick() => self.expire_idle(),
                msg = self.rx.recv() => match msg {
                    Some(msg) => self.handle(&msg),
                    None => break,
                },
                Some(cmd) = self.control.recv() => self.answer(cmd),
            }
        }
        info!("monitor de contratos encerrado");
        Ok(())
    }
}

fn wire_state(state: Option<SessionState>) -> WireState {
    match state {
        Some(SessionState::Active) => WireState::Active,
        Some(SessionState::Closed) => WireState::Closed,
        None => WireState::Unknown,
    }
}
