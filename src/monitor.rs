//! Monitor de contratos: assina o barramento, aplica as regras de STT e intenção e persiste o estado durável.
//!
//! Não executa ações nem autoriza nada. Regras de durabilidade:
//! - Stream é gravado antes de ser aceito (`OPEN`) e fechado ao terminar.
//! - Admissão só conta depois do `COMMIT` da transação que consome a chave.
//! - Falha de gravação de admissão = `NOT_READY(STORE_UNAVAILABLE)`. Nunca `ADMIT`.
//! - Logs sem texto transcrito e sem valores de parâmetro.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::SpeechConfig;
use crate::error::Result;
use crate::intent::{
    Decision, InterpretationPayload, InterpretationTracker, NotReadyReason, ReadinessGate,
    RiskClass, SourceSnapshot,
};
use crate::protocol::{KavoMessage, MessageType};
use crate::runtime::{Component, ComponentContext};
use crate::speech::{Acceptance, CancelPayload, SpeechPayload, StreamStatus, StreamTracker};
use crate::store::{AdmissionRow, CommitOutcome, Store, StreamDbStatus};

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
}

pub type SharedStats = Arc<Mutex<MonitorStats>>;

pub struct ContractMonitor {
    rx: mpsc::Receiver<KavoMessage>,
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

fn db_status(status: &StreamStatus) -> Option<StreamDbStatus> {
    match status {
        StreamStatus::Active => None,
        StreamStatus::Ended => Some(StreamDbStatus::Ended),
        StreamStatus::Cancelled => Some(StreamDbStatus::Cancelled),
        StreamStatus::Failed(_) => Some(StreamDbStatus::Failed),
        StreamStatus::TimedOut => Some(StreamDbStatus::TimedOut),
    }
}

impl ContractMonitor {
    /// Carrega o estado durável e cria o monitor. Falha de carga = erro (fail-closed).
    /// `rx` deve vir de `EventBus::subscribe` antes do início do IPC.
    pub fn open(
        rx: mpsc::Receiver<KavoMessage>,
        cfg: &SpeechConfig,
        stats: SharedStats,
        mut store: Store,
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

    fn bump(&self, f: impl FnOnce(&mut MonitorStats)) {
        let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut stats);
    }

    fn handle(&mut self, msg: &KavoMessage) {
        match msg.kind {
            MessageType::SttResult => self.handle_speech(msg),
            MessageType::IntentDetected => self.handle_intent(msg),
            MessageType::Cancel => self.handle_cancel(msg),
            MessageType::Heartbeat | MessageType::Error => {}
        }
    }

    fn handle_speech(&mut self, msg: &KavoMessage) {
        let Some(value) = msg.payload.as_ref() else {
            return;
        };
        let payload = match SpeechPayload::from_value(value) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    classe = e.kind().as_str(),
                    "STT_RESULT inválido ignorado pelo monitor"
                );
                return;
            }
        };
        let session = msg.session_id.as_str();
        let stream = payload.stream_id;

        // Identidade primeiro: um stream novo só começa depois de gravado.
        let opening = payload.sequence == 1 && self.speech.is_unknown(session, &stream);
        if opening {
            match self.store.open_stream(session, &stream, wall_ms()) {
                Ok(true) => {}
                Ok(false) => {
                    warn!(stream_id = %stream, "stream já gravado; início recusado");
                    self.bump(|s| s.speech_rejected += 1);
                    return;
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
                    return;
                }
            }
        }

        let before = self.speech.status(session, &stream).cloned();
        let now = self.now_ms();
        let outcome = self.speech.apply(session, &payload, now);

        if opening && outcome != Acceptance::Applied {
            // Gravado mas não aceito: fecha como rejeitado, para que o id não fique "aberto" no banco.
            self.persist_status(session, &stream, StreamDbStatus::Rejected);
        }
        self.persist_transition(session, &stream, before.as_ref());

        self.bump(|s| match &outcome {
            Acceptance::Applied => s.speech_applied += 1,
            Acceptance::Duplicate => s.speech_duplicate += 1,
            Acceptance::Stale { .. } => s.speech_stale += 1,
            Acceptance::Gap { .. } => s.speech_gap += 1,
            Acceptance::Rejected(_) => s.speech_rejected += 1,
        });
        match outcome {
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
    }

    fn handle_intent(&mut self, msg: &KavoMessage) {
        let Some(value) = msg.payload.as_ref() else {
            return;
        };
        let payload = match InterpretationPayload::from_value(value) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    classe = e.kind().as_str(),
                    "INTENT_DETECTED inválido ignorado pelo monitor"
                );
                return;
            }
        };
        let session = msg.session_id.as_str();
        match self.interpretations.apply(session, &payload) {
            Acceptance::Applied => {
                self.bump(|s| s.interpretation_applied += 1);
                let row = InterpretationTracker::row_for(session, &payload);
                // Falha aqui não admite nada: a admissão grava a interpretação na própria transação.
                if let Err(e) = self.store.upsert_interpretation(&row, wall_ms()) {
                    warn!(classe = e.kind().as_str(), "interpretação não gravada");
                    self.bump(|s| s.store_errors += 1);
                }
            }
            Acceptance::Duplicate => {
                self.bump(|s| s.interpretation_duplicate += 1);
                debug!(interpretation_id = %payload.interpretation_id, "interpretação duplicada ignorada");
                return;
            }
            other => {
                self.bump(|s| s.interpretation_rejected += 1);
                warn!(interpretation_id = %payload.interpretation_id, resultado = other.label(), "interpretação não aplicada");
                return;
            }
        }

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
            .evaluate(session, &payload, source, RiskClass::Read, false);

        let decision = match decision.admitted_key() {
            None => decision,
            Some(key) => {
                let row = InterpretationTracker::row_for(session, &payload);
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

    fn handle_cancel(&mut self, msg: &KavoMessage) {
        self.bump(|s| s.cancels += 1);
        let Some(value) = msg.payload.as_ref() else {
            // Alvo é uma requisição (correlation_id). Não há tarefas no núcleo ainda.
            debug!("CANCEL de requisição recebido; nenhuma tarefa ativa para cancelar");
            return;
        };
        match CancelPayload::from_value(value) {
            Ok(cancel) => {
                let session = msg.session_id.as_str();
                let before = self.speech.status(session, &cancel.stream_id).cloned();
                // Persistido antes da memória. Se falhar, a memória ainda bloqueia nesta execução,
                // e no reinício o stream fica INTERRUPTED: nada é retomado.
                if let Err(e) = self
                    .store
                    .cancel_stream(session, &cancel.stream_id, wall_ms())
                {
                    warn!(classe = e.kind().as_str(), "cancelamento não gravado");
                    self.bump(|s| s.store_errors += 1);
                }
                let changed = self.speech.cancel(session, &cancel.stream_id);
                let interps = self
                    .interpretations
                    .cancel_stream(session, &cancel.stream_id);
                info!(stream_id = %cancel.stream_id, alterado = changed, interpretacoes = interps, "CANCEL de stream");
                if let Some(status) = self.speech.status(session, &cancel.stream_id).cloned() {
                    if before.as_ref().is_none_or(|b| !b.is_terminal()) && status.is_terminal() {
                        self.interpretations
                            .forget_stream(session, &cancel.stream_id);
                    }
                }
            }
            Err(e) => warn!(
                classe = e.kind().as_str(),
                "CANCEL inválido ignorado pelo monitor"
            ),
        }
    }

    fn expire_idle(&mut self) {
        let now = self.now_ms();
        for (session, stream) in self.speech.expire_idle(now, self.idle_timeout_ms) {
            self.bump(|s| s.streams_timed_out += 1);
            warn!(stream_id = %stream, "stream expirado por inatividade");
            self.persist_status(&session, &stream, StreamDbStatus::TimedOut);
            self.interpretations.forget_stream(&session, &stream);
        }
    }

    /// Grava a transição para estado terminal, se houve uma.
    fn persist_transition(&mut self, session: &str, stream: &Uuid, before: Option<&StreamStatus>) {
        let Some(after) = self.speech.status(session, stream).cloned() else {
            return;
        };
        let was_terminal = before.is_some_and(StreamStatus::is_terminal);
        if !was_terminal {
            if let Some(status) = db_status(&after) {
                self.persist_status(session, stream, status);
                if after.is_terminal() {
                    self.interpretations.forget_stream(session, stream);
                }
            }
        }
    }

    fn persist_status(&mut self, session: &str, stream: &Uuid, status: StreamDbStatus) {
        if let Err(e) = self.store.close_stream(session, stream, status, wall_ms()) {
            // Em memória o stream já está terminal; no banco continua OPEN e vira INTERRUPTED no reinício.
            warn!(
                classe = e.kind().as_str(),
                "estado final do stream não gravado"
            );
            self.bump(|s| s.store_errors += 1);
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
                    // Drena o que já chegou antes de sair: eventos aceitos pelo barramento não são descartados.
                    while let Ok(msg) = self.rx.try_recv() {
                        self.handle(&msg);
                    }
                    break;
                }
                _ = tick.tick() => self.expire_idle(),
                msg = self.rx.recv() => match msg {
                    Some(msg) => self.handle(&msg),
                    None => break,
                },
            }
        }
        info!("monitor de contratos encerrado");
        Ok(())
    }
}
