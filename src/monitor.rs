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
                return;
            }
        };

        // Só fecha como REJECTED se o stream NÃO entrou na memória (recusa por capacidade).
        // Se entrou, a memória o mantém ativo e o banco precisa continuar OPEN.
        if opening && self.speech.status(session, &stream).is_none() {
            self.persist_status(session, &stream, StreamDbStatus::Rejected);
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
                // A durável decide. A memória pode estar esquecida (stream terminal), então não basta.
                match self.store.upsert_interpretation(&row, wall_ms()) {
                    Ok(InterpWrite::Inserted | InterpWrite::Updated) => {}
                    Ok(InterpWrite::Unchanged) => {
                        self.bump(|s| s.interpretation_duplicate += 1);
                        debug!(interpretation_id = %payload.interpretation_id, "reenvio idêntico da interpretação");
                        return;
                    }
                    Ok(InterpWrite::Stale(stored) | InterpWrite::Conflict(stored)) => {
                        // Restaura a memória com o que está gravado e recusa a revisão.
                        self.interpretations.restore([stored]);
                        self.bump(|s| s.interpretation_rejected += 1);
                        warn!(interpretation_id = %payload.interpretation_id, resultado = "STALE_OR_CONFLICT", "revisão de interpretação recusada pela durável");
                        return;
                    }
                    Err(e) => {
                        // Fail-closed: sem gravação da revisão, nada é avaliado para admissão.
                        warn!(classe = e.kind().as_str(), "interpretação não gravada");
                        self.bump(|s| {
                            s.store_errors += 1;
                            s.not_ready += 1;
                        });
                        return;
                    }
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
                // A decisão é do SQLite: a memória só segue o resultado persistido.
                let outcome = match self
                    .store
                    .cancel_stream(session, &cancel.stream_id, wall_ms())
                {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        warn!(
                            classe = e.kind().as_str(),
                            "cancelamento não gravado; nada foi alterado"
                        );
                        self.bump(|st| st.store_errors += 1);
                        return;
                    }
                };
                info!(stream_id = %cancel.stream_id, resultado = outcome.label(), "CANCEL de stream");
                match outcome {
                    CancelOutcome::CancelledOpen => {
                        self.speech.cancel(session, &cancel.stream_id);
                        self.interpretations
                            .cancel_stream(session, &cancel.stream_id);
                        self.interpretations
                            .forget_stream(session, &cancel.stream_id);
                    }
                    CancelOutcome::Tombstoned => {
                        // Id desconhecido: aposentado também na memória, para não abrir nesta execução.
                        self.speech.cancel(session, &cancel.stream_id);
                    }
                    CancelOutcome::AlreadyTerminal(_) => {
                        // No-op completo: nem a memória do stream nem as interpretações são tocadas.
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

    /// Grava um estado terminal que não altera a memória (ex.: abertura recusada).
    fn persist_status(&mut self, session: &str, stream: &Uuid, status: StreamDbStatus) {
        if let Err(e) = self.store.close_stream(session, stream, status, wall_ms()) {
            warn!(classe = e.kind().as_str(), "estado de recusa não gravado");
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
