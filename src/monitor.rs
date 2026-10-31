//! Monitor de contratos: assina o barramento e aplica as regras de STT e intenção.
//!
//! Não executa ações nem autoriza nada. Só rastreia ordem/duplicatas/timeout e registra as decisões
//! do `ReadinessGate` (sem texto transcrito nos logs). Quando não houver Policy Engine, o risco é `Read`
//! e as admissões são apenas observáveis.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::SpeechConfig;
use crate::error::Result;
use crate::intent::{
    Decision, InterpretationPayload, InterpretationTracker, ReadinessGate, RiskClass,
    SourceSnapshot,
};
use crate::protocol::{KavoMessage, MessageType};
use crate::runtime::{Component, ComponentContext};
use crate::speech::{Acceptance, CancelPayload, SpeechPayload, StreamStatus, StreamTracker};

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
    pub streams_timed_out: u64,
    pub cancels: u64,
}

pub type SharedStats = Arc<Mutex<MonitorStats>>;

pub struct ContractMonitor {
    rx: mpsc::Receiver<KavoMessage>,
    speech: StreamTracker,
    interpretations: InterpretationTracker,
    gate: ReadinessGate,
    idle_timeout_ms: u64,
    started: Instant,
    stats: SharedStats,
}

impl ContractMonitor {
    /// `rx` deve ser criado com `EventBus::subscribe` antes do início do IPC, para não perder mensagens.
    pub fn new(rx: mpsc::Receiver<KavoMessage>, cfg: &SpeechConfig, stats: SharedStats) -> Self {
        Self {
            rx,
            speech: StreamTracker::new(cfg.max_streams),
            interpretations: InterpretationTracker::default(),
            gate: ReadinessGate::new(cfg.min_confidence),
            idle_timeout_ms: cfg.stream_idle_timeout_ms,
            started: Instant::now(),
            stats,
        }
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
        let now = self.now_ms();
        let outcome = self.speech.apply(&payload, now);
        self.bump(|s| match &outcome {
            Acceptance::Applied => s.speech_applied += 1,
            Acceptance::Duplicate => s.speech_duplicate += 1,
            Acceptance::Stale { .. } => s.speech_stale += 1,
            Acceptance::Gap { .. } => s.speech_gap += 1,
            Acceptance::Rejected(_) => s.speech_rejected += 1,
        });
        self.release_if_terminal(&payload.stream_id);
        match outcome {
            Acceptance::Applied => {
                debug!(stream_id = %payload.stream_id, sequence = payload.sequence, "evento STT aplicado")
            }
            Acceptance::Duplicate => {
                debug!(stream_id = %payload.stream_id, sequence = payload.sequence, "evento STT duplicado ignorado")
            }
            other => {
                warn!(stream_id = %payload.stream_id, sequence = payload.sequence, resultado = other.label(), "evento STT não aplicado")
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
        match self.interpretations.apply(&payload) {
            Acceptance::Applied => self.bump(|s| s.interpretation_applied += 1),
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
                self.speech.status(&payload.stream_id),
                Some(StreamStatus::Active)
            ),
            segment: self.speech.segment(&payload.stream_id, &payload.segment_id),
        };
        // Sem Capability Registry/Policy Engine ainda: risco `Read`, confirmação de política falsa.
        let decision = self.gate.evaluate(&payload, source, RiskClass::Read, false);
        match &decision {
            Decision::Admit(_) => self.bump(|s| s.admitted += 1),
            Decision::Duplicate(_) => self.bump(|s| s.duplicate_admissions += 1),
            Decision::Supersede { .. } => self.bump(|s| s.superseded += 1),
            Decision::NotReady(_) => self.bump(|s| s.not_ready += 1),
        }
        // Logs sem parâmetros: só rótulo, motivo e impressão digital da chave.
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
                let changed = self.speech.cancel(&cancel.stream_id);
                self.interpretations.cancel_stream(&cancel.stream_id);
                self.release_if_terminal(&cancel.stream_id);
                info!(stream_id = %cancel.stream_id, alterado = changed, "CANCEL de stream");
            }
            Err(e) => warn!(
                classe = e.kind().as_str(),
                "CANCEL inválido ignorado pelo monitor"
            ),
        }
    }

    fn expire_idle(&mut self) {
        let now = self.now_ms();
        for id in self.speech.expire_idle(now, self.idle_timeout_ms) {
            self.bump(|s| s.streams_timed_out += 1);
            warn!(stream_id = %id, "stream expirado por inatividade");
            self.release_if_terminal(&id);
        }
    }

    /// Stream terminal não admite mais nada: libera registros de interpretação e admissão.
    fn release_if_terminal(&mut self, stream_id: &uuid::Uuid) {
        if self
            .speech
            .status(stream_id)
            .is_some_and(StreamStatus::is_terminal)
        {
            self.interpretations.forget_stream(stream_id);
            self.gate.forget_stream(stream_id);
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
