//! Contratos de STT streaming (Kavo Protocol 1.1, tipo `STT_RESULT`).
//!
//! - `Partial` é hipótese: pode ser revisada por nova revisão do mesmo segmento.
//! - `Confirmed` é terminal: segmento confirmado não aceita mais revisões.
//! - Ordem: `sequence` é estritamente crescente por stream, começando em 1.
//!   Lacuna (`Gap`) não é aplicada; o produtor deve reenviar a partir de `expected`.
//! - Texto nunca é comando. Esta camada só valida e rastreia; não executa nada.

use crate::store::sha256_hex;
use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KavoError, Result};
use crate::protocol::check_text;

pub const MAX_TEXT_CHARS: usize = 2_048;
pub const MAX_REASON_CHARS: usize = 128;
/// Textos estáveis de recusa, usados também para classificar o motivo no ACK de admissão.
pub const REJECT_STREAM_CLOSED: &str = "stream encerrado";
pub const REJECT_STREAM_LIMIT: &str = "limite de streams ativos atingido";
pub const REJECT_CONFLICT: &str = "sequência reutilizada com conteúdo diferente";

/// Quantidade de sequências recentes guardadas para detectar duplicatas.
const DIGEST_WINDOW: usize = 64;
/// Segmentos por stream. Acima disso, a transcrição nova é rejeitada.
pub const MAX_SEGMENTS_PER_STREAM: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TranscriptState {
    Partial,
    Confirmed,
}

/// Evento de STT. `event` é o discriminador na serialização (`{"event": "TRANSCRIPT", ...}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SpeechEvent {
    Transcript {
        segment_id: Uuid,
        /// Começa em 0 por segmento e aumenta de 1 em 1 a cada correção.
        revision: u32,
        state: TranscriptState,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confidence: Option<f32>,
    },
    SpeechStarted {
        segment_id: Uuid,
    },
    SpeechEnded {
        segment_id: Uuid,
    },
    /// Nenhuma fala detectada no intervalo. Nunca vira intenção.
    NoSpeech,
    /// Fala detectada mas não reconhecível. Nunca vira intenção.
    Unintelligible {
        segment_id: Uuid,
    },
    StreamEnded,
    StreamFailed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpeechPayload {
    pub stream_id: Uuid,
    pub sequence: u64,
    pub audio_start_ms: u64,
    pub audio_end_ms: u64,
    #[serde(flatten)]
    pub event: SpeechEvent,
}

impl SpeechPayload {
    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        let payload: Self = serde_json::from_value(value.clone()).map_err(|e| {
            KavoError::Protocol(format!(
                "payload STT inválido ({})",
                crate::protocol::describe_serde(&e)
            ))
        })?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<()> {
        if self.sequence == 0 {
            return Err(KavoError::Protocol("sequence começa em 1".to_owned()));
        }
        if self.audio_start_ms > self.audio_end_ms {
            return Err(KavoError::Protocol(
                "audio_start_ms maior que audio_end_ms".to_owned(),
            ));
        }
        match &self.event {
            SpeechEvent::Transcript {
                text, confidence, ..
            } => {
                if text.trim().is_empty() {
                    return Err(KavoError::Protocol(
                        "transcrição vazia; use NO_SPEECH".to_owned(),
                    ));
                }
                check_text("text", text, MAX_TEXT_CHARS)?;
                check_confidence(*confidence)?;
            }
            SpeechEvent::StreamFailed { reason } => check_text("reason", reason, MAX_REASON_CHARS)?,
            _ => {}
        }
        Ok(())
    }
}

/// Cancelamento de stream: alvo é `stream_id` (ou `correlation_id`, para requisições).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelPayload {
    pub stream_id: Uuid,
}

impl CancelPayload {
    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone()).map_err(|e| {
            KavoError::Protocol(format!(
                "payload CANCEL inválido ({})",
                crate::protocol::describe_serde(&e)
            ))
        })
    }
}

pub(crate) fn check_confidence(confidence: Option<f32>) -> Result<()> {
    match confidence {
        Some(c) if !c.is_finite() || !(0.0..=1.0).contains(&c) => Err(KavoError::Protocol(
            "confidence fora de [0.0, 1.0]".to_owned(),
        )),
        _ => Ok(()),
    }
}

/// Resultado de aplicar um evento ou uma interpretação ao rastreador.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acceptance {
    Applied,
    /// Mesma sequência e mesmo conteúdo já aplicado: ignorado.
    Duplicate,
    /// Sequência anterior ao último aplicado e não registrada como duplicata.
    Stale {
        last_sequence: u64,
    },
    /// Há sequências faltando. O evento não é aplicado.
    Gap {
        expected: u64,
        got: u64,
    },
    /// Violação de conteúdo ou de estado. A sequência é consumida e o motivo é registrado.
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamStatus {
    Active,
    Ended,
    Cancelled,
    Failed(String),
    TimedOut,
}

impl StreamStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Active)
    }
}

impl Acceptance {
    /// Rótulo sem conteúdo, para logs (o motivo pode conter texto do produtor).
    pub fn label(&self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::Duplicate => "DUPLICATE",
            Self::Stale { .. } => "STALE",
            Self::Gap { .. } => "GAP",
            Self::Rejected(_) => "REJECTED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentSnapshot {
    pub revision: u32,
    pub state: TranscriptState,
}

#[derive(Debug)]
struct SegmentState {
    revision: u32,
    state: TranscriptState,
}

#[derive(Debug)]
struct StreamState {
    status: StreamStatus,
    last_sequence: u64,
    /// (sequência, conteúdo canônico) das últimas `DIGEST_WINDOW` sequências.
    /// Texto da recusa, quando a sequência foi consumida e rejeitada. Repetir a mesma sequência
    /// devolve a mesma recusa, nunca `Duplicate`.
    recent: VecDeque<(u64, String, Option<String>)>,
    segments: HashMap<Uuid, SegmentState>,
    last_activity_ms: u64,
}

/// Identidade de um stream: sessão + `stream_id`. Mesmo `stream_id` em outra sessão é outro stream.
pub type StreamKey = (String, Uuid);

/// Rastreia streams de STT: ordem, duplicatas, revisões, estados terminais e timeout.
///
/// Identidade: um stream encerrado, cancelado ou removido por capacidade fica em `retired` para sempre
/// (no processo). Por isso nunca é reaberto, mesmo após a remoção do seu estado. Limitação: a lista
/// não sobrevive a reinício do núcleo (ver `SPEECH-INTENT-CONTRACTS.md`).
#[derive(Debug)]
pub struct StreamTracker {
    streams: HashMap<StreamKey, StreamState>,
    retired: HashSet<StreamKey>,
    /// Janela de digests de streams removidos da memória por capacidade. Repetições do que já foi
    /// consumido continuam sendo reconhecidas (`DUPLICATE`/recusa original), não tratadas como novas.
    retired_recent: HashMap<StreamKey, VecDeque<(u64, String, Option<String>)>>,
    max_streams: usize,
}

impl StreamTracker {
    pub fn new(max_streams: usize) -> Self {
        Self {
            streams: HashMap::new(),
            retired: HashSet::new(),
            retired_recent: HashMap::new(),
            max_streams: max_streams.max(1),
        }
    }

    /// Aplica um evento. `now_ms` é o relógio monotônico do núcleo, usado para timeout.
    /// Aplica um evento sem persistência externa (testes e cenários puramente em memória).
    pub fn apply(&mut self, session: &str, payload: &SpeechPayload, now_ms: u64) -> Acceptance {
        self.apply_with(session, payload, now_ms, &mut |_| Ok(()))
            .unwrap_or_else(|e| Acceptance::Rejected(e.to_string()))
    }

    /// Aplica um evento. Se o evento causar transição terminal (`StreamEnded`, `StreamFailed`),
    /// chama `persist` ANTES de qualquer mudança em memória. Se `persist` falhar, devolve o erro
    /// com a memória intacta e a sequência do evento não consumida (o produtor pode reenviar).
    pub fn apply_with(
        &mut self,
        session: &str,
        payload: &SpeechPayload,
        now_ms: u64,
        persist: &mut dyn FnMut(&TerminalTransition) -> Result<()>,
    ) -> Result<Acceptance> {
        // Digest do conteúdo: comparação exata sem guardar o texto transcrito na memória.
        let content = sha256_hex(&canonical_of(payload));
        let key: StreamKey = (session.to_owned(), payload.stream_id);

        if !self.streams.contains_key(&key) {
            if self.retired.contains(&key) {
                if let Some(recent) = self.retired_recent.get(&key) {
                    if let Some((_, known, reject)) =
                        recent.iter().find(|(seq, _, _)| *seq == payload.sequence)
                    {
                        return Ok(if *known != content {
                            Acceptance::Rejected(REJECT_CONFLICT.to_owned())
                        } else if let Some(reason) = reject {
                            Acceptance::Rejected(reason.clone())
                        } else {
                            Acceptance::Duplicate
                        });
                    }
                }
                return Ok(Acceptance::Rejected(format!(
                    "{REJECT_STREAM_CLOSED} não pode ser reaberto"
                )));
            }
            if payload.sequence != 1 {
                return Ok(Acceptance::Gap {
                    expected: 1,
                    got: payload.sequence,
                });
            }
            if !self.make_room() {
                return Ok(Acceptance::Rejected(REJECT_STREAM_LIMIT.to_owned()));
            }
            self.streams.insert(
                key.clone(),
                StreamState {
                    status: StreamStatus::Active,
                    last_sequence: 0,
                    recent: VecDeque::new(),
                    segments: HashMap::new(),
                    last_activity_ms: now_ms,
                },
            );
        }
        let stream = self
            .streams
            .get_mut(&key)
            .expect("stream presente ou inserido acima");

        if let Some(known) = stream
            .recent
            .iter()
            .find(|(seq, _, _)| *seq == payload.sequence)
        {
            return Ok(if known.1 != content {
                Acceptance::Rejected(REJECT_CONFLICT.to_owned())
            } else if let Some(reason) = &known.2 {
                Acceptance::Rejected(reason.clone())
            } else {
                Acceptance::Duplicate
            });
        }
        if payload.sequence <= stream.last_sequence {
            return Ok(Acceptance::Stale {
                last_sequence: stream.last_sequence,
            });
        }
        let expected = stream.last_sequence + 1;
        if payload.sequence > expected {
            // Atividade do produtor conta para o timeout, mesmo sem aplicar o evento.
            stream.last_activity_ms = now_ms;
            return Ok(Acceptance::Gap {
                expected,
                got: payload.sequence,
            });
        }
        if stream.status.is_terminal() {
            // Já encerrado: evento novo é consumido e recusado, sem nova transição.
            let reason = format!("{REJECT_STREAM_CLOSED} ({:?})", stream.status);
            stream.last_sequence = payload.sequence;
            stream
                .recent
                .push_back((payload.sequence, content, Some(reason.clone())));
            if stream.recent.len() > DIGEST_WINDOW {
                stream.recent.pop_front();
            }
            stream.last_activity_ms = now_ms;
            return Ok(Acceptance::Rejected(reason));
        }

        // Transição terminal: persistir antes de mudar qualquer coisa em memória.
        if let Some(transition) = terminal_of(&payload.event) {
            persist(&transition)?;
        }

        // Sequência esperada: consome o número, mesmo se o conteúdo for rejeitado.
        stream.last_sequence = payload.sequence;
        stream.last_activity_ms = now_ms;
        let outcome = apply_event(stream, &payload.event).err();
        stream
            .recent
            .push_back((payload.sequence, content, outcome.clone()));
        if stream.recent.len() > DIGEST_WINDOW {
            stream.recent.pop_front();
        }
        Ok(match outcome {
            None => Acceptance::Applied,
            Some(reason) => Acceptance::Rejected(reason),
        })
    }

    /// Cancela um stream ativo. Se o stream ainda não existe, grava um aposento: ele não poderá começar.
    /// Retorna `true` só se houve mudança de estado.
    pub fn cancel(&mut self, session: &str, stream_id: &Uuid) -> bool {
        let key: StreamKey = (session.to_owned(), *stream_id);
        match self.streams.get_mut(&key) {
            Some(stream) if !stream.status.is_terminal() => {
                stream.status = StreamStatus::Cancelled;
                true
            }
            Some(_) => false,
            None => {
                self.retired.insert(key);
                false
            }
        }
    }

    /// Streams ativos sem atividade há `idle_ms`. Não altera nada: quem chama grava antes e só então
    /// chama `mark_timed_out`.
    pub fn idle_candidates(&self, now_ms: u64, idle_ms: u64) -> Vec<StreamKey> {
        self.streams
            .iter()
            .filter(|(_, s)| {
                !s.status.is_terminal() && now_ms.saturating_sub(s.last_activity_ms) >= idle_ms
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Marca um stream ativo como expirado. Chamar só depois da gravação `TIMED_OUT` ter sido confirmada.
    pub fn mark_timed_out(&mut self, key: &StreamKey) -> bool {
        match self.streams.get_mut(key) {
            Some(s) if !s.status.is_terminal() => {
                s.status = StreamStatus::TimedOut;
                true
            }
            _ => false,
        }
    }

    /// Conveniência para testes: expira já, sem persistência.
    pub fn expire_idle(&mut self, now_ms: u64, idle_ms: u64) -> Vec<StreamKey> {
        let candidates = self.idle_candidates(now_ms, idle_ms);
        candidates
            .into_iter()
            .filter(|k| self.mark_timed_out(k))
            .collect()
    }

    /// Streams ainda ativos de uma sessão. Usado no encerramento de sessão.
    pub fn active_streams(&self, session: &str) -> Vec<Uuid> {
        self.streams
            .iter()
            .filter(|(key, state)| key.0 == session && state.status == StreamStatus::Active)
            .map(|(key, _)| key.1)
            .collect()
    }

    pub fn status(&self, session: &str, stream_id: &Uuid) -> Option<&StreamStatus> {
        self.streams
            .get(&(session.to_owned(), *stream_id))
            .map(|s| &s.status)
    }

    pub fn segment(
        &self,
        session: &str,
        stream_id: &Uuid,
        segment_id: &Uuid,
    ) -> Option<SegmentSnapshot> {
        self.streams
            .get(&(session.to_owned(), *stream_id))?
            .segments
            .get(segment_id)
            .map(|s| SegmentSnapshot {
                revision: s.revision,
                state: s.state,
            })
    }

    /// Carrega streams aposentados do banco. Nenhum deles pode voltar a aceitar eventos.
    pub fn restore_retired(&mut self, keys: impl IntoIterator<Item = StreamKey>) {
        self.retired.extend(keys);
    }

    /// `true` se o stream nunca foi visto (nem ativo, nem aposentado).
    pub fn is_unknown(&self, session: &str, stream_id: &Uuid) -> bool {
        let key: StreamKey = (session.to_owned(), *stream_id);
        !self.streams.contains_key(&key) && !self.retired.contains(&key)
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// Libera espaço removendo o terminal menos recente. O id continua aposentado. `false` se todos estão ativos.
    fn make_room(&mut self) -> bool {
        if self.streams.len() < self.max_streams {
            return true;
        }
        let victim = self
            .streams
            .iter()
            .filter(|(_, s)| s.status.is_terminal())
            .min_by_key(|(key, s)| (s.last_activity_ms, key.1))
            .map(|(key, _)| key.clone());
        match victim {
            Some(key) => {
                if let Some(state) = self.streams.remove(&key) {
                    self.retired_recent.insert(key.clone(), state.recent);
                }
                self.retired.insert(key);
                true
            }
            None => false,
        }
    }
}

/// Transição terminal que um evento causa. Deve ser gravada antes de valer em memória.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalTransition {
    Ended,
    Failed(String),
}

fn terminal_of(event: &SpeechEvent) -> Option<TerminalTransition> {
    match event {
        SpeechEvent::StreamEnded => Some(TerminalTransition::Ended),
        SpeechEvent::StreamFailed { reason } => Some(TerminalTransition::Failed(reason.clone())),
        _ => None,
    }
}

fn apply_event(stream: &mut StreamState, event: &SpeechEvent) -> std::result::Result<(), String> {
    match event {
        SpeechEvent::Transcript {
            segment_id,
            revision,
            state,
            ..
        } => {
            match stream.segments.get(segment_id) {
                None if *revision != 0 => {
                    return Err(format!(
                        "segmento novo deve começar em revisão 0, veio {revision}"
                    ));
                }
                None if stream.segments.len() >= MAX_SEGMENTS_PER_STREAM => {
                    return Err("limite de segmentos do stream atingido".to_owned());
                }
                None => {}
                Some(seg) if seg.state == TranscriptState::Confirmed => {
                    return Err("segmento já confirmado não aceita revisões".to_owned());
                }
                Some(seg) => {
                    let expected = seg
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| "revisão esgotada".to_owned())?;
                    if *revision != expected {
                        return Err(format!(
                            "revisão fora de ordem: esperada {expected}, veio {revision}"
                        ));
                    }
                }
            }
            stream.segments.insert(
                *segment_id,
                SegmentState {
                    revision: *revision,
                    state: *state,
                },
            );
            Ok(())
        }
        SpeechEvent::StreamEnded => {
            stream.status = StreamStatus::Ended;
            Ok(())
        }
        SpeechEvent::StreamFailed { reason } => {
            stream.status = StreamStatus::Failed(reason.clone());
            Ok(())
        }
        SpeechEvent::SpeechStarted { .. }
        | SpeechEvent::SpeechEnded { .. }
        | SpeechEvent::NoSpeech
        | SpeechEvent::Unintelligible { .. } => Ok(()),
    }
}

/// Forma canônica do conteúdo, para comparar reenvios. Comparação exata: sem colisão de hash.
pub(crate) fn canonical_of<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}
