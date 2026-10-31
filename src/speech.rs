//! Contratos de STT streaming (Kavo Protocol 1.1, tipo `STT_RESULT`).
//!
//! - `Partial` é hipótese: pode ser revisada por nova revisão do mesmo segmento.
//! - `Confirmed` é terminal: segmento confirmado não aceita mais revisões.
//! - Ordem: `sequence` é estritamente crescente por stream, começando em 1.
//!   Lacuna (`Gap`) não é aplicada; o produtor deve reenviar a partir de `expected`.
//! - Texto nunca é comando. Esta camada só valida e rastreia; não executa nada.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KavoError, Result};
use crate::protocol::check_text;

pub const MAX_TEXT_CHARS: usize = 2_048;
pub const MAX_REASON_CHARS: usize = 128;
/// Quantidade de sequências recentes guardadas para detectar duplicatas.
const DIGEST_WINDOW: usize = 64;
/// Segmentos por stream. Acima disso, a transcrição nova é rejeitada.
pub const MAX_SEGMENTS_PER_STREAM: usize = 256;
/// Ids de streams removidos por capacidade, lembrados para impedir reabertura com `sequence` 1.
const RETIRED_LIMIT: usize = 256;

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
        let payload: Self = serde_json::from_value(value.clone())
            .map_err(|e| KavoError::Protocol(format!("payload STT inválido: {e}")))?;
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
        serde_json::from_value(value.clone())
            .map_err(|e| KavoError::Protocol(format!("payload CANCEL inválido: {e}")))
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
    recent: VecDeque<(u64, String)>,
    segments: HashMap<Uuid, SegmentState>,
    last_activity_ms: u64,
}

/// Rastreia streams de STT: ordem, duplicatas, revisões, estados terminais e timeout.
#[derive(Debug)]
pub struct StreamTracker {
    streams: HashMap<Uuid, StreamState>,
    max_streams: usize,
    retired: VecDeque<Uuid>,
}

impl StreamTracker {
    pub fn new(max_streams: usize) -> Self {
        Self {
            streams: HashMap::new(),
            max_streams: max_streams.max(1),
            retired: VecDeque::new(),
        }
    }

    /// Aplica um evento. `now_ms` é o relógio monotônico do núcleo, usado para timeout.
    pub fn apply(&mut self, payload: &SpeechPayload, now_ms: u64) -> Acceptance {
        let content = canonical_of(payload);
        let id = payload.stream_id;

        let is_new = !self.streams.contains_key(&id);
        if is_new {
            if self.retired.contains(&id) {
                return Acceptance::Rejected(
                    "stream encerrado e removido não pode ser reaberto".to_owned(),
                );
            }
            if payload.sequence != 1 {
                return Acceptance::Gap {
                    expected: 1,
                    got: payload.sequence,
                };
            }
            if !self.make_room() {
                return Acceptance::Rejected("limite de streams ativos atingido".to_owned());
            }
            self.streams.insert(
                id,
                StreamState {
                    status: StreamStatus::Active,
                    last_sequence: 0,
                    recent: VecDeque::new(),
                    segments: HashMap::new(),
                    last_activity_ms: now_ms,
                },
            );
        }
        let stream = self.streams.get_mut(&id).expect("stream inserido acima");

        if let Some((_, known)) = stream
            .recent
            .iter()
            .find(|(seq, _)| *seq == payload.sequence)
        {
            return if *known == content {
                Acceptance::Duplicate
            } else {
                Acceptance::Rejected("sequência reutilizada com conteúdo diferente".to_owned())
            };
        }
        if payload.sequence <= stream.last_sequence {
            return Acceptance::Stale {
                last_sequence: stream.last_sequence,
            };
        }
        let expected = stream.last_sequence + 1;
        if payload.sequence > expected {
            // Atividade do produtor conta para o timeout, mesmo sem aplicar o evento.
            stream.last_activity_ms = now_ms;
            return Acceptance::Gap {
                expected,
                got: payload.sequence,
            };
        }

        // Sequência esperada: consome o número, mesmo se o conteúdo for rejeitado.
        stream.last_sequence = payload.sequence;
        stream.recent.push_back((payload.sequence, content));
        if stream.recent.len() > DIGEST_WINDOW {
            stream.recent.pop_front();
        }
        stream.last_activity_ms = now_ms;

        if stream.status.is_terminal() {
            return Acceptance::Rejected(format!("stream já encerrado ({:?})", stream.status));
        }
        match apply_event(stream, &payload.event) {
            Ok(()) => Acceptance::Applied,
            Err(reason) => Acceptance::Rejected(reason),
        }
    }

    /// Cancela um stream ativo. Retorna `false` se não existir ou já estiver encerrado.
    pub fn cancel(&mut self, stream_id: &Uuid) -> bool {
        match self.streams.get_mut(stream_id) {
            Some(stream) if !stream.status.is_terminal() => {
                stream.status = StreamStatus::Cancelled;
                true
            }
            _ => false,
        }
    }

    /// Marca como expirados os streams ativos sem atividade há `idle_ms`. Retorna os ids expirados.
    pub fn expire_idle(&mut self, now_ms: u64, idle_ms: u64) -> Vec<Uuid> {
        let mut expired = Vec::new();
        for (id, stream) in &mut self.streams {
            if !stream.status.is_terminal()
                && now_ms.saturating_sub(stream.last_activity_ms) >= idle_ms
            {
                stream.status = StreamStatus::TimedOut;
                expired.push(*id);
            }
        }
        expired
    }

    pub fn status(&self, stream_id: &Uuid) -> Option<&StreamStatus> {
        self.streams.get(stream_id).map(|s| &s.status)
    }

    pub fn segment(&self, stream_id: &Uuid, segment_id: &Uuid) -> Option<SegmentSnapshot> {
        self.streams
            .get(stream_id)?
            .segments
            .get(segment_id)
            .map(|s| SegmentSnapshot {
                revision: s.revision,
                state: s.state,
            })
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// Libera espaço removendo um stream terminal. Retorna `false` se todos estiverem ativos.
    fn make_room(&mut self) -> bool {
        if self.streams.len() < self.max_streams {
            return true;
        }
        // Remove o terminal menos recente; o id vai para a lista de aposentados.
        let victim = self
            .streams
            .iter()
            .filter(|(_, s)| s.status.is_terminal())
            .min_by_key(|(id, s)| (s.last_activity_ms, **id))
            .map(|(id, _)| *id);
        match victim {
            Some(id) => {
                self.streams.remove(&id);
                self.retired.push_back(id);
                if self.retired.len() > RETIRED_LIMIT {
                    self.retired.pop_front();
                }
                true
            }
            None => false,
        }
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
