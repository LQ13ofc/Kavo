//! Contratos do Kavo Intent Engine (Kavo Protocol 1.1, tipo `INTENT_DETECTED`).
//!
//! O Intent Engine identifica o que o usuário provavelmente quer. Não executa, não autoriza.
//! `ReadinessGate` decide apenas se uma interpretação pode ser oferecida ao Planner/Policy,
//! e impede admissões repetidas. A autorização continua no Policy Engine.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KavoError, Result};
use crate::protocol::check_text;
use crate::speech::{Acceptance, SegmentSnapshot, TranscriptState, canonical_of, check_confidence};

const MAX_LIST: usize = 32;
const MAX_OPTIONS: usize = 8;
const MAX_CONTEXT_REFS: usize = 16;
const MAX_NAME_CHARS: usize = 64;
const MAX_VALUE_CHARS: usize = 2_048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum InterpretationState {
    /// Hipótese que pode mudar com novas palavras. Só alimenta feedback.
    Provisional,
    /// Interpretação completa para o texto de origem. Ainda não é autorização.
    Stable,
    NeedsClarification,
    NoIntent,
    Cancelled,
}

/// Valor de parâmetro tipado. `untagged`: a ordem das variantes importa na desserialização.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ParamValue {
    Boolean(bool),
    Integer(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntentCandidate {
    /// Nome estável de capacidade, ex.: `OPEN_APPLICATION`. Não é ação executável.
    pub name: String,
    /// Presente somente se o engine fornecer. Nunca preenchido pelo núcleo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub kind: String,
    pub text: String,
    /// Posições em caracteres no texto de origem.
    pub start: u32,
    pub end: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ambiguity {
    pub kind: String,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineRef {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterpretationPayload {
    pub interpretation_id: Uuid,
    pub stream_id: Uuid,
    /// Segmento de transcrição de origem.
    pub segment_id: Uuid,
    /// Revisão do segmento usada na interpretação.
    pub source_revision: u32,
    /// Revisão desta interpretação. Começa em 0 e aumenta de 1 em 1.
    pub revision: u32,
    pub state: InterpretationState,
    pub engine: EngineRef,
    #[serde(default)]
    pub intent: Option<IntentCandidate>,
    #[serde(default)]
    pub entities: Vec<Entity>,
    #[serde(default)]
    pub parameters: BTreeMap<String, ParamValue>,
    /// Parâmetros obrigatórios ainda ausentes.
    #[serde(default)]
    pub missing: Vec<String>,
    #[serde(default)]
    pub ambiguities: Vec<Ambiguity>,
    /// Referências opacas a contexto relevante da sessão.
    #[serde(default)]
    pub context_refs: Vec<String>,
}

impl InterpretationPayload {
    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        let payload: Self = serde_json::from_value(value.clone())
            .map_err(|e| KavoError::Protocol(format!("payload INTENT inválido: {e}")))?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(intent) = &self.intent {
            if !is_capability_name(&intent.name) {
                return Err(KavoError::Protocol(
                    "intent.name inválido (use MAIÚSCULAS, dígitos e _, 3 a 64 caracteres)"
                        .to_owned(),
                ));
            }
            check_confidence(intent.confidence)?;
        }
        check_text("engine.name", &self.engine.name, MAX_NAME_CHARS)?;
        check_text("engine.version", &self.engine.version, MAX_NAME_CHARS)?;
        check_list("entities", self.entities.len(), MAX_LIST)?;
        for e in &self.entities {
            check_text("entity.kind", &e.kind, MAX_NAME_CHARS)?;
            check_text("entity.text", &e.text, MAX_VALUE_CHARS)?;
            if e.start > e.end {
                return Err(KavoError::Protocol(
                    "entity.start maior que entity.end".to_owned(),
                ));
            }
        }
        check_list("parameters", self.parameters.len(), MAX_LIST)?;
        for (key, value) in &self.parameters {
            check_param_key(key)?;
            if let ParamValue::Text(text) = value {
                check_text("parameter", text, MAX_VALUE_CHARS)?;
            }
        }
        check_list("missing", self.missing.len(), MAX_LIST)?;
        for key in &self.missing {
            check_param_key(key)?;
        }
        check_list("ambiguities", self.ambiguities.len(), MAX_LIST)?;
        for a in &self.ambiguities {
            check_text("ambiguity.kind", &a.kind, MAX_NAME_CHARS)?;
            if a.options.len() < 2 || a.options.len() > MAX_OPTIONS {
                return Err(KavoError::Protocol(
                    "ambiguidade exige de 2 a 8 opções".to_owned(),
                ));
            }
            for option in &a.options {
                check_text("ambiguity.option", option, MAX_NAME_CHARS)?;
            }
        }
        check_list("context_refs", self.context_refs.len(), MAX_CONTEXT_REFS)?;
        for r in &self.context_refs {
            check_text("context_ref", r, 128)?;
        }
        self.validate_state()
    }

    /// Consistência entre estado e conteúdo. Ex.: `Stable` exige intenção sem pendências.
    fn validate_state(&self) -> Result<()> {
        let bad = |msg: &str| Err(KavoError::Protocol(format!("{:?}: {msg}", self.state)));
        match self.state {
            InterpretationState::Stable => {
                if self.intent.is_none() {
                    return bad("exige intenção");
                }
                if !self.missing.is_empty() || !self.ambiguities.is_empty() {
                    return bad("não pode ter pendências (missing/ambiguities)");
                }
            }
            InterpretationState::NeedsClarification => {
                if self.missing.is_empty() && self.ambiguities.is_empty() {
                    return bad("exige missing ou ambiguities");
                }
            }
            InterpretationState::NoIntent | InterpretationState::Cancelled => {
                if self.intent.is_some() {
                    return bad("não pode ter intenção");
                }
            }
            InterpretationState::Provisional => {}
        }
        Ok(())
    }
}

/// Nome de capacidade: letra maiúscula inicial, depois maiúsculas, dígitos ou `_`. 3 a 64 caracteres.
pub fn is_capability_name(name: &str) -> bool {
    let len = name.chars().count();
    (3..=MAX_NAME_CHARS).contains(&len)
        && name.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

fn check_param_key(key: &str) -> Result<()> {
    let ok = !key.is_empty()
        && key.chars().count() <= MAX_NAME_CHARS
        && key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && key.chars().next().is_some_and(|c| c.is_ascii_lowercase());
    if ok {
        Ok(())
    } else {
        Err(KavoError::Protocol(
            "nome de parâmetro inválido (snake_case, até 64 caracteres)".to_owned(),
        ))
    }
}

fn check_list(field: &str, len: usize, max: usize) -> Result<()> {
    if len > max {
        return Err(KavoError::Protocol(format!("{field} excede {max} itens")));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct InterpretationRecord {
    stream_id: Uuid,
    segment_id: Uuid,
    revision: u32,
    source_revision: u32,
    /// Conteúdo canônico da última revisão, para distinguir reenvio idêntico de conflito.
    content: String,
    cancelled: bool,
}

/// Rastreia revisões de interpretações: ordem, duplicatas, origem fixa, regressão e cancelamento.
#[derive(Debug, Default)]
pub struct InterpretationTracker {
    records: HashMap<Uuid, InterpretationRecord>,
}

impl InterpretationTracker {
    pub fn apply(&mut self, payload: &InterpretationPayload) -> Acceptance {
        let content = canonical_of(payload);
        let Some(last) = self.records.get(&payload.interpretation_id).cloned() else {
            if payload.revision != 0 {
                return Acceptance::Gap {
                    expected: 0,
                    got: u64::from(payload.revision),
                };
            }
            self.records
                .insert(payload.interpretation_id, record(payload, content));
            return Acceptance::Applied;
        };

        if payload.stream_id != last.stream_id || payload.segment_id != last.segment_id {
            return Acceptance::Rejected(
                "interpretação não pode trocar stream ou segmento".to_owned(),
            );
        }
        if payload.revision == last.revision {
            return if last.content == content {
                Acceptance::Duplicate
            } else {
                Acceptance::Rejected("revisão reutilizada com conteúdo diferente".to_owned())
            };
        }
        if payload.revision < last.revision {
            return Acceptance::Stale {
                last_sequence: u64::from(last.revision),
            };
        }
        let Some(expected) = last.revision.checked_add(1) else {
            return Acceptance::Rejected("revisão esgotada".to_owned());
        };
        if payload.revision > expected {
            return Acceptance::Gap {
                expected: u64::from(expected),
                got: u64::from(payload.revision),
            };
        }
        if last.cancelled {
            return Acceptance::Rejected("interpretação cancelada não aceita revisões".to_owned());
        }
        if payload.source_revision < last.source_revision {
            return Acceptance::Rejected("source_revision regrediu".to_owned());
        }
        self.records
            .insert(payload.interpretation_id, record(payload, content));
        Acceptance::Applied
    }

    /// Marca como canceladas todas as interpretações de um stream.
    pub fn cancel_stream(&mut self, stream_id: &Uuid) -> usize {
        let mut changed = 0;
        for rec in self.records.values_mut() {
            if rec.stream_id == *stream_id && !rec.cancelled {
                rec.cancelled = true;
                changed += 1;
            }
        }
        changed
    }

    /// Esquece as interpretações de um stream que já terminou.
    pub fn forget_stream(&mut self, stream_id: &Uuid) {
        self.records.retain(|_, rec| rec.stream_id != *stream_id);
    }
}

fn record(payload: &InterpretationPayload, content: String) -> InterpretationRecord {
    InterpretationRecord {
        stream_id: payload.stream_id,
        segment_id: payload.segment_id,
        revision: payload.revision,
        source_revision: payload.source_revision,
        content,
        cancelled: payload.state == InterpretationState::Cancelled,
    }
}

/// Risco da capacidade, informado por quem conhece o catálogo (hoje: ninguém; contrato apenas).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskClass {
    Read,
    Reversible,
    Sensitive,
    Irreversible,
}

/// Estado do texto de origem no momento da avaliação.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSnapshot {
    pub stream_active: bool,
    /// Revisão atual conhecida do segmento; `None` se o segmento não existe.
    pub segment: Option<SegmentSnapshot>,
}

/// Chave de execução: segmento + intenção + parâmetros canônicos.
/// Mesma chave não é admitida duas vezes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionKey(String);

impl ExecutionKey {
    fn new(payload: &InterpretationPayload, intent: &IntentCandidate) -> Self {
        let params = serde_json::to_string(&payload.parameters).unwrap_or_default();
        Self(format!("{}|{}|{}", payload.segment_id, intent.name, params))
    }

    /// Impressão digital para logs: não expõe os parâmetros (podem conter dados do usuário).
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

impl Decision {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Admit(_) => "ADMIT",
            Self::Duplicate(_) => "DUPLICATE",
            Self::Supersede { .. } => "SUPERSEDE",
            Self::NotReady(_) => "NOT_READY",
        }
    }

    pub fn fingerprint(&self) -> Option<u64> {
        match self {
            Self::Admit(k) | Self::Duplicate(k) => Some(k.fingerprint()),
            Self::Supersede { next, .. } => Some(next.fingerprint()),
            Self::NotReady(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotReadyReason {
    Provisional,
    NeedsClarification,
    NoIntent,
    Cancelled,
    StreamInactive,
    SourceUnconfirmed,
    SourceRevised { current: u32 },
    MissingParameters,
    BelowMinConfidence,
    ConfirmationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Pode ser oferecida ao Planner/Policy. Não é autorização.
    Admit(ExecutionKey),
    /// Mesma chave já admitida: nada a fazer.
    Duplicate(ExecutionKey),
    /// Nova chave substitui a admitida para o mesmo segmento. Quem executou a anterior deve cancelá-la.
    Supersede {
        previous: ExecutionKey,
        next: ExecutionKey,
    },
    NotReady(NotReadyReason),
}

/// Decide quando uma interpretação está estável o bastante para seguir. Mantém o registro de admissões.
#[derive(Debug)]
pub struct ReadinessGate {
    /// `None` = sem limite. Não há valor padrão: o limite deve vir de medição (ver docs).
    min_confidence: Option<f32>,
    /// Segmento → (stream dono, chave admitida).
    admitted: HashMap<Uuid, (Uuid, ExecutionKey)>,
}

impl ReadinessGate {
    pub fn new(min_confidence: Option<f32>) -> Self {
        Self {
            min_confidence,
            admitted: HashMap::new(),
        }
    }

    pub fn evaluate(
        &mut self,
        payload: &InterpretationPayload,
        source: SourceSnapshot,
        risk: RiskClass,
        policy_confirmation: bool,
    ) -> Decision {
        let not_ready = Decision::NotReady;
        match payload.state {
            InterpretationState::Provisional => return not_ready(NotReadyReason::Provisional),
            InterpretationState::NeedsClarification => {
                return not_ready(NotReadyReason::NeedsClarification);
            }
            InterpretationState::NoIntent => return not_ready(NotReadyReason::NoIntent),
            InterpretationState::Cancelled => return not_ready(NotReadyReason::Cancelled),
            InterpretationState::Stable => {}
        }
        let Some(intent) = payload.intent.as_ref() else {
            return not_ready(NotReadyReason::NoIntent);
        };
        if !payload.missing.is_empty() {
            return not_ready(NotReadyReason::MissingParameters);
        }
        if !source.stream_active {
            return not_ready(NotReadyReason::StreamInactive);
        }
        // Só texto confirmado pode avançar. Parcial nunca vira execução.
        match source.segment {
            Some(seg) if seg.state == TranscriptState::Confirmed => {
                if seg.revision != payload.source_revision {
                    return not_ready(NotReadyReason::SourceRevised {
                        current: seg.revision,
                    });
                }
            }
            _ => return not_ready(NotReadyReason::SourceUnconfirmed),
        }
        if let Some(min) = self.min_confidence {
            match intent.confidence {
                Some(c) if c >= min => {}
                _ => return not_ready(NotReadyReason::BelowMinConfidence),
            }
        }
        if matches!(risk, RiskClass::Sensitive | RiskClass::Irreversible) && !policy_confirmation {
            return not_ready(NotReadyReason::ConfirmationRequired);
        }

        let key = ExecutionKey::new(payload, intent);
        let owner = payload.stream_id;
        match self
            .admitted
            .get(&payload.segment_id)
            .map(|(_, k)| k.clone())
        {
            Some(previous) if previous == key => Decision::Duplicate(key),
            Some(previous) => {
                self.admitted
                    .insert(payload.segment_id, (owner, key.clone()));
                Decision::Supersede {
                    previous,
                    next: key,
                }
            }
            None => {
                self.admitted
                    .insert(payload.segment_id, (owner, key.clone()));
                Decision::Admit(key)
            }
        }
    }

    /// Esquece admissões de um stream que terminou. Quem admitiu deve ter cancelado antes, se necessário.
    pub fn forget_stream(&mut self, stream_id: &Uuid) {
        self.admitted.retain(|_, (owner, _)| owner != stream_id);
    }
}
