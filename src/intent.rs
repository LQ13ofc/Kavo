//! Contratos do Kavo Intent Engine (Kavo Protocol 1.1, tipo `INTENT_DETECTED`).
//!
//! O Intent Engine identifica o que o usuário provavelmente quer. Não executa, não autoriza.
//! `ReadinessGate` decide apenas se uma interpretação pode ser oferecida ao Planner/Policy,
//! e impede admissões repetidas. A autorização continua no Policy Engine.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{KavoError, Result};
use crate::protocol::check_text;
use crate::speech::{Acceptance, SegmentSnapshot, TranscriptState, canonical_of, check_confidence};
use crate::store::{InterpretationRow, sha256_hex};

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
        let payload: Self = serde_json::from_value(value.clone()).map_err(|e| {
            KavoError::Protocol(format!(
                "payload INTENT inválido ({})",
                crate::protocol::describe_serde(&e)
            ))
        })?;
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
pub struct InterpretationRecord {
    stream_id: Uuid,
    segment_id: Uuid,
    revision: u32,
    source_revision: u32,
    /// Digest SHA-256 do conteúdo da última revisão: distingue reenvio idêntico de conflito.
    content_digest: String,
    cancelled: bool,
}

/// Chave de interpretação: sessão + `interpretation_id`.
pub type InterpretationKey = (String, Uuid);

/// Rastreia revisões de interpretações: ordem, duplicatas, origem fixa, regressão e cancelamento.
#[derive(Debug, Default)]
pub struct InterpretationTracker {
    records: HashMap<InterpretationKey, InterpretationRecord>,
}

/// Textos estáveis de recusa de interpretação, classificados no ACK de admissão.
pub const REJECT_STREAM_CLOSED: &str = "interpretação cancelada";
pub const REJECT_STALE: &str = "source_revision regrediu";
pub const REJECT_CONFLICT: &str = "revisão reutilizada com conteúdo diferente";

impl InterpretationTracker {
    /// Carrega registros duráveis após reinício.
    pub fn restore(&mut self, rows: impl IntoIterator<Item = InterpretationRow>) {
        for row in rows {
            self.records.insert(
                (row.session_id.clone(), row.interpretation_id),
                InterpretationRecord {
                    stream_id: row.stream_id,
                    segment_id: row.segment_id,
                    revision: row.revision,
                    source_revision: row.source_revision,
                    content_digest: row.content_digest,
                    cancelled: row.cancelled,
                },
            );
        }
    }

    /// Linha durável correspondente a uma revisão aceita.
    pub fn row_for(session: &str, payload: &InterpretationPayload) -> InterpretationRow {
        InterpretationRow {
            session_id: session.to_owned(),
            interpretation_id: payload.interpretation_id,
            stream_id: payload.stream_id,
            segment_id: payload.segment_id,
            revision: payload.revision,
            source_revision: payload.source_revision,
            content_digest: sha256_hex(&canonical_of(payload)),
            cancelled: payload.state == InterpretationState::Cancelled,
        }
    }

    pub fn apply(&mut self, session: &str, payload: &InterpretationPayload) -> Acceptance {
        let digest = sha256_hex(&canonical_of(payload));
        let key: InterpretationKey = (session.to_owned(), payload.interpretation_id);
        let Some(last) = self.records.get(&key).cloned() else {
            if payload.revision != 0 {
                return Acceptance::Gap {
                    expected: 0,
                    got: u64::from(payload.revision),
                };
            }
            self.records.insert(key, record(payload, digest));
            return Acceptance::Applied;
        };

        if payload.stream_id != last.stream_id || payload.segment_id != last.segment_id {
            return Acceptance::Rejected(
                "interpretação não pode trocar stream ou segmento".to_owned(),
            );
        }
        if payload.revision == last.revision {
            return if last.content_digest == digest {
                Acceptance::Duplicate
            } else {
                Acceptance::Rejected(REJECT_CONFLICT.to_owned())
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
            return Acceptance::Rejected(format!("{REJECT_STREAM_CLOSED} não aceita revisões"));
        }
        if payload.source_revision < last.source_revision {
            return Acceptance::Rejected(REJECT_STALE.to_owned());
        }
        self.records.insert(key, record(payload, digest));
        Acceptance::Applied
    }

    /// Registro atual de uma interpretação, para desfazer a memória se a gravação falhar.
    pub fn snapshot(&self, session: &str, interpretation: &Uuid) -> Option<InterpretationRecord> {
        self.records
            .get(&(session.to_owned(), *interpretation))
            .cloned()
    }

    /// Desfaz `apply` quando a gravação durável não ocorreu: a memória volta ao estado anterior.
    pub fn rollback(
        &mut self,
        session: &str,
        interpretation: &Uuid,
        previous: Option<InterpretationRecord>,
    ) {
        let key = (session.to_owned(), *interpretation);
        match previous {
            Some(record) => {
                self.records.insert(key, record);
            }
            None => {
                self.records.remove(&key);
            }
        }
    }

    /// Marca como canceladas as interpretações de um stream. Retorna quantas mudaram.
    pub fn cancel_stream(&mut self, session: &str, stream_id: &Uuid) -> usize {
        let mut changed = 0;
        for (key, rec) in self.records.iter_mut() {
            if key.0 == session && rec.stream_id == *stream_id && !rec.cancelled {
                rec.cancelled = true;
                changed += 1;
            }
        }
        changed
    }

    /// Libera registros de memória de um stream que terminou. Os duráveis permanecem no banco.
    /// Revisões antigas depois disso caem em `Gap` (o estado de origem não é mais conhecido).
    pub fn forget_stream(&mut self, session: &str, stream_id: &Uuid) {
        self.records
            .retain(|key, rec| !(key.0 == session && rec.stream_id == *stream_id));
    }
}

fn record(payload: &InterpretationPayload, content_digest: String) -> InterpretationRecord {
    InterpretationRecord {
        stream_id: payload.stream_id,
        segment_id: payload.segment_id,
        revision: payload.revision,
        source_revision: payload.source_revision,
        content_digest,
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

/// Chave de execução: digest de segmento + intenção + parâmetros canônicos.
/// Mesma chave não é admitida duas vezes na mesma sessão. Só o digest é gravado.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionKey(String);

impl ExecutionKey {
    fn new(payload: &InterpretationPayload, intent: &IntentCandidate) -> Self {
        let params = serde_json::to_string(&payload.parameters).unwrap_or_default();
        Self(sha256_hex(&format!(
            "{}|{}|{}",
            payload.segment_id, intent.name, params
        )))
    }

    /// Chave lida do banco.
    pub fn from_stored(digest: String) -> Self {
        Self(digest)
    }

    /// Digest persistido.
    pub fn as_stored(&self) -> &str {
        &self.0
    }

    /// Impressão curta para logs. Não expõe parâmetros.
    pub fn fingerprint(&self) -> String {
        self.0.chars().take(12).collect()
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

    pub fn fingerprint(&self) -> Option<String> {
        match self {
            Self::Admit(k) | Self::Duplicate(k) => Some(k.fingerprint()),
            Self::Supersede { next, .. } => Some(next.fingerprint()),
            Self::NotReady(_) => None,
        }
    }

    /// Chave que o monitor precisa gravar antes de considerar a admissão feita.
    pub fn admitted_key(&self) -> Option<&ExecutionKey> {
        match self {
            Self::Admit(k) | Self::Supersede { next: k, .. } => Some(k),
            _ => None,
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
    SourceRevised {
        current: u32,
    },
    MissingParameters,
    BelowMinConfidence,
    ConfirmationRequired,
    /// Chave já admitida antes nesta sessão, mas não é a atual do segmento.
    AlreadyAdmitted,
    /// Gravação durável indisponível: nada foi admitido.
    StoreUnavailable,
    /// Revisão antiga ou conflitante com a gravada: nada foi alterado.
    StaleRevision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Pode ser oferecida ao Planner/Policy, após gravação durável. Não é autorização.
    Admit(ExecutionKey),
    /// Mesma chave já admitida: nada a fazer.
    Duplicate(ExecutionKey),
    /// Nova chave substitui a admitida para o mesmo segmento. A anterior permanece consumida.
    Supersede {
        previous: ExecutionKey,
        next: ExecutionKey,
    },
    NotReady(NotReadyReason),
}

/// Decide quando uma interpretação está pronta para seguir. `evaluate` não altera estado:
/// só `record`, depois da gravação durável, muda a memória.
#[derive(Debug)]
pub struct ReadinessGate {
    /// `None` = sem limite. Não há valor padrão: o limite deve vir de medição (ver docs).
    min_confidence: Option<f32>,
    /// (sessão, segmento) → chave atual.
    current: HashMap<(String, Uuid), ExecutionKey>,
    /// (sessão, chave) já admitida alguma vez. Nunca é removida.
    consumed: HashSet<(String, ExecutionKey)>,
}

impl ReadinessGate {
    pub fn new(min_confidence: Option<f32>) -> Self {
        Self {
            min_confidence,
            current: HashMap::new(),
            consumed: HashSet::new(),
        }
    }

    /// Restaura o estado durável após reinício.
    pub fn restore(
        &mut self,
        consumed: impl IntoIterator<Item = (String, String)>,
        current: impl IntoIterator<Item = (String, Uuid, String)>,
    ) {
        for (session, key) in consumed {
            self.consumed
                .insert((session, ExecutionKey::from_stored(key)));
        }
        for (session, segment, key) in current {
            self.current
                .insert((session, segment), ExecutionKey::from_stored(key));
        }
    }

    pub fn evaluate(
        &self,
        session: &str,
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
        let slot = (session.to_owned(), payload.segment_id);
        let current = self.current.get(&slot);
        if current == Some(&key) {
            return Decision::Duplicate(key);
        }
        if self.consumed.contains(&(session.to_owned(), key.clone())) {
            return not_ready(NotReadyReason::AlreadyAdmitted);
        }
        match current {
            Some(previous) => Decision::Supersede {
                previous: previous.clone(),
                next: key,
            },
            None => Decision::Admit(key),
        }
    }

    /// Aplica uma decisão já gravada no banco. Não chamar antes da gravação.
    pub fn record(&mut self, session: &str, segment_id: Uuid, decision: &Decision) {
        if let Some(key) = decision.admitted_key() {
            self.consumed.insert((session.to_owned(), key.clone()));
            self.current
                .insert((session.to_owned(), segment_id), key.clone());
        }
    }
}
