//! Testes dos contratos de STT streaming e do Intent Engine, do monitor e do Kavo Protocol 1.1.
//! Unitários para as regras; integração com barramento e socket real para o protocolo.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use kavo::bus::EventBus;
use kavo::config::SpeechConfig;
use kavo::error::ErrorKind;
use kavo::intent::{
    Ambiguity, Decision, EngineRef, Entity, IntentCandidate, InterpretationPayload,
    InterpretationState, InterpretationTracker, NotReadyReason, ParamValue, ReadinessGate,
    RiskClass, SourceSnapshot, is_capability_name,
};
use kavo::ipc::{self, IpcServer, IpcSettings};
use kavo::monitor::{ContractMonitor, MonitorStats};
use kavo::protocol::{Delivery, KavoMessage, MessageType};
use kavo::runtime::{Component, ComponentContext, Supervisor};
use kavo::speech::{
    Acceptance, MAX_SEGMENTS_PER_STREAM, SegmentSnapshot, SpeechEvent, SpeechPayload, StreamStatus,
    StreamTracker, TranscriptState,
};
use serde_json::json;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// ---------- fixtures ----------

fn speech(stream: Uuid, seq: u64, event: SpeechEvent) -> SpeechPayload {
    SpeechPayload {
        stream_id: stream,
        sequence: seq,
        audio_start_ms: 0,
        audio_end_ms: 100,
        event,
    }
}

fn transcript(seg: Uuid, rev: u32, state: TranscriptState, text: &str) -> SpeechEvent {
    SpeechEvent::Transcript {
        segment_id: seg,
        revision: rev,
        state,
        text: text.to_owned(),
        confidence: None,
    }
}

fn interp(
    seg: Uuid,
    stream: Uuid,
    state: InterpretationState,
    rev: u32,
    intent: Option<&str>,
) -> InterpretationPayload {
    InterpretationPayload {
        interpretation_id: Uuid::from_u128(0xabc),
        stream_id: stream,
        segment_id: seg,
        source_revision: 0,
        revision: rev,
        state,
        engine: EngineRef {
            name: "fixture".into(),
            version: "0".into(),
        },
        intent: intent.map(|n| IntentCandidate {
            name: n.into(),
            confidence: None,
        }),
        entities: vec![],
        parameters: Default::default(),
        missing: vec![],
        ambiguities: vec![],
        context_refs: vec![],
    }
}

fn confirmed_source(seg_rev: u32) -> SourceSnapshot {
    SourceSnapshot {
        stream_active: true,
        segment: Some(SegmentSnapshot {
            revision: seg_rev,
            state: TranscriptState::Confirmed,
        }),
    }
}

fn partial_source(seg_rev: u32) -> SourceSnapshot {
    SourceSnapshot {
        stream_active: true,
        segment: Some(SegmentSnapshot {
            revision: seg_rev,
            state: TranscriptState::Partial,
        }),
    }
}

// ---------- STT: estados, revisões, ordem ----------

#[test]
fn partial_then_confirmed_is_applied_in_order() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(
        t.apply(
            &speech(s, 1, transcript(seg, 0, TranscriptState::Partial, "abre")),
            0
        ),
        Acceptance::Applied
    );
    assert_eq!(
        t.apply(
            &speech(s, 2, transcript(seg, 1, TranscriptState::Partial, "abre o")),
            1
        ),
        Acceptance::Applied
    );
    assert_eq!(
        t.apply(
            &speech(
                s,
                3,
                transcript(seg, 2, TranscriptState::Confirmed, "abre o brave")
            ),
            2
        ),
        Acceptance::Applied
    );
    assert_eq!(
        t.segment(&s, &seg),
        Some(SegmentSnapshot {
            revision: 2,
            state: TranscriptState::Confirmed
        })
    );
}

#[test]
fn confirmed_segment_rejects_further_revisions() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    t.apply(
        &speech(s, 1, transcript(seg, 0, TranscriptState::Confirmed, "fim")),
        0,
    );
    let out = t.apply(
        &speech(s, 2, transcript(seg, 1, TranscriptState::Partial, "fim!")),
        1,
    );
    assert!(
        matches!(out, Acceptance::Rejected(ref m) if m.contains("confirmado")),
        "{out:?}"
    );
}

#[test]
fn revision_skipping_is_rejected_and_sequence_is_consumed() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    t.apply(
        &speech(s, 1, transcript(seg, 0, TranscriptState::Partial, "a")),
        0,
    );
    let out = t.apply(
        &speech(s, 2, transcript(seg, 2, TranscriptState::Partial, "b")),
        0,
    );
    assert!(
        matches!(out, Acceptance::Rejected(ref m) if m.contains("fora de ordem")),
        "{out:?}"
    );
    // Sequência 2 foi consumida: o próximo evento válido é 3.
    assert_eq!(
        t.apply(
            &speech(s, 3, transcript(seg, 1, TranscriptState::Partial, "a b")),
            0
        ),
        Acceptance::Applied
    );
}

#[test]
fn new_segment_must_start_at_revision_zero() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    let out = t.apply(
        &speech(s, 1, transcript(seg, 3, TranscriptState::Partial, "x")),
        0,
    );
    assert!(matches!(out, Acceptance::Rejected(_)), "{out:?}");
}

#[test]
fn out_of_order_sequences_are_gap_then_filled() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(
        t.apply(
            &speech(s, 1, SpeechEvent::SpeechStarted { segment_id: seg }),
            0
        ),
        Acceptance::Applied
    );
    // 3 chega antes de 2: lacuna, não aplicado.
    assert_eq!(
        t.apply(
            &speech(s, 3, transcript(seg, 0, TranscriptState::Partial, "b")),
            0
        ),
        Acceptance::Gap {
            expected: 2,
            got: 3
        }
    );
    assert_eq!(
        t.apply(&speech(s, 2, SpeechEvent::NoSpeech), 0),
        Acceptance::Applied
    );
    assert_eq!(
        t.apply(
            &speech(s, 3, transcript(seg, 0, TranscriptState::Partial, "b")),
            0
        ),
        Acceptance::Applied
    );
}

#[test]
fn stream_must_start_at_sequence_one() {
    let mut t = StreamTracker::new(4);
    let out = t.apply(&speech(Uuid::new_v4(), 2, SpeechEvent::NoSpeech), 0);
    assert_eq!(
        out,
        Acceptance::Gap {
            expected: 1,
            got: 2
        }
    );
}

#[test]
fn duplicate_same_content_is_ignored_and_conflicting_reuse_is_rejected() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    let first = speech(s, 1, transcript(seg, 0, TranscriptState::Partial, "liga"));
    assert_eq!(t.apply(&first, 0), Acceptance::Applied);
    assert_eq!(t.apply(&first, 1), Acceptance::Duplicate);
    let conflicting = speech(
        s,
        1,
        transcript(seg, 0, TranscriptState::Partial, "desliga"),
    );
    assert!(matches!(t.apply(&conflicting, 2), Acceptance::Rejected(_)));
}

#[test]
fn old_sequence_outside_window_is_stale() {
    let mut t = StreamTracker::new(4);
    let s = Uuid::new_v4();
    for seq in 1..=3 {
        t.apply(&speech(s, seq, SpeechEvent::NoSpeech), 0);
    }
    assert_eq!(
        t.apply(&speech(s, 1, SpeechEvent::NoSpeech), 0),
        Acceptance::Duplicate,
        "janela recente"
    );
    assert_eq!(
        t.apply(&speech(s, 2, SpeechEvent::NoSpeech), 0),
        Acceptance::Duplicate
    );
}

#[test]
fn ended_stream_rejects_new_events_but_repeats_end_as_duplicate() {
    let mut t = StreamTracker::new(4);
    let s = Uuid::new_v4();
    t.apply(&speech(s, 1, SpeechEvent::StreamEnded), 0);
    assert_eq!(t.status(&s), Some(&StreamStatus::Ended));
    let out = t.apply(&speech(s, 2, SpeechEvent::NoSpeech), 0);
    assert!(
        matches!(out, Acceptance::Rejected(ref m) if m.contains("encerrado")),
        "{out:?}"
    );
    assert_eq!(
        t.apply(&speech(s, 1, SpeechEvent::StreamEnded), 0),
        Acceptance::Duplicate
    );
}

#[test]
fn cancel_stops_stream_and_is_idempotent() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    t.apply(
        &speech(s, 1, transcript(seg, 0, TranscriptState::Partial, "abr")),
        0,
    );
    assert!(t.cancel(&s));
    assert!(!t.cancel(&s), "já cancelado");
    assert_eq!(t.status(&s), Some(&StreamStatus::Cancelled));
    let out = t.apply(
        &speech(s, 2, transcript(seg, 1, TranscriptState::Partial, "abre")),
        1,
    );
    assert!(matches!(out, Acceptance::Rejected(_)), "{out:?}");
}

#[test]
fn idle_stream_times_out_and_active_one_does_not() {
    let mut t = StreamTracker::new(4);
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    t.apply(&speech(a, 1, SpeechEvent::NoSpeech), 0);
    t.apply(&speech(b, 1, SpeechEvent::NoSpeech), 900);
    let expired = t.expire_idle(1_000, 500);
    assert_eq!(expired, vec![a]);
    assert_eq!(t.status(&a), Some(&StreamStatus::TimedOut));
    assert_eq!(t.status(&b), Some(&StreamStatus::Active));
}

#[test]
fn stream_capacity_evicts_terminal_only() {
    let mut t = StreamTracker::new(2);
    let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    t.apply(&speech(a, 1, SpeechEvent::NoSpeech), 0);
    t.apply(&speech(b, 1, SpeechEvent::NoSpeech), 0);
    let out = t.apply(&speech(c, 1, SpeechEvent::NoSpeech), 0);
    assert!(
        matches!(out, Acceptance::Rejected(ref m) if m.contains("limite")),
        "{out:?}"
    );
    t.cancel(&a);
    assert_eq!(
        t.apply(&speech(c, 1, SpeechEvent::NoSpeech), 0),
        Acceptance::Applied
    );
    assert_eq!(t.len(), 2);
}

#[test]
fn no_speech_and_unintelligible_never_carry_text() {
    let mut t = StreamTracker::new(4);
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(
        t.apply(&speech(s, 1, SpeechEvent::NoSpeech), 0),
        Acceptance::Applied
    );
    assert_eq!(
        t.apply(
            &speech(s, 2, SpeechEvent::Unintelligible { segment_id: seg }),
            0
        ),
        Acceptance::Applied
    );
    assert!(t.segment(&s, &seg).is_none(), "não cria segmento");
}

#[test]
fn stream_failed_is_terminal() {
    let mut t = StreamTracker::new(4);
    let s = Uuid::new_v4();
    t.apply(
        &speech(
            s,
            1,
            SpeechEvent::StreamFailed {
                reason: "ENGINE_UNAVAILABLE".into(),
            },
        ),
        0,
    );
    assert!(matches!(t.status(&s), Some(StreamStatus::Failed(r)) if r == "ENGINE_UNAVAILABLE"));
}

// ---------- STT: validação de payload ----------

#[test]
fn speech_payload_validation_rules() {
    let s = Uuid::new_v4();
    let seg = Uuid::new_v4();
    let base = json!({"stream_id": s, "sequence": 1, "audio_start_ms": 0, "audio_end_ms": 10,
                      "event": "TRANSCRIPT", "segment_id": seg, "revision": 0, "state": "PARTIAL", "text": "oi"});
    assert!(SpeechPayload::from_value(&base).is_ok());

    let mut zero_seq = base.clone();
    zero_seq["sequence"] = json!(0);
    assert!(SpeechPayload::from_value(&zero_seq).is_err());

    let mut empty = base.clone();
    empty["text"] = json!("   ");
    assert!(
        SpeechPayload::from_value(&empty).is_err(),
        "texto vazio deve usar NO_SPEECH"
    );

    let mut bad_conf = base.clone();
    bad_conf["confidence"] = json!(1.5);
    assert!(SpeechPayload::from_value(&bad_conf).is_err());

    let mut inverted = base.clone();
    inverted["audio_start_ms"] = json!(20);
    assert!(SpeechPayload::from_value(&inverted).is_err());

    let mut unknown_event = base.clone();
    unknown_event["event"] = json!("TRANSCRIPT_PARCIAL_INVENTADO");
    assert!(SpeechPayload::from_value(&unknown_event).is_err());

    let mut bad_state = base.clone();
    bad_state["state"] = json!("FINAL");
    assert!(SpeechPayload::from_value(&bad_state).is_err());

    let mut bad_uuid = base.clone();
    bad_uuid["stream_id"] = json!("nao-uuid");
    assert!(SpeechPayload::from_value(&bad_uuid).is_err());
}

#[test]
fn confidence_is_optional_and_never_invented() {
    let s = Uuid::new_v4();
    let seg = Uuid::new_v4();
    let v = json!({"stream_id": s, "sequence": 1, "audio_start_ms": 0, "audio_end_ms": 10,
                   "event": "TRANSCRIPT", "segment_id": seg, "revision": 0, "state": "PARTIAL", "text": "oi"});
    let p = SpeechPayload::from_value(&v).unwrap();
    match p.event {
        SpeechEvent::Transcript { confidence, .. } => assert_eq!(confidence, None),
        other => panic!("{other:?}"),
    }
}

// ---------- Intent: validação ----------

#[test]
fn capability_name_rules() {
    assert!(is_capability_name("OPEN_APPLICATION"));
    assert!(is_capability_name("GET_TIME2"));
    assert!(!is_capability_name("open_application"));
    assert!(!is_capability_name("OP"));
    assert!(!is_capability_name("1ABC"));
    assert!(!is_capability_name("ABRIR APP"));
}

#[test]
fn interpretation_state_must_match_content() {
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    assert!(
        interp(
            seg,
            s,
            InterpretationState::Stable,
            0,
            Some("OPEN_APPLICATION")
        )
        .validate()
        .is_ok()
    );
    assert!(
        interp(seg, s, InterpretationState::Stable, 0, None)
            .validate()
            .is_err(),
        "estável sem intenção"
    );

    let mut stable_missing = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    stable_missing.missing = vec!["application".into()];
    assert!(stable_missing.validate().is_err(), "estável com pendência");

    let clarify = interp(seg, s, InterpretationState::NeedsClarification, 0, None);
    assert!(clarify.validate().is_err(), "clarificação sem pendência");

    assert!(
        interp(
            seg,
            s,
            InterpretationState::NoIntent,
            0,
            Some("OPEN_APPLICATION")
        )
        .validate()
        .is_err()
    );
    assert!(
        interp(seg, s, InterpretationState::Provisional, 0, None)
            .validate()
            .is_ok()
    );
}

#[test]
fn interpretation_limits_and_names_are_validated() {
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let mut p = interp(seg, s, InterpretationState::Provisional, 0, None);
    p.parameters
        .insert("Bad-Key".into(), ParamValue::Integer(1));
    assert!(p.validate().is_err());

    let mut p = interp(seg, s, InterpretationState::Provisional, 0, None);
    p.ambiguities = vec![Ambiguity {
        kind: "APP".into(),
        options: vec!["a".into()],
    }];
    assert!(p.validate().is_err(), "ambiguidade exige 2+ opções");

    let mut p = interp(seg, s, InterpretationState::Provisional, 0, None);
    p.entities = vec![Entity {
        kind: "APP".into(),
        text: "brave".into(),
        start: 5,
        end: 2,
    }];
    assert!(p.validate().is_err());

    let mut p = interp(seg, s, InterpretationState::Provisional, 0, None);
    p.intent = Some(IntentCandidate {
        name: "open_app".into(),
        confidence: None,
    });
    assert!(p.validate().is_err());
}

// ---------- Intent: revisões ----------

#[test]
fn interpretation_revisions_apply_in_order_and_dedupe() {
    let mut t = InterpretationTracker::default();
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let r0 = interp(seg, s, InterpretationState::Provisional, 0, None);
    let r1 = interp(
        seg,
        s,
        InterpretationState::Stable,
        1,
        Some("OPEN_APPLICATION"),
    );
    assert_eq!(t.apply(&r0), Acceptance::Applied);
    assert_eq!(t.apply(&r0), Acceptance::Duplicate);
    assert_eq!(t.apply(&r1), Acceptance::Applied);
    assert!(matches!(t.apply(&r0), Acceptance::Stale { .. }));
}

#[test]
fn interpretation_gap_and_source_regression_are_detected() {
    let mut t = InterpretationTracker::default();
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let r2 = interp(seg, s, InterpretationState::Provisional, 2, None);
    assert!(matches!(t.apply(&r2), Acceptance::Gap { .. }));

    let mut r0 = interp(seg, s, InterpretationState::Provisional, 0, None);
    r0.source_revision = 3;
    t.apply(&r0);
    let mut r1 = interp(seg, s, InterpretationState::Provisional, 1, None);
    r1.source_revision = 2;
    assert!(matches!(t.apply(&r1), Acceptance::Rejected(ref m) if m.contains("regrediu")));
}

#[test]
fn cancelled_interpretation_rejects_later_revisions() {
    let mut t = InterpretationTracker::default();
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    t.apply(&interp(seg, s, InterpretationState::Provisional, 0, None));
    assert_eq!(t.cancel_stream(&s), 1);
    let out = t.apply(&interp(
        seg,
        s,
        InterpretationState::Stable,
        1,
        Some("OPEN_APPLICATION"),
    ));
    assert!(matches!(out, Acceptance::Rejected(_)), "{out:?}");
}

// ---------- Prontidão: nada provisório executa; nada duplica ----------

#[test]
fn provisional_never_admits() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(
        seg,
        s,
        InterpretationState::Provisional,
        0,
        Some("OPEN_APPLICATION"),
    );
    assert_eq!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::Provisional)
    );
}

#[test]
fn stable_on_partial_source_is_not_admitted() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    assert_eq!(
        gate.evaluate(&p, partial_source(0), RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::SourceUnconfirmed)
    );
}

#[test]
fn stable_on_confirmed_source_admits_once_then_reports_duplicate() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    assert!(matches!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::Admit(_)
    ));
    assert!(matches!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::Duplicate(_)
    ));
}

#[test]
fn intent_change_on_same_segment_supersedes_admission() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let a = interp(
        seg,
        s,
        InterpretationState::Stable,
        1,
        Some("OPEN_APPLICATION"),
    );
    let b = interp(seg, s, InterpretationState::Stable, 2, Some("GET_TIME"));
    gate.evaluate(&a, confirmed_source(0), RiskClass::Read, false);
    assert!(matches!(
        gate.evaluate(&b, confirmed_source(0), RiskClass::Read, false),
        Decision::Supersede { .. }
    ));
}

#[test]
fn source_revised_after_interpretation_is_not_admitted() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    assert_eq!(
        gate.evaluate(&p, confirmed_source(1), RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::SourceRevised { current: 1 })
    );
}

#[test]
fn sensitive_action_needs_policy_confirmation_even_if_stable() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(seg, s, InterpretationState::Stable, 0, Some("DELETE_FILE"));
    assert_eq!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Irreversible, false),
        Decision::NotReady(NotReadyReason::ConfirmationRequired)
    );
    assert!(matches!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Irreversible, true),
        Decision::Admit(_)
    ));
}

#[test]
fn confidence_threshold_is_never_invented() {
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let mut p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );

    // Sem limite configurado: admite sem confiança informada.
    assert!(matches!(
        ReadinessGate::new(None).evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::Admit(_)
    ));
    // Com limite configurado e confiança ausente: não admite.
    assert_eq!(
        ReadinessGate::new(Some(0.8)).evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::BelowMinConfidence)
    );
    p.intent = Some(IntentCandidate {
        name: "OPEN_APPLICATION".into(),
        confidence: Some(0.9),
    });
    assert!(matches!(
        ReadinessGate::new(Some(0.8)).evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::Admit(_)
    ));
}

#[test]
fn inactive_stream_blocks_admission() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    let src = SourceSnapshot {
        stream_active: false,
        ..confirmed_source(0)
    };
    assert_eq!(
        gate.evaluate(&p, src, RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::StreamInactive)
    );
}

#[test]
fn clarification_and_no_intent_never_admit() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let mut clar = interp(seg, s, InterpretationState::NeedsClarification, 0, None);
    clar.ambiguities = vec![Ambiguity {
        kind: "APP".into(),
        options: vec!["a".into(), "b".into()],
    }];
    assert_eq!(
        gate.evaluate(&clar, confirmed_source(0), RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::NeedsClarification)
    );
    let none = interp(seg, s, InterpretationState::NoIntent, 0, None);
    assert_eq!(
        gate.evaluate(&none, confirmed_source(0), RiskClass::Read, false),
        Decision::NotReady(NotReadyReason::NoIntent)
    );
}

#[test]
fn parameter_values_roundtrip_with_correct_types() {
    let v = json!({"b": true, "i": 3, "t": "x"});
    let parsed: std::collections::BTreeMap<String, ParamValue> = serde_json::from_value(v).unwrap();
    assert_eq!(parsed["b"], ParamValue::Boolean(true));
    assert_eq!(parsed["i"], ParamValue::Integer(3));
    assert_eq!(parsed["t"], ParamValue::Text("x".into()));
}

// ---------- Protocolo 1.1 ----------

fn stt_message(
    seg: Uuid,
    stream: Uuid,
    seq: u64,
    rev: u32,
    state: &str,
    text: &str,
) -> KavoMessage {
    let mut m = KavoMessage::new(MessageType::SttResult, "sessao-e2e", "stt-fixture");
    m.payload = Some(json!({
        "stream_id": stream, "sequence": seq, "audio_start_ms": 0, "audio_end_ms": 100,
        "event": "TRANSCRIPT", "segment_id": seg, "revision": rev, "state": state, "text": text
    }));
    m
}

#[test]
fn protocol_11_types_are_guaranteed_and_typed() {
    assert_eq!(MessageType::SttResult.delivery(), Delivery::Guaranteed);
    assert_eq!(MessageType::IntentDetected.delivery(), Delivery::Guaranteed);
    let m = stt_message(Uuid::new_v4(), Uuid::new_v4(), 1, 0, "PARTIAL", "oi");
    let line = m.to_json_line().unwrap();
    assert!(KavoMessage::from_json_slice(&line[..line.len() - 1]).is_ok());
}

#[test]
fn stt_result_without_payload_or_with_bad_payload_is_rejected() {
    let mut m = KavoMessage::new(MessageType::SttResult, "s", "x");
    assert!(m.validate().is_err(), "exige payload");
    m.payload = Some(json!({"sequence": 1}));
    assert!(m.validate().is_err());
}

#[test]
fn payload_forbidden_on_heartbeat_and_error() {
    let mut hb = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    hb.payload = Some(json!({}));
    assert!(hb.validate().is_err());
    let mut err = KavoMessage::new(MessageType::Error, "s", "x");
    err.error_code = Some(1100);
    err.payload = Some(json!({}));
    assert!(err.validate().is_err());
}

#[test]
fn cancel_accepts_stream_target_or_request_target() {
    let stream = Uuid::new_v4();
    let mut by_stream = KavoMessage::new(MessageType::Cancel, "s", "x");
    by_stream.payload = Some(json!({"stream_id": stream}));
    assert!(by_stream.validate().is_ok());

    let by_request = KavoMessage::new(MessageType::Cancel, "s", "x")
        .with_correlation_id(Uuid::new_v4().to_string());
    assert!(by_request.validate().is_ok());

    let neither = KavoMessage::new(MessageType::Cancel, "s", "x");
    assert!(neither.validate().is_err());
}

#[test]
fn schema_11_and_legacy_10_messages_are_both_accepted() {
    let mut m = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    m.schema_version = "1.0".into();
    assert!(m.validate().is_ok());
    m.schema_version = "1.1".into();
    assert!(m.validate().is_ok());
    m.schema_version = "2.0".into();
    assert_eq!(
        m.validate().unwrap_err().kind(),
        ErrorKind::UnsupportedVersion
    );
}

#[test]
fn reserved_types_are_still_unsupported() {
    let line = br#"{"type":"WAKE_WORD_DETECTED","schema_version":"1.1","message_id":"3f2b8c1e-9d4a-4b7e-8f21-6a5c0d9e7b10","session_id":"s","timestamp":1.0,"sender":"x"}"#;
    assert_eq!(
        KavoMessage::from_json_slice(line).unwrap_err().kind(),
        ErrorKind::UnsupportedMessageType
    );
}

// ---------- Monitor no barramento ----------

fn monitor_setup(
    cfg: SpeechConfig,
) -> (
    EventBus,
    Arc<Mutex<MonitorStats>>,
    CancellationToken,
    tokio::task::JoinHandle<kavo::error::Result<()>>,
) {
    let bus = EventBus::new(64, Duration::from_secs(2));
    let rx = bus.subscribe();
    let stats: Arc<Mutex<MonitorStats>> = Default::default();
    let monitor = ContractMonitor::new(rx, &cfg, stats.clone());
    let cancel = CancellationToken::new();
    let ctx = ComponentContext {
        cancel: cancel.clone(),
        bus: bus.clone(),
    };
    let handle = tokio::spawn(monitor.run(ctx));
    (bus, stats, cancel, handle)
}

#[tokio::test]
async fn monitor_applies_stt_and_admits_once_end_to_end() {
    let (bus, stats, cancel, handle) = monitor_setup(SpeechConfig::default());
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    bus.publish(stt_message(seg, s, 1, 0, "PARTIAL", "abre"))
        .await
        .unwrap();
    bus.publish(stt_message(seg, s, 2, 1, "CONFIRMED", "abre o brave"))
        .await
        .unwrap();
    // Duplicata de transporte: mesma sequência e mesmo conteúdo.
    bus.publish(stt_message(seg, s, 2, 1, "CONFIRMED", "abre o brave"))
        .await
        .unwrap();

    // Interpretação baseada na revisão 1 (confirmada) do segmento.
    let mut based = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    based.source_revision = 1;
    let mut intent = KavoMessage::new(MessageType::IntentDetected, "s", "intent-fixture");
    intent.payload = Some(serde_json::to_value(based).unwrap());
    bus.publish(intent.clone()).await.unwrap();
    // Reenvio idêntico: duplicata de transporte, ignorada pelo rastreador de interpretações.
    bus.publish(intent.clone()).await.unwrap();
    // Nova revisão com a mesma intenção e mesmos parâmetros: não pode executar de novo.
    let mut again = based_revision_one(seg, s);
    again.revision = 1;
    let mut intent2 = KavoMessage::new(MessageType::IntentDetected, "s", "intent-fixture");
    intent2.payload = Some(serde_json::to_value(again).unwrap());
    bus.publish(intent2).await.unwrap();

    cancel.cancel();
    handle.await.unwrap().unwrap();
    let st = stats.lock().unwrap().clone();
    assert_eq!(st.speech_applied, 2);
    assert_eq!(st.speech_duplicate, 1);
    assert_eq!(st.interpretation_duplicate, 1);
    assert_eq!(st.admitted, 1);
    assert_eq!(st.duplicate_admissions, 1);
}

fn based_revision_one(seg: Uuid, s: Uuid) -> InterpretationPayload {
    let mut p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    p.source_revision = 1;
    p
}

#[tokio::test]
async fn monitor_handles_out_of_order_and_cancel_stream() {
    let (bus, stats, cancel, handle) = monitor_setup(SpeechConfig::default());
    let (s, seg) = (Uuid::new_v4(), Uuid::new_v4());
    bus.publish(stt_message(seg, s, 1, 0, "PARTIAL", "a"))
        .await
        .unwrap();
    bus.publish(stt_message(seg, s, 3, 1, "PARTIAL", "a b"))
        .await
        .unwrap(); // lacuna
    let mut cancel_msg = KavoMessage::new(MessageType::Cancel, "s", "cli");
    cancel_msg.payload = Some(json!({"stream_id": s}));
    bus.publish(cancel_msg).await.unwrap();
    bus.publish(stt_message(seg, s, 2, 1, "PARTIAL", "a b"))
        .await
        .unwrap(); // rejeitado: cancelado

    cancel.cancel();
    handle.await.unwrap().unwrap();
    let st = stats.lock().unwrap().clone();
    assert_eq!(st.speech_gap, 1);
    assert_eq!(st.speech_applied, 1);
    assert_eq!(st.speech_rejected, 1);
    assert_eq!(st.cancels, 1);
}

#[tokio::test]
async fn monitor_expires_idle_streams() {
    let cfg = SpeechConfig {
        stream_idle_timeout_ms: 100,
        ..SpeechConfig::default()
    };
    let (bus, stats, cancel, handle) = monitor_setup(cfg);
    bus.publish(stt_message(
        Uuid::new_v4(),
        Uuid::new_v4(),
        1,
        0,
        "PARTIAL",
        "x",
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    cancel.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(stats.lock().unwrap().streams_timed_out, 1);
}

// ---------- Protocolo sobre socket real com o núcleo ----------

fn settings() -> IpcSettings {
    IpcSettings {
        max_frame_bytes: 8192,
        request_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(5),
    }
}

async fn start_core(
    path: &std::path::Path,
) -> (
    CancellationToken,
    Arc<Mutex<MonitorStats>>,
    tokio::task::JoinHandle<()>,
) {
    let bus = EventBus::new(256, Duration::from_secs(2));
    let rx = bus.subscribe();
    let stats: Arc<Mutex<MonitorStats>> = Default::default();
    let mut sup = Supervisor::new(bus.clone());
    sup.spawn(ContractMonitor::new(
        rx,
        &SpeechConfig::default(),
        stats.clone(),
    ));
    let server = IpcServer::bind(path, settings()).unwrap();
    let cancel = sup.cancel_token();
    let handle = tokio::spawn(async move {
        let _ = server
            .run(ComponentContext {
                cancel: cancel.clone(),
                bus: bus.clone(),
            })
            .await;
        let _ = sup.shutdown(Duration::from_secs(2)).await;
    });
    let token = CancellationToken::new();
    (token, stats, handle)
}

async fn send_line(path: &std::path::Path, line: &[u8]) -> KavoMessage {
    let stream = UnixStream::connect(path).await.unwrap();
    let (r, mut w) = stream.into_split();
    w.write_all(line).await.unwrap();
    let mut reader = BufReader::new(r);
    let frame = ipc::read_frame(&mut reader, 8192).await.unwrap().unwrap();
    KavoMessage::from_json_slice(&frame).unwrap()
}

#[tokio::test]
async fn stt_result_over_socket_is_accepted_without_reply_and_invalid_gets_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    let path = dir.path().join("k.sock");
    let (_tok, stats, handle) = start_core(&path).await;
    // aguarda socket
    for _ in 0..50 {
        if UnixStream::connect(&path).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Mensagem válida: sem resposta. Verificamos pelo monitor que foi aplicada.
    let valid = stt_message(Uuid::new_v4(), Uuid::new_v4(), 1, 0, "PARTIAL", "oi");
    let stream = UnixStream::connect(&path).await.unwrap();
    let (_r, mut w) = stream.into_split();
    w.write_all(&valid.to_json_line().unwrap()).await.unwrap();
    w.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(stats.lock().unwrap().speech_applied, 1);

    // Payload inválido: erro com classe.
    let mut bad = stt_message(Uuid::new_v4(), Uuid::new_v4(), 1, 0, "PARTIAL", "oi");
    bad.payload = Some(json!({"stream_id": "x"}));
    let mut line = serde_json::to_vec(&bad).unwrap();
    line.push(b'\n');
    let reply = send_line(&path, &line).await;
    assert_eq!(reply.kind, MessageType::Error);
    assert!(
        reply
            .error_message
            .unwrap()
            .starts_with("[INVALID_MESSAGE]")
    );

    // Heartbeat continua respondendo.
    let hb = KavoMessage::new(MessageType::Heartbeat, "s", "cli");
    let reply = send_line(&path, &hb.to_json_line().unwrap()).await;
    assert_eq!(reply.kind, MessageType::Heartbeat);

    _tok.cancel();
    handle.abort();
}

// ---------- Revisão independente (Phase 4.2): correções ----------

#[test]
fn gap_counts_as_producer_activity_for_timeout() {
    let mut t = StreamTracker::new(4);
    let s = Uuid::new_v4();
    t.apply(&speech(s, 1, SpeechEvent::NoSpeech), 0);
    // Lacuna em t=900: o produtor está vivo, mesmo sem evento aplicado.
    assert_eq!(
        t.apply(&speech(s, 3, SpeechEvent::NoSpeech), 900),
        Acceptance::Gap {
            expected: 2,
            got: 3
        }
    );
    assert!(
        t.expire_idle(1_000, 500).is_empty(),
        "atividade recente não expira"
    );
}

#[test]
fn cancelled_or_evicted_stream_cannot_be_reopened_with_sequence_one() {
    let mut t = StreamTracker::new(1);
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    t.apply(&speech(a, 1, SpeechEvent::NoSpeech), 0);
    assert!(t.cancel(&a));
    // Novo stream força a remoção do terminal `a`.
    assert_eq!(
        t.apply(&speech(b, 1, SpeechEvent::NoSpeech), 1),
        Acceptance::Applied
    );
    let out = t.apply(&speech(a, 1, SpeechEvent::NoSpeech), 2);
    assert!(
        matches!(out, Acceptance::Rejected(ref m) if m.contains("reaberto")),
        "{out:?}"
    );
}

#[test]
fn interpretation_cannot_switch_segment_or_stream() {
    let mut t = InterpretationTracker::default();
    let (seg_a, seg_b, s) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(
        t.apply(&interp(
            seg_a,
            s,
            InterpretationState::Stable,
            0,
            Some("OPEN_APPLICATION")
        )),
        Acceptance::Applied
    );
    let out = t.apply(&interp(
        seg_b,
        s,
        InterpretationState::Stable,
        1,
        Some("OPEN_APPLICATION"),
    ));
    assert!(
        matches!(out, Acceptance::Rejected(ref m) if m.contains("trocar")),
        "{out:?}"
    );
}

#[test]
fn segments_per_stream_are_capped() {
    let mut t = StreamTracker::new(4);
    let s = Uuid::new_v4();
    for seq in 1..=(MAX_SEGMENTS_PER_STREAM as u64) {
        let seg = Uuid::new_v4();
        assert_eq!(
            t.apply(
                &speech(s, seq, transcript(seg, 0, TranscriptState::Partial, "x")),
                0
            ),
            Acceptance::Applied
        );
    }
    let extra = speech(
        s,
        MAX_SEGMENTS_PER_STREAM as u64 + 1,
        transcript(Uuid::new_v4(), 0, TranscriptState::Partial, "y"),
    );
    assert!(
        matches!(t.apply(&extra, 0), Acceptance::Rejected(ref m) if m.contains("limite de segmentos"))
    );
}

#[test]
fn admission_is_forgotten_when_stream_is_released() {
    let mut gate = ReadinessGate::new(None);
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let p = interp(
        seg,
        s,
        InterpretationState::Stable,
        0,
        Some("OPEN_APPLICATION"),
    );
    assert!(matches!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::Admit(_)
    ));
    gate.forget_stream(&s);
    assert!(matches!(
        gate.evaluate(&p, confirmed_source(0), RiskClass::Read, false),
        Decision::Admit(_)
    ));
}

#[test]
fn validation_errors_do_not_echo_producer_values() {
    let (seg, s) = (Uuid::new_v4(), Uuid::new_v4());
    let mut p = interp(seg, s, InterpretationState::Provisional, 0, None);
    p.intent = Some(IntentCandidate {
        name: "segredo_do_usuario".into(),
        confidence: None,
    });
    let msg = p.validate().unwrap_err().to_string();
    assert!(!msg.contains("segredo"), "{msg}");

    let mut q = interp(seg, s, InterpretationState::Provisional, 0, None);
    q.parameters
        .insert("Chave-Secreta".into(), ParamValue::Integer(1));
    let msg = q.validate().unwrap_err().to_string();
    assert!(!msg.contains("Chave"), "{msg}");
}

#[test]
fn unknown_type_name_is_escaped_in_error() {
    let line = "{\"type\":\"A\\nB\\u001b[31m\",\"schema_version\":\"1.1\"}";
    let err = KavoMessage::from_json_slice(line.as_bytes()).unwrap_err();
    let text = err.to_string();
    assert!(!text.contains('\n') && !text.contains('\u{1b}'), "{text:?}");
    assert_eq!(err.kind(), ErrorKind::UnsupportedMessageType);
}

#[test]
fn acceptance_labels_hide_reasons() {
    assert_eq!(
        Acceptance::Rejected("texto do produtor".into()).label(),
        "REJECTED"
    );
    assert!(StreamStatus::Failed("x".into()).is_terminal());
}
