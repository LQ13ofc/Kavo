//! Testes do ACK de admissão (D-58): contrato do protocolo, classificação de motivos, rollback de
//! memória, recusa repetida e backpressure determinística pelo IPC. Socket e SQLite são reais.

use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kavo::bus::EventBus;
use kavo::config::{KavoConfig, SpeechConfig};
use kavo::control::{
    self, AckEvent, AdmissionAck, AdmissionStatus, CONTROL_SCHEMA_VERSION, ControlResult, Reason,
};
use kavo::intent::{self, InterpretationTracker};
use kavo::ipc::{self, IpcServer, IpcSettings};
use kavo::monitor::{ContractMonitor, MonitorStats};
use kavo::protocol::{KavoMessage, MessageType};
use kavo::runtime::Supervisor;
use kavo::speech::{self, Acceptance, SpeechEvent, SpeechPayload, StreamTracker, TranscriptState};
use kavo::store::Store;
use uuid::Uuid;

const SOURCE: &str = "teste-ack";

fn settings() -> IpcSettings {
    IpcSettings {
        max_frame_bytes: 65536,
        request_timeout: Duration::from_secs(3),
        idle_timeout: Duration::from_secs(30),
    }
}

fn v12(mut msg: KavoMessage) -> KavoMessage {
    msg.schema_version = CONTROL_SCHEMA_VERSION.to_owned();
    msg
}

fn stt(session: &str, stream: Uuid, sequence: u64, event: SpeechEvent) -> KavoMessage {
    let payload = SpeechPayload {
        stream_id: stream,
        sequence,
        audio_start_ms: 0,
        audio_end_ms: 100,
        event,
    };
    v12(KavoMessage::new(MessageType::SttResult, session, SOURCE)
        .with_payload(&payload)
        .unwrap())
}

fn intent_msg(session: &str, stream: Uuid, seg: Uuid, interp: Uuid, revision: u32) -> KavoMessage {
    let mut msg = KavoMessage::new(MessageType::IntentDetected, session, SOURCE);
    msg.payload = Some(serde_json::json!({
        "interpretation_id": interp,
        "stream_id": stream,
        "segment_id": seg,
        "source_revision": 0,
        "revision": revision,
        "state": "PROVISIONAL",
        "engine": {"name": "fixture", "version": "0"},
    }));
    v12(msg)
}

#[test]
fn ack_is_1_2_and_correlates_with_the_event() {
    let event = stt("s", Uuid::new_v4(), 1, SpeechEvent::NoSpeech);
    let ack = AdmissionAck::for_event(&event, AdmissionStatus::Accepted, None);
    let reply = ack.reply_to(&event).unwrap();
    assert_eq!(reply.kind, MessageType::AdmissionAck);
    assert_eq!(reply.schema_version, "1.2");
    assert_eq!(
        reply.correlation_id.as_deref(),
        Some(event.message_id.as_str())
    );
    assert_eq!(
        AdmissionAck::from_value(reply.payload.as_ref().unwrap()).unwrap(),
        ack
    );
}

#[test]
fn ack_identifies_stream_and_interpretation_without_content() {
    let stream = Uuid::new_v4();
    let event = stt("s", stream, 7, SpeechEvent::NoSpeech);
    let ack = AdmissionAck::for_event(&event, AdmissionStatus::Accepted, None);
    assert_eq!(ack.event, AckEvent::SttResult);
    assert_eq!((ack.stream_id, ack.sequence), (Some(stream), Some(7)));
    let json = serde_json::to_string(&ack).unwrap();
    assert!(!json.contains("texto"), "{json}");
}

#[test]
fn ack_and_control_result_do_not_parse_as_each_other() {
    let ack = AdmissionAck {
        event: AckEvent::SttResult,
        status: AdmissionStatus::Accepted,
        reason: None,
        stream_id: None,
        sequence: None,
        interpretation_id: None,
        revision: None,
    };
    let ack_value = serde_json::to_value(&ack).unwrap();
    assert!(ControlResult::from_value(&ack_value).is_err());
    let control = ControlResult::new(
        kavo::control::ControlOp::Open,
        kavo::control::Outcome::Applied,
        kavo::control::WireState::Active,
    );
    let control_value = serde_json::to_value(&control).unwrap();
    assert!(AdmissionAck::from_value(&control_value).is_err());
}

#[test]
fn ack_type_requires_schema_1_2_and_rejects_1_1() {
    let event = stt("s", Uuid::new_v4(), 1, SpeechEvent::NoSpeech);
    let ack = AdmissionAck::for_event(&event, AdmissionStatus::Accepted, None);
    let mut reply = ack.reply_to(&event).unwrap();
    reply.schema_version = "1.1".to_owned();
    let raw = serde_json::to_vec(&reply).unwrap();
    assert!(KavoMessage::from_json_slice(&raw).is_err());
}

#[test]
fn stt_reject_text_maps_to_stable_reasons() {
    assert_eq!(
        control::stt_reject_reason(speech::REJECT_STREAM_CLOSED),
        Reason::StreamClosed
    );
    assert_eq!(
        control::stt_reject_reason(&format!("{} (Ended)", speech::REJECT_STREAM_CLOSED)),
        Reason::StreamClosed
    );
    assert_eq!(
        control::stt_reject_reason(speech::REJECT_STREAM_LIMIT),
        Reason::StreamLimit
    );
    assert_eq!(
        control::stt_reject_reason(speech::REJECT_CONFLICT),
        Reason::Conflict
    );
    assert_eq!(
        control::stt_reject_reason("revisão fora de ordem: esperada 0, veio 9"),
        Reason::InvalidContent
    );
}

#[test]
fn intent_reject_text_maps_to_stable_reasons() {
    assert_eq!(
        control::intent_reject_reason(intent::REJECT_STALE),
        Reason::StaleRevision
    );
    assert_eq!(
        control::intent_reject_reason(intent::REJECT_CONFLICT),
        Reason::Conflict
    );
    assert_eq!(
        control::intent_reject_reason(&format!(
            "{} não aceita revisões",
            intent::REJECT_STREAM_CLOSED
        )),
        Reason::StreamClosed
    );
}

#[test]
fn repeated_rejected_sequence_returns_the_same_rejection_not_duplicate() {
    let mut t = StreamTracker::new(4);
    let (session, stream, seg) = ("s", Uuid::new_v4(), Uuid::new_v4());
    let start = SpeechPayload {
        stream_id: stream,
        sequence: 1,
        audio_start_ms: 0,
        audio_end_ms: 100,
        event: SpeechEvent::SpeechStarted { segment_id: seg },
    };
    assert_eq!(t.apply(session, &start, 0), Acceptance::Applied);
    let bad = SpeechPayload {
        stream_id: stream,
        sequence: 2,
        audio_start_ms: 100,
        audio_end_ms: 200,
        event: SpeechEvent::Transcript {
            segment_id: seg,
            revision: 9,
            state: TranscriptState::Partial,
            text: "x".to_owned(),
            confidence: None,
        },
    };
    let first = t.apply(session, &bad, 0);
    assert!(matches!(first, Acceptance::Rejected(_)), "{first:?}");
    let again = t.apply(session, &bad, 0);
    assert_eq!(first, again, "a repetição devolve a mesma recusa");
}

#[test]
fn interpretation_rollback_restores_previous_record() {
    let mut t = InterpretationTracker::default();
    let (session, interp) = ("s", Uuid::new_v4());
    let stream = Uuid::new_v4();
    let seg = Uuid::new_v4();
    let payload = intent::InterpretationPayload::from_value(
        intent_msg(session, stream, seg, interp, 0)
            .payload
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    let previous = t.snapshot(session, &interp);
    assert!(previous.is_none());
    assert_eq!(t.apply(session, &payload), Acceptance::Applied);
    assert!(t.snapshot(session, &interp).is_some());
    t.rollback(session, &interp, previous);
    assert!(
        t.snapshot(session, &interp).is_none(),
        "memória desfeita após falha de gravação"
    );
    assert_eq!(
        t.apply(session, &payload),
        Acceptance::Applied,
        "retry não vira duplicata"
    );
}

#[test]
fn queue_and_admission_config_are_validated() {
    assert_eq!(KavoConfig::default().ipc.admission_queue, 64);
    let mut cfg = KavoConfig::default();
    cfg.ipc.admission_queue = 0;
    assert!(cfg.validate().is_err());
    cfg.ipc.admission_queue = 5000;
    assert!(cfg.validate().is_err());
    cfg.ipc.admission_queue = 1;
    assert!(cfg.validate().is_ok());
    let mut env_cfg = KavoConfig::default();
    env_cfg
        .apply_env(|k| (k == "KAVO_IPC_ADMISSION_QUEUE").then(|| "7".to_owned()))
        .unwrap();
    assert_eq!(env_cfg.ipc.admission_queue, 7);
}

struct Daemon {
    socket: std::path::PathBuf,
    supervisor: Option<Supervisor>,
    _dir: tempfile::TempDir,
}

async fn start(queue: usize) -> Daemon {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("kavo.sock");
    let store = Store::open(&dir.path().join("kavo.db")).unwrap();
    let bus = EventBus::new(256, Duration::from_secs(2));
    let rx = bus.subscribe();
    let (tx, control_rx) = control::channel_with(queue);
    let monitor = ContractMonitor::open_with_control(
        rx,
        &SpeechConfig::default(),
        Arc::new(Mutex::new(MonitorStats::default())),
        store,
        control_rx,
    )
    .unwrap();
    let mut supervisor = Supervisor::new(bus);
    supervisor.spawn(monitor);
    let server = IpcServer::bind(&socket, settings())
        .unwrap()
        .with_control(tx);
    supervisor.spawn(server);
    tokio::time::sleep(Duration::from_millis(50)).await;
    Daemon {
        socket,
        supervisor: Some(supervisor),
        _dir: dir,
    }
}

impl Daemon {
    async fn send(&self, msg: &KavoMessage) -> KavoMessage {
        ipc::request(&self.socket, msg, settings())
            .await
            .expect("resposta")
    }
    async fn stop(&mut self) {
        if let Some(s) = self.supervisor.take() {
            s.shutdown(Duration::from_secs(5)).await;
        }
    }
}

fn open(session: &str) -> KavoMessage {
    v12(
        KavoMessage::new(MessageType::SessionControl, session, SOURCE)
            .with_payload(&kavo::control::SessionControl::Open)
            .unwrap(),
    )
}

fn status_of(reply: &KavoMessage) -> Option<AdmissionStatus> {
    AdmissionAck::from_value(reply.payload.as_ref()?)
        .ok()
        .map(|a| a.status)
}

#[tokio::test]
async fn stt_over_socket_gets_ack_and_duplicate_on_retry() {
    let mut d = start(64).await;
    let s = "sessao-ack-socket";
    d.send(&open(s)).await;
    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());
    let start_msg = stt(s, st, 1, SpeechEvent::SpeechStarted { segment_id: seg });
    let reply = d.send(&start_msg).await;
    assert_eq!(reply.kind, MessageType::AdmissionAck);
    assert_eq!(
        reply.correlation_id.as_deref(),
        Some(start_msg.message_id.as_str())
    );
    assert_eq!(status_of(&reply), Some(AdmissionStatus::Accepted));
    let again = d.send(&start_msg).await;
    assert_eq!(status_of(&again), Some(AdmissionStatus::Duplicate));
    d.stop().await;
}

#[tokio::test]
async fn full_admission_queue_answers_busy_immediately() {
    // Capacidade 1 e nenhum consumidor: o primeiro pedido ocupa a fila, o segundo recebe BUSY sem esperar.
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("kavo.sock");
    let (tx, rx) = control::channel_with(1);
    let server = IpcServer::bind(&socket, settings())
        .unwrap()
        .with_control(tx);
    let cancel = tokio_util::sync::CancellationToken::new();
    let serve_cancel = cancel.clone();
    let bus = EventBus::new(16, Duration::from_secs(1));
    let handle = tokio::spawn(server.serve(bus, serve_cancel));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first_socket = socket.clone();
    let parked = tokio::spawn(async move {
        let msg = stt("s-busy", Uuid::new_v4(), 1, SpeechEvent::NoSpeech);
        ipc::request(&first_socket, &msg, settings()).await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let second = stt("s-busy-2", Uuid::new_v4(), 1, SpeechEvent::NoSpeech);
    let started = std::time::Instant::now();
    let reply = ipc::request(&socket, &second, settings()).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "BUSY deve ser imediato"
    );
    let ack = AdmissionAck::from_value(reply.payload.as_ref().unwrap()).unwrap();
    assert_eq!(ack.status, AdmissionStatus::NotReady);
    assert_eq!(ack.reason, Some(Reason::Busy));
    assert_eq!(
        reply.correlation_id.as_deref(),
        Some(second.message_id.as_str())
    );

    drop(rx);
    parked.abort();
    cancel.cancel();
    let _ = handle.await;
}

#[tokio::test]
async fn legacy_1_1_event_gets_no_ack() {
    let mut d = start(64).await;
    let legacy = KavoMessage::new(MessageType::SttResult, "legado-teste", SOURCE)
        .with_payload(&SpeechPayload {
            stream_id: Uuid::new_v4(),
            sequence: 1,
            audio_start_ms: 0,
            audio_end_ms: 1,
            event: SpeechEvent::NoSpeech,
        })
        .unwrap();
    // 1.1 não tem resposta: a conexão fecha sem ACK.
    assert!(ipc::request(&d.socket, &legacy, settings()).await.is_err());
    d.stop().await;
}

#[tokio::test]
async fn ack_sent_by_client_is_refused() {
    let mut d = start(64).await;
    let ack = AdmissionAck {
        event: AckEvent::SttResult,
        status: AdmissionStatus::Accepted,
        reason: None,
        stream_id: None,
        sequence: None,
        interpretation_id: None,
        revision: None,
    };
    let msg = v12(KavoMessage::new(MessageType::AdmissionAck, "s", SOURCE)
        .with_payload(&ack)
        .unwrap());
    let reply = d.send(&msg).await;
    assert_eq!(reply.kind, MessageType::Error);
    d.stop().await;
}

#[tokio::test]
async fn intent_over_socket_is_accepted_once() {
    let mut d = start(64).await;
    let s = "sessao-intent-socket";
    d.send(&open(s)).await;
    let (st, seg, interp) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    d.send(&stt(
        s,
        st,
        1,
        SpeechEvent::SpeechStarted { segment_id: seg },
    ))
    .await;
    let msg = intent_msg(s, st, seg, interp, 0);
    assert_eq!(
        status_of(&d.send(&msg).await),
        Some(AdmissionStatus::Accepted)
    );
    assert_eq!(
        status_of(&d.send(&msg).await),
        Some(AdmissionStatus::Duplicate)
    );
    d.stop().await;
}

#[tokio::test]
async fn missing_ack_within_deadline_is_an_error_never_an_ack() {
    // Fila aceita o evento, mas ninguém responde: o cliente recebe ERROR [TIMEOUT], nunca um ACK falso.
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("kavo.sock");
    let (tx, rx) = control::channel_with(4);
    let short = IpcSettings {
        max_frame_bytes: 65536,
        request_timeout: Duration::from_millis(300),
        idle_timeout: Duration::from_secs(30),
    };
    let server = IpcServer::bind(&socket, short).unwrap().with_control(tx);
    let cancel = tokio_util::sync::CancellationToken::new();
    let handle =
        tokio::spawn(server.serve(EventBus::new(16, Duration::from_secs(1)), cancel.clone()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = stt("s-timeout", Uuid::new_v4(), 1, SpeechEvent::NoSpeech);
    // Cliente com prazo maior que o do servidor: a resposta de erro chega antes da desistência.
    let client = IpcSettings {
        request_timeout: Duration::from_secs(3),
        ..short
    };
    let reply = ipc::request(&socket, &msg, client).await.unwrap();
    assert_eq!(reply.kind, MessageType::Error, "{reply:?}");
    assert!(
        reply
            .error_message
            .unwrap_or_default()
            .contains("[TIMEOUT]")
    );
    assert_eq!(
        reply.correlation_id.as_deref(),
        Some(msg.message_id.as_str())
    );

    drop(rx);
    cancel.cancel();
    let _ = handle.await;
}
