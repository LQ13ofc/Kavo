//! Testes de controle de sessões e requisições (Kavo Protocol 1.2): validação do protocolo e integração
//! com socket Unix real, monitor e SQLite temporário. Mocks não são usados aqui.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kavo::bus::EventBus;
use kavo::config::SpeechConfig;
use kavo::control::{
    self, CONTROL_SCHEMA_VERSION, ControlOp, ControlResult, Outcome, Reason, RequestKind,
    SessionControl, WireState,
};
use kavo::ipc::{self, IpcServer, IpcSettings};
use kavo::monitor::{ContractMonitor, MonitorStats};
use kavo::protocol::{KavoMessage, MessageType};
use kavo::runtime::Supervisor;
use kavo::store::Store;
use uuid::Uuid;

const SOURCE: &str = "teste-controle";

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

fn control_msg(session: &str, op: SessionControl) -> KavoMessage {
    v12(
        KavoMessage::new(MessageType::SessionControl, session, SOURCE)
            .with_payload(&op)
            .expect("payload"),
    )
}

struct Daemon {
    socket: PathBuf,
    db: PathBuf,
    supervisor: Option<Supervisor>,
    _dir: tempfile::TempDir,
}

impl Daemon {
    async fn start(dir: tempfile::TempDir) -> Self {
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = dir.path().join("kavo.db");
        let socket = dir.path().join("kavo.sock");
        let store = Store::open(&db).expect("store");
        let bus = EventBus::new(256, Duration::from_secs(2));
        let rx = bus.subscribe();
        let (control_tx, control_rx) = control::channel();
        let monitor = ContractMonitor::open_with_control(
            rx,
            &SpeechConfig::default(),
            Arc::new(Mutex::new(MonitorStats::default())),
            store,
            control_rx,
        )
        .expect("monitor");
        let mut supervisor = Supervisor::new(bus);
        supervisor.spawn(monitor);
        let server = IpcServer::bind(&socket, settings())
            .expect("bind")
            .with_control(control_tx);
        supervisor.spawn(server);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Self {
            socket,
            db,
            supervisor: Some(supervisor),
            _dir: dir,
        }
    }

    async fn ask(&self, msg: &KavoMessage) -> ControlResult {
        let reply = ipc::request(&self.socket, msg, settings())
            .await
            .expect("resposta");
        assert_eq!(
            reply.kind,
            MessageType::ControlResult,
            "resposta inesperada: {reply:?}"
        );
        assert_eq!(
            reply.correlation_id.as_deref(),
            Some(msg.message_id.as_str())
        );
        assert_eq!(reply.schema_version, CONTROL_SCHEMA_VERSION);
        ControlResult::from_value(reply.payload.as_ref().expect("payload")).expect("resultado")
    }

    async fn stop(&mut self) {
        if let Some(s) = self.supervisor.take() {
            s.shutdown(Duration::from_secs(5)).await;
        }
    }
}

fn begin(session: &str, stream: Uuid) -> (KavoMessage, Uuid) {
    let msg = control_msg(
        session,
        SessionControl::RequestBegin {
            kind: RequestKind::SttStream,
            stream_id: stream,
        },
    );
    let id = Uuid::parse_str(&msg.message_id).unwrap();
    (msg, id)
}

fn is(res: &ControlResult, outcome: Outcome, reason: Option<Reason>, state: WireState) -> bool {
    res.outcome == outcome && res.reason == reason && res.state == state
}

#[tokio::test]
async fn session_lifecycle_over_socket() {
    let mut d = Daemon::start(tempfile::tempdir().unwrap()).await;
    let s = "sessao-socket";
    assert!(is(
        &d.ask(&control_msg(s, SessionControl::Open)).await,
        Outcome::Applied,
        None,
        WireState::Active
    ));
    assert!(is(
        &d.ask(&control_msg(s, SessionControl::Open)).await,
        Outcome::Noop,
        None,
        WireState::Active
    ));
    assert!(is(
        &d.ask(&control_msg(s, SessionControl::Close)).await,
        Outcome::Applied,
        None,
        WireState::Closed
    ));
    assert!(is(
        &d.ask(&control_msg(s, SessionControl::Close)).await,
        Outcome::Noop,
        None,
        WireState::Closed
    ));
    assert!(is(
        &d.ask(&control_msg(s, SessionControl::Open)).await,
        Outcome::Rejected,
        Some(Reason::SessionClosed),
        WireState::Closed
    ));
    assert!(is(
        &d.ask(&control_msg("nunca", SessionControl::Close)).await,
        Outcome::Rejected,
        Some(Reason::SessionUnknown),
        WireState::Unknown
    ));
    d.stop().await;
}

#[tokio::test]
async fn request_cancel_is_scoped_to_its_session_over_socket() {
    let mut d = Daemon::start(tempfile::tempdir().unwrap()).await;
    let (a, b) = ("sessao-a-sock", "sessao-b-sock");
    d.ask(&control_msg(a, SessionControl::Open)).await;
    d.ask(&control_msg(b, SessionControl::Open)).await;
    let (msg_a, req_a) = begin(a, Uuid::new_v4());
    assert_eq!(d.ask(&msg_a).await.outcome, Outcome::Applied);
    let (msg_b, req_b) = begin(b, Uuid::new_v4());
    assert_eq!(d.ask(&msg_b).await.outcome, Outcome::Applied);

    // Cancelamento enviado para A com id de requisição de B: não alcança B.
    let cross = v12(
        KavoMessage::new(MessageType::Cancel, a, SOURCE).with_correlation_id(req_b.to_string())
    );
    assert!(is(
        &d.ask(&cross).await,
        Outcome::Rejected,
        Some(Reason::RequestUnknown),
        WireState::Active
    ));
    let end_b = control_msg(b, SessionControl::RequestEnd { request_id: req_b });
    assert!(is(
        &d.ask(&end_b).await,
        Outcome::Applied,
        None,
        WireState::Active
    ));

    // Cancelamento correto de A, duas vezes.
    let cancel_a = v12(
        KavoMessage::new(MessageType::Cancel, a, SOURCE).with_correlation_id(req_a.to_string())
    );
    assert_eq!(d.ask(&cancel_a).await.outcome, Outcome::Applied);
    assert!(is(
        &d.ask(&cancel_a).await,
        Outcome::Noop,
        None,
        WireState::Active
    ));
    d.stop().await;
}

#[tokio::test]
async fn invalid_control_and_inbound_results_get_error() {
    let mut d = Daemon::start(tempfile::tempdir().unwrap()).await;
    // Tipo de resposta enviado pelo cliente.
    let bogus = v12(KavoMessage::new(MessageType::ControlResult, "s", SOURCE)
        .with_payload(&ControlResult::new(
            ControlOp::Open,
            Outcome::Applied,
            WireState::Active,
        ))
        .unwrap());
    let reply = ipc::request(&d.socket, &bogus, settings())
        .await
        .expect("resposta");
    assert_eq!(reply.kind, MessageType::Error);
    // Operação desconhecida no payload.
    let mut raw = serde_json::to_value(control_msg("s", SessionControl::Status)).unwrap();
    raw["payload"]["op"] = serde_json::json!("NAO_EXISTE");
    let mut line = raw.to_string().into_bytes();
    line.push(b'\n');
    let stream = tokio::net::UnixStream::connect(&d.socket).await.unwrap();
    let (r, mut w) = stream.into_split();
    tokio::io::AsyncWriteExt::write_all(&mut w, &line)
        .await
        .unwrap();
    let mut reader = tokio::io::BufReader::new(r);
    let frame = ipc::read_frame(&mut reader, 65536)
        .await
        .unwrap()
        .expect("resposta");
    let reply = KavoMessage::from_json_slice(&frame).unwrap();
    assert_eq!(reply.kind, MessageType::Error);
    d.stop().await;
}

#[tokio::test]
async fn legacy_1_1_cancel_by_request_gets_no_control_reply() {
    let mut d = Daemon::start(tempfile::tempdir().unwrap()).await;
    let legacy = KavoMessage::new(MessageType::Cancel, "legado", SOURCE)
        .with_correlation_id(Uuid::new_v4().to_string());
    // Modo 1.1 segue sem resposta: a conexão fecha sem CONTROL_RESULT.
    assert!(ipc::request(&d.socket, &legacy, settings()).await.is_err());
    d.stop().await;
}

#[tokio::test]
async fn daemon_store_file_exists_after_control() {
    let mut d = Daemon::start(tempfile::tempdir().unwrap()).await;
    d.ask(&control_msg("s-db", SessionControl::Open)).await;
    d.stop().await;
    assert!(d.db.exists());
}

#[test]
fn session_control_requires_schema_1_2_and_valid_ids() {
    let op = SessionControl::Open;
    let v11 = KavoMessage::new(MessageType::SessionControl, "s", SOURCE)
        .with_payload(&op)
        .unwrap();
    let raw = serde_json::to_vec(&v11).unwrap();
    assert!(
        KavoMessage::from_json_slice(&raw).is_err(),
        "1.1 não transporta SESSION_CONTROL"
    );

    let nil = SessionControl::RequestEnd {
        request_id: Uuid::nil(),
    };
    let msg = control_msg("s", nil);
    let raw = serde_json::to_vec(&msg).unwrap();
    assert!(
        KavoMessage::from_json_slice(&raw).is_err(),
        "request_id nil recusado"
    );

    let bad_stream = SessionControl::RequestBegin {
        kind: RequestKind::SttStream,
        stream_id: Uuid::nil(),
    };
    let raw = serde_json::to_vec(&control_msg("s", bad_stream)).unwrap();
    assert!(
        KavoMessage::from_json_slice(&raw).is_err(),
        "stream_id nil recusado"
    );
}

#[test]
fn control_result_reply_correlates_and_roundtrips() {
    let req = control_msg("s", SessionControl::Status);
    let result = ControlResult::rejected(
        ControlOp::Status,
        Reason::SessionUnknown,
        WireState::Unknown,
    );
    let reply = result.reply_to(&req).unwrap();
    assert_eq!(reply.kind, MessageType::ControlResult);
    assert_eq!(
        reply.correlation_id.as_deref(),
        Some(req.message_id.as_str())
    );
    assert_eq!(reply.schema_version, "1.2");
    let back = ControlResult::from_value(reply.payload.as_ref().unwrap()).unwrap();
    assert_eq!(back, result);
}

#[test]
fn cancel_forms_are_validated_by_version() {
    let by_request = v12(KavoMessage::new(MessageType::Cancel, "s", SOURCE)
        .with_correlation_id(Uuid::new_v4().to_string()));
    assert!(KavoMessage::from_json_slice(&serde_json::to_vec(&by_request).unwrap()).is_ok());
    let legacy = KavoMessage::new(MessageType::Cancel, "s", SOURCE)
        .with_correlation_id(Uuid::new_v4().to_string());
    assert!(KavoMessage::from_json_slice(&serde_json::to_vec(&legacy).unwrap()).is_ok());
    let neither = v12(KavoMessage::new(MessageType::Cancel, "s", SOURCE));
    assert!(KavoMessage::from_json_slice(&serde_json::to_vec(&neither).unwrap()).is_err());
}

#[test]
fn control_result_rejects_unknown_wire_values() {
    let bad = serde_json::json!({"op": "OPEN", "outcome": "TALVEZ", "state": "ACTIVE"});
    assert!(ControlResult::from_value(&bad).is_err());
    let bad_state = serde_json::json!({"op": "OPEN", "outcome": "APPLIED", "state": "FALHO"});
    assert!(ControlResult::from_value(&bad_state).is_err());
}
