//! Testes de integração da fundação: configuração, protocolo, IPC real em socket Unix, runtime e lock.
//! Usam diretórios temporários próprios; nenhum estado global do processo é alterado.

use std::path::{Path, PathBuf};
use std::time::Duration;

use kavo::bus::EventBus;
use kavo::config::KavoConfig;
use kavo::error::ErrorKind;
use kavo::error::KavoError;
use kavo::instance::InstanceLock;
use kavo::ipc::{self, IpcServer, IpcSettings};
use kavo::protocol::{Delivery, KavoMessage, MessageType, error_code};
use kavo::runtime::{Component, ComponentContext, Supervisor, TaskOutcome};
use kavo::verification::Verification;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;

fn no_env(_: &str) -> Option<String> {
    None
}

fn settings() -> IpcSettings {
    IpcSettings {
        max_frame_bytes: 4096,
        request_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(5),
    }
}

/// Diretório temporário privado (0700), como o núcleo exige para o socket.
fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn socket_in(dir: &Path) -> PathBuf {
    dir.join("kavo-test.sock")
}

// ---------- configuração ----------

#[test]
fn defaults_are_valid() {
    KavoConfig::default()
        .validate()
        .expect("padrões devem ser válidos");
}

#[test]
fn missing_optional_file_falls_back_to_defaults() {
    let dir = private_tempdir();
    let cfg = KavoConfig::load_from(&dir.path().join("nao-existe.toml"), false, no_env).unwrap();
    assert_eq!(cfg, KavoConfig::default());
}

#[test]
fn missing_required_file_is_error() {
    let dir = private_tempdir();
    let err = KavoConfig::load_from(&dir.path().join("nao-existe.toml"), true, no_env).unwrap_err();
    assert!(matches!(err, KavoError::Io { .. }), "{err:?}");
}

#[test]
fn unknown_key_is_rejected() {
    let dir = private_tempdir();
    let path = dir.path().join("kavo.toml");
    std::fs::write(&path, "[logging]\nlevle = \"debug\"\n").unwrap();
    let err = KavoConfig::load_from(&path, true, no_env).unwrap_err();
    assert!(err.to_string().contains("levle"), "{err}");
}

#[test]
fn file_values_are_read() {
    let dir = private_tempdir();
    let path = dir.path().join("kavo.toml");
    std::fs::write(
        &path,
        "[runtime]\nshutdown_timeout_ms = 1234\n[logging]\njson = false\n",
    )
    .unwrap();
    let cfg = KavoConfig::load_from(&path, true, no_env).unwrap();
    assert_eq!(cfg.runtime.shutdown_timeout_ms, 1234);
    assert!(!cfg.logging.json);
    assert_eq!(cfg.logging.level, "info");
}

#[test]
fn env_overrides_file_and_invalid_env_is_error() {
    let mut cfg = KavoConfig::default();
    cfg.apply_env(|k| (k == "KAVO_LOG_LEVEL").then(|| "debug".to_owned()))
        .unwrap();
    assert_eq!(cfg.logging.level, "debug");

    let err = KavoConfig::default()
        .apply_env(|k| (k == "KAVO_LOG_JSON").then(|| "talvez".to_owned()))
        .unwrap_err();
    assert!(err.to_string().contains("KAVO_LOG_JSON"), "{err}");

    let err = KavoConfig::default()
        .apply_env(|k| (k == "KAVO_SHUTDOWN_TIMEOUT_MS").then(|| "-1".to_owned()))
        .unwrap_err();
    assert!(
        err.to_string().contains("KAVO_SHUTDOWN_TIMEOUT_MS"),
        "{err}"
    );
}

#[test]
fn invalid_values_are_rejected_at_validation() {
    let mut cfg = KavoConfig::default();
    cfg.runtime.shutdown_timeout_ms = 0;
    assert!(cfg.validate().is_err());

    let mut cfg = KavoConfig::default();
    cfg.ipc.max_frame_bytes = 10;
    assert!(cfg.validate().is_err());

    let mut cfg = KavoConfig::default();
    cfg.ipc.socket_path = Some(PathBuf::from("relativo.sock"));
    assert!(cfg.validate().is_err());

    let mut cfg = KavoConfig::default();
    cfg.logging.level = "[[[".to_owned();
    assert!(cfg.validate().is_err());
}

// ---------- Kavo Protocol ----------

const UUID_A: &str = "3f2b8c1e-9d4a-4b7e-8f21-6a5c0d9e7b10";

fn raw(json: &str) -> Vec<u8> {
    json.as_bytes().to_vec()
}

#[test]
fn message_roundtrip_is_one_json_line() {
    let msg = KavoMessage::new(MessageType::Heartbeat, "sessao-1", "teste");
    let line = msg.to_json_line().unwrap();
    assert_eq!(line.iter().filter(|&&b| b == b'\n').count(), 1);
    assert_eq!(line.last(), Some(&b'\n'));
    let back = KavoMessage::from_json_slice(&line[..line.len() - 1]).unwrap();
    assert_eq!(back, msg);
}

#[test]
fn wire_names_match_contract() {
    let msg = KavoMessage::new(MessageType::Heartbeat, "s", "kavo-core");
    let json: serde_json::Value = serde_json::from_slice(&msg.to_json_line().unwrap()).unwrap();
    assert_eq!(json["type"], "HEARTBEAT");
    assert_eq!(json["schema_version"], "1.1");
    assert!(json.get("message_id").is_some());
    assert!(json.get("hermes").is_none());
    assert!(
        json.get("transport_version").is_none(),
        "campo removido em 1.0-draft"
    );
}

#[test]
fn reserved_types_are_reported_as_unsupported_not_unknown() {
    let line = raw(&format!(
        r#"{{"type":"COMMAND_EXECUTED","schema_version":"1.0","message_id":"{UUID_A}","session_id":"s","timestamp":1.0,"sender":"x"}}"#
    ));
    let err = KavoMessage::from_json_slice(&line).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::UnsupportedMessageType);
    assert!(err.to_string().contains("reservado"));
}

#[test]
fn unknown_message_type_is_unsupported() {
    let line = raw(&format!(
        r#"{{"type":"TIPO_INVENTADO","schema_version":"1.0","message_id":"{UUID_A}","session_id":"s","timestamp":1.0,"sender":"x"}}"#
    ));
    assert_eq!(
        KavoMessage::from_json_slice(&line).unwrap_err().kind(),
        ErrorKind::UnsupportedMessageType
    );
}

#[test]
fn schema_version_rules() {
    for (version, ok) in [
        ("1.0", true),
        ("1.7", true),
        ("2.0", false),
        ("1", false),
        ("1.", false),
        ("1.x", false),
        ("v1.0", false),
    ] {
        let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
        msg.schema_version = version.to_owned();
        assert_eq!(msg.validate().is_ok(), ok, "versão {version}");
    }
    let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    msg.schema_version = "2.0".to_owned();
    assert_eq!(
        msg.validate().unwrap_err().kind(),
        ErrorKind::UnsupportedVersion
    );
}

#[test]
fn missing_required_field_is_rejected() {
    let line = raw(&format!(
        r#"{{"type":"HEARTBEAT","schema_version":"1.0","message_id":"{UUID_A}","session_id":"s","timestamp":1.0}}"#
    ));
    assert!(
        KavoMessage::from_json_slice(&line).is_err(),
        "sender ausente"
    );
}

#[test]
fn identifiers_must_be_uuid() {
    let line = raw(
        r#"{"type":"HEARTBEAT","schema_version":"1.0","message_id":"nao-uuid","session_id":"s","timestamp":1.0,"sender":"x"}"#,
    );
    assert!(KavoMessage::from_json_slice(&line).is_err());

    let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    msg.correlation_id = Some("nao-uuid".to_owned());
    assert!(msg.validate().is_err());
}

#[test]
fn semantic_violations_are_rejected() {
    let msg = KavoMessage::new(MessageType::Error, "s", "x");
    assert!(msg.validate().is_err(), "ERROR sem error_code");

    let msg = KavoMessage::new(MessageType::Cancel, "s", "x");
    assert!(msg.validate().is_err(), "CANCEL sem alvo (correlation_id)");

    let msg = KavoMessage::new(MessageType::Heartbeat, "", "x");
    assert!(msg.validate().is_err(), "session_id vazio");

    let msg = KavoMessage::new(MessageType::Heartbeat, "s", "bad\nsender");
    assert!(msg.validate().is_err(), "sender com controle");

    let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    msg.timestamp = f64::NAN;
    assert!(msg.validate().is_err());
}

#[test]
fn reply_links_to_request_message_id() {
    let req = KavoMessage::new(MessageType::Heartbeat, "sess", "cli");
    let rep = req.reply(MessageType::Heartbeat);
    assert_ne!(
        rep.message_id, req.message_id,
        "cada mensagem tem id próprio"
    );
    assert_eq!(rep.correlation_id.as_deref(), Some(req.message_id.as_str()));
    assert_eq!(rep.session_id, "sess");
    assert_eq!(rep.sender, "kavo-core");

    let err = req.error_reply(error_code::GENERIC_ERROR, "falha");
    assert_eq!(err.kind, MessageType::Error);
    assert_eq!(err.error_code, Some(1100));
    assert!(err.validate().is_ok());
}

#[test]
fn uuid_must_be_v4() {
    let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    msg.message_id = "6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_owned(); // UUID v1
    assert!(msg.validate().is_err());
    msg.message_id = "00000000-0000-0000-0000-000000000000".to_owned(); // nil
    assert!(msg.validate().is_err());
}

#[test]
fn error_code_only_allowed_in_error() {
    let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    msg.error_code = Some(1100);
    assert!(msg.validate().is_err());
}

#[test]
fn session_id_length_counts_characters() {
    let mut msg = KavoMessage::new(MessageType::Heartbeat, "s", "x");
    msg.session_id = "á".repeat(128);
    assert!(
        msg.validate().is_ok(),
        "128 caracteres acentuados são válidos"
    );
    msg.session_id = "á".repeat(129);
    assert!(msg.validate().is_err());
}

#[test]
fn delivery_classes() {
    assert_eq!(MessageType::Error.delivery(), Delivery::Guaranteed);
    assert_eq!(MessageType::Cancel.delivery(), Delivery::Guaranteed);
    assert_eq!(MessageType::Heartbeat.delivery(), Delivery::BestEffort);
}

#[test]
fn error_kinds_map_from_errors() {
    use kavo::error::KavoError;
    assert_eq!(
        KavoError::FrameTooLarge { limit: 1 }.kind(),
        ErrorKind::FrameTooLarge
    );
    assert_eq!(
        KavoError::Timeout(Duration::from_secs(1)).kind(),
        ErrorKind::Timeout
    );
    assert_eq!(
        KavoError::DeliveryFailed("x".into()).kind(),
        ErrorKind::DeliveryFailed
    );
    assert_eq!(
        KavoError::Config("x".into()).kind(),
        ErrorKind::ConfigInvalid
    );
    assert_eq!(ErrorKind::FrameTooLarge.as_str(), "FRAME_TOO_LARGE");
}

// ---------- barramento ----------

#[tokio::test]
async fn bus_guaranteed_message_waits_for_slow_subscriber() {
    let bus = EventBus::new(1, Duration::from_secs(2));
    let mut rx = bus.subscribe();
    bus.publish(KavoMessage::new(MessageType::Error, "a", "x").error_reply(1100, "primeira"))
        .await
        .unwrap();

    // Fila cheia: a segunda ERROR precisa esperar o consumidor, não ser descartada.
    let second = KavoMessage::new(MessageType::Error, "b", "x");
    let publisher = {
        let bus = bus.clone();
        tokio::spawn(async move { bus.publish(second).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(rx.recv().await.unwrap().session_id, "a");
    let report = publisher
        .await
        .unwrap()
        .expect("entrega garantida deve completar");
    assert_eq!(report.delivered, 1);
    assert_eq!(rx.recv().await.unwrap().session_id, "b");
}

#[tokio::test]
async fn bus_guaranteed_message_fails_explicitly_on_timeout() {
    let bus = EventBus::new(1, Duration::from_millis(50));
    let _rx = bus.subscribe(); // assinante que nunca consome
    bus.publish(KavoMessage::new(MessageType::Heartbeat, "a", "x"))
        .await
        .unwrap(); // enche a fila
    let err = bus
        .publish(KavoMessage::new(MessageType::Error, "b", "x").error_reply(1100, "x"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::DeliveryFailed);
}

#[tokio::test]
async fn bus_best_effort_drops_and_counts_without_error() {
    let bus = EventBus::new(1, Duration::from_millis(50));
    let mut rx = bus.subscribe();
    let first = bus
        .publish(KavoMessage::new(MessageType::Heartbeat, "a", "x"))
        .await
        .unwrap();
    assert_eq!(first.delivered, 1);
    let second = bus
        .publish(KavoMessage::new(MessageType::Heartbeat, "b", "x"))
        .await
        .unwrap();
    assert_eq!(second.dropped, 1);
    assert_eq!(rx.recv().await.unwrap().session_id, "a");
}

#[tokio::test]
async fn bus_removes_closed_subscribers_and_fans_out() {
    let bus = EventBus::new(4, Duration::from_millis(50));
    let mut alive = bus.subscribe();
    let dead = bus.subscribe();
    drop(dead);
    let report = bus
        .publish(KavoMessage::new(MessageType::Error, "a", "x").error_reply(1100, "x"))
        .await
        .unwrap();
    assert_eq!(report.delivered, 1);
    assert_eq!(alive.recv().await.unwrap().session_id, "a");
}

#[tokio::test]
async fn bus_guaranteed_without_subscriber_fails_explicitly() {
    let bus = EventBus::new(4, Duration::from_millis(50));
    let err = bus
        .publish(KavoMessage::new(MessageType::Cancel, "a", "x").with_correlation_id(UUID_A))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::DeliveryFailed);
}

// ---------- framing ----------

#[tokio::test]
async fn read_frame_handles_normal_eof_and_limits() {
    let data: &[u8] = b"uma linha\noutra\n";
    let mut reader = BufReader::new(data);
    assert_eq!(
        ipc::read_frame(&mut reader, 64).await.unwrap(),
        Some(b"uma linha".to_vec())
    );
    assert_eq!(
        ipc::read_frame(&mut reader, 64).await.unwrap(),
        Some(b"outra".to_vec())
    );
    assert_eq!(ipc::read_frame(&mut reader, 64).await.unwrap(), None);

    let big: &[u8] = b"0123456789\n";
    let mut reader = BufReader::new(big);
    assert!(matches!(
        ipc::read_frame(&mut reader, 5).await,
        Err(KavoError::FrameTooLarge { limit: 5 })
    ));

    let exact: &[u8] = b"12345\n";
    let mut reader = BufReader::new(exact);
    assert_eq!(
        ipc::read_frame(&mut reader, 5).await.unwrap(),
        Some(b"12345".to_vec())
    );

    let partial: &[u8] = b"sem fim";
    let mut reader = BufReader::new(partial);
    assert!(ipc::read_frame(&mut reader, 64).await.is_err());
}

// ---------- verificação ----------

#[test]
fn verification_serializes_with_status() {
    let json = serde_json::to_value(Verification::Unknown("timeout".to_owned())).unwrap();
    assert_eq!(json["status"], "UNKNOWN");
    assert_eq!(json["detail"], "timeout");
}

// ---------- lock de instância ----------

#[test]
fn instance_lock_prevents_second_instance() {
    let dir = private_tempdir();
    let path = dir.path().join("kavo.lock");
    let first = InstanceLock::acquire(&path).unwrap();
    let err = InstanceLock::acquire(&path).unwrap_err();
    assert!(matches!(err, KavoError::AlreadyRunning { .. }), "{err:?}");
    drop(first);
    InstanceLock::acquire(&path).expect("após liberar, nova instância deve obter o lock");
}

// ---------- supervisor ----------

struct Stops;
impl Component for Stops {
    fn name(&self) -> &'static str {
        "stops"
    }
    async fn run(self, ctx: ComponentContext) -> kavo::error::Result<()> {
        ctx.cancel.cancelled().await;
        Ok(())
    }
}

struct Panics;
impl Component for Panics {
    fn name(&self) -> &'static str {
        "panics"
    }
    async fn run(self, _ctx: ComponentContext) -> kavo::error::Result<()> {
        panic!("componente defeituoso")
    }
}

struct Hangs;
impl Component for Hangs {
    fn name(&self) -> &'static str {
        "hangs"
    }
    async fn run(self, _ctx: ComponentContext) -> kavo::error::Result<()> {
        // Ignora o cancelamento de propósito para testar o prazo de encerramento.
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    }
}

#[tokio::test]
async fn supervisor_isolates_panic_and_reports_timeout() {
    let mut sup = Supervisor::new(EventBus::new(8, Duration::from_secs(2)));
    sup.spawn(Stops);
    sup.spawn(Panics);
    sup.spawn(Hangs);

    let first = sup.next().await.expect("deve haver um relatório");
    assert_eq!(first.name, "panics");
    assert!(
        matches!(first.outcome, TaskOutcome::Panicked(_)),
        "{first:?}"
    );

    let reports = sup.shutdown(Duration::from_millis(200)).await;
    let stops = reports.iter().find(|r| r.name == "stops").unwrap();
    assert_eq!(stops.outcome, TaskOutcome::Stopped);
    let hangs = reports.iter().find(|r| r.name == "hangs").unwrap();
    assert_eq!(hangs.outcome, TaskOutcome::TimedOut);
}

// ---------- IPC real ----------

async fn start_server(
    path: &Path,
) -> (
    CancellationToken,
    tokio::task::JoinHandle<kavo::error::Result<()>>,
) {
    let server = IpcServer::bind(path, settings()).unwrap();
    let cancel = CancellationToken::new();
    let handle =
        tokio::spawn(server.serve(EventBus::new(16, Duration::from_secs(2)), cancel.clone()));
    // Aguarda o socket aceitar conexões antes de prosseguir.
    for _ in 0..50 {
        if UnixStream::connect(path).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (cancel, handle)
}

#[tokio::test]
async fn ping_succeeds_against_running_server() {
    let dir = private_tempdir();
    let path = socket_in(dir.path());
    let (cancel, handle) = start_server(&path).await;

    let status = ipc::ping(&path, settings()).await;
    assert!(status.is_success(), "{status:?}");

    cancel.cancel();
    handle.await.unwrap().unwrap();
    assert!(!path.exists(), "socket deve ser removido ao encerrar");
}

#[tokio::test]
async fn ping_reports_failed_when_nothing_listens() {
    let dir = private_tempdir();
    let status = ipc::ping(&socket_in(dir.path()), settings()).await;
    assert!(matches!(status, Verification::Failed(_)), "{status:?}");
}

#[tokio::test]
async fn malformed_input_gets_error_and_connection_survives() {
    let dir = private_tempdir();
    let path = socket_in(dir.path());
    let (cancel, handle) = start_server(&path).await;

    let stream = UnixStream::connect(&path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"isto nao e json\n").await.unwrap();
    let frame = ipc::read_frame(&mut reader, 4096).await.unwrap().unwrap();
    let reply = KavoMessage::from_json_slice(&frame).unwrap();
    assert_eq!(reply.kind, MessageType::Error);
    assert_eq!(reply.error_code, Some(error_code::GENERIC_ERROR));

    // A conexão continua utilizável após uma mensagem inválida.
    let hb = KavoMessage::new(MessageType::Heartbeat, "s", "teste");
    let hb_id = hb.message_id.clone();
    write_half
        .write_all(&hb.to_json_line().unwrap())
        .await
        .unwrap();
    let frame = ipc::read_frame(&mut reader, 4096).await.unwrap().unwrap();
    let reply = KavoMessage::from_json_slice(&frame).unwrap();
    assert_eq!(reply.kind, MessageType::Heartbeat);
    assert_eq!(reply.correlation_id.as_deref(), Some(hb_id.as_str()));

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn oversized_frame_gets_error_then_connection_closes() {
    let dir = private_tempdir();
    let path = socket_in(dir.path());
    let (cancel, handle) = start_server(&path).await;

    let stream = UnixStream::connect(&path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let huge = vec![b'a'; 8192];
    let _ = write_half.write_all(&huge).await;
    let _ = write_half.write_all(b"\n").await;

    let frame = ipc::read_frame(&mut reader, 4096).await.unwrap().unwrap();
    let reply = KavoMessage::from_json_slice(&frame).unwrap();
    assert_eq!(reply.kind, MessageType::Error);
    assert!(
        ipc::read_frame(&mut reader, 4096).await.unwrap().is_none(),
        "conexão deve fechar"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn bind_replaces_stale_socket_file() {
    let dir = private_tempdir();
    let path = socket_in(dir.path());
    // Socket órfão real: um listener descartado deixa o arquivo no disco.
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    assert!(path.exists());
    let (cancel, handle) = start_server(&path).await;
    assert!(ipc::ping(&path, settings()).await.is_success());
    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn socket_is_private_to_owner() {
    use std::os::unix::fs::PermissionsExt;
    let dir = private_tempdir();
    let path = socket_in(dir.path());
    let (cancel, handle) = start_server(&path).await;
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    cancel.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn bind_refuses_to_remove_regular_file() {
    let dir = private_tempdir();
    let path = dir.path().join("importante.txt");
    std::fs::write(&path, b"dado do usuario").unwrap();
    let err = IpcServer::bind(&path, settings()).unwrap_err();
    assert!(matches!(err, KavoError::Config(_)), "{err:?}");
    assert_eq!(std::fs::read(&path).unwrap(), b"dado do usuario");
}

#[tokio::test]
async fn bind_refuses_socket_in_public_directory() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let err = IpcServer::bind(&socket_in(dir.path()), settings()).unwrap_err();
    assert!(matches!(err, KavoError::Config(_)), "{err:?}");
}
