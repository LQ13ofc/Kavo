//! B1: valores enviados pelo cliente não aparecem em respostas de erro nem em logs.
//! Usa socket Unix real e captura real dos logs do tracing.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kavo::bus::EventBus;
use kavo::ipc::{self, IpcServer, IpcSettings};
use kavo::protocol::{KavoMessage, MessageType};
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::MakeWriter;

const SECRET: &str = "SEGREDO-CLIENTE-7731";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn envelope(kind: MessageType) -> Value {
    serde_json::to_value(KavoMessage::new(kind, "sessao", "cliente")).unwrap()
}

/// Quadros malformados em que o cliente coloca um valor reconhecível em cada campo.
fn hostile_frames() -> Vec<Vec<u8>> {
    let mut frames: Vec<Value> = Vec::new();
    let mut tipo = envelope(MessageType::Heartbeat);
    tipo["type"] = json!(SECRET);
    frames.push(tipo);
    let mut versao = envelope(MessageType::Heartbeat);
    versao["schema_version"] = json!(SECRET);
    frames.push(versao);
    let mut uuid = envelope(MessageType::Heartbeat);
    uuid["message_id"] = json!(SECRET);
    frames.push(uuid);
    let mut stt = envelope(MessageType::SttResult);
    stt["payload"] = json!({"stream_id": SECRET, "sequence": 1, "audio_start_ms": 0,
        "audio_end_ms": 1, "event": "NO_SPEECH"});
    frames.push(stt);
    let mut intent = envelope(MessageType::IntentDetected);
    intent["payload"] = json!({"interpretation_id": SECRET});
    frames.push(intent);
    let mut cancel = envelope(MessageType::Cancel);
    cancel["payload"] = json!({"stream_id": SECRET});
    frames.push(cancel);
    let mut sessao = envelope(MessageType::Heartbeat);
    sessao["session_id"] = json!(SECRET.repeat(10));
    frames.push(sessao);
    let mut erro = envelope(MessageType::Error);
    erro["error_code"] = json!(999);
    erro["error_message"] = json!(SECRET);
    frames.push(erro);
    frames
        .into_iter()
        .map(|mut v| {
            // Campo fora de esquema com o valor do cliente: deve ser ignorado, sem eco.
            v["campo_extra"] = json!(SECRET);
            let mut line = serde_json::to_vec(&v).unwrap();
            line.push(b'\n');
            line
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn client_values_never_appear_in_error_replies_or_logs() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);

    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    let path = dir.path().join("k.sock");
    let settings = IpcSettings {
        max_frame_bytes: 8192,
        request_timeout: Duration::from_secs(2),
        idle_timeout: Duration::from_secs(5),
    };
    let server = IpcServer::bind(&path, settings).unwrap();
    let bus = EventBus::new(16, Duration::from_secs(2));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(server.serve(bus, cancel.clone()));
    for _ in 0..50 {
        if UnixStream::connect(&path).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut replies = Vec::new();
    for frame in hostile_frames() {
        let stream = UnixStream::connect(&path).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        write_half.write_all(&frame).await.unwrap();
        let mut reader = BufReader::new(read_half);
        let line = ipc::read_frame(&mut reader, 8192).await.unwrap().unwrap();
        replies.push(String::from_utf8(line).unwrap());
    }

    cancel.cancel();
    handle.await.unwrap().unwrap();
    drop(guard);

    // Respostas: cada quadro malformado gera um ERROR com classe; nenhuma ecoa o valor do cliente.
    assert_eq!(replies.len(), 8);
    for reply in &replies {
        assert!(
            !reply.contains(SECRET),
            "resposta ecoou valor do cliente: {reply}"
        );
        assert!(reply.contains("\"ERROR\""), "{reply}");
        assert!(reply.contains("error_message"), "{reply}");
    }

    // Logs: nenhuma linha contém o valor do cliente nem o padrão de parser que o expunha.
    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(
        !logs.contains(SECRET),
        "log ecoou valor do cliente:\n{logs}"
    );
    assert!(
        !logs.contains("found `"),
        "log com caractere de parser:\n{logs}"
    );
    assert!(
        logs.contains("mensagem rejeitada"),
        "rejeições são registradas: {logs}"
    );
}

#[test]
fn error_text_does_not_echo_type_or_version_values() {
    let line = format!(
        r#"{{"type":"{SECRET}","schema_version":"1.1","message_id":"3f2b8c1e-9d4a-4b7e-8f21-6a5c0d9e7b10","session_id":"s","timestamp":1.0,"sender":"x"}}"#
    );
    let text = KavoMessage::from_json_slice(line.as_bytes())
        .unwrap_err()
        .to_string();
    assert!(!text.contains(SECRET), "{text}");

    let line = format!(
        r#"{{"type":"HEARTBEAT","schema_version":"{SECRET}","message_id":"3f2b8c1e-9d4a-4b7e-8f21-6a5c0d9e7b10","session_id":"s","timestamp":1.0,"sender":"x"}}"#
    );
    let text = KavoMessage::from_json_slice(line.as_bytes())
        .unwrap_err()
        .to_string();
    assert!(!text.contains(SECRET), "{text}");
}

#[test]
fn serde_errors_report_category_and_column_only() {
    use kavo::speech::SpeechPayload;
    let v = json!({"stream_id": SECRET, "sequence": 1, "audio_start_ms": 0, "audio_end_ms": 1, "event": "NO_SPEECH"});
    let text = SpeechPayload::from_value(&v).unwrap_err().to_string();
    assert!(!text.contains(SECRET), "{text}");
    assert!(
        text.contains("coluna"),
        "diagnóstico útil preservado: {text}"
    );
}
