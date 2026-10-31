//! Testes de durabilidade das admissões (D-31). Usam SQLite real em arquivo temporário.
//! "Reiniciar" = encerrar o monitor, fechar o banco e abrir tudo de novo a partir do arquivo.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use kavo::bus::EventBus;
use kavo::config::SpeechConfig;
use kavo::error::ErrorKind;
use kavo::monitor::{ContractMonitor, MonitorStats};
use kavo::protocol::{KavoMessage, MessageType};
use kavo::runtime::{Component, ComponentContext};
use kavo::store::{AdmissionRow, CommitOutcome, InterpretationRow, Store, sha256_hex};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const SESSION: &str = "sessao-duravel";

struct Ids {
    stream: Uuid,
    segment: Uuid,
    interp: Uuid,
}

fn ids() -> Ids {
    Ids {
        stream: Uuid::new_v4(),
        segment: Uuid::new_v4(),
        interp: Uuid::new_v4(),
    }
}

fn stt(stream: Uuid, seq: u64, segment: Uuid, rev: u32, state: &str, text: &str) -> KavoMessage {
    let mut m = KavoMessage::new(MessageType::SttResult, SESSION, "stt-fixture");
    m.payload = Some(json!({
        "stream_id": stream, "sequence": seq, "audio_start_ms": 0, "audio_end_ms": 100,
        "event": "TRANSCRIPT", "segment_id": segment, "revision": rev, "state": state, "text": text
    }));
    m
}

#[allow(clippy::too_many_arguments)]
fn intent(
    id: &Ids,
    rev: u32,
    source_rev: u32,
    state: &str,
    intent: &str,
    app: &str,
) -> KavoMessage {
    let mut m = KavoMessage::new(MessageType::IntentDetected, SESSION, "intent-fixture");
    m.payload = Some(json!({
        "interpretation_id": id.interp, "stream_id": id.stream, "segment_id": id.segment,
        "source_revision": source_rev, "revision": rev, "state": state,
        "engine": {"name": "fixture", "version": "0"},
        "intent": {"name": intent}, "parameters": {"application": app}
    }));
    m
}

/// Sobe um monitor sobre o banco em `path`, entrega `msgs`, drena a fila e encerra.
/// Retorna as estatísticas. Equivale a um ciclo de vida completo do núcleo.
async fn run_monitor(path: &Path, msgs: Vec<KavoMessage>) -> kavo::error::Result<MonitorStats> {
    let bus = EventBus::new(256, Duration::from_secs(2));
    let rx = bus.subscribe();
    let stats: Arc<Mutex<MonitorStats>> = Default::default();
    let store = Store::open(path)?;
    let monitor = ContractMonitor::open(rx, &SpeechConfig::default(), stats.clone(), store)?;
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(monitor.run(ComponentContext {
        cancel: cancel.clone(),
        bus: bus.clone(),
    }));
    for m in msgs {
        bus.publish(m).await.unwrap();
    }
    cancel.cancel();
    handle.await.unwrap().unwrap();
    let out = stats.lock().unwrap().clone();
    Ok(out)
}

fn db_path() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kavo.db");
    (dir, path)
}

fn adm_row(id: &Ids, rev: u32, key: &str) -> AdmissionRow {
    AdmissionRow {
        session_id: SESSION.into(),
        segment_id: id.segment,
        stream_id: id.stream,
        execution_key: key.into(),
        intent: "OPEN_APPLICATION".into(),
        interpretation: InterpretationRow {
            session_id: SESSION.into(),
            interpretation_id: id.interp,
            stream_id: id.stream,
            segment_id: id.segment,
            revision: rev,
            source_revision: 1,
            content_digest: sha256_hex(key),
            cancelled: false,
        },
    }
}

fn admit_flow(id: &Ids) -> Vec<KavoMessage> {
    vec![
        stt(id.stream, 1, id.segment, 0, "PARTIAL", "abre"),
        stt(id.stream, 2, id.segment, 1, "CONFIRMED", "abre o brave"),
        intent(id, 0, 1, "STABLE", "OPEN_APPLICATION", "brave"),
    ]
}

// 1 + 2: admitir, reiniciar, reenviar a mesma intenção: não é admitida de novo.
#[tokio::test]
async fn admitted_intent_is_not_admitted_again_after_restart() {
    let (_d, path) = db_path();
    let id = ids();
    let first = run_monitor(&path, admit_flow(&id)).await.unwrap();
    assert_eq!(first.admitted, 1, "primeira execução admite");

    // Reinício: novo processo abre o mesmo arquivo. Reenvio completo do fluxo.
    let second = run_monitor(&path, admit_flow(&id)).await.unwrap();
    assert_eq!(second.admitted, 0, "após reinício, nada é admitido de novo");
    assert_eq!(
        second.speech_rejected, 2,
        "stream aposentado não reabre: início e evento seguinte rejeitados"
    );

    // Mesma intenção com revisão nova também não admite.
    let third = run_monitor(
        &path,
        vec![intent(&id, 1, 1, "STABLE", "OPEN_APPLICATION", "brave")],
    )
    .await
    .unwrap();
    assert_eq!(third.admitted, 0);
    assert_eq!(Store::open(&path).unwrap().admission_count().unwrap(), 1);
}

// 3: revisão antiga depois de revisão mais nova, atravessando um reinício.
#[tokio::test]
async fn old_revision_after_newer_is_stale_across_restart() {
    let (_d, path) = db_path();
    let id = ids();
    let mut flow = admit_flow(&id);
    flow.push(intent(&id, 1, 1, "STABLE", "OPEN_APPLICATION", "brave"));
    let before = run_monitor(&path, flow).await.unwrap();
    assert_eq!(before.interpretation_applied, 2);

    let after = run_monitor(
        &path,
        vec![intent(&id, 0, 1, "STABLE", "OPEN_APPLICATION", "brave")],
    )
    .await
    .unwrap();
    assert_eq!(
        after.interpretation_rejected, 1,
        "revisão 0 depois da 1 é obsoleta"
    );
    assert_eq!(after.admitted, 0);
}

// 4: duas admissões simultâneas da mesma identidade, cada uma com sua conexão.
#[test]
fn concurrent_admissions_of_same_identity_admit_exactly_once() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    let id = ids();
    let row = adm_row(&id, 0, "chave-unica");
    let barrier = Arc::new(Barrier::new(12));
    let handles: Vec<_> = (0..12)
        .map(|_| {
            let path = path.clone();
            let row = row.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut store = Store::open(&path).unwrap();
                barrier.wait();
                store.commit_admission(&row, 1).unwrap()
            })
        })
        .collect();
    let outcomes: Vec<CommitOutcome> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let committed = outcomes
        .iter()
        .filter(|o| **o == CommitOutcome::Committed)
        .count();
    assert_eq!(committed, 1, "exatamente uma admissão: {outcomes:?}");
    assert_eq!(Store::open(&path).unwrap().admission_count().unwrap(), 1);
}

// 5: interrupção no meio de uma gravação: a transação não confirmada não existe depois.
#[test]
fn interrupted_write_leaves_no_partial_admission() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    {
        // Simula crash: BEGIN IMMEDIATE, insere, e o processo "morre" sem COMMIT.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.execute(
            "INSERT INTO admission_keys (session_id, execution_key, segment_id, stream_id,
                interpretation_id, intent, admitted_at_ms)
             VALUES ('s', 'k', ?1, ?2, ?3, 'X', 1)",
            rusqlite::params![
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string()
            ],
        )
        .unwrap();
        // conexão descartada sem COMMIT
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.admission_count().unwrap(),
        0,
        "gravação não confirmada não sobrevive"
    );
    assert!(store.integrity_ok().unwrap());
}

// 6: banco indisponível durante a operação: nada é admitido, o núcleo continua vivo.
#[tokio::test]
async fn database_failure_blocks_admission_fail_closed() {
    let (_d, path) = db_path();
    let bus = EventBus::new(256, Duration::from_secs(2));
    let rx = bus.subscribe();
    let stats: Arc<Mutex<MonitorStats>> = Default::default();
    let monitor = ContractMonitor::open(
        rx,
        &SpeechConfig::default(),
        stats.clone(),
        Store::open(&path).unwrap(),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(monitor.run(ComponentContext {
        cancel: cancel.clone(),
        bus: bus.clone(),
    }));

    let id = ids();
    // Falha induzida por outra conexão: a tabela de admissões some antes da confirmação.
    let sabotage = rusqlite::Connection::open(&path).unwrap();
    sabotage.execute_batch("DROP TABLE admission_keys").unwrap();

    for m in admit_flow(&id) {
        bus.publish(m).await.unwrap();
    }
    cancel.cancel();
    handle.await.unwrap().unwrap();
    let st = stats.lock().unwrap().clone();
    assert_eq!(st.admitted, 0, "sem gravação durável não há ADMIT");
    assert!(st.store_errors >= 1, "falha registrada");
    assert!(st.not_ready >= 1);
}

#[test]
fn corrupted_database_refuses_to_open() {
    let (_d, path) = db_path();
    std::fs::write(&path, b"isto nao e um banco sqlite, e tem mais de cem bytes para garantir que o cabecalho seja lido pelo motor de forma valida ok").unwrap();
    let err = Store::open(&path).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::StoreUnavailable);
}

#[test]
fn unsupported_schema_version_refuses_to_open() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE meta SET value = '99' WHERE key = 'schema_version'",
        [],
    )
    .unwrap();
    drop(conn);
    assert!(Store::open(&path).is_err());
}

#[test]
fn database_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
}

// 7: cancelar, reiniciar e verificar o estado.
#[tokio::test]
async fn cancel_survives_restart_and_blocks_new_admissions() {
    let (_d, path) = db_path();
    let id = ids();
    let mut flow = admit_flow(&id);
    let mut cancel = KavoMessage::new(MessageType::Cancel, SESSION, "cli");
    cancel.payload = Some(json!({"stream_id": id.stream}));
    flow.push(cancel);
    run_monitor(&path, flow).await.unwrap();

    {
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.stream_status(SESSION, &id.stream).unwrap().as_deref(),
            Some("CANCELLED")
        );
        assert_eq!(
            store
                .segment_status(SESSION, &id.segment)
                .unwrap()
                .as_deref(),
            Some("CANCELLED")
        );
        assert_eq!(
            store.admission_count().unwrap(),
            1,
            "chave admitida permanece consumida"
        );
    }

    // Após reinício, uma interpretação nova do mesmo segmento não é admitida.
    let other = Ids {
        stream: id.stream,
        segment: id.segment,
        interp: Uuid::new_v4(),
    };
    let st = run_monitor(
        &path,
        vec![intent(&other, 0, 1, "STABLE", "OPEN_APPLICATION", "outro")],
    )
    .await
    .unwrap();
    assert_eq!(st.admitted, 0);
    assert_eq!(Store::open(&path).unwrap().admission_count().unwrap(), 1);
}

// Supersede durável: A admitida, B substitui, A volta na mesma execução: AlreadyAdmitted.
// Depois de reinício o stream está aposentado: nada é admitido, e A continua consumida.
#[tokio::test]
async fn superseded_key_is_never_admitted_again() {
    let (_d, path) = db_path();
    let id = ids();
    let mut flow = admit_flow(&id); // A
    flow.push(intent(&id, 1, 1, "STABLE", "GET_TIME", "x")); // B substitui
    flow.push(intent(&id, 2, 1, "STABLE", "OPEN_APPLICATION", "brave")); // A de novo
    let first = run_monitor(&path, flow).await.unwrap();
    assert_eq!(first.admitted, 1);
    assert_eq!(first.superseded, 1);
    assert_eq!(first.already_admitted, 1, "A já foi consumida nesta sessão");

    let back = run_monitor(
        &path,
        vec![intent(&id, 3, 1, "STABLE", "OPEN_APPLICATION", "brave")],
    )
    .await
    .unwrap();
    assert_eq!(
        back.admitted, 0,
        "após reinício nada é admitido: stream aposentado"
    );
    assert_eq!(Store::open(&path).unwrap().admission_count().unwrap(), 2);
}

#[tokio::test]
async fn stream_identity_survives_restart_and_cannot_be_reopened() {
    let (_d, path) = db_path();
    let id = ids();
    run_monitor(
        &path,
        vec![stt(id.stream, 1, id.segment, 0, "PARTIAL", "a")],
    )
    .await
    .unwrap();
    let st = run_monitor(
        &path,
        vec![stt(id.stream, 1, id.segment, 0, "PARTIAL", "a")],
    )
    .await
    .unwrap();
    assert_eq!(st.speech_rejected, 1);
    assert_eq!(st.speech_applied, 0);
}

#[test]
fn admission_row_stores_digests_not_parameters() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let row = adm_row(&id, 0, "digest-da-chave");
    store.commit_admission(&row, 1).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    let dump: String = conn
        .query_row("SELECT group_concat(content_digest || execution_key) FROM interpretations, admission_keys", [], |r| r.get(0))
        .unwrap();
    assert!(!dump.contains("brave"));
}

#[test]
fn journal_side_files_are_private_from_creation() {
    use std::os::unix::fs::PermissionsExt;
    let (_d, path) = db_path();
    let store = Store::open(&path).unwrap();
    let mut store = store;
    store.commit_admission(&adm_row(&ids(), 0, "k"), 1).unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let p = PathBuf::from(format!("{}{}", path.display(), suffix));
        if let Ok(meta) = std::fs::metadata(&p) {
            assert_eq!(
                meta.permissions().mode() & 0o077,
                0,
                "{} aberto",
                p.display()
            );
        }
    }
}

#[test]
fn database_in_public_directory_is_refused_by_config() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut cfg = kavo::config::KavoConfig::default();
    cfg.store.path = Some(dir.path().join("kavo.db"));
    assert!(cfg.store_path().is_err());
}
