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

// ---------- M1: integridade monotônica das revisões (correção pós-revisão) ----------

fn row_for_test(id: &Ids, rev: u32, source_rev: u32, content: &str) -> InterpretationRow {
    InterpretationRow {
        session_id: SESSION.into(),
        interpretation_id: id.interp,
        stream_id: id.stream,
        segment_id: id.segment,
        revision: rev,
        source_revision: source_rev,
        content_digest: sha256_hex(content),
        cancelled: false,
    }
}

fn durable_digest(path: &Path, interp: &Uuid) -> (i64, String) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT revision, content_digest FROM interpretations WHERE interpretation_id = ?1",
        rusqlite::params![interp.to_string()],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .unwrap()
}

// Store: revisão antiga não altera digest; igual com conteúdo diferente é conflito; reinício preserva.
#[test]
fn store_rejects_stale_and_conflicting_revisions_and_survives_restart() {
    use kavo::store::InterpWrite;
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let r1 = row_for_test(&id, 1, 1, "conteudo-r1");
    assert_eq!(
        store.upsert_interpretation(&r1, 1).unwrap(),
        InterpWrite::Inserted
    );
    let before = durable_digest(&path, &id.interp);

    // Revisão menor: não altera.
    let r0 = row_for_test(&id, 0, 1, "conteudo-r0-antigo");
    assert!(matches!(
        store.upsert_interpretation(&r0, 2).unwrap(),
        InterpWrite::Stale(_)
    ));
    assert_eq!(durable_digest(&path, &id.interp), before);

    // Mesma revisão, conteúdo diferente: conflito, não altera.
    let r1_diff = row_for_test(&id, 1, 1, "conteudo-r1-outro");
    assert!(matches!(
        store.upsert_interpretation(&r1_diff, 3).unwrap(),
        InterpWrite::Conflict(_)
    ));
    assert_eq!(durable_digest(&path, &id.interp), before);

    // Mesma revisão, mesmo conteúdo: reenvio legítimo, sem mudança.
    assert_eq!(
        store.upsert_interpretation(&r1, 4).unwrap(),
        InterpWrite::Unchanged
    );

    // Reinício: as mesmas regras valem com o estado recarregado.
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert!(matches!(
        store.upsert_interpretation(&r0, 5).unwrap(),
        InterpWrite::Stale(_)
    ));
    assert!(matches!(
        store.upsert_interpretation(&r1_diff, 6).unwrap(),
        InterpWrite::Conflict(_)
    ));
    assert_eq!(
        store.upsert_interpretation(&r1, 7).unwrap(),
        InterpWrite::Unchanged
    );
    assert_eq!(durable_digest(&path, &id.interp), before);

    // Revisão maior é aceita e persistida.
    let r2 = row_for_test(&id, 2, 1, "conteudo-r2");
    assert_eq!(
        store.upsert_interpretation(&r2, 8).unwrap(),
        InterpWrite::Updated
    );
    assert_eq!(durable_digest(&path, &id.interp).0, 2);
}

// Store: após cancelamento, revisão nova é conflito; origem não pode regredir.
#[test]
fn store_blocks_newer_revision_after_cancel_and_source_regression() {
    use kavo::store::InterpWrite;
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    // Stream precisa estar OPEN para o cancelamento alcançar interpretações.
    store.open_stream(SESSION, &id.stream, 0).unwrap();
    store
        .upsert_interpretation(&row_for_test(&id, 0, 2, "a"), 1)
        .unwrap();
    store.cancel_stream(SESSION, &id.stream, 2).unwrap();
    assert!(matches!(
        store
            .upsert_interpretation(&row_for_test(&id, 1, 2, "b"), 3)
            .unwrap(),
        InterpWrite::Conflict(_)
    ));
    // Sem cancelamento, origem regredindo também é conflito.
    let (_d2, path2) = db_path();
    let mut store2 = Store::open(&path2).unwrap();
    store2
        .upsert_interpretation(&row_for_test(&id, 0, 2, "a"), 1)
        .unwrap();
    assert!(matches!(
        store2
            .upsert_interpretation(&row_for_test(&id, 1, 1, "b"), 2)
            .unwrap(),
        InterpWrite::Conflict(_)
    ));
}

// Admissão: revisão rejeitada não consome a chave; rollback não deixa a interpretação alterada.
#[test]
fn admission_with_stale_interpretation_consumes_nothing_and_rolls_back() {
    use kavo::store::{CommitOutcome, InterpWrite};
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    // Admissão com revisão 0 cria a linha.
    let mut first = adm_row(&id, 0, "chave-m1");
    first.interpretation = row_for_test(&id, 0, 1, "r0");
    assert_eq!(
        store.commit_admission(&first, 1).unwrap(),
        CommitOutcome::Committed
    );

    // Outra chave com revisão 1: rejeitada por revisão obsoleta? Não: 1 > 0 é aceita.
    // Para testar rejeição, primeiro grava a revisão 2 diretamente.
    store
        .upsert_interpretation(&row_for_test(&id, 2, 1, "r2"), 2)
        .unwrap();
    let mut stale = adm_row(&id, 0, "chave-m1-nova");
    stale.interpretation = row_for_test(&id, 1, 1, "r1-antiga");
    let out = store.commit_admission(&stale, 3).unwrap();
    assert!(
        matches!(out, CommitOutcome::InterpretationRejected(ref s) if s.revision == 2),
        "{out:?}"
    );
    assert_eq!(store.admission_count().unwrap(), 1, "chave não consumida");

    // Rollback atômico: chave já consumida + revisão maior -> nenhuma das duas gravada.
    let mut collide = adm_row(&id, 0, "chave-m1");
    collide.interpretation = row_for_test(&id, 3, 1, "r3");
    assert_eq!(
        store.commit_admission(&collide, 4).unwrap(),
        CommitOutcome::AlreadyConsumed
    );
    assert_eq!(
        durable_digest(&path, &id.interp).0,
        2,
        "revisão 3 não foi gravada pelo commit rejeitado"
    );
    let _ = InterpWrite::Unchanged;
}

// Monitor + reinício: o cenário original. Revisão 0 PROVISIONAL, fim do stream, revisão 0 STABLE com outro conteúdo.
fn m1_flow_provisional(id: &Ids) -> Vec<KavoMessage> {
    vec![
        stt(id.stream, 1, id.segment, 0, "PARTIAL", "abr"),
        intent(id, 0, 0, "PROVISIONAL", "OPEN_APPLICATION", "brave"),
        {
            let mut m = KavoMessage::new(MessageType::SttResult, SESSION, "stt-fixture");
            m.payload = Some(
                json!({"stream_id": id.stream, "sequence": 2, "audio_start_ms": 0,
                "audio_end_ms": 9, "event": "STREAM_ENDED"}),
            );
            m
        },
    ]
}

/// Digest durável da revisão 0 após o fluxo provisório (linha de referência).
async fn reference_digest(id: &Ids) -> (i64, String) {
    let (_d, path) = db_path();
    run_monitor(&path, m1_flow_provisional(id)).await.unwrap();
    durable_digest(&path, &id.interp)
}

// Cenário original, MESMA execução: stream termina (memória esquecida) e a revisão 0 chega com
// conteúdo diferente. Antes da correção, o upsert sobrescrevia o digest durável.
#[tokio::test]
async fn m1_same_run_after_stream_end_does_not_overwrite_durable_revision() {
    let id = ids();
    let expected = reference_digest(&id).await;
    let (_d, path) = db_path();
    let mut flow = m1_flow_provisional(&id);
    flow.push(intent(&id, 0, 0, "STABLE", "OPEN_APPLICATION", "outro-app"));
    let st = run_monitor(&path, flow).await.unwrap();
    assert_eq!(st.admitted, 0);
    assert_eq!(
        durable_digest(&path, &id.interp),
        expected,
        "digest durável não pode mudar"
    );
    assert!(
        st.interpretation_rejected >= 1,
        "revisão 0 diferente é recusada: {st:?}"
    );
}

// Mesmo cenário com reinício entre o fim do stream e a revisão 0 conflitante.
#[tokio::test]
async fn m1_restart_then_conflicting_revision_keeps_durable_digest() {
    let id = ids();
    let expected = reference_digest(&id).await;
    let (_d, path) = db_path();
    run_monitor(&path, m1_flow_provisional(&id)).await.unwrap();
    assert_eq!(durable_digest(&path, &id.interp), expected);
    // Reinício: conteúdo diferente com a mesma revisão 0.
    let st = run_monitor(
        &path,
        vec![intent(&id, 0, 0, "STABLE", "OPEN_APPLICATION", "outro-app")],
    )
    .await
    .unwrap();
    assert_eq!(st.admitted, 0);
    assert_eq!(
        durable_digest(&path, &id.interp),
        expected,
        "reinício não permite sobrescrita"
    );
}

// Reenvio legítimo idêntico após reinício é duplicidade, sem erro e sem alterar nada.
#[tokio::test]
async fn m1_legitimate_resend_after_restart_is_duplicate() {
    let id = ids();
    let expected = reference_digest(&id).await;
    let (_d, path) = db_path();
    run_monitor(&path, m1_flow_provisional(&id)).await.unwrap();
    let st = run_monitor(
        &path,
        vec![intent(
            &id,
            0,
            0,
            "PROVISIONAL",
            "OPEN_APPLICATION",
            "brave",
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        st.interpretation_duplicate, 1,
        "reenvio legítimo é duplicidade: {st:?}"
    );
    assert_eq!(st.interpretation_rejected, 0);
    assert_eq!(durable_digest(&path, &id.interp), expected);
}

// ---------- B2: conflito explícito na chave de admissão ----------

#[test]
fn unexpected_constraint_is_not_treated_as_duplicate() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    // Trigger de teste: aborta inserção com intenção "BOOM" (constraint inesperada, não PRIMARY KEY).
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER boom BEFORE INSERT ON admission_keys WHEN NEW.intent = 'BOOM'
             BEGIN SELECT RAISE(ABORT, 'constraint de teste'); END;",
        )
        .unwrap();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let mut bad = adm_row(&id, 0, "chave-boom");
    bad.intent = "BOOM".into();
    let err = store.commit_admission(&bad, 1).unwrap_err();
    assert!(err.to_string().contains("inesperada"), "{err}");
    assert_eq!(store.admission_count().unwrap(), 0, "nada consumido");
    // A mesma chave, com intenção válida, ainda pode ser admitida: não foi "consumida" pela falha.
    let ok = adm_row(&id, 0, "chave-boom");
    assert_eq!(
        store.commit_admission(&ok, 2).unwrap(),
        kavo::store::CommitOutcome::Committed
    );
}

#[test]
fn legitimate_duplicate_key_is_consumed_not_error() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let row = adm_row(&id, 0, "chave-dup");
    assert_eq!(
        store.commit_admission(&row, 1).unwrap(),
        kavo::store::CommitOutcome::Committed
    );
    assert_eq!(
        store.commit_admission(&row, 2).unwrap(),
        kavo::store::CommitOutcome::AlreadyConsumed
    );
}

// B2: trigger que IGNORA a inserção (RAISE(IGNORE)) não pode virar "chave já consumida".
#[test]
fn silently_ignored_insert_is_not_treated_as_duplicate() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER ignora BEFORE INSERT ON admission_keys WHEN NEW.intent = 'IGNORA'
             BEGIN SELECT RAISE(IGNORE); END;",
        )
        .unwrap();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let mut row = adm_row(&id, 0, "chave-ignorada");
    row.intent = "IGNORA".into();
    let result = store.commit_admission(&row, 1);
    assert!(
        result.is_err(),
        "inserção sem efeito não é admissão nem duplicidade: {result:?}"
    );
    assert_eq!(store.admission_count().unwrap(), 0);
}

// ---------- Auditoria INSERT OR IGNORE: streams (open_stream e tombstone de cancelamento) ----------

fn sql_trigger(path: &Path, sql: &str) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}

fn stream_row_exists(path: &Path, stream: &Uuid) -> bool {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM streams WHERE stream_id = ?1",
        rusqlite::params![stream.to_string()],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
        == 1
}

// Duplicidade legítima: primeira abertura é Ok(true); repetir é Ok(false); após reinício continua Ok(false).
#[test]
fn open_stream_duplicate_is_legitimate_and_survives_restart() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    assert!(store.open_stream(SESSION, &id.stream, 1).unwrap());
    assert!(!store.open_stream(SESSION, &id.stream, 2).unwrap());
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert!(
        !store.open_stream(SESSION, &id.stream, 3).unwrap(),
        "stream gravado não reabre"
    );
    // Mesmo stream_id em outra sessão é outro stream.
    assert!(store.open_stream("outra-sessao", &id.stream, 4).unwrap());
}

// Constraint inesperada (trigger RAISE(ABORT)) em open_stream: erro, nunca "já gravado".
#[test]
fn open_stream_unexpected_constraint_is_error() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    sql_trigger(
        &path,
        "CREATE TRIGGER falha_abre BEFORE INSERT ON streams
        WHEN NEW.status = 'OPEN' BEGIN SELECT RAISE(ABORT, 'teste'); END;",
    );
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let r = store.open_stream(SESSION, &id.stream, 1);
    assert!(
        r.is_err(),
        "constraint inesperada não pode virar duplicidade: {r:?}"
    );
    assert!(!stream_row_exists(&path, &id.stream));
}

// Inserção ignorada por trigger (RAISE(IGNORE)) em open_stream: erro, sem linha.
#[test]
fn open_stream_silently_ignored_insert_is_error() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    sql_trigger(
        &path,
        "CREATE TRIGGER ignora_abre BEFORE INSERT ON streams
        WHEN NEW.status = 'OPEN' BEGIN SELECT RAISE(IGNORE); END;",
    );
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let r = store.open_stream(SESSION, &id.stream, 1);
    assert!(r.is_err(), "inserção sem efeito não é 'já gravado': {r:?}");
    assert!(!stream_row_exists(&path, &id.stream));
}

// Cancelamento repetido: idempotente, estado CANCELLED persiste após reinício.
#[test]
fn cancel_stream_repeated_is_idempotent_and_survives_restart() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    store.open_stream(SESSION, &id.stream, 1).unwrap();
    store.cancel_stream(SESSION, &id.stream, 2).unwrap();
    store.cancel_stream(SESSION, &id.stream, 3).unwrap();
    assert_eq!(
        store.stream_status(SESSION, &id.stream).unwrap().as_deref(),
        Some("CANCELLED")
    );
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.stream_status(SESSION, &id.stream).unwrap().as_deref(),
        Some("CANCELLED")
    );
}

// Tombstone com constraint inesperada: cancelamento não gravado é erro, nada fica pela metade.
#[test]
fn cancel_tombstone_unexpected_constraint_is_error() {
    let (_d, path) = db_path();
    drop(Store::open(&path).unwrap());
    sql_trigger(
        &path,
        "CREATE TRIGGER ignora_tomb BEFORE INSERT ON streams
        WHEN NEW.status = 'CANCELLED' BEGIN SELECT RAISE(IGNORE); END;",
    );
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let r = store.cancel_stream(SESSION, &id.stream, 1);
    assert!(
        r.is_err(),
        "tombstone não gravada não pode ser sucesso: {r:?}"
    );
    assert!(!stream_row_exists(&path, &id.stream));
}

// Cancelamento de stream desconhecido persiste aposentadoria: após reinício, o id não reabre.
#[tokio::test]
async fn cancel_before_start_tombstone_survives_restart() {
    let (_d, path) = db_path();
    let id = ids();
    let mut cancel = KavoMessage::new(MessageType::Cancel, SESSION, "cli");
    cancel.payload = Some(json!({"stream_id": id.stream}));
    run_monitor(&path, vec![cancel]).await.unwrap();
    assert!(stream_row_exists(&path, &id.stream), "tombstone gravada");
    let st = run_monitor(
        &path,
        vec![stt(id.stream, 1, id.segment, 0, "PARTIAL", "a")],
    )
    .await
    .unwrap();
    assert_eq!(
        st.speech_applied, 0,
        "stream cancelado antes de começar não reabre"
    );
    assert_eq!(st.speech_rejected, 1);
}

// Falha de gravação do cancelamento no monitor: nada muda em memória; o stream segue como estava.
#[tokio::test]
async fn failed_cancel_persistence_changes_nothing_in_memory() {
    let (_d, path) = db_path();
    let id = ids();
    drop(Store::open(&path).unwrap());
    sql_trigger(
        &path,
        "CREATE TRIGGER falha_cancel BEFORE UPDATE OF status ON streams
        WHEN NEW.status = 'CANCELLED' BEGIN SELECT RAISE(ABORT, 'teste'); END;",
    );
    let mut flow = vec![stt(id.stream, 1, id.segment, 0, "PARTIAL", "a")];
    let mut cancel = KavoMessage::new(MessageType::Cancel, SESSION, "cli");
    cancel.payload = Some(json!({"stream_id": id.stream}));
    flow.push(cancel);
    flow.push(stt(id.stream, 2, id.segment, 1, "PARTIAL", "a b"));
    let st = run_monitor(&path, flow).await.unwrap();
    assert_eq!(st.store_errors, 1, "falha registrada");
    assert_eq!(
        st.speech_applied, 2,
        "stream não foi cancelado em memória sem gravação"
    );
}

// Atomicidade de commit_admission: a interpretação é atualizada (revisão 5) e a chave falha por
// constraint inesperada. A transação inteira é descartada: a durável continua na revisão anterior.
#[test]
fn admission_update_and_key_failure_roll_back_together() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    let mut first = adm_row(&id, 0, "chave-base");
    first.interpretation = row_for_test(&id, 2, 1, "r2");
    assert_eq!(
        store.commit_admission(&first, 1).unwrap(),
        kavo::store::CommitOutcome::Committed
    );
    drop(store);
    sql_trigger(
        &path,
        "CREATE TRIGGER falha_chave BEFORE INSERT ON admission_keys
        WHEN NEW.intent = 'FALHA' BEGIN SELECT RAISE(ABORT, 'teste'); END;",
    );
    let mut store = Store::open(&path).unwrap();
    let mut bad = adm_row(&id, 0, "chave-nova");
    bad.intent = "FALHA".into();
    bad.interpretation = row_for_test(&id, 5, 1, "r5");
    assert!(store.commit_admission(&bad, 2).is_err());
    assert_eq!(
        durable_digest(&path, &id.interp).0,
        2,
        "atualização de interpretação revertida"
    );
    assert_eq!(
        store.admission_count().unwrap(),
        1,
        "nenhuma chave nova consumida"
    );
}

// ---------- Semântica final de CANCEL por estado (decisão aprovada) ----------

use kavo::store::{CancelOutcome, StreamDbStatus};

/// Fotografia de tudo que um CANCEL poderia tocar: stream, interpretação e admissão do segmento.
fn snapshot_rows(
    path: &Path,
    id: &Ids,
) -> (Option<String>, Option<i64>, Option<String>, Option<String>) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let stream: Option<String> = conn
        .query_row(
            "SELECT status FROM streams WHERE stream_id = ?1",
            rusqlite::params![id.stream.to_string()],
            |r| r.get(0),
        )
        .ok();
    let interp: Option<i64> = conn
        .query_row(
            "SELECT cancelled FROM interpretations WHERE interpretation_id = ?1",
            rusqlite::params![id.interp.to_string()],
            |r| r.get(0),
        )
        .ok();
    let seg: Option<String> = conn
        .query_row(
            "SELECT status FROM segment_admission WHERE segment_id = ?1",
            rusqlite::params![id.segment.to_string()],
            |r| r.get(0),
        )
        .ok();
    let key: Option<String> = conn
        .query_row(
            "SELECT execution_key FROM admission_keys WHERE segment_id = ?1",
            rusqlite::params![id.segment.to_string()],
            |r| r.get(0),
        )
        .ok();
    (stream, interp, seg, key)
}

/// Stream com interpretação e admissão já gravadas, no estado inicial OPEN.
fn open_with_admission(path: &Path) -> Ids {
    let id = ids();
    let mut store = Store::open(path).unwrap();
    store.open_stream(SESSION, &id.stream, 1).unwrap();
    let mut adm = adm_row(&id, 0, "chave-cancel");
    adm.interpretation = row_for_test(&id, 0, 1, "r0");
    assert_eq!(
        store.commit_admission(&adm, 2).unwrap(),
        kavo::store::CommitOutcome::Committed
    );
    id
}

// 1. OPEN -> CANCEL: stream, interpretação e admissão cancelados; chave permanece consumida.
#[test]
fn cancel_open_stream_cancels_everything_associated() {
    let (_d, path) = db_path();
    let id = open_with_admission(&path);
    let mut store = Store::open(&path).unwrap();
    assert_eq!(
        store.cancel_stream(SESSION, &id.stream, 3).unwrap(),
        CancelOutcome::CancelledOpen
    );
    let (stream, interp, seg, key) = snapshot_rows(&path, &id);
    assert_eq!(stream.as_deref(), Some("CANCELLED"));
    assert_eq!(interp, Some(1));
    assert_eq!(seg.as_deref(), Some("CANCELLED"));
    assert!(key.is_some(), "chave não é liberada");
    // 7. Reinício preserva o resultado.
    drop(store);
    let _ = Store::open(&path).unwrap();
    assert_eq!(snapshot_rows(&path, &id).0.as_deref(), Some("CANCELLED"));
}

// 2. ENDED -> CANCEL: nenhuma linha muda (comparação byte a byte da fotografia).
#[test]
fn cancel_after_natural_end_is_complete_noop() {
    let (_d, path) = db_path();
    let id = open_with_admission(&path);
    let mut store = Store::open(&path).unwrap();
    store
        .close_stream(SESSION, &id.stream, StreamDbStatus::Ended, 3)
        .unwrap();
    let before = snapshot_rows(&path, &id);
    assert_eq!(before.0.as_deref(), Some("ENDED"));
    let outcome = store.cancel_stream(SESSION, &id.stream, 4).unwrap();
    assert_eq!(outcome, CancelOutcome::AlreadyTerminal("ENDED".to_owned()));
    assert_eq!(
        snapshot_rows(&path, &id),
        before,
        "histórico de stream ENDED não é reescrito"
    );
    assert_eq!(before.1, Some(0), "interpretação não recebe cancelled = 1");
    assert_eq!(
        before.2.as_deref(),
        Some("ADMITTED"),
        "admissão continua ADMITTED"
    );
    // 7. Reinício.
    drop(store);
    let _ = Store::open(&path).unwrap();
    assert_eq!(snapshot_rows(&path, &id), before);
}

// 3. CANCELLED -> CANCEL: idempotente, sem mudança.
#[test]
fn repeated_cancel_is_idempotent_noop() {
    let (_d, path) = db_path();
    let id = open_with_admission(&path);
    let mut store = Store::open(&path).unwrap();
    assert_eq!(
        store.cancel_stream(SESSION, &id.stream, 3).unwrap(),
        CancelOutcome::CancelledOpen
    );
    let before = snapshot_rows(&path, &id);
    assert_eq!(
        store.cancel_stream(SESSION, &id.stream, 4).unwrap(),
        CancelOutcome::AlreadyTerminal("CANCELLED".to_owned())
    );
    assert_eq!(snapshot_rows(&path, &id), before);
    drop(store);
    let _ = Store::open(&path).unwrap();
    assert_eq!(snapshot_rows(&path, &id), before);
}

// 4 e 5. FAILED e TIMED_OUT -> CANCEL: estado preservado.
#[test]
fn cancel_preserves_failed_and_timed_out() {
    for (status, label) in [
        (StreamDbStatus::Failed, "FAILED"),
        (StreamDbStatus::TimedOut, "TIMED_OUT"),
    ] {
        let (_d, path) = db_path();
        let id = open_with_admission(&path);
        let mut store = Store::open(&path).unwrap();
        store.close_stream(SESSION, &id.stream, status, 3).unwrap();
        let before = snapshot_rows(&path, &id);
        assert_eq!(
            store.cancel_stream(SESSION, &id.stream, 4).unwrap(),
            CancelOutcome::AlreadyTerminal(label.to_owned())
        );
        assert_eq!(snapshot_rows(&path, &id), before, "{label} preservado");
        drop(store);
        let _ = Store::open(&path).unwrap();
        assert_eq!(snapshot_rows(&path, &id), before, "{label} após reinício");
    }
}

// 6 e 7. Stream desconhecido -> tombstone durável, que sobrevive ao reinício e impede abertura.
#[test]
fn unknown_stream_gets_durable_tombstone() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    assert_eq!(
        store.cancel_stream(SESSION, &id.stream, 1).unwrap(),
        CancelOutcome::Tombstoned
    );
    assert_eq!(
        store.stream_status(SESSION, &id.stream).unwrap().as_deref(),
        Some("CANCELLED")
    );
    assert_eq!(
        store.cancel_stream(SESSION, &id.stream, 2).unwrap(),
        CancelOutcome::AlreadyTerminal("CANCELLED".to_owned())
    );
    drop(store);
    let mut store = Store::open(&path).unwrap();
    assert!(
        !store.open_stream(SESSION, &id.stream, 3).unwrap(),
        "tombstone impede abertura"
    );
    assert_eq!(
        store.stream_status(SESSION, &id.stream).unwrap().as_deref(),
        Some("CANCELLED")
    );
}

// 8. Falha no meio do cancelamento (admissão falha depois do stream e da interpretação): rollback total.
#[test]
fn cancel_failure_mid_transaction_leaves_no_partial_state() {
    let (_d, path) = db_path();
    let id = open_with_admission(&path);
    sql_trigger(
        &path,
        "CREATE TRIGGER falha_seg BEFORE UPDATE OF status ON segment_admission
        WHEN NEW.status = 'CANCELLED' BEGIN SELECT RAISE(ABORT, 'teste'); END;",
    );
    let before = snapshot_rows(&path, &id);
    let mut store = Store::open(&path).unwrap();
    assert!(store.cancel_stream(SESSION, &id.stream, 3).is_err());
    assert_eq!(snapshot_rows(&path, &id), before, "nenhum estado parcial");
    assert_eq!(before.0.as_deref(), Some("OPEN"));
}

// 9. Cancelamentos concorrentes do mesmo stream: exatamente um efeito, sem corrupção.
#[test]
fn concurrent_cancels_of_same_open_stream_have_one_effect() {
    let (_d, path) = db_path();
    let id = open_with_admission(&path);
    run_concurrent_cancels(&path, &id.stream, 4, "CancelledOpen", "CANCELLED");
    let (stream, interp, seg, _) = snapshot_rows(&path, &id);
    assert_eq!(stream.as_deref(), Some("CANCELLED"));
    assert_eq!(interp, Some(1));
    assert_eq!(seg.as_deref(), Some("CANCELLED"));
}

#[test]
fn concurrent_cancels_of_unknown_stream_have_one_tombstone() {
    let (_d, path) = db_path();
    let stream = Uuid::new_v4();
    run_concurrent_cancels(&path, &stream, 4, "Tombstoned", "CANCELLED");
}

fn run_concurrent_cancels(path: &Path, stream: &Uuid, n: usize, first_label: &str, status: &str) {
    drop(Store::open(path).unwrap());
    let barrier = Arc::new(Barrier::new(n));
    let handles: Vec<_> = (0..n)
        .map(|_| {
            let path = path.to_path_buf();
            let barrier = barrier.clone();
            let stream = *stream;
            std::thread::spawn(move || {
                let mut store = Store::open(&path).unwrap();
                barrier.wait();
                store.cancel_stream(SESSION, &stream, 9).unwrap()
            })
        })
        .collect();
    let outcomes: Vec<CancelOutcome> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let effective = outcomes
        .iter()
        .filter(|o| matches!(o, CancelOutcome::CancelledOpen | CancelOutcome::Tombstoned))
        .count();
    assert_eq!(
        effective, 1,
        "exatamente um cancelamento efetivo: {outcomes:?}"
    );
    assert!(
        outcomes
            .iter()
            .any(|o| format!("{o:?}").starts_with(first_label))
    );
    for o in outcomes
        .iter()
        .filter(|o| !matches!(o, CancelOutcome::CancelledOpen | CancelOutcome::Tombstoned))
    {
        assert_eq!(*o, CancelOutcome::AlreadyTerminal(status.to_owned()));
    }
}

// Monitor: o cenário B-1 original. CANCEL após STREAM_ENDED não altera admissão nem interpretação.
#[tokio::test]
async fn monitor_cancel_after_natural_end_keeps_admission_and_interpretation() {
    let (_d, path) = db_path();
    let id = ids();
    let mut flow = admit_flow(&id);
    flow.push({
        let mut m = KavoMessage::new(MessageType::SttResult, SESSION, "stt-fixture");
        m.payload = Some(
            json!({"stream_id": id.stream, "sequence": 3, "audio_start_ms": 0,
            "audio_end_ms": 9, "event": "STREAM_ENDED"}),
        );
        m
    });
    let mut cancel = KavoMessage::new(MessageType::Cancel, SESSION, "cli");
    cancel.payload = Some(json!({"stream_id": id.stream}));
    flow.push(cancel);
    let st = run_monitor(&path, flow).await.unwrap();
    assert_eq!(st.admitted, 1);
    let (stream, interp, seg, _) = snapshot_rows(&path, &id);
    assert_eq!(stream.as_deref(), Some("ENDED"));
    assert_eq!(interp, Some(0));
    assert_eq!(seg.as_deref(), Some("ADMITTED"));
    // Reinício: continua igual.
    let _ = run_monitor(&path, vec![]).await.unwrap();
    assert_eq!(snapshot_rows(&path, &id).0.as_deref(), Some("ENDED"));
    assert_eq!(snapshot_rows(&path, &id).2.as_deref(), Some("ADMITTED"));
}

// ---------- Transições terminais: gravar antes de mudar a memória ----------

use kavo::monitor::SharedStats;

/// Monitor vivo para testes de sequência: mensagens processadas em ordem, barreira determinística.
struct Live {
    bus: EventBus,
    stats: SharedStats,
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<kavo::error::Result<()>>,
    cancels_sent: u64,
}

fn start_live(path: &Path, cfg: SpeechConfig) -> Live {
    let bus = EventBus::new(256, Duration::from_secs(2));
    let rx = bus.subscribe();
    let stats: SharedStats = Default::default();
    let monitor =
        ContractMonitor::open(rx, &cfg, stats.clone(), Store::open(path).unwrap()).unwrap();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(monitor.run(ComponentContext {
        cancel: cancel.clone(),
        bus: bus.clone(),
    }));
    Live {
        bus,
        stats,
        cancel,
        handle,
        cancels_sent: 0,
    }
}

impl Live {
    async fn publish(&self, m: KavoMessage) {
        self.bus.publish(m).await.unwrap();
    }

    fn snapshot(&self) -> MonitorStats {
        self.stats.lock().unwrap().clone()
    }

    /// Envia um CANCEL e espera o monitor processá-lo. Como o monitor é uma única tarefa, quando o
    /// contador sobe todas as mensagens anteriores já foram aplicadas e gravadas.
    async fn cancel_and_wait(&mut self, stream: Uuid) {
        self.cancels_sent += 1;
        let mut m = KavoMessage::new(MessageType::Cancel, SESSION, "cli");
        m.payload = Some(json!({"stream_id": stream}));
        self.publish(m).await;
        let target = self.cancels_sent;
        wait_for(&self.stats, move |s| s.cancels >= target).await;
    }

    async fn barrier(&mut self) {
        self.cancel_and_wait(Uuid::new_v4()).await;
    }

    async fn stop(self) {
        self.cancel.cancel();
        self.handle.await.unwrap().unwrap();
    }
}

async fn wait_for(stats: &SharedStats, pred: impl Fn(&MonitorStats) -> bool) {
    for _ in 0..800 {
        if pred(&stats.lock().unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condição não atingida no tempo esperado");
}

fn event_msg(id: &Ids, seq: u64, event: &str, reason: Option<&str>) -> KavoMessage {
    let mut p = json!({"stream_id": id.stream, "sequence": seq, "audio_start_ms": 0,
        "audio_end_ms": 9, "event": event});
    if let Some(r) = reason {
        p["reason"] = json!(r);
    }
    let mut m = KavoMessage::new(MessageType::SttResult, SESSION, "stt-fixture");
    m.payload = Some(p);
    m
}

fn stream_db_status(path: &Path, id: &Ids) -> Option<String> {
    snapshot_rows(path, id).0
}

const FAIL_TRIGGER: &str = "falha_terminal";

fn fail_on(path: &Path, status: &str) {
    sql_trigger(
        path,
        &format!(
            "CREATE TRIGGER {FAIL_TRIGGER} BEFORE UPDATE OF status ON streams
             WHEN NEW.status = '{status}' BEGIN SELECT RAISE(ABORT, 'teste'); END;"
        ),
    );
}

fn heal(path: &Path) {
    sql_trigger(path, &format!("DROP TRIGGER {FAIL_TRIGGER}"));
}

// Cenário principal, para ENDED e FAILED: falha ao gravar, memória preservada, retry, CANCEL depois.
async fn terminal_failure_then_retry(status: &str, event: &str, reason: Option<&str>) {
    let (_d, path) = db_path();
    let id = ids();
    let mut live = start_live(&path, SpeechConfig::default());
    for m in admit_flow(&id) {
        live.publish(m).await;
    }
    live.barrier().await;
    assert_eq!(live.snapshot().admitted, 1);

    fail_on(&path, status);
    live.publish(event_msg(&id, 3, event, reason)).await;
    live.barrier().await;
    let st = live.snapshot();
    assert!(st.store_errors >= 1, "falha registrada: {st:?}");
    assert_eq!(
        stream_db_status(&path, &id).as_deref(),
        Some("OPEN"),
        "banco continua OPEN"
    );

    // Memória não mudou: o evento terminal não foi consumido, então seq4 é lacuna, nunca aplicado.
    live.publish(event_msg(&id, 4, "NO_SPEECH", None)).await;
    live.barrier().await;
    let st = live.snapshot();
    assert_eq!(st.speech_gap, 1, "seq3 não consumida: {st:?}");
    assert_eq!(st.speech_rejected, 0, "memória não virou terminal");

    // Retry com o banco saudável: transição aplicada e gravada.
    heal(&path);
    live.publish(event_msg(&id, 3, event, reason)).await;
    live.barrier().await;
    assert_eq!(stream_db_status(&path, &id).as_deref(), Some(status));
    assert_eq!(snapshot_rows(&path, &id).2.as_deref(), Some("ADMITTED"));

    // CANCEL depois do encerramento: no-op completo.
    let before = snapshot_rows(&path, &id);
    live.cancel_and_wait(id.stream).await;
    assert_eq!(
        snapshot_rows(&path, &id),
        before,
        "CANCEL após {status} não altera nada"
    );
    live.stop().await;

    // Reinício: mesma verdade.
    let _ = run_monitor(&path, vec![]).await.unwrap();
    assert_eq!(snapshot_rows(&path, &id), before, "após reinício");
}

#[tokio::test]
async fn ended_persist_failure_is_retried_and_cancel_after_is_noop() {
    terminal_failure_then_retry("ENDED", "STREAM_ENDED", None).await;
}

#[tokio::test]
async fn failed_persist_failure_is_retried_and_cancel_after_is_noop() {
    terminal_failure_then_retry("FAILED", "STREAM_FAILED", Some("ENGINE_UNAVAILABLE")).await;
}

// TIMED_OUT: falha na expiração. O stream segue ativo nos dois lados; CANCEL ainda cancela (DB OPEN).
#[tokio::test]
async fn timeout_persist_failure_keeps_stream_active_and_cancel_still_applies() {
    let (_d, path) = db_path();
    let id = ids();
    let mut live = start_live(
        &path,
        SpeechConfig {
            stream_idle_timeout_ms: 300,
            ..SpeechConfig::default()
        },
    );
    for m in admit_flow(&id) {
        live.publish(m).await;
    }
    live.barrier().await;
    fail_on(&path, "TIMED_OUT");
    wait_for(&live.stats, |s| s.store_errors >= 1).await;
    assert_eq!(
        live.snapshot().streams_timed_out,
        0,
        "expiração não contabilizada sem gravação"
    );
    assert_eq!(stream_db_status(&path, &id).as_deref(), Some("OPEN"));

    // Memória ativa: evento novo é aplicado (seq3), consistente com o banco OPEN.
    live.publish(event_msg(&id, 3, "NO_SPEECH", None)).await;
    live.barrier().await;
    assert_eq!(live.snapshot().speech_applied, 3, "stream ativo na memória");

    // CANCEL com banco ainda OPEN: cancelamento válido e completo.
    live.cancel_and_wait(id.stream).await;
    assert_eq!(stream_db_status(&path, &id).as_deref(), Some("CANCELLED"));
    assert_eq!(snapshot_rows(&path, &id).2.as_deref(), Some("CANCELLED"));
    live.stop().await;
    heal(&path);
}

#[tokio::test]
async fn timeout_persist_retry_then_cancel_after_is_noop() {
    let (_d, path) = db_path();
    let id = ids();
    let mut live = start_live(
        &path,
        SpeechConfig {
            stream_idle_timeout_ms: 300,
            ..SpeechConfig::default()
        },
    );
    for m in admit_flow(&id) {
        live.publish(m).await;
    }
    live.barrier().await;
    fail_on(&path, "TIMED_OUT");
    wait_for(&live.stats, |s| s.store_errors >= 1).await;
    heal(&path);
    wait_for(&live.stats, |s| s.streams_timed_out >= 1).await;
    assert_eq!(stream_db_status(&path, &id).as_deref(), Some("TIMED_OUT"));
    let before = snapshot_rows(&path, &id);
    live.cancel_and_wait(id.stream).await;
    assert_eq!(
        snapshot_rows(&path, &id),
        before,
        "CANCEL após TIMED_OUT é no-op"
    );
    live.stop().await;
    let _ = run_monitor(&path, vec![]).await.unwrap();
    assert_eq!(snapshot_rows(&path, &id), before, "após reinício");
}

// Reinício depois de falha ao gravar ENDED: stream OPEN no banco vira INTERRUPTED, nunca reabre.
#[tokio::test]
async fn restart_after_failed_end_interrupts_stream_and_never_reopens() {
    let (_d, path) = db_path();
    let id = ids();
    let mut live = start_live(&path, SpeechConfig::default());
    for m in admit_flow(&id) {
        live.publish(m).await;
    }
    live.barrier().await;
    fail_on(&path, "ENDED");
    live.publish(event_msg(&id, 3, "STREAM_ENDED", None)).await;
    live.barrier().await;
    live.stop().await;
    heal(&path);

    let st = run_monitor(&path, vec![event_msg(&id, 3, "STREAM_ENDED", None)])
        .await
        .unwrap();
    assert_eq!(stream_db_status(&path, &id).as_deref(), Some("INTERRUPTED"));
    assert_eq!(st.speech_applied, 0, "stream não reabre após reinício");
    assert_eq!(snapshot_rows(&path, &id).2.as_deref(), Some("ADMITTED"));
}

// Sucesso normal de cada transição, sem falhas: banco e memória iguais, CANCEL é no-op.
#[tokio::test]
async fn normal_terminal_transitions_persist_and_cancel_is_noop() {
    let cases: [(&str, &str, Option<&str>, Option<u64>); 3] = [
        ("ENDED", "STREAM_ENDED", None, None),
        ("FAILED", "STREAM_FAILED", Some("ENGINE_UNAVAILABLE"), None),
        ("TIMED_OUT", "", None, Some(300)),
    ];
    for (status, event, reason, idle) in cases {
        let (_d, path) = db_path();
        let id = ids();
        let cfg = SpeechConfig {
            stream_idle_timeout_ms: idle.unwrap_or(10_000),
            ..SpeechConfig::default()
        };
        let mut live = start_live(&path, cfg);
        for m in admit_flow(&id) {
            live.publish(m).await;
        }
        live.barrier().await;
        if status == "TIMED_OUT" {
            wait_for(&live.stats, |s| s.streams_timed_out >= 1).await;
        } else {
            live.publish(event_msg(&id, 3, event, reason)).await;
            live.barrier().await;
        }
        assert_eq!(
            stream_db_status(&path, &id).as_deref(),
            Some(status),
            "{status} gravado"
        );
        let before = snapshot_rows(&path, &id);
        live.cancel_and_wait(id.stream).await;
        assert_eq!(
            snapshot_rows(&path, &id),
            before,
            "{status}: CANCEL é no-op"
        );
        live.stop().await;
    }
}

// Store: fechamento exige exatamente uma linha OPEN. Ausente, já terminal ou alvo OPEN são erro.
#[test]
fn close_stream_requires_exactly_one_open_row() {
    let (_d, path) = db_path();
    let id = ids();
    let mut store = Store::open(&path).unwrap();
    assert!(
        store
            .close_stream(SESSION, &id.stream, StreamDbStatus::Ended, 1)
            .is_err(),
        "ausente"
    );
    store.open_stream(SESSION, &id.stream, 2).unwrap();
    assert!(
        store
            .close_stream(SESSION, &id.stream, StreamDbStatus::Open, 3)
            .is_err(),
        "alvo OPEN"
    );
    store
        .close_stream(SESSION, &id.stream, StreamDbStatus::Ended, 4)
        .unwrap();
    assert!(
        store
            .close_stream(SESSION, &id.stream, StreamDbStatus::Failed, 5)
            .is_err(),
        "já terminal"
    );
    assert_eq!(
        store.stream_status(SESSION, &id.stream).unwrap().as_deref(),
        Some("ENDED")
    );
}

// Revisão: primeiro evento rejeitado por conteúdo não pode deixar o banco REJECTED com memória ativa.
#[tokio::test]
async fn rejected_first_event_keeps_db_and_memory_consistent() {
    let (_d, path) = db_path();
    let id = ids();
    // Revisão 2 para segmento novo é inválida: evento rejeitado, sequência 1 consumida.
    let bad = stt(id.stream, 1, id.segment, 2, "PARTIAL", "x");
    let ok = stt(id.stream, 2, id.segment, 0, "PARTIAL", "ok");
    let st = run_monitor(&path, vec![bad, ok]).await.unwrap();
    assert_eq!(st.speech_rejected, 1);
    assert_eq!(st.speech_applied, 1, "stream segue ativo na memória");
    assert_eq!(
        stream_db_status(&path, &id).as_deref(),
        Some("OPEN"),
        "banco coerente com a memória"
    );
}
