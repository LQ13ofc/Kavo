//! Testes do harness A0 e do Session Manager. Os cenários rodam contra barramento, monitor e SQLite reais,
//! em diretório temporário próprio. `ipc-stream` exige um núcleo em execução e é validado pela CLI.

use std::time::Duration;

use kavo::harness::{self, Options, SCENARIOS};
use kavo::ipc::IpcSettings;
use kavo::session::{
    CancelOutcome, CloseOutcome, EventDisposition, RequestState, SessionError, SessionManager,
    validate_session_id,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn options(dir: &std::path::Path) -> Options {
    Options {
        dir: dir.to_owned(),
        socket: None,
        store: None,
        ipc: IpcSettings {
            max_frame_bytes: 65536,
            request_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_secs(30),
        },
    }
}

#[tokio::test]
async fn in_process_scenarios_pass() {
    let dir = tempfile::tempdir().expect("diretório temporário");
    let opts = options(dir.path());
    for (name, _) in SCENARIOS.iter().filter(|(n, _)| !n.starts_with("ipc-")) {
        let report = harness::run(name, &opts).await.expect("cenário executa");
        let failed: Vec<_> = report.checks.iter().filter(|c| !c.ok).collect();
        assert!(report.passed(), "cenário {name} reprovado: {failed:?}");
    }
}

#[tokio::test]
async fn ipc_scenario_requires_socket_and_store() {
    let dir = tempfile::tempdir().expect("diretório temporário");
    let opts = options(dir.path());
    assert!(harness::run("ipc-stream", &opts).await.is_err());
}

#[test]
fn session_id_rules_match_envelope() {
    assert!(validate_session_id("sessao-1").is_ok());
    assert!(
        validate_session_id(&"á".repeat(128)).is_ok(),
        "128 caracteres Unicode são aceitos"
    );
    assert!(validate_session_id(&"á".repeat(129)).is_err());
    assert!(validate_session_id("a\tb").is_err());
}

#[test]
fn cancel_token_reaches_only_its_request() {
    let mut m = SessionManager::new();
    m.open("s").expect("abre");
    m.open("t").expect("abre");
    let (r1, r2) = (Uuid::new_v4(), Uuid::new_v4());
    let t1 = m.begin_request("s", r1).expect("registra");
    let t2 = m.begin_request("t", r2).expect("registra");
    assert_eq!(m.cancel_request("s", r1), Ok(CancelOutcome::Cancelled));
    assert!(t1.is_cancelled());
    assert!(!t2.is_cancelled());
    assert_eq!(m.request_state("t", r2), Some(RequestState::Running));
}

#[test]
fn duplicate_request_id_is_rejected_within_session_only() {
    let mut m = SessionManager::new();
    m.open("s").expect("abre");
    m.open("t").expect("abre");
    let r = Uuid::new_v4();
    m.begin_request("s", r).expect("primeiro registro");
    assert_eq!(
        m.begin_request("s", r).err(),
        Some(SessionError::DuplicateRequest)
    );
    assert!(
        m.begin_request("t", r).is_ok(),
        "mesmo request_id em outra sessão é outra requisição"
    );
}

#[test]
fn close_is_idempotent_and_blocks_reopen_and_events() {
    let mut m = SessionManager::new();
    m.open("s").expect("abre");
    assert_eq!(
        m.close("s"),
        Ok(CloseOutcome::Closed {
            cancelled_requests: 0
        })
    );
    assert_eq!(m.close("s"), Ok(CloseOutcome::AlreadyClosed));
    assert_eq!(m.open("s").err(), Some(SessionError::ClosedSession));
    assert_eq!(m.check_event("s"), EventDisposition::RejectClosedSession);
}

#[test]
fn finished_request_is_not_cancelled_by_session_close() {
    let mut m = SessionManager::new();
    m.open("s").expect("abre");
    let r = Uuid::new_v4();
    let token: CancellationToken = m.begin_request("s", r).expect("registra");
    m.finish_request("s", r).expect("conclui");
    assert_eq!(
        m.close("s"),
        Ok(CloseOutcome::Closed {
            cancelled_requests: 0
        })
    );
    assert!(!token.is_cancelled());
    assert_eq!(m.request_state("s", r), Some(RequestState::Finished));
}
