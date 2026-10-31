//! Harness de testes (A0): cenários reproduzíveis contra o barramento, o monitor e o SQLite reais.
//!
//! A fonte virtual gera eventos de STT **sintéticos**, com emissor `fonte-virtual`. Não captura áudio,
//! não há microfone nem reconhecimento de fala: o texto e a sequência são fixos em cada cenário.
//! Cada cenário imprime uma linha JSON por verificação e termina com código 0 (tudo ok) ou 1 (falha).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use uuid::Uuid;

use crate::bus::EventBus;
use crate::config::SpeechConfig;
use crate::control::{
    self, AckEvent, AdmissionAck, AdmissionStatus, CONTROL_SCHEMA_VERSION, ControlCommand,
    ControlOp, ControlResult, ControlSender, Outcome, Reason, RequestKind, SessionControl,
    WireState,
};
use crate::error::{KavoError, Result};
use crate::ipc::{self, IpcSettings};
use crate::monitor::{ContractMonitor, MonitorStats, SharedStats};
use crate::protocol::{KavoMessage, MessageType};
use crate::runtime::Supervisor;
use crate::session::{
    CancelOutcome, CloseOutcome, EventDisposition, OpenOutcome, RequestState, SessionError,
    SessionManager,
};
use crate::speech::{CancelPayload, SpeechEvent, SpeechPayload, TranscriptState};
use crate::store::Store;
use crate::verification::Verification;

/// Emissor de todos os eventos sintéticos. Identifica a origem nos logs e no banco.
pub const SOURCE: &str = "fonte-virtual";
const WAIT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(10);
const SHUTDOWN: Duration = Duration::from_secs(5);
/// Limite de linha do cliente de teste: igual ao padrão de `ipc.max_frame_bytes`.
const FRAME_LIMIT: usize = 64 * 1024;

/// Cenários disponíveis. `ipc-stream` exige um núcleo em execução e não entra em `all`.
pub const SCENARIOS: &[(&str, &str)] = &[
    ("sessions", "ciclo de vida de sessões e isolamento"),
    (
        "cancel",
        "cancelamento de requisição e de stream, idempotente",
    ),
    (
        "stt-stream",
        "stream completo, duplicata e lacuna de sequência",
    ),
    ("late-events", "evento atrasado após CANCEL não é aplicado"),
    ("restart", "reinício do monitor: stream aberto não retoma"),
    (
        "concurrency",
        "duas sessões com o mesmo stream_id ficam isoladas",
    ),
    (
        "store-open-failure",
        "arquivo inválido impede o início (fail-closed)",
    ),
    (
        "store-fault",
        "falha de gravação do SQLite não é declarada como sucesso",
    ),
    (
        "control-lifecycle",
        "sessão: abrir, consultar, encerrar, repetir, eventos de sessão inválida",
    ),
    (
        "control-requests",
        "requisições: vínculo, cancelamento, isolamento entre sessões, corridas",
    ),
    (
        "control-events",
        "STT 1.2: duplicata, lacuna, atraso após cancelamento e encerramento",
    ),
    (
        "control-shutdown",
        "encerramento com sessão e requisição ativas; reinício não retoma",
    ),
    (
        "control-store-fault",
        "falha de gravação durante encerramento e cancelamento",
    ),
    (
        "compat",
        "clientes 1.1 continuam funcionando; mensagens 1.2 são validadas por versão",
    ),
    (
        "admission",
        "ACK de admissão 1.2: aceito, duplicado, rejeitado, falha de gravação, compatibilidade 1.1",
    ),
    (
        "admission-faults",
        "idempotência sob falha: recusa por capacidade, evicção, fim de stream, reinício",
    ),
    (
        "pending-lifecycle",
        "ciclo de vida de aberturas pendentes: limpeza, falha na limpeza, teto de crescimento",
    ),
    (
        "admission-load",
        "carga de parciais 1.2: latência de ACK e vazão (em processo)",
    ),
    (
        "ipc-admission",
        "ACK de admissão pelo socket: aceito, duplicado, sessão, legado 1.1, privacidade",
    ),
    (
        "ipc-backpressure",
        "contrapressão real: fila pequena, BUSY e retry com a mesma identidade",
    ),
    (
        "ipc-bench",
        "latência e vazão de ACK pelo socket (somente medidas)",
    ),
    (
        "ipc-stream",
        "fluxo STT contra núcleo em execução via socket",
    ),
    (
        "ipc-interrupt-prepare",
        "cria sessão, requisição e stream ativos antes de encerrar o núcleo",
    ),
    (
        "ipc-interrupt-verify",
        "após reinício, confirma que o estado ativo não voltou",
    ),
    (
        "ipc-control",
        "sessões, requisições e cancelamento contra núcleo em execução via socket",
    ),
];

/// Opções vindas da CLI.
#[derive(Debug, Clone)]
pub struct Options {
    /// Diretório dos bancos temporários dos cenários.
    pub dir: PathBuf,
    /// Socket do núcleo em execução (`ipc-stream`).
    pub socket: Option<PathBuf>,
    /// Banco do núcleo em execução (`ipc-stream`).
    pub store: Option<PathBuf>,
    pub ipc: IpcSettings,
}

/// Uma verificação: nome estável, resultado e detalhe sem conteúdo transcrito.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub scenario: String,
    pub checks: Vec<Check>,
    /// Medidas numéricas (latência, vazão, contagens). Não são verificações pass/fail.
    pub metrics: Vec<(String, f64)>,
}

impl Report {
    fn new(scenario: &str) -> Self {
        Self {
            scenario: scenario.to_owned(),
            checks: Vec::new(),
            metrics: Vec::new(),
        }
    }

    fn metric(&mut self, name: &str, value: f64) {
        self.metrics.push((name.to_owned(), value));
    }

    fn check(&mut self, name: &str, ok: bool, detail: impl Into<String>) {
        self.checks.push(Check {
            name: name.to_owned(),
            ok,
            detail: detail.into(),
        });
    }

    pub fn passed(&self) -> bool {
        !self.checks.is_empty() && self.checks.iter().all(|c| c.ok)
    }
}

/// Executa um cenário pelo nome. Erro de infraestrutura (ex.: banco que não abre) vira `Err`.
pub async fn run(name: &str, opts: &Options) -> Result<Report> {
    match name {
        "sessions" => sessions(),
        "cancel" => cancel(&opts.dir).await,
        "stt-stream" => stt_stream(&opts.dir).await,
        "late-events" => late_events(&opts.dir).await,
        "restart" => restart(&opts.dir).await,
        "concurrency" => concurrency(&opts.dir).await,
        "store-open-failure" => store_open_failure(&opts.dir),
        "store-fault" => store_fault(&opts.dir).await,
        "control-lifecycle" => control_lifecycle(&opts.dir).await,
        "control-requests" => control_requests(&opts.dir).await,
        "control-events" => control_events(&opts.dir).await,
        "control-shutdown" => control_shutdown(&opts.dir).await,
        "control-store-fault" => control_store_fault(&opts.dir).await,
        "compat" => compat(&opts.dir).await,
        "admission" => admission(&opts.dir).await,
        "admission-load" => admission_load(&opts.dir).await,
        "pending-lifecycle" => pending_lifecycle(&opts.dir).await,
        "admission-faults" => admission_faults(&opts.dir).await,
        "ipc-stream" => ipc_stream(opts).await,
        "ipc-control" => ipc_control(opts).await,
        "ipc-interrupt-prepare" => ipc_interrupt_prepare(opts).await,
        "ipc-admission" => ipc_admission(opts).await,
        "ipc-backpressure" => ipc_backpressure(opts).await,
        "ipc-bench" => ipc_bench(opts).await,
        "ipc-interrupt-verify" => ipc_interrupt_verify(opts).await,
        other => Err(KavoError::Config(format!("cenário desconhecido '{other}'"))),
    }
}

// ---------- fonte virtual ----------

fn speech_msg(
    session: &str,
    stream: Uuid,
    sequence: u64,
    event: SpeechEvent,
) -> Result<KavoMessage> {
    let payload = SpeechPayload {
        stream_id: stream,
        sequence,
        audio_start_ms: sequence.saturating_sub(1) * 100,
        audio_end_ms: sequence * 100,
        event,
    };
    KavoMessage::new(MessageType::SttResult, session, SOURCE).with_payload(&payload)
}

fn cancel_stream_msg(session: &str, stream: Uuid) -> Result<KavoMessage> {
    let payload = CancelPayload { stream_id: stream };
    KavoMessage::new(MessageType::Cancel, session, SOURCE).with_payload(&payload)
}

fn started(segment: Uuid) -> SpeechEvent {
    SpeechEvent::SpeechStarted {
        segment_id: segment,
    }
}

fn transcript(segment: Uuid, revision: u32, state: TranscriptState) -> SpeechEvent {
    SpeechEvent::Transcript {
        segment_id: segment,
        revision,
        state,
        text: "texto sintético".to_owned(),
        confidence: None,
    }
}

// ---------- pipeline real: barramento + monitor + SQLite ----------

struct Pipeline {
    control_tx: ControlSender,
    bus: EventBus,
    supervisor: Option<Supervisor>,
    stats: SharedStats,
    store_path: PathBuf,
}

impl Pipeline {
    async fn start(store_path: &Path) -> Result<Self> {
        Self::start_cfg(store_path, SpeechConfig::default()).await
    }

    async fn start_cfg(store_path: &Path, cfg: SpeechConfig) -> Result<Self> {
        let store = Store::open(store_path)?;
        let bus = EventBus::new(256, Duration::from_secs(2));
        let rx = bus.subscribe();
        let stats: SharedStats = Arc::new(Mutex::new(MonitorStats::default()));
        let (control_tx, control_rx) = control::channel();
        let monitor =
            ContractMonitor::open_with_control(rx, &cfg, stats.clone(), store, control_rx)?;
        let mut supervisor = Supervisor::new(bus.clone());
        supervisor.spawn(monitor);
        Ok(Self {
            control_tx,
            bus,
            supervisor: Some(supervisor),
            stats,
            store_path: store_path.to_owned(),
        })
    }

    async fn send(&self, msg: KavoMessage) -> Result<()> {
        self.bus.publish(msg).await.map(|_| ())
    }

    fn snapshot(&self) -> MonitorStats {
        self.stats.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Espera o predicado ser verdadeiro (ou o prazo acabar) e devolve o último estado.
    async fn wait_for(&self, pred: impl Fn(&MonitorStats) -> bool) -> MonitorStats {
        let deadline = Instant::now() + WAIT;
        loop {
            let snap = self.snapshot();
            if pred(&snap) || Instant::now() >= deadline {
                return snap;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Pedido com resposta pelo mesmo canal que o IPC usa. Devolve a mensagem de resposta crua.
    async fn reply(&self, msg: KavoMessage) -> Result<KavoMessage> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.control_tx
            .send(ControlCommand {
                msg,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KavoError::Component {
                component: "monitor".to_owned(),
                reason: "canal de controle fechado".to_owned(),
            })?;
        tokio::time::timeout(WAIT, reply_rx)
            .await
            .map_err(|_| KavoError::Timeout(WAIT))?
            .map_err(|_| KavoError::Component {
                component: "monitor".to_owned(),
                reason: "resposta de controle descartada".to_owned(),
            })
    }

    /// Admissão de evento 1.2: devolve o ACK decodificado.
    async fn admit(&self, msg: KavoMessage) -> Result<(KavoMessage, AdmissionAck)> {
        let reply = self.reply(msg.clone()).await?;
        if reply.kind != MessageType::AdmissionAck {
            return Err(KavoError::Protocol(format!(
                "resposta inesperada: {:?}",
                reply.kind
            )));
        }
        let payload = reply
            .payload
            .as_ref()
            .ok_or_else(|| KavoError::Protocol("ACK sem payload".to_owned()))?;
        let ack = AdmissionAck::from_value(payload)?;
        Ok((reply, ack))
    }

    /// Pedido de controle pelo mesmo canal que o IPC usa. Espera a resposta do monitor.
    async fn control(&self, msg: KavoMessage) -> Result<ControlResult> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.control_tx
            .send(ControlCommand {
                msg,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KavoError::Component {
                component: "monitor".to_owned(),
                reason: "canal de controle fechado".to_owned(),
            })?;
        let reply = tokio::time::timeout(WAIT, reply_rx)
            .await
            .map_err(|_| KavoError::Timeout(WAIT))?
            .map_err(|_| KavoError::Component {
                component: "monitor".to_owned(),
                reason: "resposta de controle descartada".to_owned(),
            })?;
        let payload = reply
            .payload
            .as_ref()
            .ok_or_else(|| KavoError::Protocol("resposta de controle sem payload".to_owned()))?;
        ControlResult::from_value(payload)
    }

    fn stream_status(&self, session: &str, stream: &Uuid) -> Result<Option<String>> {
        Store::open(&self.store_path)?.stream_status(session, stream)
    }

    async fn stop(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown(SHUTDOWN).await;
        }
    }
}

fn stats_detail(s: &MonitorStats) -> String {
    format!(
        "applied={} duplicate={} gap={} stale={} rejected={} cancels={}",
        s.speech_applied,
        s.speech_duplicate,
        s.speech_gap,
        s.speech_stale,
        s.speech_rejected,
        s.cancels
    )
}

fn fresh_store(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(format!("{name}.db"));
    let _ = std::fs::remove_file(&path);
    path
}

// ---------- cenários ----------

fn sessions() -> Result<Report> {
    let mut r = Report::new("sessions");
    let mut m = SessionManager::new();
    let (a, b) = ("sessao-a", "sessao-b");
    let (ra, rb) = (Uuid::new_v4(), Uuid::new_v4());

    let opened = m.open(a).map_err(session_err)?;
    r.check(
        "abrir sessão nova",
        opened == OpenOutcome::Opened,
        format!("{opened:?}"),
    );
    let again = m.open(a).map_err(session_err)?;
    r.check(
        "reabrir sessão ativa é no-op",
        again == OpenOutcome::AlreadyActive,
        format!("{again:?}"),
    );
    m.open(b).map_err(session_err)?;

    let token_a = m.begin_request(a, ra).map_err(session_err)?;
    let token_b = m.begin_request(b, rb).map_err(session_err)?;

    let closed = m.close(a).map_err(session_err)?;
    r.check(
        "encerrar cancela só a requisição da sessão",
        closed
            == CloseOutcome::Closed {
                cancelled_requests: 1,
            },
        format!("{closed:?}"),
    );
    r.check(
        "token da sessão encerrada cancelado",
        token_a.is_cancelled(),
        "",
    );
    r.check(
        "requisição da outra sessão segue ativa",
        !token_b.is_cancelled() && m.request_state(b, rb) == Some(RequestState::Running),
        format!("{:?}", m.request_state(b, rb)),
    );
    let closed_again = m.close(a).map_err(session_err)?;
    r.check(
        "encerrar de novo é idempotente",
        closed_again == CloseOutcome::AlreadyClosed,
        format!("{closed_again:?}"),
    );
    let reopen = m.open(a);
    r.check(
        "sessão encerrada não reabre",
        reopen == Err(SessionError::ClosedSession),
        format!("{reopen:?}"),
    );
    let begin_closed = m.begin_request(a, Uuid::new_v4());
    r.check(
        "requisição em sessão encerrada recusada",
        begin_closed == Err(SessionError::ClosedSession),
        format!("{begin_closed:?}"),
    );
    r.check(
        "evento de sessão encerrada rejeitado",
        m.check_event(a) == EventDisposition::RejectClosedSession,
        format!("{:?}", m.check_event(a)),
    );
    r.check(
        "evento de sessão desconhecida rejeitado",
        m.check_event("nunca-aberta") == EventDisposition::RejectUnknownSession,
        "",
    );
    r.check(
        "evento de sessão ativa aceito",
        m.check_event(b) == EventDisposition::Accept,
        "",
    );

    let invalid = [
        ("vazio", String::new()),
        ("longo", "x".repeat(129)),
        ("controle", "ab\ncd".to_owned()),
    ];
    for (label, id) in invalid {
        let res = m.open(&id);
        r.check(
            &format!("session_id inválido recusado ({label})"),
            matches!(res, Err(SessionError::InvalidId(_))),
            "",
        );
    }
    let unknown = m.close("nunca-aberta");
    r.check(
        "encerrar sessão desconhecida é erro",
        unknown == Err(SessionError::UnknownSession),
        format!("{unknown:?}"),
    );

    m.finish_request(b, rb).map_err(session_err)?;
    let cancel_done = m.cancel_request(b, rb).map_err(session_err)?;
    r.check(
        "cancelar requisição já concluída não altera",
        cancel_done == CancelOutcome::AlreadyFinished,
        format!("{cancel_done:?}"),
    );
    Ok(r)
}

async fn cancel(dir: &Path) -> Result<Report> {
    let mut r = Report::new("cancel");
    let mut m = SessionManager::new();
    let session = "sessao-cancel";
    m.open(session).map_err(session_err)?;

    let req = Uuid::new_v4();
    let token = m.begin_request(session, req).map_err(session_err)?;
    let first = m.cancel_request(session, req).map_err(session_err)?;
    let second = m.cancel_request(session, req).map_err(session_err)?;
    r.check(
        "primeiro cancelamento aplicado",
        first == CancelOutcome::Cancelled,
        format!("{first:?}"),
    );
    r.check(
        "segundo cancelamento é no-op",
        second == CancelOutcome::AlreadyCancelled,
        format!("{second:?}"),
    );
    r.check("token recebe o cancelamento", token.is_cancelled(), "");
    m.finish_request(session, req).map_err(session_err)?;
    r.check(
        "concluir após cancelar não reabre",
        m.request_state(session, req) == Some(RequestState::Cancelled),
        format!("{:?}", m.request_state(session, req)),
    );
    let unknown = m.cancel_request(session, Uuid::new_v4());
    r.check(
        "cancelar requisição desconhecida é erro",
        unknown == Err(SessionError::UnknownRequest),
        format!("{unknown:?}"),
    );

    let path = fresh_store(dir, "cancel");
    let mut p = Pipeline::start(&path).await?;
    let stream = Uuid::new_v4();
    p.send(speech_msg(session, stream, 1, started(Uuid::new_v4()))?)
        .await?;
    p.wait_for(|s| s.speech_applied >= 1).await;
    p.send(cancel_stream_msg(session, stream)?).await?;
    let snap = p.wait_for(|s| s.cancels >= 1).await;
    let status = p.stream_status(session, &stream)?;
    r.check(
        "CANCEL de stream grava CANCELLED",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );
    r.check(
        "CANCEL contado pelo monitor",
        snap.cancels == 1,
        stats_detail(&snap),
    );

    p.send(cancel_stream_msg(session, stream)?).await?;
    let sentinel = Uuid::new_v4();
    p.send(speech_msg(session, sentinel, 1, started(Uuid::new_v4()))?)
        .await?;
    let snap = p.wait_for(|s| s.speech_applied >= 2).await;
    let status = p.stream_status(session, &stream)?;
    r.check(
        "segundo CANCEL não muda estado terminal",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );
    r.check(
        "sentinela de controle aplicada",
        snap.speech_applied == 2,
        stats_detail(&snap),
    );
    p.stop().await;
    Ok(r)
}

async fn stt_stream(dir: &Path) -> Result<Report> {
    let mut r = Report::new("stt-stream");
    let path = fresh_store(dir, "stt-stream");
    let mut p = Pipeline::start(&path).await?;
    let (session, a, b) = ("sessao-stt", Uuid::new_v4(), Uuid::new_v4());
    let seg_a = Uuid::new_v4();

    p.send(speech_msg(session, a, 1, started(seg_a))?).await?;
    p.send(speech_msg(
        session,
        a,
        2,
        transcript(seg_a, 0, TranscriptState::Partial),
    )?)
    .await?;
    p.send(speech_msg(
        session,
        a,
        3,
        transcript(seg_a, 1, TranscriptState::Confirmed),
    )?)
    .await?;
    p.send(speech_msg(
        session,
        a,
        3,
        transcript(seg_a, 1, TranscriptState::Confirmed),
    )?)
    .await?;
    p.send(speech_msg(
        session,
        a,
        4,
        SpeechEvent::SpeechEnded { segment_id: seg_a },
    )?)
    .await?;
    p.send(speech_msg(session, a, 5, SpeechEvent::StreamEnded)?)
        .await?;

    // Stream B: a sequência 3 chega antes da 2 (lacuna). Não é aplicada.
    p.send(speech_msg(session, b, 1, started(Uuid::new_v4()))?)
        .await?;
    p.send(speech_msg(session, b, 3, SpeechEvent::NoSpeech)?)
        .await?;

    let snap = p
        .wait_for(|s| s.speech_applied == 6 && s.speech_duplicate == 1 && s.speech_gap == 1)
        .await;
    r.check(
        "eventos válidos aplicados e duplicata/lacuna contadas",
        snap.speech_applied == 6 && snap.speech_duplicate == 1 && snap.speech_gap == 1,
        stats_detail(&snap),
    );
    let status_a = p.stream_status(session, &a)?;
    r.check(
        "stream completo gravado como ENDED",
        status_a.as_deref() == Some("ENDED"),
        format!("{status_a:?}"),
    );
    let status_b = p.stream_status(session, &b)?;
    r.check(
        "stream com lacuna segue OPEN",
        status_b.as_deref() == Some("OPEN"),
        format!("{status_b:?}"),
    );
    p.stop().await;
    Ok(r)
}

async fn late_events(dir: &Path) -> Result<Report> {
    let mut r = Report::new("late-events");
    let path = fresh_store(dir, "late-events");
    let mut p = Pipeline::start(&path).await?;
    let (session, stream, seg) = ("sessao-late", Uuid::new_v4(), Uuid::new_v4());

    p.send(speech_msg(session, stream, 1, started(seg))?)
        .await?;
    p.wait_for(|s| s.speech_applied >= 1).await;
    p.send(cancel_stream_msg(session, stream)?).await?;
    p.wait_for(|s| s.cancels >= 1).await;
    // Atrasado: chega depois do CANCEL. Não pode ser aplicado nem reabrir o stream.
    p.send(speech_msg(
        session,
        stream,
        2,
        transcript(seg, 0, TranscriptState::Confirmed),
    )?)
    .await?;
    let sentinel = Uuid::new_v4();
    p.send(speech_msg(session, sentinel, 1, started(Uuid::new_v4()))?)
        .await?;
    let snap = p.wait_for(|s| s.speech_applied >= 2).await;
    r.check(
        "evento atrasado após CANCEL não aplicado",
        snap.speech_applied == 2 && snap.speech_rejected + snap.speech_stale >= 1,
        stats_detail(&snap),
    );
    let status = p.stream_status(session, &stream)?;
    r.check(
        "stream cancelado continua CANCELLED",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );
    p.stop().await;
    Ok(r)
}

async fn restart(dir: &Path) -> Result<Report> {
    let mut r = Report::new("restart");
    let path = fresh_store(dir, "restart");
    let (session, stream, seg) = ("sessao-restart", Uuid::new_v4(), Uuid::new_v4());

    let mut first = Pipeline::start(&path).await?;
    first
        .send(speech_msg(session, stream, 1, started(seg))?)
        .await?;
    first
        .send(speech_msg(
            session,
            stream,
            2,
            transcript(seg, 0, TranscriptState::Partial),
        )?)
        .await?;
    let snap = first.wait_for(|s| s.speech_applied >= 2).await;
    r.check(
        "antes do reinício: stream aceito",
        snap.speech_applied == 2,
        stats_detail(&snap),
    );
    first.stop().await;

    let mut second = Pipeline::start(&path).await?;
    second
        .send(speech_msg(
            session,
            stream,
            3,
            transcript(seg, 1, TranscriptState::Confirmed),
        )?)
        .await?;
    let sentinel = Uuid::new_v4();
    second
        .send(speech_msg(session, sentinel, 1, started(Uuid::new_v4()))?)
        .await?;
    let snap = second
        .wait_for(|s| {
            s.speech_applied >= 1 && s.speech_rejected + s.speech_stale + s.speech_gap >= 1
        })
        .await;
    r.check(
        "após reinício: stream interrompido não retoma",
        snap.speech_applied == 1 && snap.speech_rejected + snap.speech_stale + snap.speech_gap >= 1,
        stats_detail(&snap),
    );
    let status = second.stream_status(session, &stream)?;
    r.check(
        "stream interrompido não gravado como ENDED",
        status.as_deref() != Some("ENDED"),
        format!("{status:?}"),
    );
    second.stop().await;
    Ok(r)
}

async fn concurrency(dir: &Path) -> Result<Report> {
    let mut r = Report::new("concurrency");
    let path = fresh_store(dir, "concurrency");
    let mut p = Pipeline::start(&path).await?;
    let (s1, s2) = ("sessao-um", "sessao-dois");
    let shared = Uuid::new_v4();
    let (seg1, seg2) = (Uuid::new_v4(), Uuid::new_v4());

    p.send(speech_msg(s1, shared, 1, started(seg1))?).await?;
    p.send(speech_msg(s2, shared, 1, started(seg2))?).await?;
    p.send(speech_msg(
        s1,
        shared,
        2,
        transcript(seg1, 0, TranscriptState::Partial),
    )?)
    .await?;
    p.send(speech_msg(
        s2,
        shared,
        2,
        transcript(seg2, 0, TranscriptState::Confirmed),
    )?)
    .await?;
    p.send(cancel_stream_msg(s1, shared)?).await?;
    p.send(speech_msg(s2, shared, 3, SpeechEvent::StreamEnded)?)
        .await?;
    // Após o CANCEL de s1, um evento de s1 não pode ser aplicado.
    p.send(speech_msg(
        s1,
        shared,
        3,
        transcript(seg1, 1, TranscriptState::Confirmed),
    )?)
    .await?;
    let sentinel = Uuid::new_v4();
    p.send(speech_msg(s1, sentinel, 1, started(Uuid::new_v4()))?)
        .await?;

    // Aplicados: s1 início+parcial, s2 início+confirmado+fim, sentinela = 6. Rejeitado: s1 após CANCEL.
    let snap = p
        .wait_for(|s| s.speech_applied == 6 && s.cancels == 1)
        .await;
    r.check(
        "mesmo stream_id em sessões distintas: só o evento válido de cada uma aplicado",
        snap.speech_applied == 6 && snap.speech_rejected == 1 && snap.cancels == 1,
        stats_detail(&snap),
    );
    let st1 = p.stream_status(s1, &shared)?;
    let st2 = p.stream_status(s2, &shared)?;
    r.check(
        "CANCEL de s1 não cancela s2",
        st1.as_deref() == Some("CANCELLED") && st2.as_deref() == Some("ENDED"),
        format!("s1={st1:?} s2={st2:?}"),
    );
    p.stop().await;
    Ok(r)
}

fn store_open_failure(dir: &Path) -> Result<Report> {
    let mut r = Report::new("store-open-failure");
    let path = dir.join("store-open-failure.db");
    std::fs::write(&path, b"isto nao e um banco sqlite, nao deve ser aceito\n")
        .map_err(|e| KavoError::io("preparar arquivo inválido", e))?;
    let opened = Store::open(&path);
    r.check(
        "arquivo que não é SQLite impede a abertura",
        opened.is_err(),
        if opened.is_ok() { "abriu" } else { "recusado" },
    );
    Ok(r)
}

/// Falha real do SQLite: trigger aborta a atualização de status. Nada pode ser declarado como gravado.
async fn store_fault(dir: &Path) -> Result<Report> {
    const TRIGGER: &str = "CREATE TRIGGER falha_injetada BEFORE UPDATE OF status ON streams \
        BEGIN SELECT RAISE(ABORT, 'falha injetada'); END;";
    let mut r = Report::new("store-fault");
    let path = fresh_store(dir, "store-fault");
    let mut p = Pipeline::start(&path).await?;
    let (session, stream, seg) = ("sessao-fault", Uuid::new_v4(), Uuid::new_v4());

    p.send(speech_msg(session, stream, 1, started(seg))?)
        .await?;
    p.wait_for(|s| s.speech_applied >= 1).await;

    let conn = rusqlite::Connection::open(&path).map_err(|e| KavoError::Store(e.to_string()))?;
    conn.busy_timeout(WAIT)
        .map_err(|e| KavoError::Store(e.to_string()))?;
    conn.execute_batch(TRIGGER)
        .map_err(|e| KavoError::Store(e.to_string()))?;

    p.send(speech_msg(session, stream, 2, SpeechEvent::StreamEnded)?)
        .await?;
    let sentinel = Uuid::new_v4();
    p.send(speech_msg(session, sentinel, 1, started(Uuid::new_v4()))?)
        .await?;
    let snap = p.wait_for(|s| s.speech_applied >= 2).await;
    r.check(
        "falha de gravação contada e evento não aplicado",
        snap.store_errors >= 1 && snap.speech_applied == 2,
        stats_detail(&snap),
    );
    let during = p.stream_status(session, &stream)?;
    r.check(
        "status no banco continua OPEN durante a falha",
        during.as_deref() == Some("OPEN"),
        format!("{during:?}"),
    );

    conn.execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    // Reenvio idêntico após a falha: o produtor recupera pelo mesmo evento (D-35).
    p.send(speech_msg(session, stream, 2, SpeechEvent::StreamEnded)?)
        .await?;
    let after_sentinel = Uuid::new_v4();
    p.send(speech_msg(
        session,
        after_sentinel,
        1,
        started(Uuid::new_v4()),
    )?)
    .await?;
    let snap = p.wait_for(|s| s.speech_applied >= 4).await;
    let after = p.stream_status(session, &stream)?;
    r.check(
        "reenvio após remover a falha é gravado como ENDED",
        after.as_deref() == Some("ENDED") && snap.speech_applied == 4,
        format!("{after:?} {}", stats_detail(&snap)),
    );
    p.stop().await;
    Ok(r)
}

// ---------- controle (1.2) ----------

fn v12(mut msg: KavoMessage) -> KavoMessage {
    msg.schema_version = CONTROL_SCHEMA_VERSION.to_owned();
    msg
}

fn control_msg(session: &str, op: SessionControl) -> Result<KavoMessage> {
    Ok(v12(KavoMessage::new(
        MessageType::SessionControl,
        session,
        SOURCE,
    )
    .with_payload(&op)?))
}

fn speech_v12(
    session: &str,
    stream: Uuid,
    sequence: u64,
    event: SpeechEvent,
) -> Result<KavoMessage> {
    Ok(v12(speech_msg(session, stream, sequence, event)?))
}

/// CANCEL 1.2 de requisição: `correlation_id` = `message_id` da requisição, sem payload.
fn cancel_request_msg(session: &str, request: Uuid) -> KavoMessage {
    v12(KavoMessage::new(MessageType::Cancel, session, SOURCE)
        .with_correlation_id(request.to_string()))
}

fn cancel_stream_v12(session: &str, stream: Uuid) -> Result<KavoMessage> {
    Ok(v12(cancel_stream_msg(session, stream)?))
}

/// REQUEST_BEGIN: devolve a mensagem e o `request_id` (= seu `message_id`).
fn begin_msg(session: &str, stream: Uuid) -> Result<(KavoMessage, Uuid)> {
    let msg = control_msg(
        session,
        SessionControl::RequestBegin {
            kind: RequestKind::SttStream,
            stream_id: stream,
        },
    )?;
    let id = Uuid::parse_str(&msg.message_id).map_err(|e| KavoError::Protocol(e.to_string()))?;
    Ok((msg, id))
}

fn is(res: &ControlResult, outcome: Outcome, reason: Option<Reason>, state: WireState) -> bool {
    res.outcome == outcome && res.reason == reason && res.state == state
}

/// Inicia um stream 1.2 e espera o monitor aplicar o início.
async fn start_stream(p: &Pipeline, session: &str, stream: Uuid) -> Result<()> {
    let before = p.snapshot().speech_applied;
    p.send(speech_v12(session, stream, 1, started(Uuid::new_v4()))?)
        .await?;
    p.wait_for(move |s| s.speech_applied > before).await;
    Ok(())
}

/// Abre uma conexão direta ao banco do cenário para injetar falha de gravação.
fn fault_conn(path: &Path) -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(path).map_err(|e| KavoError::Store(e.to_string()))?;
    conn.busy_timeout(WAIT)
        .map_err(|e| KavoError::Store(e.to_string()))?;
    Ok(conn)
}

const FAULT_TRIGGER: &str = "CREATE TRIGGER falha_injetada BEFORE UPDATE OF status ON streams \
    BEGIN SELECT RAISE(ABORT, 'falha injetada'); END;";

async fn control_lifecycle(dir: &Path) -> Result<Report> {
    let mut r = Report::new("control-lifecycle");
    let path = fresh_store(dir, "control-lifecycle");
    let mut p = Pipeline::start(&path).await?;
    let s = "sessao-ciclo";
    let stream = Uuid::new_v4();

    let res = p.control(control_msg(s, SessionControl::Open)?).await?;
    r.check(
        "abrir sessão: APPLIED ACTIVE",
        is(&res, Outcome::Applied, None, WireState::Active),
        format!("{res:?}"),
    );
    let res = p.control(control_msg(s, SessionControl::Open)?).await?;
    r.check(
        "abrir de novo: NOOP ACTIVE",
        is(&res, Outcome::Noop, None, WireState::Active),
        format!("{res:?}"),
    );
    let res = p.control(control_msg(s, SessionControl::Status)?).await?;
    r.check(
        "consultar sessão ativa",
        is(&res, Outcome::Noop, None, WireState::Active),
        format!("{res:?}"),
    );

    let res = p.control(control_msg(s, SessionControl::Close)?).await?;
    r.check(
        "encerrar sessão: APPLIED CLOSED",
        is(&res, Outcome::Applied, None, WireState::Closed),
        format!("{res:?}"),
    );
    let res = p.control(control_msg(s, SessionControl::Close)?).await?;
    r.check(
        "encerrar duas vezes: NOOP CLOSED",
        is(&res, Outcome::Noop, None, WireState::Closed),
        format!("{res:?}"),
    );
    let res = p.control(control_msg(s, SessionControl::Open)?).await?;
    r.check(
        "sessão encerrada não reabre",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::SessionClosed),
            WireState::Closed,
        ),
        format!("{res:?}"),
    );

    let unknown = "sessao-nunca-aberta";
    let res = p
        .control(control_msg(unknown, SessionControl::Close)?)
        .await?;
    r.check(
        "encerrar sessão desconhecida: REJECTED SESSION_UNKNOWN",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::SessionUnknown),
            WireState::Unknown,
        ),
        format!("{res:?}"),
    );
    let res = p
        .control(control_msg(unknown, SessionControl::Status)?)
        .await?;
    r.check(
        "consultar sessão desconhecida: estado UNKNOWN",
        res.state == WireState::Unknown,
        format!("{res:?}"),
    );

    // Evento 1.2 para sessão encerrada e para sessão desconhecida: ambos recusados, nada gravado.
    p.send(speech_v12(s, stream, 1, started(Uuid::new_v4()))?)
        .await?;
    p.send(speech_v12(
        unknown,
        Uuid::new_v4(),
        1,
        started(Uuid::new_v4()),
    )?)
    .await?;
    let snap = p.wait_for(|st| st.session_rejected >= 2).await;
    r.check(
        "evento 1.2 em sessão encerrada e desconhecida rejeitado",
        snap.session_rejected == 2 && snap.speech_applied == 0,
        stats_detail(&snap),
    );
    let st = p.stream_status(s, &stream)?;
    r.check(
        "evento recusado não grava stream",
        st.is_none(),
        format!("{st:?}"),
    );

    // Evento 1.1 em sessão encerrada também é recusado: o modo legado nunca reabre sessão encerrada.
    p.send(speech_msg(s, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    let snap = p.wait_for(|st| st.session_rejected >= 3).await;
    r.check(
        "evento 1.1 em sessão encerrada rejeitado",
        snap.session_rejected == 3,
        stats_detail(&snap),
    );
    p.stop().await;
    Ok(r)
}

async fn control_requests(dir: &Path) -> Result<Report> {
    let mut r = Report::new("control-requests");
    let path = fresh_store(dir, "control-requests");
    let mut p = Pipeline::start(&path).await?;
    let (a, b) = ("sessao-req-a", "sessao-req-b");
    for session in [a, b] {
        p.control(control_msg(session, SessionControl::Open)?)
            .await?;
    }

    let (sa1, sa2, sa5, sa8, sa9) = (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let (m1, r1) = begin_msg(a, sa1)?;
    let res = p.control(m1.clone()).await?;
    r.check(
        "registrar requisição: APPLIED",
        is(&res, Outcome::Applied, None, WireState::Active),
        format!("{res:?}"),
    );
    let res = p.control(m1).await?;
    r.check(
        "mesma requisição registrada de novo: REQUEST_DUPLICATE",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::RequestDuplicate),
            WireState::Active,
        ),
        format!("{res:?}"),
    );
    let (m2, r2) = begin_msg(a, sa2)?;
    let res = p.control(m2).await?;
    r.check(
        "segunda requisição na mesma sessão: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    let (m_taken, _) = begin_msg(a, sa1)?;
    let res = p.control(m_taken).await?;
    r.check(
        "stream já vinculado a requisição ativa: STREAM_BOUND",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::StreamBound),
            WireState::Active,
        ),
        format!("{res:?}"),
    );

    // Propagação: stream vinculado já iniciado é cancelado junto com a requisição.
    start_stream(&p, a, sa2).await?;
    let res = p.control(cancel_request_msg(a, r2)).await?;
    r.check(
        "cancelar requisição com stream aberto: APPLIED e 1 stream cancelado",
        is(&res, Outcome::Applied, None, WireState::Active) && res.cancelled_streams == 1,
        format!("{res:?}"),
    );
    let st = p.stream_status(a, &sa2)?;
    r.check(
        "stream vinculado gravado como CANCELLED",
        st.as_deref() == Some("CANCELLED"),
        format!("{st:?}"),
    );
    let res = p.control(cancel_request_msg(a, r2)).await?;
    r.check(
        "cancelar a mesma requisição de novo: NOOP",
        is(&res, Outcome::Noop, None, WireState::Active),
        format!("{res:?}"),
    );
    let res = p
        .control(control_msg(
            a,
            SessionControl::RequestEnd { request_id: r2 },
        )?)
        .await?;
    r.check(
        "concluir requisição cancelada: REQUEST_CANCELLED",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::RequestCancelled),
            WireState::Active,
        ),
        format!("{res:?}"),
    );
    let res = p.control(cancel_request_msg(a, Uuid::new_v4())).await?;
    r.check(
        "cancelar requisição inexistente: REQUEST_UNKNOWN",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::RequestUnknown),
            WireState::Active,
        ),
        format!("{res:?}"),
    );

    // Isolamento: requisição de B não é alcançada por cancelamento enviado para A.
    let (mb, rb) = begin_msg(b, sa9)?;
    p.control(mb).await?;
    let res = p.control(cancel_request_msg(a, rb)).await?;
    r.check(
        "requisição de outra sessão não é alcançada: REQUEST_UNKNOWN",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::RequestUnknown),
            WireState::Active,
        ),
        format!("{res:?}"),
    );

    // Corrida conclusão × cancelamento: a ordem de chegada decide, de forma determinística.
    let (m6, r6) = begin_msg(a, Uuid::new_v4())?;
    p.control(m6).await?;
    let res = p
        .control(control_msg(
            a,
            SessionControl::RequestEnd { request_id: r6 },
        )?)
        .await?;
    r.check(
        "conclusão primeiro: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    let res = p.control(cancel_request_msg(a, r6)).await?;
    r.check(
        "cancelamento depois da conclusão: REQUEST_FINISHED",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::RequestFinished),
            WireState::Active,
        ),
        format!("{res:?}"),
    );
    let (m7, r7) = begin_msg(a, Uuid::new_v4())?;
    p.control(m7).await?;
    let res = p.control(cancel_request_msg(a, r7)).await?;
    r.check(
        "cancelamento primeiro: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    let res = p
        .control(control_msg(
            a,
            SessionControl::RequestEnd { request_id: r7 },
        )?)
        .await?;
    r.check(
        "conclusão depois do cancelamento: REQUEST_CANCELLED",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::RequestCancelled),
            WireState::Active,
        ),
        format!("{res:?}"),
    );

    let res = p
        .control(control_msg(
            a,
            SessionControl::RequestEnd { request_id: r1 },
        )?)
        .await?;
    r.check(
        "concluir requisição r1 antes do encerramento: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    // Encerrar sessão com requisições ativas: cancela as dela e o stream vinculado; B segue.
    let (m8, _r8) = begin_msg(a, sa8)?;
    p.control(m8).await?;
    let (m5, _r5) = begin_msg(a, sa5)?;
    p.control(m5).await?;
    start_stream(&p, a, sa8).await?;
    let res = p.control(control_msg(a, SessionControl::Close)?).await?;
    r.check(
        "encerrar sessão com 2 requisições ativas: cancelled_requests=2, 1 stream",
        res.outcome == Outcome::Applied
            && res.cancelled_requests == 2
            && res.cancelled_streams == 1,
        format!("{res:?}"),
    );
    let st = p.stream_status(a, &sa8)?;
    r.check(
        "stream da sessão encerrada CANCELLED",
        st.as_deref() == Some("CANCELLED"),
        format!("{st:?}"),
    );
    let res = p
        .control(control_msg(
            b,
            SessionControl::RequestEnd { request_id: rb },
        )?)
        .await?;
    r.check(
        "encerrar A não afeta requisição de B",
        is(&res, Outcome::Applied, None, WireState::Active),
        format!("{res:?}"),
    );
    p.stop().await;
    Ok(r)
}

async fn control_events(dir: &Path) -> Result<Report> {
    let mut r = Report::new("control-events");
    let path = fresh_store(dir, "control-events");
    let mut p = Pipeline::start(&path).await?;
    let s = "sessao-evt";
    p.control(control_msg(s, SessionControl::Open)?).await?;

    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());
    p.send(speech_v12(s, st, 1, started(seg))?).await?;
    p.send(speech_v12(
        s,
        st,
        2,
        transcript(seg, 0, TranscriptState::Partial),
    )?)
    .await?;
    p.send(speech_v12(
        s,
        st,
        2,
        transcript(seg, 0, TranscriptState::Partial),
    )?)
    .await?; // duplicata
    p.send(speech_v12(
        s,
        st,
        4,
        SpeechEvent::SpeechEnded { segment_id: seg },
    )?)
    .await?; // lacuna
    p.send(speech_v12(
        s,
        st,
        3,
        transcript(seg, 1, TranscriptState::Confirmed),
    )?)
    .await?;
    p.send(speech_v12(
        s,
        st,
        4,
        SpeechEvent::SpeechEnded { segment_id: seg },
    )?)
    .await?;
    p.send(speech_v12(s, st, 5, SpeechEvent::StreamEnded)?)
        .await?;
    let snap = p
        .wait_for(|x| x.speech_applied == 5 && x.speech_duplicate == 1 && x.speech_gap == 1)
        .await;
    r.check(
        "duplicata e lacuna tratadas; stream completo aplicado",
        snap.speech_applied == 5 && snap.speech_duplicate == 1 && snap.speech_gap == 1,
        stats_detail(&snap),
    );
    let status = p.stream_status(s, &st)?;
    r.check(
        "stream completo ENDED",
        status.as_deref() == Some("ENDED"),
        format!("{status:?}"),
    );

    // Cancelamento de stream 1.2 por payload, depois evento atrasado do mesmo stream.
    let st2 = Uuid::new_v4();
    start_stream(&p, s, st2).await?;
    let res = p.control(cancel_stream_v12(s, st2)?).await?;
    r.check(
        "CANCEL de stream 1.2: APPLIED com 1 stream",
        res.outcome == Outcome::Applied
            && res.cancelled_streams == 1
            && res.op == ControlOp::StreamCancel,
        format!("{res:?}"),
    );
    let before = p.snapshot().speech_applied;
    p.send(speech_v12(
        s,
        st2,
        2,
        transcript(seg, 0, TranscriptState::Confirmed),
    )?)
    .await?;
    // Sentinela 1.1 em sessão própria: o barramento é FIFO, então o atraso já foi processado.
    p.send(speech_msg(
        "sessao-sentinela",
        Uuid::new_v4(),
        1,
        started(Uuid::new_v4()),
    )?)
    .await?;
    let snap = p.wait_for(|x| x.speech_applied > before).await;
    r.check(
        "evento atrasado após CANCEL não aplicado",
        snap.speech_applied == before + 1 && snap.speech_rejected + snap.speech_stale >= 1,
        stats_detail(&snap),
    );
    let status = p.stream_status(s, &st2)?;
    r.check(
        "stream cancelado segue CANCELLED",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );

    // Encerrar a sessão, depois evento do mesmo tipo: recusado.
    let res = p.control(control_msg(s, SessionControl::Close)?).await?;
    r.check(
        "encerrar sessão: APPLIED sem streams abertos",
        res.outcome == Outcome::Applied && res.cancelled_streams == 0,
        format!("{res:?}"),
    );
    let before_rej = p.snapshot().session_rejected;
    p.send(speech_v12(s, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    let snap = p.wait_for(|x| x.session_rejected > before_rej).await;
    r.check(
        "evento após encerramento rejeitado",
        snap.session_rejected == before_rej + 1,
        stats_detail(&snap),
    );
    let status = p.stream_status(s, &st)?;
    r.check(
        "stream ENDED não é alterado pelo encerramento",
        status.as_deref() == Some("ENDED"),
        format!("{status:?}"),
    );
    p.stop().await;
    Ok(r)
}

async fn control_shutdown(dir: &Path) -> Result<Report> {
    let mut r = Report::new("control-shutdown");
    let path = fresh_store(dir, "control-shutdown");
    let (s, st) = ("sessao-shut", Uuid::new_v4());
    let (m, req) = begin_msg(s, st)?;

    let mut first = Pipeline::start(&path).await?;
    first.control(control_msg(s, SessionControl::Open)?).await?;
    first.control(m).await?;
    start_stream(&first, s, st).await?;
    // Encerramento com sessão, requisição e stream ativos.
    first.stop().await;

    let mut second = Pipeline::start(&path).await?;
    let res = second
        .control(control_msg(s, SessionControl::Status)?)
        .await?;
    r.check(
        "após reinício: sessão em memória some (UNKNOWN)",
        res.state == WireState::Unknown,
        format!("{res:?}"),
    );
    let res = second
        .control(control_msg(
            s,
            SessionControl::RequestEnd { request_id: req },
        )?)
        .await?;
    r.check(
        "após reinício: requisição antiga não existe",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::SessionUnknown),
            WireState::Unknown,
        ),
        format!("{res:?}"),
    );
    second
        .send(speech_v12(s, st, 2, SpeechEvent::StreamEnded)?)
        .await?;
    let snap = second.wait_for(|x| x.session_rejected >= 1).await;
    r.check(
        "evento 1.2 de sessão anterior ao reinício rejeitado",
        snap.session_rejected >= 1 && snap.speech_applied == 0,
        stats_detail(&snap),
    );
    // Mensagem 1.1 para o mesmo stream: sessão implícita, mas o stream interrompido não retoma.
    second
        .send(speech_msg(s, st, 2, SpeechEvent::StreamEnded)?)
        .await?;
    second
        .send(speech_msg(
            "sessao-sent-shut",
            Uuid::new_v4(),
            1,
            started(Uuid::new_v4()),
        )?)
        .await?;
    let snap = second.wait_for(|x| x.speech_applied >= 1).await;
    r.check(
        "após reinício: stream interrompido não retoma (1.1)",
        snap.speech_applied == 1 && snap.speech_rejected >= 1,
        stats_detail(&snap),
    );
    let status = second.stream_status(s, &st)?;
    r.check(
        "stream interrompido gravado como INTERRUPTED",
        status.as_deref() == Some("INTERRUPTED"),
        format!("{status:?}"),
    );
    second.stop().await;
    Ok(r)
}

async fn control_store_fault(dir: &Path) -> Result<Report> {
    let mut r = Report::new("control-store-fault");
    let path = fresh_store(dir, "control-store-fault");
    let mut p = Pipeline::start(&path).await?;
    let (s, st) = ("sessao-fault-ctl", Uuid::new_v4());
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (m, req) = begin_msg(s, st)?;
    p.control(m).await?;
    start_stream(&p, s, st).await?;

    let conn = fault_conn(&path)?;
    conn.execute_batch(FAULT_TRIGGER)
        .map_err(|e| KavoError::Store(e.to_string()))?;

    let res = p.control(cancel_request_msg(s, req)).await?;
    r.check(
        "cancelamento com falha de gravação: REJECTED STORE_UNAVAILABLE",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::StoreUnavailable),
            WireState::Active,
        ),
        format!("{res:?}"),
    );
    let res = p.control(control_msg(s, SessionControl::Close)?).await?;
    r.check(
        "encerramento com falha: REJECTED e sessão segue ativa",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::StoreUnavailable),
            WireState::Active,
        ),
        format!("{res:?}"),
    );
    let status = p.stream_status(s, &st)?;
    r.check(
        "stream segue OPEN no banco após falha",
        status.as_deref() == Some("OPEN"),
        format!("{status:?}"),
    );
    let res = p
        .control(control_msg(
            s,
            SessionControl::RequestEnd { request_id: req },
        )?)
        .await?;
    r.check(
        "requisição segue em andamento após falha",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );

    conn.execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let res = p.control(control_msg(s, SessionControl::Close)?).await?;
    r.check(
        "nova tentativa de encerramento após remover falha: APPLIED com 1 stream",
        res.outcome == Outcome::Applied && res.cancelled_streams == 1,
        format!("{res:?}"),
    );
    let status = p.stream_status(s, &st)?;
    r.check(
        "stream CANCELLED após nova tentativa",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );
    p.stop().await;
    Ok(r)
}

async fn compat(dir: &Path) -> Result<Report> {
    let mut r = Report::new("compat");
    let path = fresh_store(dir, "compat");
    let mut p = Pipeline::start(&path).await?;

    // Cliente 1.1 sem abrir sessão: comportamento legado preservado.
    let (s, st) = ("sessao-legado", Uuid::new_v4());
    p.send(speech_msg(s, st, 1, started(Uuid::new_v4()))?)
        .await?;
    p.send(speech_msg(s, st, 2, SpeechEvent::NoSpeech)?).await?;
    let snap = p
        .wait_for(|x| x.speech_applied >= 1 && x.session_implicit_open >= 1)
        .await;
    r.check(
        "1.1 sem sessão aberta: aceito e sessão aberta implicitamente",
        snap.speech_applied >= 1 && snap.session_implicit_open >= 1,
        stats_detail(&snap),
    );
    let status = p.stream_status(s, &st)?;
    r.check(
        "1.1 gravado no banco normalmente",
        status.as_deref() == Some("OPEN"),
        format!("{status:?}"),
    );

    // CANCEL 1.1 por stream: mesmo efeito de antes.
    p.send(cancel_stream_msg(s, st)?).await?;
    let snap = p.wait_for(|x| x.cancels >= 1).await;
    let status = p.stream_status(s, &st)?;
    r.check(
        "CANCEL 1.1 por stream grava CANCELLED",
        snap.cancels >= 1 && status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );

    // CANCEL 1.1 por correlation_id: sem resposta, sem alterar requisições (não existe nenhuma).
    let before_req = p.snapshot().requests_cancelled;
    p.send(
        KavoMessage::new(MessageType::Cancel, s, SOURCE)
            .with_correlation_id(Uuid::new_v4().to_string()),
    )
    .await?;
    let sentinel = Uuid::new_v4();
    p.send(speech_msg(
        "sessao-sentinela-compat",
        sentinel,
        1,
        started(Uuid::new_v4()),
    )?)
    .await?;
    let snap = p.wait_for(|x| x.speech_applied >= 2).await;
    r.check(
        "CANCEL 1.1 por requisição inexistente não altera requisições",
        snap.requests_cancelled == before_req,
        stats_detail(&snap),
    );

    // Validação por versão, sem rede: 1.1 não pode transportar tipos 1.2.
    let op = SessionControl::Open;
    let mut as_11 =
        KavoMessage::new(MessageType::SessionControl, "sessao-v", SOURCE).with_payload(&op)?;
    let raw_11 = serde_json::to_vec(&as_11).map_err(|e| KavoError::Protocol(e.to_string()))?;
    r.check(
        "SESSION_CONTROL com schema 1.1 é recusado",
        KavoMessage::from_json_slice(&raw_11).is_err(),
        "",
    );
    as_11 = v12(as_11);
    let raw_12 = serde_json::to_vec(&as_11).map_err(|e| KavoError::Protocol(e.to_string()))?;
    r.check(
        "SESSION_CONTROL com schema 1.2 é aceito",
        KavoMessage::from_json_slice(&raw_12).is_ok(),
        "",
    );
    let reply = KavoMessage::new(MessageType::ControlResult, "sessao-v", SOURCE).with_payload(
        &ControlResult::new(ControlOp::Open, Outcome::Applied, WireState::Active),
    )?;
    let raw_reply = serde_json::to_vec(&reply).map_err(|e| KavoError::Protocol(e.to_string()))?;
    r.check(
        "CONTROL_RESULT com schema 1.1 é recusado",
        KavoMessage::from_json_slice(&raw_reply).is_err(),
        "",
    );
    p.stop().await;
    Ok(r)
}

async fn ipc_stream(opts: &Options) -> Result<Report> {
    let mut r = Report::new("ipc-stream");
    let (Some(socket), Some(store_path)) = (opts.socket.as_deref(), opts.store.as_deref()) else {
        return Err(KavoError::Config(
            "ipc-stream exige --socket e --store".to_owned(),
        ));
    };
    let timeout = opts.ipc.request_timeout;
    let before = ipc::ping(socket, opts.ipc).await;
    r.check(
        "núcleo responde antes do cenário",
        matches!(before, Verification::Success(_)),
        format!("{before:?}"),
    );
    let store = Store::open(store_path)?;

    // Um stream sequencial: uma conexão, em ordem.
    let (session, stream, seg) = ("sessao-ipc", Uuid::new_v4(), Uuid::new_v4());
    let events = [
        speech_msg(session, stream, 1, started(seg))?,
        speech_msg(
            session,
            stream,
            2,
            transcript(seg, 0, TranscriptState::Confirmed),
        )?,
        speech_msg(session, stream, 3, SpeechEvent::StreamEnded)?,
    ];
    // Uma conexão para o stream inteiro: conexões separadas podem ser processadas fora de ordem.
    send_ordered(socket, &events, timeout).await?;
    let status = wait_status(&store, session, &stream, "ENDED").await?;
    r.check(
        "stream enviado por socket gravado como ENDED",
        status.as_deref() == Some("ENDED"),
        format!("{status:?}"),
    );

    // Quatro sessões em paralelo, cada uma com a própria conexão e ordem interna.
    let mut jobs = Vec::new();
    let mut ids = Vec::new();
    for i in 0..4 {
        let sid = format!("sessao-ipc-par-{i}");
        let st = Uuid::new_v4();
        let sg = Uuid::new_v4();
        let msgs = vec![
            speech_msg(&sid, st, 1, started(sg))?,
            speech_msg(&sid, st, 2, transcript(sg, 0, TranscriptState::Confirmed))?,
            speech_msg(&sid, st, 3, SpeechEvent::StreamEnded)?,
        ];
        ids.push((sid.clone(), st));
        jobs.push(async move { send_ordered(socket, &msgs, timeout).await });
    }
    for result in futures::future::join_all(jobs).await {
        result?;
    }
    let mut parallel_ok = true;
    let mut detail = String::new();
    for (sid, st) in &ids {
        let status = wait_status(&store, sid, st, "ENDED").await?;
        parallel_ok &= status.as_deref() == Some("ENDED");
        detail.push_str(&format!("{status:?} "));
    }
    r.check(
        "quatro sessões concorrentes gravadas como ENDED",
        parallel_ok,
        detail.trim_end(),
    );

    // CANCEL por socket: só o stream alvo é cancelado.
    let (csid, cstream, cseg) = ("sessao-ipc-cancel", Uuid::new_v4(), Uuid::new_v4());
    let keep = Uuid::new_v4();
    send_ordered(
        socket,
        &[
            speech_msg(csid, cstream, 1, started(cseg))?,
            speech_msg(csid, keep, 1, started(Uuid::new_v4()))?,
            cancel_stream_msg(csid, cstream)?,
        ],
        timeout,
    )
    .await?;
    let cancelled = wait_status(&store, csid, &cstream, "CANCELLED").await?;
    r.check(
        "CANCEL por socket grava CANCELLED sem afetar outro stream da sessão",
        cancelled.as_deref() == Some("CANCELLED")
            && store.stream_status(csid, &keep)?.as_deref() == Some("OPEN"),
        format!("{cancelled:?}"),
    );

    let after = ipc::ping(socket, opts.ipc).await;
    r.check(
        "núcleo segue respondendo após o cenário",
        matches!(after, Verification::Success(_)),
        format!("{after:?}"),
    );
    Ok(r)
}

/// Pedido de controle pelo socket real e leitura do `CONTROL_RESULT`.
async fn ask(socket: &Path, msg: &KavoMessage, settings: IpcSettings) -> Result<ControlResult> {
    let reply = ipc::request(socket, msg, settings).await?;
    if reply.kind != MessageType::ControlResult {
        return Err(KavoError::Protocol(format!(
            "resposta inesperada ao controle: {:?}",
            reply.kind
        )));
    }
    let payload = reply
        .payload
        .as_ref()
        .ok_or_else(|| KavoError::Protocol("CONTROL_RESULT sem payload".to_owned()))?;
    ControlResult::from_value(payload)
}

/// Envia uma linha crua e lê a primeira resposta do núcleo.
async fn raw_exchange(path: &Path, line: &[u8], settings: IpcSettings) -> Result<KavoMessage> {
    let mut bytes = line.to_vec();
    bytes.push(b'\n');
    let mut stream = tokio::time::timeout(settings.request_timeout, UnixStream::connect(path))
        .await
        .map_err(|_| KavoError::Timeout(settings.request_timeout))?
        .map_err(|e| KavoError::io(format!("conectar em {}", path.display()), e))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| KavoError::io("enviar linha", e))?;
    let (read_half, _write_half) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(read_half);
    let frame = tokio::time::timeout(
        settings.request_timeout,
        ipc::read_frame(&mut reader, settings.max_frame_bytes),
    )
    .await
    .map_err(|_| KavoError::Timeout(settings.request_timeout))??;
    let frame =
        frame.ok_or_else(|| KavoError::Protocol("servidor fechou sem responder".to_owned()))?;
    KavoMessage::from_json_slice(&frame)
}

async fn ipc_control(opts: &Options) -> Result<Report> {
    let mut r = Report::new("ipc-control");
    let (Some(socket), Some(store_path)) = (opts.socket.as_deref(), opts.store.as_deref()) else {
        return Err(KavoError::Config(
            "ipc-control exige --socket e --store".to_owned(),
        ));
    };
    let settings = opts.ipc;
    let timeout = settings.request_timeout;
    let before = ipc::ping(socket, settings).await;
    r.check(
        "núcleo responde antes do cenário",
        matches!(before, Verification::Success(_)),
        format!("{before:?}"),
    );
    let store = Store::open(store_path)?;

    // Sessão, requisição e cancelamento pelo socket.
    let a = format!("sessao-ipc-ctl-{}", Uuid::new_v4());
    let st = Uuid::new_v4();
    let seg = Uuid::new_v4();
    let res = ask(socket, &control_msg(&a, SessionControl::Open)?, settings).await?;
    r.check(
        "OPEN por socket: APPLIED ACTIVE",
        is(&res, Outcome::Applied, None, WireState::Active),
        format!("{res:?}"),
    );
    let res = ask(socket, &control_msg(&a, SessionControl::Open)?, settings).await?;
    r.check(
        "OPEN repetido por socket: NOOP",
        is(&res, Outcome::Noop, None, WireState::Active),
        format!("{res:?}"),
    );
    let (begin, req) = begin_msg(&a, st)?;
    let res = ask(socket, &begin, settings).await?;
    r.check(
        "REQUEST_BEGIN por socket: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    send_ordered(
        socket,
        &[
            speech_v12(&a, st, 1, started(seg))?,
            speech_v12(&a, st, 2, transcript(seg, 0, TranscriptState::Confirmed))?,
        ],
        timeout,
    )
    .await?;
    let status = wait_status(&store, &a, &st, "OPEN").await?;
    r.check(
        "stream 1.2 por socket gravado como OPEN",
        status.as_deref() == Some("OPEN"),
        format!("{status:?}"),
    );
    let res = ask(socket, &cancel_request_msg(&a, req), settings).await?;
    r.check(
        "CANCEL de requisição por socket: APPLIED com 1 stream",
        res.outcome == Outcome::Applied && res.cancelled_streams == 1,
        format!("{res:?}"),
    );
    let status = wait_status(&store, &a, &st, "CANCELLED").await?;
    r.check(
        "stream vinculado CANCELLED no banco",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );

    // Entrada inválida de controle e resposta do núcleo recebida como entrada.
    let mut bad = serde_json::to_value(control_msg(&a, SessionControl::Status)?)
        .map_err(|e| KavoError::Protocol(e.to_string()))?;
    bad["payload"]["op"] = serde_json::Value::String("NAO_EXISTE".to_owned());
    let reply = raw_exchange(socket, bad.to_string().as_bytes(), settings).await;
    r.check(
        "payload de controle inválido recebe ERROR",
        matches!(&reply, Ok(m) if m.kind == MessageType::Error),
        format!("{:?}", reply.as_ref().map(|m| m.kind)),
    );
    let reply_in = KavoMessage::new(MessageType::ControlResult, &a, SOURCE).with_payload(
        &ControlResult::new(ControlOp::Open, Outcome::Applied, WireState::Active),
    )?;
    let mut reply_in = v12(reply_in);
    reply_in.schema_version = CONTROL_SCHEMA_VERSION.to_owned();
    let reply = raw_exchange(
        socket,
        &serde_json::to_vec(&reply_in).map_err(|e| KavoError::Protocol(e.to_string()))?,
        settings,
    )
    .await;
    r.check(
        "CONTROL_RESULT enviado pelo cliente recebe ERROR",
        matches!(&reply, Ok(m) if m.kind == MessageType::Error),
        format!("{:?}", reply.as_ref().map(|m| m.kind)),
    );

    // Encerramento e evento posterior, verificado por um sentinela de outra sessão.
    let res = ask(socket, &control_msg(&a, SessionControl::Close)?, settings).await?;
    r.check(
        "CLOSE por socket: APPLIED CLOSED",
        is(&res, Outcome::Applied, None, WireState::Closed),
        format!("{res:?}"),
    );
    let res = ask(socket, &control_msg(&a, SessionControl::Close)?, settings).await?;
    r.check(
        "CLOSE repetido por socket: NOOP",
        is(&res, Outcome::Noop, None, WireState::Closed),
        format!("{res:?}"),
    );
    let late_stream = Uuid::new_v4();
    send_ordered(
        socket,
        &[speech_v12(&a, late_stream, 1, started(Uuid::new_v4()))?],
        timeout,
    )
    .await?;
    let sentinel = format!("sessao-ipc-sent-{}", Uuid::new_v4());
    let sentinel_stream = Uuid::new_v4();
    send_ordered(
        socket,
        &[
            speech_msg(&sentinel, sentinel_stream, 1, started(Uuid::new_v4()))?,
            speech_msg(&sentinel, sentinel_stream, 2, SpeechEvent::StreamEnded)?,
        ],
        timeout,
    )
    .await?;
    let sent = wait_status(&store, &sentinel, &sentinel_stream, "ENDED").await?;
    let late = store.stream_status(&a, &late_stream)?;
    r.check(
        "evento 1.2 para sessão encerrada não grava stream",
        sent.as_deref() == Some("ENDED") && late.is_none(),
        format!("sentinela={sent:?} atrasado={late:?}"),
    );

    // Quatro sessões em paralelo, com encerramento de metade delas.
    let mut jobs = Vec::new();
    let mut plan = Vec::new();
    for i in 0..4 {
        let sid = format!("sessao-ipc-par-ctl-{}-{i}", Uuid::new_v4());
        let stream = Uuid::new_v4();
        let seg = Uuid::new_v4();
        plan.push((sid.clone(), stream));
        jobs.push(async move {
            let open = ask(socket, &control_msg(&sid, SessionControl::Open)?, settings).await?;
            let (begin, _) = begin_msg(&sid, stream)?;
            let begun = ask(socket, &begin, settings).await?;
            send_ordered(
                socket,
                &[
                    speech_v12(&sid, stream, 1, started(seg))?,
                    speech_v12(
                        &sid,
                        stream,
                        2,
                        transcript(seg, 0, TranscriptState::Confirmed),
                    )?,
                ],
                timeout,
            )
            .await?;
            Ok::<_, KavoError>((open, begun))
        });
    }
    let mut opened_ok = true;
    for result in futures::future::join_all(jobs).await {
        let (open, begun) = result?;
        opened_ok &= open.outcome == Outcome::Applied && begun.outcome == Outcome::Applied;
    }
    r.check(
        "quatro sessões concorrentes abertas e com requisição",
        opened_ok,
        "",
    );
    for (sid, stream) in &plan[..2] {
        let res = ask(socket, &control_msg(sid, SessionControl::Close)?, settings).await?;
        r.check(
            "encerrar sessão concorrente: APPLIED com 1 stream",
            res.outcome == Outcome::Applied && res.cancelled_streams == 1,
            format!("{res:?}"),
        );
        let status = wait_status(&store, sid, stream, "CANCELLED").await?;
        r.check(
            "stream da sessão encerrada CANCELLED",
            status.as_deref() == Some("CANCELLED"),
            format!("{status:?}"),
        );
    }
    for (sid, stream) in &plan[2..] {
        let status = store.stream_status(sid, stream)?;
        r.check(
            "sessão não encerrada segue com stream OPEN",
            status.as_deref() == Some("OPEN"),
            format!("{status:?}"),
        );
        send_ordered(
            socket,
            &[speech_v12(sid, *stream, 3, SpeechEvent::StreamEnded)?],
            timeout,
        )
        .await?;
        let status = wait_status(&store, sid, stream, "ENDED").await?;
        r.check(
            "sessão ativa conclui stream após encerramento de outras",
            status.as_deref() == Some("ENDED"),
            format!("{status:?}"),
        );
    }

    let after = ipc::ping(socket, settings).await;
    r.check(
        "núcleo segue respondendo após o cenário",
        matches!(after, Verification::Success(_)),
        format!("{after:?}"),
    );
    Ok(r)
}

/// Prepara estado ativo (sessão, requisição, stream aberto) para ser interrompido por encerramento.
/// Os IDs são gravados em `dir/ipc-interrupt.json`, sem conteúdo de transcrição.
async fn ipc_interrupt_prepare(opts: &Options) -> Result<Report> {
    let mut r = Report::new("ipc-interrupt-prepare");
    let Some(socket) = opts.socket.as_deref() else {
        return Err(KavoError::Config(
            "ipc-interrupt-prepare exige --socket".to_owned(),
        ));
    };
    let settings = opts.ipc;
    let (s, st) = (format!("sessao-int-{}", Uuid::new_v4()), Uuid::new_v4());
    let seg = Uuid::new_v4();
    let res = ask(socket, &control_msg(&s, SessionControl::Open)?, settings).await?;
    r.check(
        "sessão aberta antes do encerramento",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    let (begin, req) = begin_msg(&s, st)?;
    let res = ask(socket, &begin, settings).await?;
    r.check(
        "requisição ativa antes do encerramento",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    send_ordered(
        socket,
        &[speech_v12(&s, st, 1, started(seg))?],
        settings.request_timeout,
    )
    .await?;
    let state = serde_json::json!({"session_id": s, "stream_id": st, "request_id": req});
    std::fs::write(opts.dir.join("ipc-interrupt.json"), state.to_string())
        .map_err(|e| KavoError::io("gravar estado do cenário", e))?;
    r.check("estado ativo gravado para verificação", true, "");
    Ok(r)
}

/// Verifica, após reinício do núcleo, que o estado ativo anterior não voltou.
async fn ipc_interrupt_verify(opts: &Options) -> Result<Report> {
    let mut r = Report::new("ipc-interrupt-verify");
    let (Some(socket), Some(store_path)) = (opts.socket.as_deref(), opts.store.as_deref()) else {
        return Err(KavoError::Config(
            "ipc-interrupt-verify exige --socket e --store".to_owned(),
        ));
    };
    let raw = std::fs::read_to_string(opts.dir.join("ipc-interrupt.json"))
        .map_err(|e| KavoError::io("ler estado do cenário", e))?;
    let state: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| KavoError::Protocol(e.to_string()))?;
    let s = state["session_id"].as_str().unwrap_or_default().to_owned();
    let st = Uuid::parse_str(state["stream_id"].as_str().unwrap_or_default())
        .map_err(|e| KavoError::Protocol(e.to_string()))?;
    let req = Uuid::parse_str(state["request_id"].as_str().unwrap_or_default())
        .map_err(|e| KavoError::Protocol(e.to_string()))?;
    let settings = opts.ipc;

    let res = ask(socket, &control_msg(&s, SessionControl::Status)?, settings).await?;
    r.check(
        "após reinício: sessão desconhecida",
        res.state == WireState::Unknown,
        format!("{res:?}"),
    );
    let res = ask(
        socket,
        &control_msg(&s, SessionControl::RequestEnd { request_id: req })?,
        settings,
    )
    .await?;
    r.check(
        "após reinício: requisição anterior inexistente",
        is(
            &res,
            Outcome::Rejected,
            Some(Reason::SessionUnknown),
            WireState::Unknown,
        ),
        format!("{res:?}"),
    );
    let store = Store::open(store_path)?;
    let status = store.stream_status(&s, &st)?;
    r.check(
        "após reinício: stream interrompido gravado como INTERRUPTED",
        status.as_deref() == Some("INTERRUPTED"),
        format!("{status:?}"),
    );
    Ok(r)
}

/// Conexão persistente de teste: uma mensagem por vez, com a resposta lida antes da próxima.
struct Wire {
    write: tokio::net::unix::OwnedWriteHalf,
    read: tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>,
}

impl Wire {
    async fn open(path: &Path, timeout: Duration) -> Result<Self> {
        let stream = tokio::time::timeout(timeout, UnixStream::connect(path))
            .await
            .map_err(|_| KavoError::Timeout(timeout))?
            .map_err(|e| KavoError::io(format!("conectar em {}", path.display()), e))?;
        let (read, write) = stream.into_split();
        Ok(Self {
            write,
            read: tokio::io::BufReader::new(read),
        })
    }

    /// Envia `msg` e espera a resposta, se houver. Sem resposta (1.1), devolve `None`.
    async fn exchange(
        &mut self,
        msg: &KavoMessage,
        expect_reply: bool,
    ) -> Result<Option<KavoMessage>> {
        let mut line = serde_json::to_vec(msg).map_err(|e| KavoError::Protocol(e.to_string()))?;
        line.push(b'\n');
        self.write
            .write_all(&line)
            .await
            .map_err(|e| KavoError::io("enviar mensagem", e))?;
        if !expect_reply {
            return Ok(None);
        }
        let frame = ipc::read_frame(&mut self.read, FRAME_LIMIT)
            .await?
            .ok_or_else(|| KavoError::Protocol("servidor fechou sem ACK".to_owned()))?;
        KavoMessage::from_json_slice(&frame).map(Some)
    }
}

fn ack_of(reply: &KavoMessage) -> Option<AdmissionAck> {
    if reply.kind != MessageType::AdmissionAck {
        return None;
    }
    reply
        .payload
        .as_ref()
        .and_then(|p| AdmissionAck::from_value(p).ok())
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 - 1.0) * q).round() as usize]
}

async fn ipc_admission(opts: &Options) -> Result<Report> {
    let mut r = Report::new("ipc-admission");
    let (Some(socket), Some(store_path)) = (opts.socket.as_deref(), opts.store.as_deref()) else {
        return Err(KavoError::Config(
            "ipc-admission exige --socket e --store".to_owned(),
        ));
    };
    let settings = opts.ipc;
    let store = Store::open(store_path)?;
    let s = format!("sessao-adm-{}", Uuid::new_v4());
    ask(socket, &control_msg(&s, SessionControl::Open)?, settings).await?;
    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());
    let start = speech_v12(&s, st, 1, started(seg))?;
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&start),
        settings.request_timeout,
    )
    .await?;
    let ack = replies.first().and_then(ack_of);
    r.check(
        "STT 1.2 pelo socket: ACCEPTED com correlação",
        ack.as_ref()
            .is_some_and(|a| a.status == AdmissionStatus::Accepted)
            && replies[0].correlation_id.as_deref() == Some(start.message_id.as_str()),
        format!("{ack:?}"),
    );
    let text = serde_json::to_string(&replies).unwrap_or_default();
    r.check(
        "ACK pelo socket sem texto transcrito",
        !text.contains("texto sint"),
        "",
    );

    let partial = speech_v12(&s, st, 2, transcript(seg, 0, TranscriptState::Partial))?;
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&partial),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "parcial pelo socket: ACCEPTED",
        replies.first().and_then(ack_of).map(|a| a.status) == Some(AdmissionStatus::Accepted),
        format!("{replies:?}"),
    );
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&partial),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "mesma mensagem reenviada (ACK perdido): DUPLICATE",
        replies.first().and_then(ack_of).map(|a| a.status) == Some(AdmissionStatus::Duplicate),
        format!("{replies:?}"),
    );

    let interp = Uuid::new_v4();
    let intent = intent_v12(&s, st, seg, interp, 0, "0");
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&intent),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "INTENT pelo socket: ACCEPTED",
        replies.first().and_then(ack_of).map(|a| a.status) == Some(AdmissionStatus::Accepted),
        format!("{replies:?}"),
    );
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&intent),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "INTENT reenviado: DUPLICATE",
        replies.first().and_then(ack_of).map(|a| a.status) == Some(AdmissionStatus::Duplicate),
        format!("{replies:?}"),
    );

    let before_admissions = store.admission_count()?;
    let many: Vec<KavoMessage> = (3..40)
        .map(|seq| {
            speech_v12(
                &s,
                st,
                seq,
                transcript(
                    seg,
                    u32::try_from(seq - 2).unwrap_or(0),
                    TranscriptState::Partial,
                ),
            )
        })
        .collect::<Result<_>>()?;
    let replies = send_ordered(socket, &many, settings.request_timeout).await?;
    let all_ok = replies.len() == many.len()
        && replies
            .iter()
            .all(|m| ack_of(m).is_some_and(|a| a.status == AdmissionStatus::Accepted));
    r.check(
        "lote de parciais: um ACK por evento, todos ACCEPTED",
        all_ok,
        format!("{} respostas", replies.len()),
    );
    r.check(
        "parciais não criam admissão de execução",
        store.admission_count()? == before_admissions,
        format!(
            "antes={before_admissions} depois={}",
            store.admission_count()?
        ),
    );

    let unknown = speech_v12(
        &format!("nunca-{}", Uuid::new_v4()),
        Uuid::new_v4(),
        1,
        started(seg),
    )?;
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&unknown),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "sessão desconhecida pelo socket: REJECTED SESSION_UNKNOWN",
        replies
            .first()
            .and_then(ack_of)
            .is_some_and(|a| a.reason == Some(Reason::SessionUnknown)),
        format!("{replies:?}"),
    );
    ask(socket, &control_msg(&s, SessionControl::Close)?, settings).await?;
    let closed = speech_v12(&s, st, 40, SpeechEvent::NoSpeech)?;
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&closed),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "sessão encerrada pelo socket: REJECTED SESSION_CLOSED",
        replies
            .first()
            .and_then(ack_of)
            .is_some_and(|a| a.reason == Some(Reason::SessionClosed)),
        format!("{replies:?}"),
    );

    // Legado 1.1 pelo socket: nenhuma resposta; o núcleo continua respondendo.
    let legacy = speech_msg(
        &format!("legado-{}", Uuid::new_v4()),
        Uuid::new_v4(),
        1,
        started(seg),
    )?;
    let replies = send_ordered(
        socket,
        std::slice::from_ref(&legacy),
        settings.request_timeout,
    )
    .await?;
    r.check(
        "1.1 pelo socket: sem ACK (comportamento legado)",
        replies.is_empty(),
        format!("{}", replies.len()),
    );

    // ACK recebido como entrada é recusado; ACK não é lido como CONTROL_RESULT.
    let fake = KavoMessage::new(MessageType::AdmissionAck, "s", "cliente").with_payload(
        &AdmissionAck {
            event: AckEvent::SttResult,
            status: AdmissionStatus::Accepted,
            reason: None,
            stream_id: None,
            sequence: None,
            interpretation_id: None,
            revision: None,
        },
    )?;
    let mut fake = v12(fake);
    fake.schema_version = CONTROL_SCHEMA_VERSION.to_owned();
    let reply = raw_exchange(
        socket,
        &serde_json::to_vec(&fake).map_err(|e| KavoError::Protocol(e.to_string()))?,
        settings,
    )
    .await;
    r.check(
        "ACK enviado pelo cliente recebe ERROR",
        matches!(&reply, Ok(m) if m.kind == MessageType::Error),
        format!("{:?}", reply.as_ref().map(|m| m.kind)),
    );
    let ack_only = replies
        .first()
        .and_then(|m| m.payload.as_ref())
        .is_some_and(|v| ControlResult::from_value(v).is_err());
    r.check(
        "ACK não confundido com CONTROL_RESULT",
        ack_only || replies.is_empty(),
        "",
    );

    let after = ipc::ping(socket, settings).await;
    r.check(
        "núcleo segue respondendo",
        matches!(after, Verification::Success(_)),
        format!("{after:?}"),
    );
    Ok(r)
}

/// Contrapressão real: fila de admissão pequena (configurada no daemon) e muitos clientes.
/// Cada cliente reenvia o mesmo evento após `BUSY`, antes do próximo. Nada deve ser admitido em dobro.
async fn ipc_backpressure(opts: &Options) -> Result<Report> {
    const CLIENTS: usize = 12;
    const EVENTS: u64 = 150;
    let mut r = Report::new("ipc-backpressure");
    let Some(socket) = opts.socket.as_deref() else {
        return Err(KavoError::Config(
            "ipc-backpressure exige --socket".to_owned(),
        ));
    };
    let settings = opts.ipc;
    let mut jobs = Vec::new();
    for client in 0..CLIENTS {
        let socket = socket.to_owned();
        jobs.push(async move {
            let s = format!("sessao-bp-{}-{client}", Uuid::new_v4());
            let st = Uuid::new_v4();
            let seg = Uuid::new_v4();
            let mut wire = Wire::open(&socket, settings.request_timeout).await?;
            let mut stats = [0u64; 4]; // accepted, duplicate, busy, other
            let open = control_msg(&s, SessionControl::Open)?;
            wire.exchange(&open, true).await?;
            let mut sequence = 1u64;
            while sequence <= EVENTS {
                let msg = if sequence == 1 {
                    speech_v12(&s, st, 1, started(seg))?
                } else {
                    speech_v12(
                        &s,
                        st,
                        sequence,
                        transcript(
                            seg,
                            u32::try_from(sequence - 2).unwrap_or(0),
                            TranscriptState::Partial,
                        ),
                    )?
                };
                loop {
                    let reply = wire
                        .exchange(&msg, true)
                        .await?
                        .ok_or_else(|| KavoError::Protocol("sem resposta".to_owned()))?;
                    match ack_of(&reply).map(|a| (a.status, a.reason)) {
                        Some((AdmissionStatus::Accepted, _)) => {
                            stats[0] += 1;
                            break;
                        }
                        Some((AdmissionStatus::NotReady, Some(Reason::Busy))) => {
                            stats[2] += 1;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        Some((AdmissionStatus::Duplicate, _)) => {
                            stats[1] += 1;
                            break;
                        }
                        _ => {
                            stats[3] += 1;
                            break;
                        }
                    }
                }
                sequence += 1;
            }
            Ok::<_, KavoError>(stats)
        });
    }
    let mut totals = [0u64; 4];
    let started_at = Instant::now();
    for result in futures::future::join_all(jobs).await {
        match result {
            Ok(stats) => {
                for (t, v) in totals.iter_mut().zip(stats) {
                    *t += v;
                }
            }
            Err(e) => return Err(e),
        }
    }
    let elapsed = started_at.elapsed().as_secs_f64();
    r.metric("clientes", CLIENTS as f64);
    r.metric("eventos_por_cliente", EVENTS as f64);
    r.metric("aceitos", totals[0] as f64);
    r.metric("busy", totals[2] as f64);
    r.metric("duplicados", totals[1] as f64);
    r.metric("outras_recusas", totals[3] as f64);
    r.metric("vazao_admitidos_por_s", totals[0] as f64 / elapsed);
    r.check(
        "todo evento admitido exatamente uma vez",
        totals[0] == CLIENTS as u64 * EVENTS && totals[1] == 0 && totals[3] == 0,
        format!(
            "aceitos={} duplicados={} outras={}",
            totals[0], totals[1], totals[3]
        ),
    );
    r.check(
        "backpressure exercitado (BUSY observado)",
        totals[2] > 0,
        format!(
            "busy={} — configure KAVO_IPC_ADMISSION_QUEUE pequeno no daemon",
            totals[2]
        ),
    );
    let after = ipc::ping(socket, settings).await;
    r.check(
        "núcleo segue respondendo após a carga",
        matches!(after, Verification::Success(_)),
        format!("{after:?}"),
    );
    Ok(r)
}

/// Medidas de latência e vazão (sem pass/fail): ida e volta de ACK, 1.1 sem ACK e concorrência.
async fn ipc_bench(opts: &Options) -> Result<Report> {
    const N: u64 = 1000;
    const CLIENTS: usize = 8;
    const PER_CLIENT: u64 = 250;
    let mut r = Report::new("ipc-bench");
    let Some(socket) = opts.socket.as_deref() else {
        return Err(KavoError::Config("ipc-bench exige --socket".to_owned()));
    };
    let settings = opts.ipc;

    // Ida e volta sequencial, uma conexão.
    let s = format!("sessao-bench-{}", Uuid::new_v4());
    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());
    let mut wire = Wire::open(socket, settings.request_timeout).await?;
    wire.exchange(&control_msg(&s, SessionControl::Open)?, true)
        .await?;
    wire.exchange(&speech_v12(&s, st, 1, started(seg))?, true)
        .await?;
    let mut rtt = Vec::with_capacity(N as usize);
    let t_all = Instant::now();
    let mut accepted = 0u64;
    for seq in 2..=N + 1 {
        let msg = speech_v12(
            &s,
            st,
            seq,
            transcript(
                seg,
                u32::try_from(seq - 2).unwrap_or(0),
                TranscriptState::Partial,
            ),
        )?;
        let t0 = Instant::now();
        let reply = wire
            .exchange(&msg, true)
            .await?
            .ok_or_else(|| KavoError::Protocol("sem ACK".to_owned()))?;
        rtt.push(t0.elapsed().as_micros() as f64);
        if ack_of(&reply).is_some_and(|a| a.status == AdmissionStatus::Accepted) {
            accepted += 1;
        }
    }
    let secs = t_all.elapsed().as_secs_f64();
    rtt.sort_by(|a, b| a.total_cmp(b));
    r.metric("ack_1_cliente_p50_us", percentile(&rtt, 0.50));
    r.metric("ack_1_cliente_p95_us", percentile(&rtt, 0.95));
    r.metric("ack_1_cliente_p99_us", percentile(&rtt, 0.99));
    r.metric("ack_1_cliente_max_us", percentile(&rtt, 1.0));
    r.metric("ack_1_cliente_vazao_por_s", N as f64 / secs);
    r.check(
        "parciais 1.2 aceitos no bench sequencial",
        accepted == N,
        format!("{accepted} de {N}"),
    );

    // Linha de base 1.1: envio sem ACK (mede só a escrita no socket, não o processamento).
    let legacy_s = format!("sessao-bench-legado-{}", Uuid::new_v4());
    let mut batch = Vec::new();
    for seq in 1..=N {
        let msg = if seq == 1 {
            speech_msg(&legacy_s, Uuid::new_v4(), 1, started(Uuid::new_v4()))?
        } else {
            speech_msg(&legacy_s, st, seq, SpeechEvent::NoSpeech)?
        };
        batch.push(msg);
    }
    let t1 = Instant::now();
    let _ = send_ordered(socket, &batch, settings.request_timeout).await;
    let legacy_secs = t1.elapsed().as_secs_f64();
    r.metric("base_1_1_sem_ack_envios_por_s", N as f64 / legacy_secs);

    // Concorrência: clientes em paralelo, cada um com a própria sessão e conexão.
    let t2 = Instant::now();
    let mut jobs = Vec::new();
    for c in 0..CLIENTS {
        let socket = socket.to_owned();
        jobs.push(async move {
            let s = format!("sessao-bench-par-{}-{c}", Uuid::new_v4());
            let st = Uuid::new_v4();
            let seg = Uuid::new_v4();
            let mut wire = Wire::open(&socket, settings.request_timeout).await?;
            wire.exchange(&control_msg(&s, SessionControl::Open)?, true)
                .await?;
            wire.exchange(&speech_v12(&s, st, 1, started(seg))?, true)
                .await?;
            let mut rtt = Vec::with_capacity(PER_CLIENT as usize);
            for seq in 2..=PER_CLIENT + 1 {
                let msg = speech_v12(
                    &s,
                    st,
                    seq,
                    transcript(
                        seg,
                        u32::try_from(seq - 2).unwrap_or(0),
                        TranscriptState::Partial,
                    ),
                )?;
                let t0 = Instant::now();
                wire.exchange(&msg, true).await?;
                rtt.push(t0.elapsed().as_micros() as f64);
            }
            Ok::<_, KavoError>(rtt)
        });
    }
    let mut all = Vec::new();
    for result in futures::future::join_all(jobs).await {
        all.extend(result?);
    }
    let secs = t2.elapsed().as_secs_f64();
    all.sort_by(|a, b| a.total_cmp(b));
    r.metric("ack_concorrente_clientes", CLIENTS as f64);
    r.metric("ack_concorrente_p50_us", percentile(&all, 0.50));
    r.metric("ack_concorrente_p95_us", percentile(&all, 0.95));
    r.metric("ack_concorrente_p99_us", percentile(&all, 0.99));
    r.metric("ack_concorrente_vazao_por_s", all.len() as f64 / secs);
    Ok(r)
}

/// Espera o status do stream no banco até `want` (ou o prazo acabar) e devolve o último lido.
async fn wait_status(
    store: &Store,
    session: &str,
    stream: &Uuid,
    want: &str,
) -> Result<Option<String>> {
    let deadline = Instant::now() + WAIT;
    loop {
        let status = store.stream_status(session, stream)?;
        if status.as_deref() == Some(want) || Instant::now() >= deadline {
            return Ok(status);
        }
        tokio::time::sleep(POLL * 10).await;
    }
}

/// Envia mensagens NDJSON em ordem por uma conexão e devolve os ACKs/respostas recebidos.
/// Escrita e leitura correm em paralelo: o servidor responde a cada evento 1.2 antes de ler o próximo,
/// então ler só depois de escrever tudo travaria com streams longos.
async fn send_ordered(
    path: &Path,
    msgs: &[KavoMessage],
    timeout: Duration,
) -> Result<Vec<KavoMessage>> {
    let mut bytes = Vec::new();
    let mut expected = 0usize;
    for msg in msgs {
        serde_json::to_writer(&mut bytes, msg).map_err(|e| KavoError::Protocol(e.to_string()))?;
        bytes.push(b'\n');
        if msg.minor() >= 2
            && matches!(
                msg.kind,
                MessageType::SttResult | MessageType::IntentDetected
            )
        {
            expected += 1;
        }
    }
    let stream = tokio::time::timeout(timeout, UnixStream::connect(path))
        .await
        .map_err(|_| KavoError::Timeout(timeout))?
        .map_err(|e| KavoError::io(format!("conectar em {}", path.display()), e))?;
    let (read_half, mut write_half) = stream.into_split();
    let write = async {
        write_half
            .write_all(&bytes)
            .await
            .map_err(|e| KavoError::io("enviar mensagens", e))?;
        write_half
            .shutdown()
            .await
            .map_err(|e| KavoError::io("fechar envio", e))
    };
    let read = async {
        let mut reader = tokio::io::BufReader::new(read_half);
        let mut replies = Vec::with_capacity(expected);
        for _ in 0..expected {
            let frame = ipc::read_frame(&mut reader, FRAME_LIMIT)
                .await?
                .ok_or_else(|| KavoError::Protocol("servidor fechou antes do ACK".to_owned()))?;
            replies.push(KavoMessage::from_json_slice(&frame)?);
        }
        Ok::<_, KavoError>(replies)
    };
    let (written, replies) = tokio::time::timeout(timeout * 4, async { tokio::join!(write, read) })
        .await
        .map_err(|_| KavoError::Timeout(timeout * 4))?;
    written?;
    replies
}

fn session_err(e: SessionError) -> KavoError {
    KavoError::Protocol(e.to_string())
}

// ---------- admissão de eventos (ACK, D-58) ----------

/// INTENT 1.2 mínimo e válido: interpretação provisória sem intenção.
fn intent_v12(
    session: &str,
    stream: Uuid,
    segment: Uuid,
    interpretation: Uuid,
    revision: u32,
    engine_version: &str,
) -> KavoMessage {
    let mut msg = KavoMessage::new(MessageType::IntentDetected, session, SOURCE);
    msg.payload = Some(serde_json::json!({
        "interpretation_id": interpretation,
        "stream_id": stream,
        "segment_id": segment,
        "source_revision": 0,
        "revision": revision,
        "state": "PROVISIONAL",
        "engine": {"name": "fixture", "version": engine_version},
    }));
    v12(msg)
}

fn ack_is(ack: &AdmissionAck, status: AdmissionStatus, reason: Option<Reason>) -> bool {
    ack.status == status && ack.reason == reason
}

async fn admission(dir: &Path) -> Result<Report> {
    let mut r = Report::new("admission");
    let path = fresh_store(dir, "admission");
    let mut p = Pipeline::start(&path).await?;
    let s = "sessao-ack";
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());

    // Aceito: identidade do evento preservada no ACK, sem texto.
    let m1 = speech_v12(s, st, 1, started(seg))?;
    let (raw, ack) = p.admit(m1.clone()).await?;
    r.check(
        "STT 1.2 aceito",
        ack_is(&ack, AdmissionStatus::Accepted, None),
        format!("{ack:?}"),
    );
    r.check(
        "ACK correlaciona com o message_id do evento",
        raw.correlation_id.as_deref() == Some(m1.message_id.as_str())
            && raw.schema_version == CONTROL_SCHEMA_VERSION,
        format!("{:?}", raw.correlation_id),
    );
    r.check(
        "ACK identifica stream e sequência",
        ack.event == AckEvent::SttResult && ack.stream_id == Some(st) && ack.sequence == Some(1),
        format!("{ack:?}"),
    );
    let text = serde_json::to_string(&raw).unwrap_or_default();
    r.check(
        "ACK não contém texto transcrito",
        !text.contains("texto sint"),
        "",
    );

    let m2 = speech_v12(s, st, 2, transcript(seg, 0, TranscriptState::Partial))?;
    let (_, ack) = p.admit(m2.clone()).await?;
    r.check(
        "STT parcial aceito",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );

    // Repetição com a mesma identidade (ACK perdido): DUPLICATE, sem reaplicar.
    let before = p.snapshot().speech_applied;
    let (_, ack) = p.admit(m2.clone()).await?;
    let after = p.snapshot().speech_applied;
    r.check(
        "repetição do mesmo evento: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    r.check(
        "repetição não reaplica efeito",
        after == before,
        format!("antes={before} depois={after}"),
    );

    let (_, ack) = p
        .admit(speech_v12(s, st, 5, SpeechEvent::NoSpeech)?)
        .await?;
    r.check(
        "lacuna de sequência: REJECTED SEQUENCE_GAP",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::SequenceGap)),
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(
            s,
            st,
            2,
            transcript(seg, 0, TranscriptState::Confirmed),
        )?)
        .await?;
    r.check(
        "sequência reutilizada com conteúdo diferente: REJECTED CONFLICT",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::Conflict)),
        format!("{ack:?}"),
    );

    // Conteúdo inválido consome a sequência: repetir a mesma mensagem devolve a mesma recusa, não DUPLICATE.
    let bad = speech_v12(s, st, 3, transcript(seg, 9, TranscriptState::Partial))?;
    let (_, first) = p.admit(bad.clone()).await?;
    r.check(
        "revisão fora de ordem: REJECTED INVALID_CONTENT",
        ack_is(
            &first,
            AdmissionStatus::Rejected,
            Some(Reason::InvalidContent),
        ),
        format!("{first:?}"),
    );
    let (_, again) = p.admit(bad).await?;
    r.check(
        "repetir sequência recusada devolve a mesma recusa",
        ack_is(
            &again,
            AdmissionStatus::Rejected,
            Some(Reason::InvalidContent),
        ),
        format!("{again:?}"),
    );

    // Intenção: aceita, repetida, revisão obsoleta e conflito.
    let (seg_i, interp) = (seg, Uuid::new_v4());
    let (_, ack) = p.admit(intent_v12(s, st, seg_i, interp, 0, "0")).await?;
    r.check(
        "INTENT 1.2 aceito",
        ack.event == AckEvent::IntentDetected && ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    r.check(
        "ACK identifica interpretação e revisão",
        ack.interpretation_id == Some(interp) && ack.revision == Some(0),
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(intent_v12(s, st, seg_i, interp, 0, "0")).await?;
    r.check(
        "INTENT repetido: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(intent_v12(s, st, seg_i, interp, 1, "0")).await?;
    r.check(
        "revisão 1 aceita",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(intent_v12(s, st, seg_i, interp, 0, "1")).await?;
    r.check(
        "revisão obsoleta: REJECTED STALE_REVISION",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::StaleRevision)),
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(intent_v12(s, st, seg_i, interp, 1, "2")).await?;
    r.check(
        "revisão 1 com conteúdo diferente: REJECTED CONFLICT",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::Conflict)),
        format!("{ack:?}"),
    );

    // Sessão desconhecida e encerrada.
    let (_, ack) = p
        .admit(speech_v12("sessao-nunca", Uuid::new_v4(), 1, started(seg))?)
        .await?;
    r.check(
        "sessão desconhecida: REJECTED SESSION_UNKNOWN",
        ack_is(
            &ack,
            AdmissionStatus::Rejected,
            Some(Reason::SessionUnknown),
        ),
        format!("{ack:?}"),
    );
    p.control(control_msg(s, SessionControl::Close)?).await?;
    let (_, ack) = p
        .admit(speech_v12(s, st, 3, SpeechEvent::NoSpeech)?)
        .await?;
    r.check(
        "sessão encerrada: REJECTED SESSION_CLOSED",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::SessionClosed)),
        format!("{ack:?}"),
    );

    // Stream encerrado: repetir o terminal dá DUPLICATE; evento novo, REJECTED STREAM_CLOSED.
    let s2 = "sessao-ack-2";
    p.control(control_msg(s2, SessionControl::Open)?).await?;
    let (st2, seg2) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s2, st2, 1, started(seg2))?).await?;
    let end = speech_v12(s2, st2, 2, SpeechEvent::StreamEnded)?;
    let (_, ack) = p.admit(end.clone()).await?;
    r.check(
        "fim de stream aceito",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(end).await?;
    r.check(
        "fim repetido: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(s2, st2, 3, SpeechEvent::NoSpeech)?)
        .await?;
    r.check(
        "evento tardio após fim: REJECTED STREAM_CLOSED",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::StreamClosed)),
        format!("{ack:?}"),
    );

    // Falha de persistência no terminal de STT: NOT_READY e retry com a mesma identidade.
    let (st3, seg3) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s2, st3, 1, started(seg3))?).await?;
    let fault = fault_conn(&path)?;
    fault
        .execute_batch(FAULT_TRIGGER)
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let end3 = speech_v12(s2, st3, 2, SpeechEvent::StreamEnded)?;
    let (_, ack) = p.admit(end3.clone()).await?;
    r.check(
        "falha de gravação: NOT_READY STORE_UNAVAILABLE",
        ack_is(
            &ack,
            AdmissionStatus::NotReady,
            Some(Reason::StoreUnavailable),
        ),
        format!("{ack:?}"),
    );
    fault
        .execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (_, ack) = p.admit(end3).await?;
    r.check(
        "retry após falha: ACCEPTED (nunca foi admitido)",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );

    // Falha de persistência em interpretação: a memória é desfeita, então o retry não vira DUPLICATE.
    let interp2 = Uuid::new_v4();
    fault
        .execute_batch("CREATE TRIGGER falha_intent BEFORE INSERT ON interpretations BEGIN SELECT RAISE(ABORT, 'falha injetada'); END;")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (st4, seg4) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s2, st4, 1, started(seg4))?).await?;
    let intent2 = intent_v12(s2, st4, seg4, interp2, 0, "0");
    let (_, ack) = p.admit(intent2.clone()).await?;
    r.check(
        "INTENT com falha de gravação: NOT_READY",
        ack_is(
            &ack,
            AdmissionStatus::NotReady,
            Some(Reason::StoreUnavailable),
        ),
        format!("{ack:?}"),
    );
    fault
        .execute_batch("DROP TRIGGER falha_intent")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (_, ack) = p.admit(intent2.clone()).await?;
    r.check(
        "INTENT após remover a falha: ACCEPTED, não DUPLICATE",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );

    // Legado 1.1: sem ACK e sem recusa de versão; sessão implícita.
    let before = p.snapshot().speech_applied;
    p.send(speech_msg(
        "sessao-legado-ack",
        Uuid::new_v4(),
        1,
        started(Uuid::new_v4()),
    )?)
    .await?;
    let snap = p.wait_for(|x| x.speech_applied > before).await;
    r.check(
        "1.1 aplicado pelo caminho legado (sem ACK)",
        snap.speech_applied == before + 1,
        stats_detail(&snap),
    );

    // ACK e CONTROL_RESULT não se confundem.
    let (raw, _) = p
        .admit(speech_v12(s2, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    let as_control = raw
        .payload
        .as_ref()
        .map(|v| ControlResult::from_value(v).is_err())
        .unwrap_or(false);
    r.check(
        "payload de ACK não é lido como CONTROL_RESULT",
        as_control,
        "",
    );
    let ctl = p.reply(control_msg(s2, SessionControl::Status)?).await?;
    let as_ack = ctl
        .payload
        .as_ref()
        .map(|v| AdmissionAck::from_value(v).is_err())
        .unwrap_or(false);
    r.check("payload de CONTROL_RESULT não é lido como ACK", as_ack, "");

    // Nenhuma admissão de execução a partir de parciais ou de texto sem intenção.
    let snap = p.snapshot();
    r.check(
        "nenhuma execução admitida por STT parcial",
        snap.admitted == 0,
        stats_detail(&snap),
    );
    p.stop().await;
    Ok(r)
}

/// Carga em processo: eventos parciais 1.2 em sequência, com latência de ACK (ida e volta ao monitor).
async fn admission_load(dir: &Path) -> Result<Report> {
    const N: u64 = 2000;
    let mut r = Report::new("admission-load");
    let path = fresh_store(dir, "admission-load");
    let mut p = Pipeline::start(&path).await?;
    let s = "sessao-carga";
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s, st, 1, started(seg))?).await?;

    let mut latencies = Vec::with_capacity(N as usize);
    let started_at = Instant::now();
    let mut accepted = 0u64;
    for seq in 2..=N + 1 {
        let msg = speech_v12(
            s,
            st,
            seq,
            transcript(
                seg,
                u32::try_from(seq - 2).unwrap_or(u32::MAX),
                TranscriptState::Partial,
            ),
        )?;
        let t0 = Instant::now();
        let (_, ack) = p.admit(msg).await?;
        latencies.push(t0.elapsed().as_micros() as f64);
        if ack.status == AdmissionStatus::Accepted {
            accepted += 1;
        }
    }
    let total = started_at.elapsed().as_secs_f64();
    latencies.sort_by(|a, b| a.total_cmp(b));
    let pct = |q: f64| latencies[((latencies.len() as f64 - 1.0) * q).round() as usize];
    r.metric("parciais", N as f64);
    r.metric("ack_p50_us", pct(0.50));
    r.metric("ack_p95_us", pct(0.95));
    r.metric("ack_p99_us", pct(0.99));
    r.metric("ack_max_us", pct(1.0));
    r.metric("vazao_eventos_por_s", N as f64 / total);
    r.check(
        "todos os parciais aceitos",
        accepted == N,
        format!("aceitos={accepted} de {N}"),
    );
    // Sequência já fora da janela de duplicatas: recusada como SEQUENCE_INVALID.
    let stale = speech_v12(s, st, 2, transcript(seg, 0, TranscriptState::Partial))?;
    let (_, ack) = p.admit(stale).await?;
    r.check(
        "sequência antiga fora da janela: REJECTED SEQUENCE_INVALID",
        ack_is(
            &ack,
            AdmissionStatus::Rejected,
            Some(Reason::SequenceInvalid),
        ),
        format!("{ack:?}"),
    );
    let snap = p.snapshot();
    r.check(
        "carga não gera execução",
        snap.admitted == 0,
        stats_detail(&snap),
    );
    p.stop().await;
    Ok(r)
}

// ---------- idempotência sob falha (auditoria A1/A1.1) ----------

async fn admission_faults(dir: &Path) -> Result<Report> {
    let mut r = Report::new("admission-faults");
    let cfg_one = SpeechConfig {
        max_streams: 1,
        ..SpeechConfig::default()
    };

    // A: recusa por capacidade repetida mantém o mesmo motivo; o banco guarda REJECTED.
    let path = fresh_store(dir, "admission-faults-a");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    let s = "sessao-cap";
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let (_, ack) = p
        .admit(speech_v12(s, a, 1, started(Uuid::new_v4()))?)
        .await?;
    r.check(
        "stream A aceito com capacidade 1",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let rejected_start = speech_v12(s, b, 1, started(Uuid::new_v4()))?;
    let (_, first) = p.admit(rejected_start.clone()).await?;
    let (_, again) = p.admit(rejected_start).await?;
    r.check(
        "recusa por capacidade: REJECTED STREAM_LIMIT",
        ack_is(&first, AdmissionStatus::Rejected, Some(Reason::StreamLimit)),
        format!("{first:?}"),
    );
    r.check(
        "repetir recusa de capacidade devolve o mesmo motivo",
        again == first,
        format!("{again:?}"),
    );
    let status = p.stream_status(s, &b)?;
    r.check(
        "banco registra REJECTED para o stream recusado",
        status.as_deref() == Some("REJECTED"),
        format!("{status:?}"),
    );
    p.stop().await;

    // B: falha ao gravar a recusa. Não há ACK REJECTED sem gravação; o retry decide a mesma recusa.
    let path = fresh_store(dir, "admission-faults-b");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (a2, c) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s, a2, 1, started(Uuid::new_v4()))?)
        .await?;
    let fault = fault_conn(&path)?;
    fault
        .execute_batch(FAULT_TRIGGER)
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let start_c = speech_v12(s, c, 1, started(Uuid::new_v4()))?;
    let (_, ack) = p.admit(start_c.clone()).await?;
    r.check(
        "falha ao gravar recusa: NOT_READY, nunca REJECTED sem gravação",
        ack_is(
            &ack,
            AdmissionStatus::NotReady,
            Some(Reason::StoreUnavailable),
        ),
        format!("{ack:?}"),
    );
    let status = p.stream_status(s, &c)?;
    r.check(
        "banco mantém OPEN enquanto a recusa não foi gravada",
        status.as_deref() == Some("OPEN"),
        format!("{status:?}"),
    );
    fault
        .execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (_, retry) = p.admit(start_c).await?;
    r.check(
        "retry após a falha: mesmo motivo da recusa (STREAM_LIMIT), não STREAM_CLOSED",
        ack_is(&retry, AdmissionStatus::Rejected, Some(Reason::StreamLimit)),
        format!("{retry:?}"),
    );
    let status = p.stream_status(s, &c)?;
    r.check(
        "banco grava REJECTED após o retry",
        status.as_deref() == Some("REJECTED"),
        format!("{status:?}"),
    );
    p.stop().await;

    // C: stream terminal evicto da memória; repetições do que foi aceito continuam DUPLICATE.
    let path = fresh_store(dir, "admission-faults-c");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    let s3 = "sessao-evicao";
    p.control(control_msg(s3, SessionControl::Open)?).await?;
    let (d, seg_d) = (Uuid::new_v4(), Uuid::new_v4());
    let d_start = speech_v12(s3, d, 1, started(seg_d))?;
    p.admit(d_start.clone()).await?;
    let d_end = speech_v12(s3, d, 2, SpeechEvent::StreamEnded)?;
    let (_, ack) = p.admit(d_end.clone()).await?;
    r.check(
        "fim de D aceito",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(s3, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    r.check(
        "novo stream E aceito após evicção de D",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(d_end).await?;
    r.check(
        "repetir fim de D após evicção: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(d_start).await?;
    r.check(
        "repetir início de D após evicção: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(s3, d, 1, started(Uuid::new_v4()))?)
        .await?;
    r.check(
        "mesma sequência de D com conteúdo diferente: CONFLICT",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::Conflict)),
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(s3, d, 3, SpeechEvent::NoSpeech)?)
        .await?;
    r.check(
        "evento novo em D após o fim: REJECTED STREAM_CLOSED",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::StreamClosed)),
        format!("{ack:?}"),
    );
    p.stop().await;

    // F: falha ao gravar a abertura de stream. Nada entra na memória; o retry é ACCEPTED, não DUPLICATE.
    let path = fresh_store(dir, "admission-faults-f");
    let mut p = Pipeline::start(&path).await?;
    let s6 = "sessao-abertura";
    p.control(control_msg(s6, SessionControl::Open)?).await?;
    let (f, seg_f) = (Uuid::new_v4(), Uuid::new_v4());
    let open_msg = speech_v12(s6, f, 1, started(seg_f))?;
    let fault = fault_conn(&path)?;
    fault
        .execute_batch("CREATE TRIGGER falha_abertura BEFORE INSERT ON streams BEGIN SELECT RAISE(ABORT, 'falha injetada'); END;")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (_, ack) = p.admit(open_msg.clone()).await?;
    r.check(
        "falha ao gravar abertura: NOT_READY STORE_UNAVAILABLE",
        ack_is(
            &ack,
            AdmissionStatus::NotReady,
            Some(Reason::StoreUnavailable),
        ),
        format!("{ack:?}"),
    );
    let status = p.stream_status(s6, &f)?;
    r.check(
        "abertura não gravada no banco",
        status.is_none(),
        format!("{status:?}"),
    );
    fault
        .execute_batch("DROP TRIGGER falha_abertura")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (_, ack) = p.admit(open_msg.clone()).await?;
    r.check(
        "retry após falha de abertura: ACCEPTED (nunca foi admitido)",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(open_msg).await?;
    r.check(
        "repetir abertura depois: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    p.stop().await;

    // E: cancelamento concorrente com admissão. Ordem de chegada decide; aceito antes continua DUPLICATE.
    let path = fresh_store(dir, "admission-faults-e");
    let mut p = Pipeline::start(&path).await?;
    let s5 = "sessao-cancel-adm";
    p.control(control_msg(s5, SessionControl::Open)?).await?;
    let (h, seg_h, ih) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s5, h, 1, started(seg_h))?).await?;
    let partial_ok = speech_v12(s5, h, 2, transcript(seg_h, 0, TranscriptState::Partial))?;
    let (_, ack) = p.admit(partial_ok.clone()).await?;
    r.check(
        "parcial aceito antes do cancelamento",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let intent_ok = intent_v12(s5, h, seg_h, ih, 0, "0");
    let (_, ack) = p.admit(intent_ok.clone()).await?;
    r.check(
        "intenção aceita antes do cancelamento",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    let res = p.control(cancel_stream_v12(s5, h)?).await?;
    r.check(
        "cancelamento de stream: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    let (_, ack) = p.admit(partial_ok).await?;
    r.check(
        "repetir parcial aceito antes do cancelamento: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(intent_ok).await?;
    r.check(
        "repetir intenção aceita antes do cancelamento: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(
            s5,
            h,
            3,
            transcript(seg_h, 1, TranscriptState::Partial),
        )?)
        .await?;
    r.check(
        "parcial depois do cancelamento: REJECTED STREAM_CLOSED",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::StreamClosed)),
        format!("{ack:?}"),
    );
    let (_, ack) = p.admit(intent_v12(s5, h, seg_h, ih, 1, "0")).await?;
    r.check(
        "revisão nova após cancelamento: REJECTED STREAM_CLOSED",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::StreamClosed)),
        format!("{ack:?}"),
    );
    p.stop().await;

    // D: interpretação de stream encerrado (memória esquecida) e após reinício: repetição é DUPLICATE.
    let path = fresh_store(dir, "admission-faults-d");
    let mut p = Pipeline::start(&path).await?;
    let s4 = "sessao-intent-fim";
    p.control(control_msg(s4, SessionControl::Open)?).await?;
    let (g, seg_g, ia) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s4, g, 1, started(seg_g))?).await?;
    p.admit(intent_v12(s4, g, seg_g, ia, 0, "0")).await?;
    let rev1 = intent_v12(s4, g, seg_g, ia, 1, "0");
    let (_, ack) = p.admit(rev1.clone()).await?;
    r.check(
        "revisão 1 aceita",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    p.admit(speech_v12(s4, g, 2, SpeechEvent::StreamEnded)?)
        .await?;
    let (_, ack) = p.admit(rev1.clone()).await?;
    r.check(
        "repetir revisão 1 após o fim do stream: DUPLICATE, não GAP",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    p.stop().await;
    let mut p = Pipeline::start(&path).await?;
    // Sessões são só em memória (D-43): antes de reabri-la, a repetição é recusada por sessão desconhecida.
    let (_, before_open) = p.admit(rev1.clone()).await?;
    r.check(
        "após reinício, sessão anterior desconhecida: REJECTED SESSION_UNKNOWN",
        ack_is(
            &before_open,
            AdmissionStatus::Rejected,
            Some(Reason::SessionUnknown),
        ),
        format!("{before_open:?}"),
    );
    p.control(control_msg(s4, SessionControl::Open)?).await?;
    let (_, ack) = p.admit(rev1).await?;
    r.check(
        "repetir revisão 1 após reabrir a sessão e reinício: DUPLICATE",
        ack.status == AdmissionStatus::Duplicate,
        format!("{ack:?}"),
    );
    p.stop().await;
    Ok(r)
}

// ---------- ciclo de vida de aberturas pendentes (pending_open) ----------

/// Stream com abertura gravada e recusa por capacidade cuja gravação falhou: fica pendente.
/// Pré-condição: `active` ocupa a única vaga do monitor (capacidade 1).
async fn make_pending(
    p: &Pipeline,
    conn: &rusqlite::Connection,
    session: &str,
    stream: Uuid,
) -> Result<()> {
    conn.execute_batch(FAULT_TRIGGER)
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let (_, ack) = p
        .admit(speech_v12(session, stream, 1, started(Uuid::new_v4()))?)
        .await?;
    if !ack_is(
        &ack,
        AdmissionStatus::NotReady,
        Some(Reason::StoreUnavailable),
    ) {
        return Err(KavoError::Protocol(format!(
            "pendência não criada: {ack:?}"
        )));
    }
    Ok(())
}

async fn pending_lifecycle(dir: &Path) -> Result<Report> {
    let mut r = Report::new("pending-lifecycle");
    let cfg_one = SpeechConfig {
        max_streams: 1,
        ..SpeechConfig::default()
    };

    // P1: encerramento normal de stream não deixa pendência; repetição continua DUPLICATE.
    let path = fresh_store(dir, "pending-p1");
    let mut p = Pipeline::start(&path).await?;
    let s = "sessao-pend-normal";
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (st, seg) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s, st, 1, started(seg))?).await?;
    r.check(
        "abertura normal não deixa pendência",
        p.snapshot().pending_open == 0,
        stats_detail(&p.snapshot()),
    );
    let end = speech_v12(s, st, 2, SpeechEvent::StreamEnded)?;
    p.admit(end.clone()).await?;
    let (_, ack) = p.admit(end).await?;
    r.check(
        "fim normal sem pendência; repetição DUPLICATE",
        ack.status == AdmissionStatus::Duplicate && p.snapshot().pending_open == 0,
        format!("{ack:?}"),
    );
    p.stop().await;

    // P2: cancelamento de stream pendente limpa a pendência; retry é recusado (aposentado), não reaberto.
    let path = fresh_store(dir, "pending-p2");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    let conn = fault_conn(&path)?;
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s, a, 1, started(Uuid::new_v4()))?)
        .await?;
    make_pending(&p, &conn, s, b).await?;
    r.check(
        "pendência criada com falha de gravação",
        p.snapshot().pending_open == 1,
        stats_detail(&p.snapshot()),
    );
    conn.execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let res = p.control(cancel_stream_v12(s, b)?).await?;
    r.check(
        "cancelar stream pendente: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    r.check(
        "cancelamento limpa a pendência",
        p.snapshot().pending_open == 0,
        stats_detail(&p.snapshot()),
    );
    let status = p.stream_status(s, &b)?;
    r.check(
        "banco grava CANCELLED",
        status.as_deref() == Some("CANCELLED"),
        format!("{status:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(s, b, 1, started(Uuid::new_v4()))?)
        .await?;
    r.check(
        "retry após cancelamento: REJECTED STREAM_CLOSED (não reabre)",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::StreamClosed)),
        format!("{ack:?}"),
    );
    r.check(
        "retry não recria pendência",
        p.snapshot().pending_open == 0,
        stats_detail(&p.snapshot()),
    );
    p.stop().await;

    // P3: encerramento de sessão com pendências limpa as da sessão; outra sessão mantém a sua.
    let path = fresh_store(dir, "pending-p3");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    let conn = fault_conn(&path)?;
    let (s1, s2) = ("sessao-pend-1", "sessao-pend-2");
    p.control(control_msg(s1, SessionControl::Open)?).await?;
    p.control(control_msg(s2, SessionControl::Open)?).await?;
    let c = Uuid::new_v4();
    p.admit(speech_v12(s1, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    make_pending(&p, &conn, s1, c).await?;
    conn.execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let res = p.control(control_msg(s1, SessionControl::Close)?).await?;
    r.check(
        "encerrar sessão com pendência: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    r.check(
        "encerramento limpa pendências da sessão",
        p.snapshot().pending_open == 0,
        stats_detail(&p.snapshot()),
    );
    let (_, ack) = p
        .admit(speech_v12(s1, c, 1, started(Uuid::new_v4()))?)
        .await?;
    r.check(
        "retry em sessão encerrada: REJECTED SESSION_CLOSED",
        ack_is(&ack, AdmissionStatus::Rejected, Some(Reason::SessionClosed)),
        format!("{ack:?}"),
    );
    let (_, ack) = p
        .admit(speech_v12(s2, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    r.check(
        "outra sessão admitida após liberar a vaga (stream de s1 cancelado)",
        ack.status == AdmissionStatus::Accepted,
        format!("{ack:?}"),
    );
    p.stop().await;

    // P4: falha de gravação durante a limpeza (cancelamento). A pendência só sai depois da gravação.
    let path = fresh_store(dir, "pending-p4");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    let conn = fault_conn(&path)?;
    p.control(control_msg(s, SessionControl::Open)?).await?;
    let (a4, d) = (Uuid::new_v4(), Uuid::new_v4());
    p.admit(speech_v12(s, a4, 1, started(Uuid::new_v4()))?)
        .await?;
    make_pending(&p, &conn, s, d).await?;
    let res = p.control(cancel_stream_v12(s, d)?).await?;
    r.check(
        "falha na limpeza: REJECTED STORE_UNAVAILABLE",
        ack_is_control(&res, Outcome::Rejected, Some(Reason::StoreUnavailable)),
        format!("{res:?}"),
    );
    r.check(
        "falha na limpeza mantém a pendência",
        p.snapshot().pending_open == 1,
        stats_detail(&p.snapshot()),
    );
    conn.execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let res = p.control(cancel_stream_v12(s, d)?).await?;
    r.check(
        "repetir cancelamento após remover falha: APPLIED",
        res.outcome == Outcome::Applied,
        format!("{res:?}"),
    );
    r.check(
        "pendência removida após gravação bem-sucedida",
        p.snapshot().pending_open == 0,
        stats_detail(&p.snapshot()),
    );
    p.stop().await;

    // P5: sem crescimento indefinido. Clientes que abandonam streams não acumulam pendências além do teto.
    let path = fresh_store(dir, "pending-p5");
    let mut p = Pipeline::start_cfg(&path, cfg_one.clone()).await?;
    let conn = fault_conn(&path)?;
    p.control(control_msg(s, SessionControl::Open)?).await?;
    p.admit(speech_v12(s, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
        .await?;
    conn.execute_batch(FAULT_TRIGGER)
        .map_err(|e| KavoError::Store(e.to_string()))?;
    let mut peak = 0u64;
    let mut busy = 0u64;
    for _ in 0..50 {
        let (_, ack) = p
            .admit(speech_v12(s, Uuid::new_v4(), 1, started(Uuid::new_v4()))?)
            .await?;
        peak = peak.max(p.snapshot().pending_open);
        if ack_is(&ack, AdmissionStatus::NotReady, Some(Reason::Busy)) {
            busy += 1;
        }
    }
    r.check(
        "pendências nunca passam do teto (1)",
        peak == 1,
        format!("pico={peak}"),
    );
    r.check(
        "aberturas acima do teto recebem BUSY sem tocar no banco",
        busy == 49,
        format!("busy={busy}"),
    );
    r.metric("pendencias_pico", peak as f64);
    r.metric("aberturas_busy", busy as f64);
    conn.execute_batch("DROP TRIGGER falha_injetada")
        .map_err(|e| KavoError::Store(e.to_string()))?;
    p.stop().await;
    Ok(r)
}

fn ack_is_control(res: &ControlResult, outcome: Outcome, reason: Option<Reason>) -> bool {
    res.outcome == outcome && res.reason == reason
}
