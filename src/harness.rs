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
        "ipc-stream",
        "fluxo STT contra núcleo em execução via socket",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub scenario: String,
    pub checks: Vec<Check>,
}

impl Report {
    fn new(scenario: &str) -> Self {
        Self {
            scenario: scenario.to_owned(),
            checks: Vec::new(),
        }
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
        "ipc-stream" => ipc_stream(opts).await,
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
    bus: EventBus,
    supervisor: Option<Supervisor>,
    stats: SharedStats,
    store_path: PathBuf,
}

impl Pipeline {
    async fn start(store_path: &Path) -> Result<Self> {
        let store = Store::open(store_path)?;
        let bus = EventBus::new(256, Duration::from_secs(2));
        let rx = bus.subscribe();
        let stats: SharedStats = Arc::new(Mutex::new(MonitorStats::default()));
        let monitor = ContractMonitor::open(rx, &SpeechConfig::default(), stats.clone(), store)?;
        let mut supervisor = Supervisor::new(bus.clone());
        supervisor.spawn(monitor);
        Ok(Self {
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

async fn ipc_stream(opts: &Options) -> Result<Report> {
    let mut r = Report::new("ipc-stream");
    let (Some(socket), Some(store_path)) = (opts.socket.as_deref(), opts.store.as_deref()) else {
        return Err(KavoError::Config(
            "ipc-stream exige --socket e --store".to_owned(),
        ));
    };
    let before = ipc::ping(socket, opts.ipc).await;
    r.check(
        "núcleo responde antes do cenário",
        matches!(before, Verification::Success(_)),
        format!("{before:?}"),
    );

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
    send_ordered(socket, &events, opts.ipc.request_timeout).await?;

    let store = Store::open(store_path)?;
    let deadline = Instant::now() + WAIT;
    let mut status = store.stream_status(session, &stream)?;
    while status.as_deref() != Some("ENDED") && Instant::now() < deadline {
        tokio::time::sleep(POLL * 10).await;
        status = store.stream_status(session, &stream)?;
    }
    r.check(
        "stream enviado por socket gravado como ENDED",
        status.as_deref() == Some("ENDED"),
        format!("{status:?}"),
    );

    let after = ipc::ping(socket, opts.ipc).await;
    r.check(
        "núcleo segue respondendo após o cenário",
        matches!(after, Verification::Success(_)),
        format!("{after:?}"),
    );
    Ok(r)
}

/// Envia mensagens NDJSON em ordem por uma conexão e fecha o envio. STT não responde em caso de sucesso.
async fn send_ordered(path: &Path, msgs: &[KavoMessage], timeout: Duration) -> Result<()> {
    let mut bytes = Vec::new();
    for msg in msgs {
        serde_json::to_writer(&mut bytes, msg).map_err(|e| KavoError::Protocol(e.to_string()))?;
        bytes.push(b'\n');
    }
    let mut stream = tokio::time::timeout(timeout, UnixStream::connect(path))
        .await
        .map_err(|_| KavoError::Timeout(timeout))?
        .map_err(|e| KavoError::io(format!("conectar em {}", path.display()), e))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| KavoError::io("enviar mensagens", e))?;
    stream
        .shutdown()
        .await
        .map_err(|e| KavoError::io("fechar envio", e))?;
    Ok(())
}

fn session_err(e: SessionError) -> KavoError {
    KavoError::Protocol(e.to_string())
}
