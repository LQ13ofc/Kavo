//! Ciclo de vida do núcleo: componentes são tarefas supervisionadas, com cancelamento e prazo de encerramento.

use std::future::Future;
use std::time::{Duration, Instant};

use tokio::signal::unix::{SignalKind, signal};
use tokio::task::{AbortHandle, Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span, warn};

use crate::bus::EventBus;
use crate::config::KavoConfig;
use crate::error::{KavoError, Result};
use crate::instance::InstanceLock;
use crate::ipc::{IpcServer, IpcSettings};
use crate::monitor::ContractMonitor;
use crate::store::Store;

/// Recursos entregues a um componente ao iniciar.
#[derive(Debug, Clone)]
pub struct ComponentContext {
    pub cancel: CancellationToken,
    pub bus: EventBus,
}

/// Contrato de um componente: nome estável, execução até cancelamento, erro explícito.
pub trait Component: Send + 'static {
    fn name(&self) -> &'static str;

    /// Deve retornar após `ctx.cancel` ser acionado, sem deixar tarefas órfãs.
    fn run(self, ctx: ComponentContext) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    Stopped,
    Failed(String),
    Panicked(String),
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReport {
    pub name: String,
    pub outcome: TaskOutcome,
}

/// Executa componentes em tarefas separadas. Falha ou pânico de um não derruba os demais.
pub struct Supervisor {
    cancel: CancellationToken,
    bus: EventBus,
    tasks: JoinSet<Result<()>>,
    names: std::collections::HashMap<Id, String>,
}

impl Supervisor {
    pub fn new(bus: EventBus) -> Self {
        Self {
            cancel: CancellationToken::new(),
            bus,
            tasks: JoinSet::new(),
            names: std::collections::HashMap::new(),
        }
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn spawn<C: Component>(&mut self, component: C) {
        let name = component.name();
        let ctx = ComponentContext {
            cancel: self.cancel.child_token(),
            bus: self.bus.clone(),
        };
        let span = info_span!("componente", name);
        let handle: AbortHandle = self
            .tasks
            .spawn(async move { component.run(ctx).await }.instrument(span));
        self.names.insert(handle.id(), name.to_owned());
        info!(component = name, "componente iniciado");
    }

    /// Espera o próximo componente terminar. `None` quando não há tarefas.
    pub async fn next(&mut self) -> Option<TaskReport> {
        let joined = self.tasks.join_next_with_id().await?;
        Some(self.report(joined))
    }

    /// Cancela tudo e aguarda até `timeout`. Tarefas restantes são abortadas e reportadas como `TimedOut`.
    pub async fn shutdown(mut self, timeout: Duration) -> Vec<TaskReport> {
        self.cancel.cancel();
        let deadline = Instant::now() + timeout;
        let mut reports = Vec::new();

        while !self.tasks.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.tasks.join_next_with_id()).await {
                Ok(Some(joined)) => reports.push(self.report(joined)),
                Ok(None) => break,
                Err(_) => {
                    warn!(
                        pendentes = self.tasks.len(),
                        "prazo de encerramento excedido; abortando"
                    );
                    self.tasks.abort_all();
                    while let Some(joined) = self.tasks.join_next_with_id().await {
                        let mut report = self.report(joined);
                        report.outcome = TaskOutcome::TimedOut;
                        reports.push(report);
                    }
                }
            }
        }
        reports
    }

    fn report(&mut self, joined: std::result::Result<(Id, Result<()>), JoinError>) -> TaskReport {
        let (id, outcome) = match joined {
            Ok((id, Ok(()))) => (id, TaskOutcome::Stopped),
            Ok((id, Err(e))) => (id, TaskOutcome::Failed(e.to_string())),
            Err(e) if e.is_panic() => {
                (e.id(), TaskOutcome::Panicked("pânico na tarefa".to_owned()))
            }
            Err(e) => (e.id(), TaskOutcome::Failed("tarefa cancelada".to_owned())),
        };
        let name = self
            .names
            .remove(&id)
            .unwrap_or_else(|| "desconhecido".to_owned());
        TaskReport { name, outcome }
    }
}

/// Executa o núcleo até sinal de parada ou falha de componente. `Ok` = encerramento limpo.
pub async fn run(config: KavoConfig) -> Result<()> {
    let socket = config.socket_path()?;
    let _lock = InstanceLock::acquire(&config.lock_path()?)?;

    // Sinais são registrados antes de qualquer recurso ser criado: sem isso, um erro aqui deixaria o socket no disco.
    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| KavoError::io("registrar SIGTERM", e))?;
    let mut sigint =
        signal(SignalKind::interrupt()).map_err(|e| KavoError::io("registrar SIGINT", e))?;

    // Estado durável antes de aceitar conexões. Falha aqui impede o início (fail-closed).
    let store = Store::open(&config.store_path()?)?;
    info!(store = %store.path().display(), "armazenamento de admissões aberto");

    let bus = EventBus::new(256, Duration::from_millis(config.ipc.request_timeout_ms));
    // Assinatura criada antes do IPC aceitar conexões: nenhuma mensagem inicial se perde.
    let monitor_rx = bus.subscribe();
    let mut supervisor = Supervisor::new(bus);
    let monitor = ContractMonitor::open(monitor_rx, &config.speech, Default::default(), store)?;
    supervisor.spawn(monitor);
    let server = IpcServer::bind(&socket, IpcSettings::from_config(&config))?;
    supervisor.spawn(server);
    info!(version = env!("CARGO_PKG_VERSION"), "Kavo em execução");

    let stop_reason: String = tokio::select! {
        _ = sigterm.recv() => "SIGTERM".to_owned(),
        _ = sigint.recv() => "SIGINT".to_owned(),
        Some(report) = supervisor.next() => {
            format!("componente '{}' encerrou antes do esperado: {:?}", report.name, report.outcome)
        }
    };
    info!(motivo = %stop_reason, "encerrando");

    let timeout = Duration::from_millis(config.runtime.shutdown_timeout_ms);
    let reports = supervisor.shutdown(timeout).await;

    let mut failures = 0usize;
    for report in &reports {
        match &report.outcome {
            TaskOutcome::Stopped => info!(component = %report.name, "componente encerrado"),
            other => {
                failures += 1;
                error!(component = %report.name, outcome = ?other, "componente com falha no encerramento");
            }
        }
    }
    if failures > 0 {
        return Err(KavoError::Component {
            component: "núcleo".to_owned(),
            reason: format!("{failures} componente(s) não encerraram limpamente"),
        });
    }
    info!("Kavo encerrado");
    Ok(())
}
