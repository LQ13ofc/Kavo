//! Transporte IPC do Kavo Protocol: Unix domain socket com uma mensagem por linha (NDJSON).
//! Limites de tamanho, de tempo (por operação e ociosidade) e de conexões são aplicados.

use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc::error::TrySendError, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::bus::EventBus;
use crate::config::KavoConfig;
use crate::control::{AdmissionAck, AdmissionStatus, ControlCommand, ControlSender, Reason};
use crate::error::{KavoError, Result};
use crate::protocol::{KavoMessage, MessageType, error_code};
use crate::runtime::{Component, ComponentContext};
use crate::verification::Verification;

/// Conexões simultâneas máximas. Acima disso, a conexão nova é recusada com log.
pub const MAX_CONNECTIONS: usize = 64;
/// Pausa após erro transitório de `accept` (ex.: EMFILE), para não girar em loop.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy)]
pub struct IpcSettings {
    pub max_frame_bytes: usize,
    /// Prazo por operação de leitura ou escrita de uma mensagem.
    pub request_timeout: Duration,
    /// Tempo máximo sem nenhuma mensagem antes de fechar a conexão.
    pub idle_timeout: Duration,
}

impl IpcSettings {
    pub fn from_config(cfg: &KavoConfig) -> Self {
        Self {
            max_frame_bytes: cfg.ipc.max_frame_bytes,
            request_timeout: Duration::from_millis(cfg.ipc.request_timeout_ms),
            idle_timeout: Duration::from_millis(cfg.ipc.idle_timeout_ms),
        }
    }
}

/// Lê uma linha (sem `\n`). `Ok(None)` = conexão fechada limpa. Linha acima do limite = erro.
pub async fn read_frame<R>(reader: &mut R, max_bytes: usize) -> Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = Vec::new();
    // Lê no máximo max+1 bytes: sem `\n` dentro desse limite, a linha é grande demais.
    let mut limited = (&mut *reader).take(max_bytes as u64 + 1);
    let read = limited
        .read_until(b'\n', &mut buf)
        .await
        .map_err(|e| KavoError::io("ler mensagem do socket", e))?;

    if read == 0 {
        return Ok(None);
    }
    if buf.last() != Some(&b'\n') {
        if buf.len() > max_bytes {
            return Err(KavoError::FrameTooLarge { limit: max_bytes });
        }
        return Err(KavoError::Protocol(
            "conexão encerrada no meio de uma mensagem".to_owned(),
        ));
    }
    buf.pop();
    if buf.len() > max_bytes {
        return Err(KavoError::FrameTooLarge { limit: max_bytes });
    }
    Ok(Some(buf))
}

async fn write_message<W>(writer: &mut W, msg: &KavoMessage, timeout: Duration) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let line = msg.to_json_line()?;
    tokio::time::timeout(timeout, async {
        writer.write_all(&line).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| KavoError::Timeout(timeout))?
    .map_err(|e| KavoError::io("escrever mensagem no socket", e))
}

/// Descarta até `limit` bytes da entrada, com prazo. Limitado para não virar canal de DoS.
async fn drain<R>(reader: &mut R, limit: u64, timeout: Duration)
where
    R: AsyncRead + Unpin,
{
    let _ = tokio::time::timeout(
        timeout,
        tokio::io::copy(&mut reader.take(limit), &mut tokio::io::sink()),
    )
    .await;
}

/// Regras de resposta do núcleo. Retorna `None` quando nenhuma resposta é devida.
fn respond(msg: &KavoMessage) -> Option<KavoMessage> {
    match msg.kind {
        MessageType::Heartbeat => Some(msg.reply(MessageType::Heartbeat)),
        _ => None,
    }
}

fn error_text(e: &KavoError) -> String {
    format!("[{}] {e}", e.kind().as_str())
}

/// Pedido que precisa de resposta do monitor: sessão, requisição e cancelamento 1.2.
fn is_control(msg: &KavoMessage) -> bool {
    msg.kind == MessageType::SessionControl || (msg.kind == MessageType::Cancel && msg.minor() >= 2)
}

/// Evento STT ou INTENT 1.2: recebe `ADMISSION_ACK`. O 1.1 segue sem resposta.
fn is_admission(msg: &KavoMessage) -> bool {
    matches!(
        msg.kind,
        MessageType::SttResult | MessageType::IntentDetected
    ) && msg.minor() >= 2
}

/// Admissão sem bloqueio no IPC: fila cheia vira `NOT_READY BUSY` imediato (backpressure).
/// Prazo sem resposta vira `ERROR`: o cliente repete com a mesma identidade, e o monitor responde
/// `DUPLICATE` se o evento já tiver sido admitido.
async fn route_admission(
    control: Option<&ControlSender>,
    msg: &KavoMessage,
    timeout: Duration,
) -> KavoMessage {
    let Some(sender) = control else {
        return msg.error_reply(
            error_code::GENERIC_ERROR,
            "[INTERNAL] admissão indisponível neste núcleo",
        );
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    match sender.try_send(ControlCommand {
        msg: msg.clone(),
        reply: reply_tx,
    }) {
        Ok(()) => match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(reply)) => reply,
            _ => msg.error_reply(error_code::GENERIC_ERROR, "[TIMEOUT] sem ACK do monitor"),
        },
        Err(TrySendError::Full(_)) => {
            let ack = AdmissionAck::for_event(msg, AdmissionStatus::NotReady, Some(Reason::Busy));
            ack.reply_to(msg).unwrap_or_else(|e| {
                msg.error_reply(
                    error_code::GENERIC_ERROR,
                    format!("[{}] {e}", e.kind().as_str()),
                )
            })
        }
        Err(TrySendError::Closed(_)) => {
            msg.error_reply(error_code::GENERIC_ERROR, "[INTERNAL] monitor indisponível")
        }
    }
}

/// Entrega um pedido de controle ao monitor e espera a resposta, com prazo do IPC.
async fn route_control(
    control: Option<&ControlSender>,
    msg: &KavoMessage,
    timeout: Duration,
) -> KavoMessage {
    let Some(sender) = control else {
        return msg.error_reply(
            error_code::GENERIC_ERROR,
            "[INTERNAL] controle indisponível neste núcleo",
        );
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    // Com fila cheia de admissões, o envio espera no máximo o prazo do IPC: nunca bloqueia indefinidamente.
    match tokio::time::timeout(
        timeout,
        sender.send(ControlCommand {
            msg: msg.clone(),
            reply: reply_tx,
        }),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            return msg.error_reply(error_code::GENERIC_ERROR, "[INTERNAL] monitor indisponível");
        }
        Err(_) => {
            return msg.error_reply(
                error_code::GENERIC_ERROR,
                "[TIMEOUT] fila de controle cheia",
            );
        }
    }
    match tokio::time::timeout(timeout, reply_rx).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) => msg.error_reply(
            error_code::GENERIC_ERROR,
            "[INTERNAL] pedido de controle descartado",
        ),
        Err(_) => msg.error_reply(
            error_code::GENERIC_ERROR,
            "[TIMEOUT] sem resposta do monitor",
        ),
    }
}

async fn handle_connection(
    stream: UnixStream,
    settings: IpcSettings,
    bus: EventBus,
    control: Option<ControlSender>,
    cancel: CancellationToken,
) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => break,
            frame = tokio::time::timeout(settings.idle_timeout, read_frame(&mut reader, settings.max_frame_bytes)) => {
                match frame {
                    Ok(frame) => frame,
                    Err(_) => {
                        debug!("conexão ociosa encerrada");
                        break;
                    }
                }
            }
        };

        let reply = match frame {
            Ok(None) => break,
            Ok(Some(bytes)) => match KavoMessage::from_json_slice(&bytes) {
                // Respostas do núcleo não são aceitas como entrada.
                Ok(msg)
                    if matches!(
                        msg.kind,
                        MessageType::ControlResult | MessageType::AdmissionAck
                    ) =>
                {
                    Some(msg.error_reply(
                        error_code::GENERIC_ERROR,
                        "[INVALID_MESSAGE] tipo de resposta é reservado ao núcleo",
                    ))
                }
                Ok(msg) if is_admission(&msg) => {
                    debug!(kind = ?msg.kind, session_id = %msg.session_id, "evento com admissão");
                    Some(route_admission(control.as_ref(), &msg, settings.request_timeout).await)
                }
                Ok(msg) if is_control(&msg) => {
                    debug!(kind = ?msg.kind, session_id = %msg.session_id, "pedido de controle");
                    Some(route_control(control.as_ref(), &msg, settings.request_timeout).await)
                }
                Ok(msg) => {
                    debug!(kind = ?msg.kind, session_id = %msg.session_id, "mensagem recebida");
                    match bus.publish(msg.clone()).await {
                        Ok(_) => respond(&msg),
                        Err(e) => {
                            warn!(classe = e.kind().as_str(), "entrega garantida falhou");
                            Some(msg.error_reply(error_code::GENERIC_ERROR, error_text(&e)))
                        }
                    }
                }
                Err(e) => {
                    warn!(classe = e.kind().as_str(), "mensagem rejeitada");
                    Some(KavoMessage::unparseable_error(
                        error_code::GENERIC_ERROR,
                        error_text(&e),
                    ))
                }
            },
            Err(e @ KavoError::FrameTooLarge { .. }) => {
                warn!(
                    classe = e.kind().as_str(),
                    "conexão encerrada por mensagem grande demais"
                );
                let _ = write_message(
                    &mut write_half,
                    &KavoMessage::unparseable_error(error_code::GENERIC_ERROR, error_text(&e)),
                    settings.request_timeout,
                )
                .await;
                // Fecha escrita e drena o resto da entrada: fechar com dados não lidos gera RST
                // e o cliente pode perder a resposta de erro já enviada.
                let _ = write_half.shutdown().await;
                drain(
                    &mut reader,
                    settings.max_frame_bytes as u64 * 4,
                    settings.request_timeout,
                )
                .await;
                break;
            }
            Err(e) => {
                warn!(classe = e.kind().as_str(), "conexão encerrada");
                break;
            }
        };

        if let Some(reply) = reply {
            if let Err(e) = write_message(&mut write_half, &reply, settings.request_timeout).await {
                warn!(classe = e.kind().as_str(), "falha ao responder");
                break;
            }
        }
    }
}

/// Servidor de socket Unix. Requer que o chamador já possua o [`crate::instance::InstanceLock`].
#[derive(Debug)]
pub struct IpcServer {
    listener: UnixListener,
    path: PathBuf,
    settings: IpcSettings,
    /// Canal para pedidos de controle com resposta (1.2). Sem ele, esses pedidos recebem ERROR.
    control: Option<ControlSender>,
}

impl IpcServer {
    pub fn bind(path: &Path, settings: IpcSettings) -> Result<Self> {
        // O diretório pai precisa ser privado: sem isso, há uma janela entre o bind e o chmod 0600
        // em que outro usuário pode conectar.
        let parent = path.parent().ok_or_else(|| {
            KavoError::Config(format!(
                "ipc.socket_path '{}' sem diretório pai",
                path.display()
            ))
        })?;
        let parent_mode = std::fs::metadata(parent)
            .map_err(|e| KavoError::io(format!("verificar {}", parent.display()), e))?
            .permissions()
            .mode();
        if parent_mode & 0o077 != 0 {
            return Err(KavoError::Config(format!(
                "diretório '{}' do socket deve ser privado (modo 0700); atual {:o}",
                parent.display(),
                parent_mode & 0o777
            )));
        }

        // Com o lock na mão, um socket existente é resto de processo morto. Nunca remove outro tipo de arquivo.
        match std::fs::symlink_metadata(path) {
            Ok(meta) if meta.file_type().is_socket() => {
                std::fs::remove_file(path).map_err(|e| {
                    KavoError::io(format!("remover socket antigo {}", path.display()), e)
                })?
            }
            Ok(_) => {
                return Err(KavoError::Config(format!(
                    "ipc.socket_path '{}' existe e não é um socket; não será removido",
                    path.display()
                )));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(KavoError::io(format!("verificar {}", path.display()), e)),
        }
        let listener = UnixListener::bind(path)
            .map_err(|e| KavoError::io(format!("abrir socket {}", path.display()), e))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| KavoError::io(format!("definir permissões de {}", path.display()), e))?;
        Ok(Self {
            listener,
            path: path.to_path_buf(),
            settings,
            control: None,
        })
    }

    /// Liga o servidor ao monitor, que aplica os pedidos de controle.
    pub fn with_control(mut self, control: ControlSender) -> Self {
        self.control = Some(control);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Aceita conexões até o cancelamento. Conexões em andamento recebem o mesmo sinal.
    pub async fn serve(self, bus: EventBus, cancel: CancellationToken) -> Result<()> {
        info!(socket = %self.path.display(), "servidor IPC ativo");
        let control = self.control.clone();
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let mut connections = JoinSet::new();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                accepted = self.listener.accept() => {
                    let stream = match accepted {
                        Ok((stream, _)) => stream,
                        Err(e) => {
                            // Erro de accept (ex.: EMFILE, ECONNABORTED) é transitório: não derruba o servidor.
                            warn!(error = %e, "falha ao aceitar conexão; tentando de novo");
                            tokio::time::sleep(ACCEPT_BACKOFF).await;
                            continue;
                        }
                    };
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        warn!(limite = MAX_CONNECTIONS, "conexão recusada: limite de conexões atingido");
                        continue;
                    };
                    let settings = self.settings;
                    let bus = bus.clone();
                    let control = control.clone();
                    let child = cancel.child_token();
                    connections.spawn(async move {
                        let _permit = permit;
                        handle_connection(stream, settings, bus, control, child).await;
                    });
                }
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(e) = joined {
                        warn!(error = %e, "tarefa de conexão terminou com pânico");
                    }
                }
            }
        }

        while let Some(joined) = connections.join_next().await {
            if let Err(e) = joined {
                warn!(error = %e, "tarefa de conexão terminou com pânico");
            }
        }
        if let Err(e) = std::fs::remove_file(&self.path) {
            warn!(error = %e, socket = %self.path.display(), "socket não removido ao encerrar");
        }
        info!("servidor IPC encerrado");
        Ok(())
    }
}

impl Component for IpcServer {
    fn name(&self) -> &'static str {
        "ipc-server"
    }

    async fn run(self, ctx: ComponentContext) -> Result<()> {
        self.serve(ctx.bus, ctx.cancel).await
    }
}

/// Envia uma mensagem a um núcleo em execução e espera a resposta.
pub async fn request(path: &Path, msg: &KavoMessage, settings: IpcSettings) -> Result<KavoMessage> {
    let timeout = settings.request_timeout;
    let mut stream = tokio::time::timeout(timeout, UnixStream::connect(path))
        .await
        .map_err(|_| KavoError::Timeout(timeout))?
        .map_err(|e| KavoError::io(format!("conectar em {}", path.display()), e))?;

    let (read_half, mut write_half) = stream.split();
    write_message(&mut write_half, msg, timeout).await?;

    let mut reader = BufReader::new(read_half);
    let frame = tokio::time::timeout(timeout, read_frame(&mut reader, settings.max_frame_bytes))
        .await
        .map_err(|_| KavoError::Timeout(timeout))??;
    let bytes =
        frame.ok_or_else(|| KavoError::Protocol("servidor fechou sem responder".to_owned()))?;
    KavoMessage::from_json_slice(&bytes)
}

/// Verifica se o núcleo responde. Só confirma `SUCCESS` com HEARTBEAT cujo `correlation_id` é a sondagem.
pub async fn ping(path: &Path, settings: IpcSettings) -> Verification {
    let probe = KavoMessage::new(MessageType::Heartbeat, "kavo-cli", "kavo-cli");
    let probe_id = probe.message_id.clone();

    match request(path, &probe, settings).await {
        Ok(reply)
            if reply.kind == MessageType::Heartbeat
                && reply.correlation_id.as_deref() == Some(probe_id.as_str()) =>
        {
            Verification::Success(format!("núcleo respondeu em {}", path.display()))
        }
        Ok(reply) if reply.kind == MessageType::Error => Verification::Failed(
            reply
                .error_message
                .unwrap_or_else(|| "erro sem mensagem".to_owned()),
        ),
        Ok(reply) => Verification::Failed(format!("resposta inesperada: {:?}", reply.kind)),
        Err(KavoError::Io { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Verification::Failed(format!("nenhum núcleo escutando em {}", path.display()))
        }
        Err(e) => Verification::Unknown(e.to_string()),
    }
}
