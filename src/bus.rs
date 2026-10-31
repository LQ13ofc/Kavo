//! Barramento interno pub/sub. Cada assinante tem sua própria fila `mpsc` limitada.
//!
//! Por que não `tokio::broadcast`: um assinante lento recebe `Lagged` e perde mensagens sem aviso,
//! inclusive `ERROR` e `CANCEL`. Aqui, mensagens garantidas são enviadas a todos os assinantes em
//! paralelo, com um único prazo, e falham explicitamente se algum não aceitar ou se não houver
//! assinante. Mensagens de melhor esforço são descartadas e contadas.
//!
//! Entrega garantida parcial (alguns assinantes já receberam) não tem rollback: o erro informa isso
//! e o emissor decide se reenvia. Reenvio pode duplicar; por isso a deduplicação por `message_id`
//! é responsabilidade de quem consome.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::future::join_all;
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::warn;

use crate::error::{KavoError, Result};
use crate::protocol::{Delivery, KavoMessage};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishReport {
    pub delivered: usize,
    /// Assinantes de melhor esforço com fila cheia.
    pub dropped: usize,
}

#[derive(Debug, Clone)]
pub struct EventBus {
    subscribers: Arc<Mutex<Vec<mpsc::Sender<KavoMessage>>>>,
    capacity: usize,
    delivery_timeout: Duration,
}

impl EventBus {
    /// `capacity`: tamanho da fila de cada assinante. `delivery_timeout`: prazo para mensagens garantidas.
    pub fn new(capacity: usize, delivery_timeout: Duration) -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(Vec::new())),
            capacity: capacity.max(1),
            delivery_timeout,
        }
    }

    pub fn subscribe(&self) -> mpsc::Receiver<KavoMessage> {
        let (sender, receiver) = mpsc::channel(self.capacity);
        self.lock().push(sender);
        receiver
    }

    /// Publica para os assinantes ativos.
    ///
    /// Mensagem garantida: erro se não houver assinante, se algum estiver fechado, ou se algum não
    /// aceitar no prazo. Mensagem de melhor esforço: nunca erro; descartes são contados.
    pub async fn publish(&self, msg: KavoMessage) -> Result<PublishReport> {
        // Copia os emissores para não manter o lock durante awaits.
        let targets: Vec<mpsc::Sender<KavoMessage>> = {
            let mut subs = self.lock();
            subs.retain(|s| !s.is_closed());
            subs.clone()
        };

        match msg.kind.delivery() {
            Delivery::Guaranteed => self.publish_guaranteed(msg, targets).await,
            Delivery::BestEffort => Ok(publish_best_effort(&msg, &targets)),
        }
    }

    async fn publish_guaranteed(
        &self,
        msg: KavoMessage,
        targets: Vec<mpsc::Sender<KavoMessage>>,
    ) -> Result<PublishReport> {
        if targets.is_empty() {
            return Err(KavoError::DeliveryFailed(format!(
                "nenhum assinante para {:?}",
                msg.kind
            )));
        }
        let timeout = self.delivery_timeout;
        let sends = targets.iter().map(|target| {
            let payload = msg.clone();
            async move { tokio::time::timeout(timeout, target.send(payload)).await }
        });

        let mut delivered = 0usize;
        let mut failures = Vec::new();
        for outcome in join_all(sends).await {
            match outcome {
                Ok(Ok(())) => delivered += 1,
                Ok(Err(_)) => failures.push("assinante fechado".to_owned()),
                Err(_) => failures.push(format!("assinante não aceitou em {timeout:?}")),
            }
        }

        if failures.is_empty() {
            Ok(PublishReport {
                delivered,
                dropped: 0,
            })
        } else {
            Err(KavoError::DeliveryFailed(format!(
                "{:?}: {delivered} de {} entregues; {}",
                msg.kind,
                targets.len(),
                failures.join("; ")
            )))
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<mpsc::Sender<KavoMessage>>> {
        // Poison só ocorre se um assinante entrou em pânico segurando o lock; a lista continua válida.
        self.subscribers.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn publish_best_effort(msg: &KavoMessage, targets: &[mpsc::Sender<KavoMessage>]) -> PublishReport {
    let mut report = PublishReport::default();
    for target in targets {
        match target.try_send(msg.clone()) {
            Ok(()) => report.delivered += 1,
            Err(TrySendError::Full(_)) => {
                report.dropped += 1;
                warn!(kind = ?msg.kind, "mensagem de melhor esforço descartada: fila cheia");
            }
            // Fechado após a cópia da lista: será removido na próxima publicação.
            Err(TrySendError::Closed(_)) => {}
        }
    }
    report
}
