//! Armazenamento durável do monitor de contratos (SQLite, D-010).
//!
//! Garantias:
//! - Uma admissão só é reportada depois do `COMMIT` (WAL + `synchronous=FULL`).
//! - Uma identidade de execução (`session_id`, chave) é admitida no máximo uma vez, para sempre.
//!   A unicidade é imposta pelo banco (`PRIMARY KEY`), não só pela memória.
//! - Transações `BEGIN IMMEDIATE` serializam escritores concorrentes.
//! - Falha de gravação ou de abertura é erro; quem chama não admite (fail-closed).
//! - Só digests (SHA-256) de chaves e conteúdos são gravados. Parâmetros e texto não vão para o disco.
//!
//! Retenção: nada é apagado automaticamente. Admissões são imutáveis. Ver `SPEECH-INTENT-CONTRACTS.md` §10.

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::error::{KavoError, Result};

pub const SCHEMA_VERSION: i64 = 1;

/// Estado de um stream gravado. `Open` só existe enquanto o núcleo está rodando.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDbStatus {
    Open,
    Ended,
    Cancelled,
    Failed,
    TimedOut,
    /// O núcleo reiniciou com o stream aberto. Nunca é retomado.
    Interrupted,
    /// Não pôde ser admitido (ex.: limite de streams). Nunca é retomado.
    Rejected,
}

impl StreamDbStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Ended => "ENDED",
            Self::Cancelled => "CANCELLED",
            Self::Failed => "FAILED",
            Self::TimedOut => "TIMED_OUT",
            Self::Interrupted => "INTERRUPTED",
            Self::Rejected => "REJECTED",
        }
    }
}

/// Registro de interpretação. Conteúdo entra só como digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterpretationRow {
    pub session_id: String,
    pub interpretation_id: Uuid,
    pub stream_id: Uuid,
    pub segment_id: Uuid,
    pub revision: u32,
    pub source_revision: u32,
    pub content_digest: String,
    pub cancelled: bool,
}

/// Admissão a gravar atomicamente com a interpretação que a originou.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRow {
    pub session_id: String,
    pub segment_id: Uuid,
    pub stream_id: Uuid,
    pub execution_key: String,
    pub intent: String,
    pub interpretation: InterpretationRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed,
    /// A chave já foi admitida nesta sessão (em qualquer momento anterior). Nada foi gravado.
    AlreadyConsumed,
}

/// Estado restaurado ao iniciar.
#[derive(Debug, Default)]
pub struct Snapshot {
    pub streams: Vec<(String, Uuid)>,
    pub interpretations: Vec<InterpretationRow>,
    /// (sessão, chave) de toda admissão já feita.
    pub consumed: Vec<(String, String)>,
    /// (sessão, segmento, chave atual).
    pub current: Vec<(String, Uuid, String)>,
}

#[derive(Debug)]
pub struct Store {
    conn: Connection,
    path: PathBuf,
}

fn db_err(context: &str, e: rusqlite::Error) -> KavoError {
    KavoError::Store(format!("{context}: {e}"))
}

impl Store {
    /// Abre (ou cria) o banco, aplica o esquema e verifica integridade. Qualquer falha é erro.
    pub fn open(path: &Path) -> Result<Self> {
        // Arquivo novo nasce com 0600: SQLite herda o modo para -wal e -shm.
        let existed = path.exists();
        if !existed {
            use std::os::unix::fs::OpenOptionsExt;
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
            {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(KavoError::io(format!("criar {}", path.display()), e)),
            }
        }
        let conn = Connection::open(path).map_err(|e| db_err("abrir banco", e))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| db_err("busy_timeout", e))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| db_err("journal_mode=WAL", e))?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(|e| db_err("synchronous=FULL", e))?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| db_err("foreign_keys", e))?;

        let check: String = conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(|e| db_err("quick_check", e))?;
        if check != "ok" {
            return Err(KavoError::Store(format!("banco corrompido: {check}")));
        }
        if !existed {
            set_permissions_private(path)?;
        }
        check_mode_private(path)?;

        let store = Self {
            conn,
            path: path.to_path_buf(),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn migrate(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );",
            )
            .map_err(|e| db_err("criar meta", e))?;
        let current: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| db_err("ler versão", e))?;
        match current.map(|v| v.parse::<i64>()) {
            None => {
                let tx = self
                    .conn
                    .unchecked_transaction()
                    .map_err(|e| db_err("iniciar migração", e))?;
                tx.execute_batch(SCHEMA_V1)
                    .map_err(|e| db_err("esquema v1", e))?;
                tx.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?1)",
                    params![SCHEMA_VERSION.to_string()],
                )
                .map_err(|e| db_err("gravar versão", e))?;
                tx.commit().map_err(|e| db_err("commit migração", e))?;
                Ok(())
            }
            Some(Ok(v)) if v == SCHEMA_VERSION => Ok(()),
            Some(Ok(v)) => Err(KavoError::Store(format!(
                "esquema {v} não suportado (esperado {SCHEMA_VERSION}); recusando abrir"
            ))),
            Some(Err(_)) => Err(KavoError::Store("versão de esquema ilegível".to_owned())),
        }
    }

    /// Carrega o estado durável. Streams ainda `OPEN` viram `INTERRUPTED` na mesma transação.
    pub fn load(&mut self) -> Result<Snapshot> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_err("iniciar carga", e))?;
        tx.execute(
            "UPDATE streams SET status = ?1 WHERE status = ?2",
            params![
                StreamDbStatus::Interrupted.as_str(),
                StreamDbStatus::Open.as_str()
            ],
        )
        .map_err(|e| db_err("interromper streams abertos", e))?;

        let mut snap = Snapshot::default();
        {
            let mut stmt = tx
                .prepare("SELECT session_id, stream_id FROM streams")
                .map_err(|e| db_err("preparar streams", e))?;
            for row in stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| db_err("ler streams", e))?
            {
                let (session, stream) = row.map_err(|e| db_err("linha de stream", e))?;
                snap.streams.push((session, parse_uuid(&stream)?));
            }
        }
        {
            let mut stmt = tx
                .prepare(
                    "SELECT session_id, interpretation_id, stream_id, segment_id, revision,
                            source_revision, content_digest, cancelled FROM interpretations",
                )
                .map_err(|e| db_err("preparar interpretações", e))?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, u32>(4)?,
                        r.get::<_, u32>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, bool>(7)?,
                    ))
                })
                .map_err(|e| db_err("ler interpretações", e))?;
            for row in rows {
                let (
                    session_id,
                    interp,
                    stream,
                    segment,
                    revision,
                    source_revision,
                    content_digest,
                    cancelled,
                ) = row.map_err(|e| db_err("linha de interpretação", e))?;
                snap.interpretations.push(InterpretationRow {
                    session_id,
                    interpretation_id: parse_uuid(&interp)?,
                    stream_id: parse_uuid(&stream)?,
                    segment_id: parse_uuid(&segment)?,
                    revision,
                    source_revision,
                    content_digest,
                    cancelled,
                });
            }
        }
        {
            let mut stmt = tx
                .prepare("SELECT session_id, execution_key FROM admission_keys")
                .map_err(|e| db_err("preparar admissões", e))?;
            for row in stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| db_err("ler admissões", e))?
            {
                snap.consumed
                    .push(row.map_err(|e| db_err("linha de admissão", e))?);
            }
        }
        {
            let mut stmt = tx
                .prepare("SELECT session_id, segment_id, execution_key FROM segment_admission")
                .map_err(|e| db_err("preparar segmentos", e))?;
            for row in stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(|e| db_err("ler segmentos", e))?
            {
                let (session, segment, key) = row.map_err(|e| db_err("linha de segmento", e))?;
                snap.current.push((session, parse_uuid(&segment)?, key));
            }
        }
        tx.commit().map_err(|e| db_err("commit carga", e))?;
        Ok(snap)
    }

    /// Registra o início de um stream. `Ok(false)` se o id já existe (não reabre).
    pub fn open_stream(&mut self, session: &str, stream: &Uuid, now_ms: i64) -> Result<bool> {
        let changed = self
            .conn
            .execute(
                "INSERT OR IGNORE INTO streams (session_id, stream_id, status, opened_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    session,
                    stream.to_string(),
                    StreamDbStatus::Open.as_str(),
                    now_ms
                ],
            )
            .map_err(|e| db_err("abrir stream", e))?;
        Ok(changed == 1)
    }

    /// Fecha um stream com estado terminal. Só altera se ainda estiver `OPEN`.
    pub fn close_stream(
        &mut self,
        session: &str,
        stream: &Uuid,
        status: StreamDbStatus,
        now_ms: i64,
    ) -> Result<()> {
        self.conn
            .execute(
                "UPDATE streams SET status = ?3, closed_at_ms = ?4
                 WHERE session_id = ?1 AND stream_id = ?2 AND status = ?5",
                params![
                    session,
                    stream.to_string(),
                    status.as_str(),
                    now_ms,
                    StreamDbStatus::Open.as_str()
                ],
            )
            .map_err(|e| db_err("fechar stream", e))?;
        Ok(())
    }

    /// Cancela um stream: marca o stream, suas interpretações e suas admissões. Atômico.
    /// Se o stream não existir, grava um aposentado `CANCELLED` (ele não poderá começar depois).
    pub fn cancel_stream(&mut self, session: &str, stream: &Uuid, now_ms: i64) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_err("iniciar cancelamento", e))?;
        let id = stream.to_string();
        tx.execute(
            "INSERT OR IGNORE INTO streams (session_id, stream_id, status, opened_at_ms, closed_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?4)",
            params![session, id, StreamDbStatus::Cancelled.as_str(), now_ms],
        )
        .map_err(|e| db_err("tombstone de cancelamento", e))?;
        tx.execute(
            "UPDATE streams SET status = ?3, closed_at_ms = ?4
             WHERE session_id = ?1 AND stream_id = ?2 AND status = ?5",
            params![
                session,
                id,
                StreamDbStatus::Cancelled.as_str(),
                now_ms,
                StreamDbStatus::Open.as_str()
            ],
        )
        .map_err(|e| db_err("cancelar stream", e))?;
        tx.execute(
            "UPDATE interpretations SET cancelled = 1 WHERE session_id = ?1 AND stream_id = ?2",
            params![session, id],
        )
        .map_err(|e| db_err("cancelar interpretações", e))?;
        tx.execute(
            "UPDATE segment_admission SET status = 'CANCELLED', updated_at_ms = ?3
             WHERE session_id = ?1 AND stream_id = ?2 AND status = 'ADMITTED'",
            params![session, id, now_ms],
        )
        .map_err(|e| db_err("cancelar admissões", e))?;
        tx.commit().map_err(|e| db_err("commit cancelamento", e))?;
        Ok(())
    }

    /// Grava ou atualiza uma interpretação (sem admissão).
    pub fn upsert_interpretation(&mut self, row: &InterpretationRow, now_ms: i64) -> Result<()> {
        upsert_interp(&self.conn, row, now_ms).map_err(|e| db_err("gravar interpretação", e))
    }

    /// Admissão atômica: consome a chave, troca o ponteiro do segmento e grava a interpretação.
    /// Se a chave já existir (qualquer processo, qualquer momento), não grava nada.
    pub fn commit_admission(&mut self, adm: &AdmissionRow, now_ms: i64) -> Result<CommitOutcome> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_err("iniciar admissão", e))?;
        match insert_key(&tx, adm, now_ms) {
            Ok(true) => {}
            Ok(false) => return Ok(CommitOutcome::AlreadyConsumed),
            Err(e) => return Err(db_err("consumir chave", e)),
        }
        tx.execute(
            "INSERT INTO segment_admission
                (session_id, segment_id, stream_id, execution_key, status, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, 'ADMITTED', ?5)
             ON CONFLICT(session_id, segment_id) DO UPDATE SET
                stream_id = excluded.stream_id,
                execution_key = excluded.execution_key,
                status = 'ADMITTED',
                updated_at_ms = excluded.updated_at_ms",
            params![
                adm.session_id,
                adm.segment_id.to_string(),
                adm.stream_id.to_string(),
                adm.execution_key,
                now_ms
            ],
        )
        .map_err(|e| db_err("gravar segmento", e))?;
        upsert_interp(&tx, &adm.interpretation, now_ms)
            .map_err(|e| db_err("gravar interpretação", e))?;
        tx.commit().map_err(|e| db_err("commit admissão", e))?;
        Ok(CommitOutcome::Committed)
    }

    /// Conta admissões gravadas (diagnóstico e testes).
    pub fn admission_count(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM admission_keys", [], |r| r.get(0))
            .map_err(|e| db_err("contar admissões", e))
    }

    /// Verificação de integridade para testes e diagnóstico.
    pub fn integrity_ok(&self) -> Result<bool> {
        let check: String = self
            .conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(|e| db_err("quick_check", e))?;
        Ok(check == "ok")
    }

    /// Estado de um stream no banco (diagnóstico e testes).
    pub fn stream_status(&self, session: &str, stream: &Uuid) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT status FROM streams WHERE session_id = ?1 AND stream_id = ?2",
                params![session, stream.to_string()],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| db_err("ler stream", e))
    }

    /// Estado do segmento admitido (diagnóstico e testes).
    pub fn segment_status(&self, session: &str, segment: &Uuid) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT status FROM segment_admission WHERE session_id = ?1 AND segment_id = ?2",
                params![session, segment.to_string()],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| db_err("ler segmento", e))
    }
}

fn insert_key(tx: &Transaction<'_>, adm: &AdmissionRow, now_ms: i64) -> rusqlite::Result<bool> {
    let changed = tx.execute(
        "INSERT OR IGNORE INTO admission_keys
            (session_id, execution_key, segment_id, stream_id, interpretation_id, intent, admitted_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            adm.session_id,
            adm.execution_key,
            adm.segment_id.to_string(),
            adm.stream_id.to_string(),
            adm.interpretation.interpretation_id.to_string(),
            adm.intent,
            now_ms
        ],
    )?;
    Ok(changed == 1)
}

fn upsert_interp(conn: &Connection, row: &InterpretationRow, now_ms: i64) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO interpretations
            (session_id, interpretation_id, stream_id, segment_id, revision, source_revision,
             content_digest, cancelled, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(session_id, interpretation_id) DO UPDATE SET
            revision = excluded.revision,
            source_revision = excluded.source_revision,
            content_digest = excluded.content_digest,
            cancelled = MAX(interpretations.cancelled, excluded.cancelled),
            updated_at_ms = excluded.updated_at_ms",
        params![
            row.session_id,
            row.interpretation_id.to_string(),
            row.stream_id.to_string(),
            row.segment_id.to_string(),
            row.revision,
            row.source_revision,
            row.content_digest,
            row.cancelled,
            now_ms
        ],
    )?;
    Ok(())
}

fn parse_uuid(text: &str) -> Result<Uuid> {
    Uuid::parse_str(text).map_err(|_| KavoError::Store("UUID corrompido no banco".to_owned()))
}

/// Recusa banco com permissões de grupo ou outros. Vale para arquivo principal.
fn check_mode_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)
        .map_err(|e| KavoError::io(format!("verificar {}", path.display()), e))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(KavoError::Store(format!(
            "{} com permissões abertas ({:o}); recusando",
            path.display(),
            mode & 0o777
        )));
    }
    Ok(())
}

fn set_permissions_private(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| KavoError::io(format!("permissões de {}", path.display()), e))
}

const SCHEMA_V1: &str = "
CREATE TABLE streams (
    session_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    PRIMARY KEY (session_id, stream_id)
);
CREATE TABLE interpretations (
    session_id TEXT NOT NULL,
    interpretation_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    source_revision INTEGER NOT NULL,
    content_digest TEXT NOT NULL,
    cancelled INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, interpretation_id)
);
CREATE TABLE admission_keys (
    session_id TEXT NOT NULL,
    execution_key TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    interpretation_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    admitted_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, execution_key)
);
CREATE TABLE segment_admission (
    session_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    execution_key TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, segment_id),
    FOREIGN KEY (session_id, execution_key) REFERENCES admission_keys (session_id, execution_key)
);
";

/// SHA-256 em hexadecimal. Usado para chaves e conteúdos gravados: estável entre reinícios.
pub fn sha256_hex(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(text.as_bytes());
    digest.iter().fold(String::with_capacity(64), |mut out, b| {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
        out
    })
}
