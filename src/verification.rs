//! Resultado verificado de uma operação. Quem verifica decide; nunca presumir sucesso.

use serde::Serialize;

/// `SUCCESS` só com confirmação observada; `UNKNOWN` quando o resultado não pôde ser confirmado.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "status",
    content = "detail",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum Verification {
    Success(String),
    Failed(String),
    Unknown(String),
}

impl Verification {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }
}
