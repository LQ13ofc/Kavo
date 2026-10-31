//! Garante uma única instância do núcleo por usuário via `flock` não bloqueante.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use rustix::fs::{FlockOperation, flock};
use rustix::io::Errno;

use crate::error::{KavoError, Result};

/// Mantém o lock enquanto existir. O lock é liberado pelo kernel se o processo morrer.
#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
    path: PathBuf,
}

impl InstanceLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| KavoError::io(format!("abrir lock {}", path.display()), e))?;

        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {}
            Err(Errno::WOULDBLOCK) => {
                return Err(KavoError::AlreadyRunning {
                    path: path.display().to_string(),
                });
            }
            Err(e) => {
                return Err(KavoError::io(
                    format!("travar {}", path.display()),
                    std::io::Error::from(e),
                ));
            }
        }

        file.set_len(0)
            .and_then(|()| writeln!(file, "{}", std::process::id()))
            .map_err(|e| KavoError::io(format!("gravar PID em {}", path.display()), e))?;

        Ok(Self {
            _file: file,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
