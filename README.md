# Kavo — fundação do núcleo (Fase 4.1)

Núcleo em Rust: configuração, logging estruturado, erros, runtime supervisionado,
**Kavo Protocol** v1.0-draft (NDJSON sobre Unix socket) e barramento interno.
Escopo: sem voz, wake word, STT, TTS, LLM, automação ou UI.

## Documentação

- `docs/KAVO-PROTOCOL.md`: especificação do protocolo e reconciliação 03-PROTOCOLOS × 10-CONTRATOS.
- `docs/ERROR-TAXONOMY.md`: classes de erro e códigos propostos.
- `docs/DECISIONS.md`: decisões com status (aprovada, provisória, pendente).

## Estrutura

| Módulo | Responsabilidade |
|---|---|
| `config` | TOML + Serde; overrides `KAVO_*`; validação; caminhos XDG |
| `logging` | `tracing` em stderr; JSON por padrão; `RUST_LOG` tem precedência |
| `error` | `KavoError` e classes `ErrorKind` |
| `protocol` | `KavoMessage`, tipos implementados, reservados, entrega, validação |
| `ipc` | NDJSON sobre Unix socket; limite de tamanho; prazo; cliente `ping` |
| `bus` | barramento interno: filas mpsc por assinante; garantido vs. melhor esforço |
| `runtime` | trait `Component`, `Supervisor`, `run` (SIGINT/SIGTERM) |
| `instance` | lock `flock`: uma instância por usuário |
| `verification` | `SUCCESS` / `FAILED` / `UNKNOWN` |

## Uso

```bash
cargo build --release
target/release/kavo check-config   # configuração efetiva
target/release/kavo run            # núcleo; SIGINT/SIGTERM encerram
target/release/kavo ping           # JSON com SUCCESS / FAILED / UNKNOWN
```

Configuração opcional em `~/.config/kavo/kavo.toml`. Variáveis: `KAVO_LOG_LEVEL`,
`KAVO_LOG_JSON`, `KAVO_IPC_SOCKET_PATH`, `KAVO_SHUTDOWN_TIMEOUT_MS`.

Socket: `$XDG_RUNTIME_DIR/kavo/kavo.sock` (0600; diretório 0700).

## Testes

```bash
cargo test                          # 38 testes (unitários e integração)
cargo clippy --all-targets          # 0 avisos
```

Integração inclui socket Unix real, supervisor com pânico e timeout, lock de instância e barramento.
Testes E2E no computador real estão descritos em `docs/` e no relatório da fase.

## Limitações

- Protocolo sem STT, NLU, comandos ou memória; só HEARTBEAT, ERROR, CANCEL.
- Barramento só em processo; sem Message Bus externo.
- Código 1100 para quase todos os erros até a taxonomia ser aprovada.
- Apenas Linux (Unix socket, flock).
