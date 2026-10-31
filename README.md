# Kavo — fundação do núcleo (Fase 4.1)

Núcleo em Rust: configuração, logging estruturado, erros, runtime supervisionado,
**Kavo Protocol** v1.1-draft (NDJSON sobre Unix socket) e barramento interno.
Escopo: sem voz, wake word, STT, TTS, LLM, automação ou UI.

## Documentação

- `docs/KAVO-PROTOCOL.md`: especificação do protocolo e reconciliação 03-PROTOCOLOS × 10-CONTRATOS.
- `docs/ERROR-TAXONOMY.md`: classes de erro e códigos propostos.
- `docs/SPEECH-INTENT-CONTRACTS.md`: contratos de STT streaming, Intent Engine, prontidão e persistência durável (§9).
- `docs/DECISIONS.md`: decisões com status (aprovada, provisória, pendente).

## Estrutura

| Módulo | Responsabilidade |
|---|---|
| `config` | TOML + Serde; overrides `KAVO_*`; validação; caminhos XDG |
| `logging` | `tracing` em stderr; JSON por padrão; `RUST_LOG` tem precedência |
| `error` | `KavoError` e classes `ErrorKind` |
| `protocol` | `KavoMessage` 1.1, tipos implementados, reservados, entrega, validação |
| `speech` | contratos de STT streaming e `StreamTracker` (ordem, revisões, timeout) |
| `intent` | contratos do Intent Engine, `InterpretationTracker` e `ReadinessGate` |
| `monitor` | assinante do barramento que aplica os contratos e grava o estado (sem ações) |
| `store` | SQLite de admissões: transações atômicas, unicidade, restauração (D-031) |
| `ipc` | NDJSON sobre Unix socket; limite de tamanho; prazo; cliente `ping` |
| `bus` | barramento interno: filas mpsc por assinante; garantido vs. melhor esforço |
| `runtime` | trait `Component`, `Supervisor`, `run` (SIGINT/SIGTERM) |
| `instance` | lock `flock`: uma instância por usuário |
| `verification` | `SUCCESS` / `FAILED` / `UNKNOWN` |
| `session` | Session Manager (A0): sessões, requisições e tokens de cancelamento; só memória |
| `harness` | Cenários reproduzíveis de teste (A0), com fonte virtual de STT sintético |

## Uso

```bash
cargo build --release
target/release/kavo check-config   # configuração efetiva
target/release/kavo run            # núcleo; SIGINT/SIGTERM encerram
target/release/kavo ping           # JSON com SUCCESS / FAILED / UNKNOWN
```

Configuração opcional em `~/.config/kavo/kavo.toml`. Variáveis: `KAVO_LOG_LEVEL`,
`KAVO_LOG_JSON`, `KAVO_IPC_SOCKET_PATH`, `KAVO_SHUTDOWN_TIMEOUT_MS`, `KAVO_STORE_PATH`.

Banco de admissões: `$XDG_DATA_HOME/kavo/kavo.db` (ou `store.path`). Sem o banco íntegro, o núcleo não inicia.

Socket: `$XDG_RUNTIME_DIR/kavo/kavo.sock` (0600; diretório 0700).

## Testes

```bash
cargo test                          # 156 testes (unitários e integração)
cargo clippy --all-targets --all-features -- -D warnings   # 0 avisos
```

Integração inclui socket Unix real, supervisor com pânico e timeout, lock de instância e barramento.

### Harness de cenários (A0)

Fonte virtual de eventos de STT **sintéticos** (emissor `fonte-virtual`). Não há captura de áudio nem reconhecimento de fala.

```bash
target/release/kavo harness list
target/release/kavo harness all                 # cenários em processo (barramento, monitor, SQLite temporário)
target/release/kavo harness sessions cancel     # escolha de cenários
target/release/kavo harness ipc-stream --socket "$KAVO_IPC_SOCKET_PATH" --store "$KAVO_STORE_PATH"   # contra núcleo em execução
```

Saída: uma linha JSON por verificação e uma por cenário. Código 0 = tudo aprovado; 1 = reprovação ou erro; 2 = uso inválido.
`ipc-stream` exige núcleo em execução e banco do mesmo núcleo; não entra em `all`.

Session Manager (`src/session.rs`): ciclo de vida de sessões e cancelamento por requisição, só em memória. Não concede permissões.

## Limitações

- Sem mecanismo real de STT nem de NLU/JEV; só contratos e regras.
- Sem Capability Registry, Policy Engine, Planner, Task Manager ou Execution Engine.
- Protocolo: HEARTBEAT, ERROR, CANCEL, STT_RESULT, INTENT_DETECTED.
- Barramento só em processo; sem Message Bus externo.
- Código 1100 para quase todos os erros até a taxonomia ser aprovada.
- Apenas Linux (Unix socket, flock).
