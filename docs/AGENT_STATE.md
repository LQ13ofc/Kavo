# Estado do agente — Kavo

Checkpoint curto: etapa, commits, testes, bloqueios e próximo passo. Decisões em `DECISIONS.md`; contratos em `KAVO-PROTOCOL.md`.

## Etapa atual

**Auditoria final de A1 + A1.1 (preparação para consolidação).** Working tree validado. **Sem commit e sem push** nesta etapa.

- Base: `main` em `9ee7ac7` (A0, local, não enviado). `origin/main` em `5b96018`. Relação: `main` 2 commits à frente de `origin/main`.
- Não commitado: A1 (sessões, cancelamento, protocolo 1.2) e A1.1 (ACK de admissão, D-58) e a auditoria desta etapa.
- Proposta de commits: ver seção "Consolidação" em `docs/AGENT_STATE.md` abaixo.

## Problemas encontrados e corrigidos na auditoria

| # | Problema | Efeito | Correção | Regressão |
|---|---|---|---|---|
| 1 | Recusa por capacidade com falha ao gravar `REJECTED` | ACK `REJECTED`, banco `OPEN` | `NOT_READY`; abertura fica pendente (D-71) | `admission-faults` B; mutação M1 |
| 2 | Retry de recusa por capacidade mudava o motivo (`STREAM_LIMIT` → `STREAM_CLOSED`) | Resposta inconsistente | Motivo do status durável (D-72) | `admission-faults` A; mutação M4 |
| 3 | Evento aceito de stream evicto virava recusa | Aceito não reconhecido como `DUPLICATE` | Janela de digests na evicção (D-70, D-73) | `admission-faults` C; mutação M2 |
| 4 | Interpretação após fim de stream (memória esquecida) virava `GAP` | Aceita não reconhecida | Rehidratação da durável (D-74) | `admission-faults` D; mutação M3 |
| 5 | Envio ao monitor sem prazo | Bloqueio indefinido com fila cheia | Prazo do IPC (D-77) | E2E backpressure |
| 6 | Testes Fase 4.2 esperavam `REJECTED` no replay de evicto | Expectativa antiga conflitava com D-58 | Expectativa atualizada, sem enfraquecer "não reabrir" (D-70) | `tests/speech_intent.rs` |
| 7 | Memória de streams retinha texto transcrito canônico | Dado sensível fora do necessário | Digest SHA-256 (D-73) | suíte inteira |

Já corrigido antes da auditoria (A1.1): interpretação não gravada desfaz a memória (D-67); sequência recusada por conteúdo devolve a mesma recusa (D-66).

## Garantias demonstradas

- Retry com a mesma identidade após ACK perdido: `DUPLICATE` sem reaplicação (STT e INTENT, dentro das janelas da seção 11.8).
- Evento terminal, abertura, upsert de interpretação e recusa por capacidade: nenhum ACK positivo ou recusa permanente sem gravação correspondente.
- Falha transitória (trigger SQLite injetado): `NOT_READY`, retry com a mesma identidade vira `ACCEPTED`, nunca `DUPLICATE`.
- Cancelamento concorrente: serializado no monitor; aceito antes = `DUPLICATE`, depois = `REJECTED STREAM_CLOSED`.
- `correlation_id` = `message_id` do evento em todas as respostas testadas.
- Mutações: M1 a M4 reprovam os cenários certos; cada uma foi revertida e verificada.

Limites reais (ver `KAVO-PROTOCOL.md` 11.8): janela de 64 eventos por stream; última revisão por interpretação; sessão em memória após reinício; terminal após reinício vira `STREAM_CLOSED` (D-75, pendente); commit ambíguo raro.

## Testes (resultado final desta etapa)

| Gate | Resultado |
|---|---|
| `cargo fmt --check` | ok |
| `cargo clippy --all-targets --all-features -- -D warnings` | 0 avisos |
| `cargo test` | 180 aprovados, 0 falhas |
| `harness all` (em processo) | 18 cenários aprovados, 0 reprovados (inclui `pending-lifecycle`, 18 verificações) |
| `cargo build --release` | ok |
| `harness admission-faults` (29 verificações) | aprovado |
| E2E daemon real (`target/e2e.sh`, 3 daemons) | ipc-stream 5/5, ipc-control 22/22, ipc-admission 14/14, ipc-interrupt 3/3 após reinício, ipc-backpressure 3/3; daemon exit 0; 0 ERROR; 0 texto sintético; 0 IDs de sessão |

### Medidas (CachyOS, release, daemon real)

- ACK pelo socket, 1 cliente, 1000 parciais sequenciais: p50 42 µs, p95 79 µs, p99 94 µs, máx 168 µs; vazão 19.885/s.
- ACK pelo socket, 8 clientes × 250: p50 70 µs, p95 260 µs, p99 376 µs; vazão 34.259/s.
- Em processo, 2000 parciais: p50 17 µs, p95 25 µs, p99 48 µs; vazão 48.895/s.
- Backpressure, fila 1, 12 clientes × 150: 1800 admitidos, 978 BUSY, 0 duplicados, 0 outras recusas; vazão 7.701 admitidos/s.
- Base 1.1 sem ACK: 110.366 envios/s. Mede só escrita no socket; não comparável.

## Fechamento: limpeza de `pending_open` (esta etapa)

- Ciclo de vida mapeado: criada só na abertura gravada (`OPEN`) com recusa por capacidade pendente de gravação. Removida ao admitir em memória, ao gravar a recusa, ao cancelar stream (depois da gravação) e ao encerrar sessão (depois da gravação).
- Antes desta etapa, cancelamento e encerramento não removiam a entrada: vazamento e consumo indevido do teto de capacidade de todas as sessões (confirmado por mutação).
- Teto = `speech.max_streams`; acima dele, `NOT_READY BUSY` sem gravação (D-79).
- Regressão: cenário `pending-lifecycle` (18 verificações). Mutações: sem limpeza no cancelamento, sem limpeza no encerramento e sem teto, cada uma reprovada e revertida.
- Gauge `pending_open` em `MonitorStats` para observação.

## Pendências

- **Falha de SQLite no encerramento de sessão:** a limpeza só ocorre depois da gravação; em falha, a sessão segue ativa e as pendências permanecem. Caminho coberto por código, não por teste dedicado (a falha injetada em P4 é de cancelamento).
- **D-75 (pendente):** digest durável do terminal para idempotência após reinício (mudança de esquema).
- **D-76 (pendente):** histórico de revisões de interpretação.
- **D-09-LOCAL (pendente):** tipos fora da 6.6.
- **D-45 (pendente):** expiração de sessão por inatividade.
- **D-36 (pendente):** retenção de admissões.
- **Sessões após reinício:** precisam de decisão própria para `DUPLICATE` entre reinícios (ver 11.8).
- **Calibração:** `ipc.admission_queue` (D-64), medido só neste CachyOS.

## Consolidação (proposta, não executada)

Ordem sugerida, sem commit feito. Todos os arquivos são do working tree atual; `target/` está ignorado pelo Git.

1. Commit A1 (sessões e protocolo 1.2, sem ACK): `src/control.rs`, parte de `src/monitor.rs`, `src/ipc.rs`, `src/protocol.rs`, `src/runtime.rs`, `src/menu.rs`, `src/main.rs`, `src/lib.rs`, `tests/control.rs`, docs e README da A1. **Dificuldade:** arquivos foram tocados pelas duas fases; separar por hunk exige `git add -p`.
2. Commit A1.1 (ACK e admissão): `src/config.rs`, `src/intent.rs`, `src/speech.rs`, `tests/admission.rs`, restante de `monitor.rs`, `ipc.rs`, `protocol.rs`, `control.rs`, `harness.rs`, `tests/harness.rs`.
3. Commit auditoria (correções de memória × persistência, testes de falha e documentação final).

Alternativa mais segura: um commit único "A1 + A1.1 + auditoria" com mensagem que lista as três partes, porque as alterações se sobrepõem em `monitor.rs`, `ipc.rs`, `protocol.rs` e `harness.rs`.

## Próximo passo

Autorização explícita para commit. Depois: decidir D-75 e D-76 e as políticas de sessão após reinício.
