# Contratos de STT streaming e Kavo Intent Engine

Status: **contratos provisórios** (Phase 4.2). Nenhum mecanismo real de STT ou NLU está implementado. Os contratos não dependem de fornecedor.

Regras que os contratos garantem:
- Texto transcrito nunca é comando. Nada é executado a partir deste pacote.
- O Intent Engine não concede permissão. O Policy Engine é a única autoridade de autorização.
- Interpretação provisória não inicia ação.
- Ação sensível ou irreversível exige confirmação de política, mesmo com interpretação estável.
- Logs não contêm texto transcrito nem valores de parâmetros.

## 1. STT streaming (`STT_RESULT`)

Envelope: `session_id` é a sessão. `payload` é `SpeechPayload`.

| Campo | Tipo | Regra |
|---|---|---|
| `stream_id` | UUID v4 | Fluxo de áudio. Produzido por quem captura o áudio. |
| `sequence` | u64 | Começa em 1. Estritamente crescente por stream. |
| `audio_start_ms` / `audio_end_ms` | u64 | Posição no áudio do stream. `start <= end`. |
| `event` | string | Discriminador: `TRANSCRIPT`, `SPEECH_STARTED`, `SPEECH_ENDED`, `NO_SPEECH`, `UNINTELLIGIBLE`, `STREAM_ENDED`, `STREAM_FAILED`. |

Campos por evento:

- `TRANSCRIPT`: `segment_id` (UUID v4), `revision` (u32), `state` (`PARTIAL` ou `CONFIRMED`), `text` (não vazio, até 2048 caracteres), `confidence` (opcional, `[0,1]`). Confiança só existe se o mecanismo fornecer.
- `SPEECH_STARTED` / `SPEECH_ENDED` / `UNINTELLIGIBLE`: `segment_id`.
- `NO_SPEECH`, `STREAM_ENDED`: sem campos.
- `STREAM_FAILED`: `reason` (até 128 caracteres).

Estados:

- `PARTIAL`: hipótese. Pode ser revisada.
- `CONFIRMED`: terminal para o segmento. Revisões seguintes são rejeitadas.

Revisões: segmento novo começa em `revision` 0. Cada revisão é exatamente a anterior + 1. Pular revisão é rejeitado.

Ciclo do stream:

```
Active --STREAM_ENDED-----> Ended
Active --STREAM_FAILED----> Failed(reason)
Active --CANCEL(stream)---> Cancelled
Active --sem atividade----> TimedOut   (speech.stream_idle_timeout_ms)
```

Após estado terminal, eventos novos são rejeitados. Reenvio idêntico do último evento é `Duplicate`.

### Ordenação e resultados (`Acceptance`)

| Resultado | Quando | Efeito |
|---|---|---|
| `Applied` | sequência esperada e conteúdo válido | aplicado |
| `Duplicate` | mesma sequência e mesmo conteúdo registrados | ignorado |
| `Stale` | sequência anterior, fora da janela de duplicatas | ignorado |
| `Gap { expected, got }` | faltam sequências | não aplicado. Não há sinal de volta ao produtor nesta versão; a lacuna fica no monitor (`speech_gap`). A atividade do produtor conta para o timeout. |
| `Rejected(motivo)` | conteúdo ou estado inválido | sequência consumida; motivo registrado |

Primeiro evento de um stream recusado por conteúdo (ex.: revisão inválida): a sequência é consumida, o stream permanece `OPEN` no banco e ativo na memória. `REJECTED` no banco só é gravado quando o stream não entra na memória (recusa por capacidade). Memória e banco sempre concordam sobre `ativo` ou `terminal`.

Janela de duplicatas: últimas 64 sequências por stream. Comparação exata do conteúdo canônico (sem hash). Mesma sequência com conteúdo diferente é `Rejected`.

Identidade do stream: `(session_id, stream_id)`. O mesmo `stream_id` em outra sessão é outro stream.

Aposentadoria (permanente no processo):
- Stream encerrado, cancelado, expirado ou removido por capacidade fica aposentado. Nunca é reaberto, mesmo que o estado tenha sido removido.
- `CANCEL` de stream desconhecido aposenta o id: o stream não pode começar depois.
- Custo: um registro de 16 bytes + sessão por stream da vida do processo. Para uso pessoal, estimado em centenas de KB por ano. Não é limitado.

Limites:
- `speech.max_streams` (padrão 16). Ao atingir o limite, remove-se o terminal menos recente. Se todos estiverem ativos, o novo é rejeitado.
- 256 segmentos por stream. Acima disso, `Rejected`.

## 2. Intent Engine (`INTENT_DETECTED`)

`payload` é `InterpretationPayload`.

| Campo | Tipo | Regra |
|---|---|---|
| `interpretation_id` | UUID v4 | Identidade da interpretação. |
| `stream_id`, `segment_id` | UUID v4 | Origem no STT. |
| `source_revision` | u32 | Revisão do segmento usada. |
| `revision` | u32 | Revisão desta interpretação. Começa em 0; +1 por atualização. |
| `state` | string | `PROVISIONAL`, `STABLE`, `NEEDS_CLARIFICATION`, `NO_INTENT`, `CANCELLED`. |
| `engine` | `{name, version}` | Quem interpretou. Obrigatório para rastreabilidade. |
| `intent` | `{name, confidence?}` | `name` é capacidade em MAIÚSCULAS (3–64 caracteres). `confidence` só se o engine fornecer. |
| `entities` | lista (máx. 32) | `kind`, `text`, `start`, `end` (posições em caracteres). |
| `parameters` | mapa (máx. 32) | Chave `snake_case`. Valor `bool`, `i64` ou texto. |
| `missing` | lista (máx. 32) | Parâmetros obrigatórios ainda ausentes. |
| `ambiguities` | lista (máx. 32) | `kind` e 2 a 8 `options`. |
| `context_refs` | lista (máx. 16) | Referências opacas a contexto de sessão. |

Consistência estado × conteúdo:

- `STABLE`: exige `intent`, sem `missing` e sem `ambiguities`.
- `NEEDS_CLARIFICATION`: exige `missing` ou `ambiguities`.
- `NO_INTENT` e `CANCELLED`: não têm `intent`.
- `PROVISIONAL`: sem restrição adicional.

Revisões (`InterpretationTracker`): mesma regra de ordem do STT. `source_revision` não pode regredir. Stream e segmento são fixados na primeira revisão; trocar é `Rejected`. Chave: `(session_id, interpretation_id)`. Interpretação `CANCELLED` rejeita revisões seguintes.

## 3. Prontidão para prosseguir (`ReadinessGate`)

Entrada: interpretação, `SourceSnapshot` (stream ativo? segmento confirmado? revisão atual), risco da capacidade e confirmação de política.

Decisão:

| Decisão | Significado |
|---|---|
| `Admit(chave)` | pode ser oferecida ao Planner/Policy. Não é autorização. |
| `Duplicate(chave)` | mesma chave já admitida para o segmento. Nada a fazer. |
| `Supersede { previous, next }` | nova chave substitui a admitida. Quem executou a anterior deve cancelá-la. |
| `NotReady(motivo)` | não admitida; ver lista abaixo. |

Motivos de `NotReady`: `PROVISIONAL`, `NEEDS_CLARIFICATION`, `NO_INTENT`, `CANCELLED`, `STREAM_INACTIVE`, `SOURCE_UNCONFIRMED`, `SOURCE_REVISED`, `MISSING_PARAMETERS`, `BELOW_MIN_CONFIDENCE`, `CONFIRMATION_REQUIRED`.

Regras:

1. Só `STABLE` pode ser admitido.
2. O segmento de origem precisa estar `CONFIRMED`. Parcial nunca admite.
3. A revisão do segmento precisa ser igual a `source_revision`. Senão, a interpretação está defasada.
4. Sem `speech.min_confidence` configurado, nenhuma confiança é exigida. Com ele configurado, engine sem confiança não passa.
5. Risco `SENSITIVE` ou `IRREVERSIBLE` exige `policy_confirmation = true`.
6. Chave de execução = `segmento | intenção | parâmetros canônicos`. Mesma chave não é admitida duas vezes. Nova intenção no mesmo segmento gera `Supersede`.
7. Quando o stream termina, cancela ou expira, os registros de **interpretação** são liberados. As **admissões** permanecem: nenhuma execução é repetida no mesmo processo, mesmo com reenvio ou reabertura.

Quem fornece o risco e a confirmação é o Capability Registry e o Policy Engine, que ainda não existem. Hoje o monitor usa `RiskClass::Read` e `policy_confirmation = false`, e não executa nada.

Limite de confiança: sem valor padrão. Deve vir de medição (baseline).

## 4. Cancelamento

- `CANCEL` com `payload.stream_id` cancela o stream e marca as interpretações dele como `CANCELLED`. Admissões seguintes são bloqueadas por `NotReady(STREAM_INACTIVE)`.
- `CANCEL` com `correlation_id` é alvo de requisição. Não há tarefas no núcleo, então só é registrado.
- **Transições terminais gravadas antes da memória.** `ENDED` e `FAILED` (evento `STREAM_ENDED`/`STREAM_FAILED`) e `TIMED_OUT` (expiração) só valem em memória depois que o SQLite confirma `close_stream`. Se a gravação falha: a memória fica como estava, a sequência do evento não é consumida (o produtor pode reenviar o mesmo evento) e a expiração tenta de novo no ciclo seguinte. Não existe estado terminal só na memória. Efeito colateral: enquanto a gravação falha, o log de expiração repete a cada 250 ms.
- Como consequência, um `CANCEL` depois de falha de gravação de transição terminal age sobre um stream que o banco ainda marca `OPEN`, e é um cancelamento válido, não um no-op indevido. Após `ENDED`/`FAILED`/`TIMED_OUT` gravados, `CANCEL` é no-op.
- Semântica de `CANCEL` por estado (decisão aprovada). A decisão é tomada dentro da transação `IMMEDIATE` do SQLite (`CancelOutcome`):

| Estado atual | Resultado | Efeito durável |
|---|---|---|
| `OPEN` | `CANCELLED_OPEN` | stream → `CANCELLED`; interpretações → `cancelled = 1`; admissões `ADMITTED` → `CANCELLED`. Chaves nunca são liberadas |
| `ENDED` | `ALREADY_TERMINAL` | no-op completo |
| `CANCELLED` | `ALREADY_TERMINAL` | no-op idempotente |
| `FAILED` | `ALREADY_TERMINAL` | no-op, estado preservado |
| `TIMED_OUT` | `ALREADY_TERMINAL` | no-op, estado preservado |
| `INTERRUPTED`, `REJECTED` | `ALREADY_TERMINAL` | no-op (estados terminais; não reabrem) |
| desconhecido | `TOMBSTONED` | tombstone `CANCELLED`; o id não abre, nem após reinício |
| falha de armazenamento | erro | rollback total; memória não muda |

Em `ALREADY_TERMINAL`, nem a memória do stream nem as interpretações são tocadas.
- **Sem confirmação ao produtor.** O protocolo não envia resposta a `CANCEL` (ver `KAVO-PROTOCOL.md`). Uma falha de gravação aparece só em log (`cancelamento não gravado; nada foi alterado`) e em `store_errors`. O produtor não recebe erro e não pode concluir sucesso ou falha a partir do protocolo. Enquanto não houver resposta, o efeito real do cancelamento deve ser verificado por consulta de estado no banco ou nos logs.

## 5. Monitor de contratos

`ContractMonitor` é um assinante do barramento. Não é um componente de negócio:
- aplica `StreamTracker`, `InterpretationTracker` e `ReadinessGate`;
- registra contadores e logs sem conteúdo;
- drena a fila no encerramento;
- expira streams ociosos a cada 250 ms.

## 6. Fora de escopo nesta fase

- Mecanismo real de STT (captura, modelo, streaming).
- Mecanismo real de NLU/JEV.
- Capability Registry, Discovery, Planner, Task Manager, Policy Engine, Execution Engine, Feedback e Verification. Os contratos citam esses componentes, mas nenhum foi implementado.
- Fast Command Engine e LLM: não há decisão aprovada sobre o mecanismo de intenção.

## 7. Logs e mensagens de erro

Ver §10 (regras atuais de logs e erros).

## 8. Lacunas e recuperação do produtor (Phase 4.2 final)

- O monitor detecta lacuna (`Gap`), mas não há sinal de volta ao produtor. Não há NACK nesta versão (decisão pendente).
- O produtor sabe de falha só quando recebe `ERROR`. Hoje isso acontece em payload inválido e em falha de entrega garantida (`[DELIVERY_FAILED]`, quando o monitor não aceita no prazo).
- Recuperação recomendada, sem NACK: reenviar o mesmo evento com a mesma `sequence` e o mesmo conteúdo. Reenvio idêntico é `Duplicate`, e reenvio de evento novo é `Applied`. Não reenvie com outro conteúdo sob a mesma `sequence` (vira `Rejected`).
- Se uma lacuna não tiver `ERROR` (perda sem resposta), o produtor não sabe. Limitação aceita até decisão sobre NACK ou consulta de estado.

## 9. Persistência durável (D-31, implementada)

Estado durável em SQLite (`src/store.rs`, D-010). Arquivo: `store.path` / `KAVO_STORE_PATH`; padrão `$XDG_DATA_HOME/kavo/kavo.db`. Modo 0600.

Esquema v1 (`meta.schema_version = 1`; versão desconhecida = recusa de abrir):

| Tabela | Chave de unicidade | Conteúdo |
|---|---|---|
| `streams` | (`session_id`, `stream_id`) | status (`OPEN`, `ENDED`, `CANCELLED`, `FAILED`, `TIMED_OUT`, `INTERRUPTED`, `REJECTED`) |
| `interpretations` | (`session_id`, `interpretation_id`) | stream, segmento, revisão, `source_revision`, digest do conteúdo, `cancelled` |
| `admission_keys` | (`session_id`, `execution_key`) | chave admitida, segmento, stream, interpretação, intenção. Nunca apagada |
| `segment_admission` | (`session_id`, `segment_id`) | ponteiro para a chave atual, status (`ADMITTED`, `CANCELLED`) |

Dados gravados: ids, status, revisões, digests SHA-256 de chaves e conteúdos e o nome da capacidade (ex.: `OPEN_APPLICATION`). Não são gravados texto transcrito, valores de parâmetro nem áudio.

Semântica de admissão:
1. Interpretação estável sobre texto confirmado, passando pelo gate → `Decision::Admit`/`Supersede` (ainda não é admissão).
2. Transação `IMMEDIATE`: insere a chave em `admission_keys` (única por sessão, para sempre), troca o ponteiro do segmento e grava a interpretação. `COMMIT` com `synchronous=FULL`.
3. Só depois do `COMMIT`, o monitor registra `ADMIT` e atualiza a memória. Chave já existente → `AlreadyAdmitted`, nada gravado.
4. Falha de gravação → `NOT_READY(STORE_UNAVAILABLE)`. Nada é admitido.

Supersede: a chave anterior permanece consumida. Se a intenção antiga voltar, é `AlreadyAdmitted`, nunca reexecutada.

Semântica de cancelamento: `CANCEL` de stream grava, em uma transação, o stream como `CANCELLED`, as interpretações como `cancelled` e as admissões do stream como `CANCELLED`. As chaves continuam consumidas. Um id desconhecido cancelado vira aposentado (`CANCELLED` sem `OPEN`). Gravar vem antes da memória: se a gravação do cancelamento falhar, nada muda em memória e a falha é registrada. Abertura de stream e tombstone usam `ON CONFLICT(session_id, stream_id) DO NOTHING` e confirmam o efeito pela linha existente: constraint inesperada, trigger `RAISE(IGNORE)` ou inserção sem efeito são erro, nunca duplicidade.

Identidade no reinício:
- Todo stream já gravado é aposentado. Um stream que estava `OPEN` no reinício vira `INTERRUPTED` e nunca é retomado. O produtor deve iniciar um stream novo com `stream_id` novo.
- Revisões antigas são rejeitadas (`Stale`) porque as revisões estão no banco. A gravação de interpretação é monotônica no SQLite (M1): revisão menor não altera a linha; revisão igual só é aceita com o mesmo conteúdo (reenvio idêntico, `Unchanged`); revisão igual com conteúdo diferente, mudança de stream/segmento, regressão de `source_revision` ou revisão após cancelamento é `Conflict`. Nenhuma dessas altera o digest durável, mesmo depois de reinício ou de a memória ter sido esquecida.
- Admissões são restauradas. Um reenvio da mesma chave é recusado como `AlreadyAdmitted` ou `Duplicate`, sem nova admissão.

Falhas:
- Banco corrompido, esquema desconhecido ou tabela ausente: o núcleo não inicia (fail-closed).
- Falha de gravação em tempo de execução: nenhuma admissão. O núcleo continua aceitando eventos.
- Interrupção no meio de gravação: transação não confirmada não existe depois do reinício.

Concorrência: escritores concorrentes são serializados por `BEGIN IMMEDIATE`. Uma chave é admitida no máximo uma vez, mesmo entre conexões diferentes (testado com 12 threads).

Retenção: nenhuma. Admissões são imutáveis e nada é apagado automaticamente. Estimativa: uma linha de ~200 bytes por admissão, sem limite. Política de retenção pendente (D-31).

Limitações restantes:
- Após reinício, um stream interrompido não é recuperado. Eventos já enviados não são reprocessados.
- Capacidade de streams ativos (`speech.max_streams`, padrão 16) limita fluxos simultâneos. Streams não encerrados ocupam vagas até o timeout.
- Ainda não há Execution Engine: `ADMIT` é só uma decisão durável, não uma execução.
- Sem NACK: lacunas continuam sem sinal ao produtor (D-35).

## 10. Logs e mensagens de erro

- Logs e erros: ver regras abaixo (B1, B2).
- Chave de admissão: só `PRIMARY KEY`/`UNIQUE` contam como consumo (`AlreadyConsumed`). Outra constraint, ou inserção sem efeito (ex.: trigger `RAISE(IGNORE)`), é erro de armazenamento, nunca duplicidade (B2).

- Logs registram rótulos (`APPLIED`, `GAP`, `REJECTED`, `ADMIT`, ...), contadores, ids e impressão curta da chave (12 caracteres do digest). Não registram texto transcrito, nome de capacidade nem valor de parâmetro.
- Nenhum valor enviado pelo cliente aparece em logs ou em `ERROR` (B1). Erros de serde são descritos só por categoria e coluna. Tipo desconhecido é recusado sem ecoar o nome. Versão recusada não é ecoada.

## 11. Riscos conhecidos da persistência síncrona (não corrigidos)

- O monitor roda numa única tarefa e grava de forma síncrona. Cada `COMMIT` faz `fsync` (WAL + `synchronous=FULL`). Escritas por evento: abertura de stream, transição terminal, interpretação, admissão e cancelamento.
- Com disco lento, o monitor fica bloqueado. A fila do barramento (256) enche e a entrega garantida falha após `ipc.request_timeout_ms` (padrão 2 s). O produtor recebe `[DELIVERY_FAILED]`. Nenhuma admissão é perdida, mas eventos são recusados.
- `busy_timeout` = 5 s. Um segundo escritor (outro processo) pode bloquear a tarefa por até esse tempo.
- `quick_check` roda a cada inicialização. Em banco grande, o início pode demorar.
- Não medido. A correção natural é mover as gravações para `spawn_blocking` ou para um escritor dedicado com fila, mantendo a ordem. Pendente de decisão.

## 12. Garantias e limites da admissão (M2)

O que está implementado:
- A chave de admissão é consumida de forma durável e única por `(session_id, execution_key)`. A unicidade é imposta pelo SQLite (`PRIMARY KEY`), dentro de transação `BEGIN IMMEDIATE`.
- `ADMIT` só é registrado depois do `COMMIT`. Falha de gravação não admite.
- Revisões de interpretação são monotônicas na durável: revisão menor, ou igual com conteúdo diferente, não altera a linha gravada (ver §9).

O que NÃO está implementado e não deve ser tomado como garantia:
- Não há Execution Engine. `ADMIT` é uma decisão durável, não uma ação executada.
- Não há outbox nem verificação do efeito da ação. Não há como saber se algo foi feito no mundo real.
- A semântica atual é de **admissão at-most-once**: uma chave é consumida antes de qualquer execução. Se um executor futuro cair depois do `COMMIT`, a ação não é retentada automaticamente.
- Execução exatamente uma vez, execução at-least-once e a recuperação após falha do executor não estão garantidas. Essa semântica deve ser decidida antes da implementação do executor.
