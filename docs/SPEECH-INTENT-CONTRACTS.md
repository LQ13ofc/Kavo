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

Janela de duplicatas: últimas 64 sequências por stream. Comparação exata do conteúdo canônico (sem hash). Mesma sequência com conteúdo diferente é `Rejected`.

Identidade do stream: `(session_id, stream_id)`. O mesmo `stream_id` em outra sessão é outro stream.

Aposentadoria (permanente no processo):
- Stream encerrado, cancelado, expirado ou removido por capacidade fica aposentado. Nunca é reaberto, mesmo que o estado tenha sido removido.
- `CANCEL` de stream desconhecido aposenta o id: o stream não pode começar depois.
- Custo: um registro de 16 bytes + sessão por stream da vida do processo. Para uso pessoal, estimado em centenas de KB por ano. Não é limitado.

Limites:
- `speech.max_streams` (padrão 16). Ao atingir o limite, remove-se o terminal menos recente. Se todos estiverem ativos, o novo é rejeitado.
- 256 segmentos por stream. Acima disso, `Rejected`.
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
- Cancelamento é idempotente: cancelar stream já encerrado retorna "não alterado".

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

- Logs registram rótulos (`APPLIED`, `GAP`, `REJECTED`, `ADMIT`, ...), contadores, ids e impressões digitais da chave. Não registram texto transcrito, nome de capacidade nem valor de parâmetro.
- Erros de validação dos payloads não ecoam valores enviados pelo produtor.
- Nomes de tipo desconhecido são limitados a 32 caracteres e escapados antes de entrar em log e em `ERROR`.

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

Semântica de cancelamento: `CANCEL` de stream grava, em uma transação, o stream como `CANCELLED`, as interpretações como `cancelled` e as admissões do stream como `CANCELLED`. As chaves continuam consumidas. Um id desconhecido cancelado vira aposentado (`CANCELLED` sem `OPEN`).

Identidade no reinício:
- Todo stream já gravado é aposentado. Um stream que estava `OPEN` no reinício vira `INTERRUPTED` e nunca é retomado. O produtor deve iniciar um stream novo com `stream_id` novo.
- Revisões antigas são rejeitadas (`Stale`) porque as revisões estão no banco.
- Admissões são restauradas. Reenvio nunca executa de novo.

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

- Logs registram rótulos (`APPLIED`, `GAP`, `REJECTED`, `ADMIT`, ...), contadores, ids e impressão curta da chave (12 caracteres do digest). Não registram texto transcrito, nome de capacidade nem valor de parâmetro.
- Erros de validação dos payloads não ecoam valores enviados pelo produtor.
- Nomes de tipo desconhecido são limitados a 32 caracteres e escapados antes de entrar em log e em `ERROR`.

## 11. Riscos conhecidos da persistência síncrona (não corrigidos)

- O monitor roda numa única tarefa e grava de forma síncrona. Cada `COMMIT` faz `fsync` (WAL + `synchronous=FULL`). Escritas por evento: abertura de stream, transição terminal, interpretação, admissão e cancelamento.
- Com disco lento, o monitor fica bloqueado. A fila do barramento (256) enche e a entrega garantida falha após `ipc.request_timeout_ms` (padrão 2 s). O produtor recebe `[DELIVERY_FAILED]`. Nenhuma admissão é perdida, mas eventos são recusados.
- `busy_timeout` = 5 s. Um segundo escritor (outro processo) pode bloquear a tarefa por até esse tempo.
- `quick_check` roda a cada inicialização. Em banco grande, o início pode demorar.
- Não medido. A correção natural é mover as gravações para `spawn_blocking` ou para um escritor dedicado com fila, mantendo a ordem. Pendente de decisão.
