# Kavo Protocol — especificação v1.2-draft (com ADMISSION_ACK, D-58)

Status: **rascunho provisório** (aprovado: framing NDJSON, TOML+Serde, extensão 1.2 para sessões e cancelamento de requisições — D-09 no Notion; pendente: restante das decisões em `DECISIONS.md`).

Versão 1.1-draft adiciona: campo `payload` opcional, tipos `STT_RESULT` e `INTENT_DETECTED`, e alvo de `CANCEL` por `stream_id`. Mensagens `1.0` continuam válidas.

O Kavo Protocol é o protocolo próprio do projeto. O Hermes JSON (OVOS/Rhasspy) é apenas uma referência externa de formato. Não há compatibilidade garantida com Hermes; qualquer interoperabilidade futura será tratada como adaptador explícito.

## 1. Transporte

- Unix domain socket, `SOCK_STREAM`. Caminho padrão: `$XDG_RUNTIME_DIR/kavo/kavo.sock`, modo `0600`.
- Framing: NDJSON. Uma mensagem = um objeto JSON UTF-8 em uma linha terminada por `\n`. Nenhum `\n` dentro da mensagem.
- Tamanho máximo por linha: `ipc.max_frame_bytes` (padrão 65536). Linha maior: o receptor responde `ERROR` e fecha a conexão.
- Prazos: `ipc.request_timeout_ms` por leitura/escrita; `ipc.idle_timeout_ms` (padrão 30000) sem nenhuma mensagem fecha a conexão.
- Limite de 64 conexões simultâneas; excedente é recusado com log.
- Socket só é criado em diretório pai privado (modo 0700).
- Campos desconhecidos em mensagens são ignorados (compatibilidade para frente em versões 1.x).

## 2. Envelope (versão 1.0)

| Campo | Obrigatório | Tipo | Regra |
|---|---|---|---|
| `type` | sim | string | Tipo da mensagem (seção 3). |
| `schema_version` | sim | string | `1.x`. Major diferente de 1 é rejeitado. |
| `message_id` | sim | string | UUID v4 (versão 4 obrigatória; nil e outras versões são rejeitados). Identifica **esta** mensagem. |
| `correlation_id` | não | string | UUID v4. `message_id` da mensagem à qual esta responde ou que cancela. |
| `session_id` | sim | string | 1–128 caracteres (contados como caracteres Unicode), sem controle. Agrupa mensagens de uma interação. |
| `sender` | sim | string | 1–64 caracteres, sem controle. Componente emissor. |
| `timestamp` | sim | number | Segundos Unix, finito, ≥ 0. |
| `error_code` | só em `ERROR` (obrigatório) | integer | Código da seção 5. Presente em outro tipo é erro. |
| `error_message` | não | string | Texto estável; ver `ERROR-TAXONOMY.md`. |
| `payload` | depende do tipo (1.1) | objeto | Conteúdo tipado. Tipos sem payload o rejeitam (ver seção 3). Schemas: `SPEECH-INTENT-CONTRACTS.md`. |

## 3. Tipos

### 3.1 Implementados em 1.1-draft

| Tipo | Entrega | Uso | Correlação |
|---|---|---|---|
| `HEARTBEAT` | melhor esforço | Sondagem de vida. Resposta é `HEARTBEAT`. | resposta com `correlation_id` = `message_id` da sondagem |
| `ERROR` | garantida | Erro de uma requisição ou de uma linha inválida. | `correlation_id` quando houver requisição identificável |
| `CANCEL` | garantida | Pede cancelamento de uma requisição (`correlation_id` = `message_id` do alvo) ou de um stream de STT (`payload.stream_id`). Exige um dos dois. | ver coluna ao lado |
| `STT_RESULT` (1.1) | garantida | Evento de STT streaming: transcrição parcial/confirmada, início/fim de fala, fim/falha do stream. Payload obrigatório. | ordenação por `payload.sequence` dentro do stream |
| `INTENT_DETECTED` (1.1) | garantida | Interpretação do Intent Engine, com revisões. Payload obrigatório. Não concede permissão. | `payload.interpretation_id` + `revision` |
| `SESSION_CONTROL` (1.2) | garantida | Pedido de sessão (`OPEN`, `CLOSE`, `STATUS`) ou de requisição (`REQUEST_BEGIN`, `REQUEST_END`). Resposta obrigatória: `CONTROL_RESULT`. | `message_id` da mensagem = `request_id` (em `REQUEST_BEGIN`) |
| `CONTROL_RESULT` (1.2) | garantida | Resultado de qualquer operação de controle e de `CANCEL` 1.2. Só o núcleo envia. | `correlation_id` = `message_id` do pedido |
| `ADMISSION_ACK` (1.2) | garantida | Confirmação de admissão de `STT_RESULT` e `INTENT_DETECTED` 1.2 (seção 11). Só o núcleo envia. | `correlation_id` = `message_id` do evento |

### 3.2 Reservados (aprovados na decisão 6.6, ainda não implementados)

`WAKE_WORD_DETECTED`, `COMMAND_EXECUTED`, `MEMORY_UPDATE`, `SYSTEM_STATUS`, `ASSISTANT_SPEAK`. (`STT_RESULT` e `INTENT_DETECTED` foram implementados em 1.1.)

Recebê-los hoje gera `ERROR` com classe `UNSUPPORTED_MESSAGE_TYPE` e mensagem "reservado, ainda não implementado". Tipo fora de implementados e reservados gera a mesma classe com "desconhecido".

### 3.3 Não aprovados (aparecem só em 10-CONTRATOS e 03-PROTOCOLOS)

`VOICE_INPUT`, `STT_PARTIAL` / `STT_STREAM_PARTIAL`, `MEMORY_SEARCH_RESULT`, `COMMAND_STARTED`, `TTS_COMPLETED`, `CONNECTION_CLOSED`, `CONNECTION_ESTABLISHED`, `USER_CONFIRMATION_NEEDED`, `STATUS_UPDATE`, `ERROR_DISPLAY`. Não reservados e não implementados. Dependem de decisão.

## 4. Identificadores

| Identificador | Escopo | Onde existe |
|---|---|---|
| `message_id` | uma mensagem no fio | envelope, todo tipo |
| `correlation_id` | liga resposta/erro/cancelamento à mensagem de origem | envelope, quando aplicável |
| `session_id` | agrupa uma interação do usuário (várias mensagens) | envelope |
| `task_id` | unidade de trabalho interna do núcleo (ex.: uma execução de comando) | **não** é transmitido em 1.1-draft; reservado |
| `component` / `sender` | origem do componente | envelope (`sender`) e spans de log |

Regra: `correlation_id` nunca identifica sessão nem tarefa. Requisição = mensagem cujo `message_id` é aguardado por resposta.

Mapeamento interno (A0, sem mudança de fio; ver `DECISIONS.md` D-41 a D-44):

| Conceito | Identificador no fio | Dono |
|---|---|---|
| sessão de interação | `session_id` | `session::SessionManager` (só memória) |
| mensagem | `message_id` | envelope |
| requisição lógica | `message_id` da mensagem aguardada (`request_id` no código) | `SessionManager::begin_request` |
| stream de fala | `payload.stream_id` (identidade `(session_id, stream_id)`, D-33) | `StreamTracker` e SQLite |
| tarefa | reservada (não transmitida) | ainda não existe |

`CANCEL` com `correlation_id` cancela a requisição; com `payload.stream_id` cancela o stream. Nenhum dos dois encerra sessão.

`cancel_id` (doc 03) foi removido: o alvo do `CANCEL` é o `correlation_id` (decisão proposta, pendente).

## 5. Erros

- Erros usam `ERROR` com `error_code`. Em 1.1-draft todo erro de protocolo ou transporte usa `1100` (provisório, aprovado pelo usuário).
- O código de classe interna (`INVALID_MESSAGE`, `FRAME_TOO_LARGE`, ...) vai como prefixo em `error_message`: `[INVALID_MESSAGE] ...`.
- Taxonomia completa e códigos propostos: `ERROR-TAXONOMY.md`.

## 6. Entrega

- **Garantida** (`ERROR`, `CANCEL`): enviada a todos os assinantes em paralelo, com um prazo único. Falha explicitamente se não houver assinante, se algum estiver fechado ou se algum não aceitar no prazo. Falha parcial não tem rollback; o erro informa quantos entregaram, e reenvio pode duplicar (deduplicação por `message_id` é do consumidor).
- **Melhor esforço** (`HEARTBEAT`): entrega se houver espaço no assinante; senão, é descartada e contada.
- Assinantes sem espaço em filas de melhor esforço não bloqueiam os demais.
- Em 1.1-draft nenhum componente assina o barramento; por isso `ERROR`/`CANCEL` recebidos do cliente recebem `DELIVERY_FAILED`. Isso é esperado até o primeiro consumidor existir.

## 7. Reconciliação 03-PROTOCOLOS × 10-CONTRATOS

| Assunto | 03-PROTOCOLOS | 10-CONTRATOS | Resolução em 1.1-draft |
|---|---|---|---|
| Campo de tipo | `type` | `type` | `type` |
| Nome do protocolo | "Hermes JSON" | "Hermes JSON" | **Kavo Protocol**; Hermes é referência |
| Campos de texto/intent/slots | opcionais | opcionais | **removidos** do envelope 1.0; voltam em 1.1 com o componente que os usa |
| `transport_version` | presente | presente | **removido**; `schema_version` basta |
| `sender` | opcional | obrigatório no exemplo | **obrigatório** (auditoria) |
| Identificador de mensagem | ausente | ausente | **`message_id` novo** |
| `correlation_id` | requisição | requisição/resposta | resposta aponta para `message_id` da requisição |
| `cancel_id` | campo próprio | campo próprio | **removido**; usa `correlation_id` |
| Tipo `COMMAND_EXECUTED` | com typo `COMMAND_EXECuted` | ok | reservado, nome canônico |
| Código de cancelamento | `"CANCELLED"` (string) | — | erro é `1100` com prefixo `[CANCELLED]` |
| Tópicos `/kavo/...` | Message Bus | Message Bus | fora do escopo; barramento interno |
| `timeout_seconds` | presente | presente | **removido** do envelope; prazo é do receptor |
| Exemplos de código | `Instant::now().await` etc. | código | não compilam; não usados |

## 8. Compatibilidade

Versão 1.1-draft. Mudanças que quebram: incremento de major. Campos opcionais novos: minor (1.1). Enquanto `DECISIONS.md` marcar a versão como provisória, a especificação pode mudar sem aviso.

## 9. Versão 1.1-draft (Phase 4.2)

- `schema_version` aceito: `1.x`. Envelope 1.0 sem `payload` continua válido.
- Tipos implementados: `HEARTBEAT`, `ERROR`, `CANCEL`, `STT_RESULT`, `INTENT_DETECTED`.
- Reservados: `WAKE_WORD_DETECTED`, `COMMAND_EXECUTED`, `MEMORY_UPDATE`, `SYSTEM_STATUS`, `ASSISTANT_SPEAK`.
- `payload`:
  - `STT_RESULT`: obrigatório. Schema em `SPEECH-INTENT-CONTRACTS.md`, seção 1.
  - `INTENT_DETECTED`: obrigatório. Schema em `SPEECH-INTENT-CONTRACTS.md`, seção 2.
  - `CANCEL`: opcional. `{"stream_id": UUID}`. Sem payload, `correlation_id` é obrigatório.
  - `SESSION_CONTROL` (1.2): obrigatório; ver seção 10.
  - `CONTROL_RESULT` (1.2): obrigatório; ver seção 10.
  - `HEARTBEAT` e `ERROR`: não aceitam payload.
- `error_code` só é permitido em `ERROR` e deve estar na taxonomia aprovada: `1001`–`1007` e `1100` (ver `error_code::KNOWN`). Códigos propostos 2xxx/3xxx não são aceitos até aprovação.
- Tipos 1.1 (`STT_RESULT`, `INTENT_DETECTED`) e qualquer `payload` exigem `schema_version` 1.1. Em 1.0 são recusados com `UNSUPPORTED_VERSION`.
- Validação de payload é feita na entrada (IPC) e antes do envio. Payload inválido recebe `ERROR` com `[INVALID_MESSAGE]`.
- Identificadores tipados no payload: `stream_id`, `segment_id`, `interpretation_id` são UUID v4.
- Entrega: `STT_RESULT` e `INTENT_DETECTED` são garantidos, porque perder evento de STT altera a sequência.

## 10. Versão 1.2-draft (sessões e cancelamento de requisições, D-09)

Aprovada no Notion em 31/10/2026 (D-09). Esta seção descreve o contrato implementado; a versão detalhada de cada decisão local está em `DECISIONS.md` (D-53 a D-61).

### 10.1 Compatibilidade

- `schema_version` aceita `1.x`. Tipos e payloads de 1.2 exigem `schema_version` 1.2; mensagem 1.1 com eles é recusada com `UNSUPPORTED_VERSION`.
- Mensagens 1.1 continuam com o comportamento legado:
  - `STT_RESULT` e `INTENT_DETECTED` em sessão desconhecida abrem a sessão implicitamente.
  - `CANCEL` 1.1 com `payload.stream_id` cancela stream, como antes.
  - `CANCEL` 1.1 com só `correlation_id` não recebe resposta e não cancela requisição (não há requisições registradas em 1.1).
- Mensagens 1.2 em sessão desconhecida ou encerrada são recusadas (`REJECTED` em `CONTROL_RESULT`, ou nada gravado para eventos de STT).
- Sessão encerrada nunca reabre, nem por mensagem 1.1.
- `KavoMessage::new` continua criando `schema_version` 1.1 (padrão de compatibilidade). Clientes que usam 1.2 definem a versão explicitamente.
- `CONTROL_RESULT` recebido como entrada do cliente é recusado com `ERROR`.

### 10.2 `SESSION_CONTROL` (cliente → núcleo)

`session_id` vem do envelope. O payload tem `op`:

| `op` | Campos | Efeito |
|---|---|---|
| `OPEN` | — | Abre a sessão. |
| `CLOSE` | — | Encerra a sessão; cancela requisições e streams dela (seção 10.4). |
| `STATUS` | — | Consulta o estado, sem alterar nada. |
| `REQUEST_BEGIN` | `kind: "STT_STREAM"`, `stream_id: UUID v4` | Registra uma requisição. Seu `request_id` é o `message_id` desta mensagem. |
| `REQUEST_END` | `request_id: UUID v4` | Conclui uma requisição em andamento. |

Exemplo:

```json
{"type":"SESSION_CONTROL","schema_version":"1.2","message_id":"6f1c9e2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b","session_id":"sessao-1","sender":"cliente","timestamp":1761900000.0,"payload":{"op":"OPEN"}}
```

### 10.3 `CONTROL_RESULT` (núcleo → cliente)

| Campo do payload | Valores |
|---|---|
| `op` | `OPEN`, `CLOSE`, `STATUS`, `REQUEST_BEGIN`, `REQUEST_END`, `REQUEST_CANCEL`, `STREAM_CANCEL` |
| `outcome` | `APPLIED` (estado mudou), `NOOP` (nenhuma mudança: repetição idempotente ou consulta), `REJECTED` (nada foi alterado) |
| `reason` | Só em `REJECTED`: `SESSION_UNKNOWN`, `SESSION_CLOSED`, `REQUEST_UNKNOWN`, `REQUEST_DUPLICATE`, `REQUEST_FINISHED`, `REQUEST_CANCELLED`, `STREAM_BOUND`, `INVALID_PAYLOAD`, `STORE_UNAVAILABLE` |
| `state` | Estado da sessão: `ACTIVE`, `CLOSED`, `UNKNOWN` (nunca vista nesta execução) |
| `cancelled_requests` | Requisições canceladas pela operação (só `CLOSE`) |
| `cancelled_streams` | Streams cancelados por propagação |

Exemplo de resposta com `correlation_id` = `message_id` do pedido:

```json
{"type":"CONTROL_RESULT","schema_version":"1.2","message_id":"b7d2a1c0-1111-4aaa-8bbb-222233334444","correlation_id":"6f1c9e2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b","session_id":"sessao-1","sender":"kavo-core","timestamp":1761900000.1,"payload":{"op":"OPEN","outcome":"APPLIED","state":"ACTIVE","cancelled_requests":0,"cancelled_streams":0}}
```

### 10.4 Cancelamento

| Entrada | Alvo | Escopo | Resposta |
|---|---|---|---|
| `CANCEL` com `correlation_id`, sem payload | requisição `correlation_id` | só dentro da sessão da mensagem | `CONTROL_RESULT` `REQUEST_CANCEL` |
| `CANCEL` com `payload.stream_id` | stream | sessão da mensagem | `CONTROL_RESULT` `STREAM_CANCEL` (1.2); nenhuma (1.1) |
| `SESSION_CONTROL` `CLOSE` | todas as requisições e streams da sessão | só a sessão | `CONTROL_RESULT` `CLOSE` com contadores |

Regras:

- Cancelar requisição cancela o stream vinculado a ela (se houver), com gravação durável antes da mudança em memória. Um stream que ainda não começou fica marcado como cancelado: nunca abre depois.
- Cancelar stream **não** conclui nem cancela a requisição vinculada. São escopos distintos.
- Um stream tem no máximo uma requisição em andamento por sessão (`STREAM_BOUND`).
- Requisição já concluída: `REJECTED REQUEST_FINISHED`. Já cancelada: `NOOP`. Inexistente: `REJECTED REQUEST_UNKNOWN`.
- Corrida conclusão × cancelamento: o monitor processa um pedido por vez; a ordem de chegada decide e o perdedor recebe `REJECTED`.
- Falha de gravação durante cancelamento ou encerramento: `REJECTED STORE_UNAVAILABLE`, nada muda em memória, a operação pode ser repetida.

### 10.5 Erros

Entrada inválida recebe `ERROR` (código 1100, `[INVALID_MESSAGE]` ou classe equivalente), como nos tipos anteriores. Erro de transporte não encerra o núcleo.

### 10.6 Não incluído

- `task_id` e mensagens de tarefa: reservados.
- Expiração por inatividade (D-45) e retenção de admissões (D-36): pendentes.
- Recuperação de sessões após reinício: sessões são só em memória (D-43). Streams interrompidos viram `INTERRUPTED`.

## 11. ADMISSION_ACK: confirmação de admissão de eventos STT e INTENT (D-58)

Aprovada no Notion (D-58, 31/10/2026). Eventos `STT_RESULT` e `INTENT_DETECTED` com `schema_version` 1.2 recebem exatamente um `ADMISSION_ACK` na mesma conexão. Clientes 1.1 não recebem ACK.

### 11.1 Semântica

- O ACK **não** significa conclusão de STT, interpretação, planejamento, execução ou verificação. Não há nova resposta de conclusão nesta etapa.
- Ponto de admissão, definido aqui: o ACK sai depois de (1) gate de sessão, (2) validação do estado do tracker (sequência, revisão, conteúdo) e (3) gravação durável exigida por aquele evento:
  - `STT_RESULT`: abertura de stream (`sequence` 1) e transição terminal (`StreamEnded`, `StreamFailed`) gravadas no SQLite antes do ACK. Parciais e confirmados não exigem gravação.
  - `INTENT_DETECTED`: `upsert` da revisão gravado antes do ACK.
- A avaliação de prontidão para execução (Readiness Gate, Policy Engine) ocorre depois do ACK e não altera seu status. Falha nessa etapa não desfaz a admissão da interpretação.
- Nenhuma admissão de evento autoriza execução. Parciais de STT nunca produzem admissão de execução.

| `status` | Significado | Cliente |
|---|---|---|
| `ACCEPTED` | Validado e admitido no ponto acima. | Considera recebido e admitido. |
| `DUPLICATE` | Mesma identidade e mesmo conteúdo já admitidos. Nada foi reaplicado. | Considera recebido. Não repetir como novo. |
| `REJECTED` | Não admitido por motivo permanente (`reason`). Nada foi alterado. | Não repetir sem mudar o evento ou o estado. |
| `NOT_READY` | Não admitido por indisponibilidade transitória (`reason`). | Repetir com a mesma identidade (`message_id` e conteúdo iguais). |

### 11.2 Campos

| Campo | Regra |
|---|---|
| `event` | `STT_RESULT` ou `INTENT_DETECTED`. |
| `status` | Um dos quatro valores acima. |
| `reason` | Só em `REJECTED` e `NOT_READY`. |
| `stream_id`, `sequence` | Evento STT. |
| `interpretation_id`, `revision` | Evento INTENT. |

O payload não contém texto transcrito, texto reconhecido, parâmetros, nem conteúdo do evento. O `correlation_id` do envelope é o `message_id` do evento original.

### 11.3 Motivos (`reason`)

| `reason` | Status | Quando |
|---|---|---|
| `SESSION_UNKNOWN` | REJECTED | Sessão nunca aberta nesta execução (só 1.2; 1.1 abre sessão). |
| `SESSION_CLOSED` | REJECTED | Sessão encerrada. |
| `STREAM_CLOSED` | REJECTED | Stream já encerrado, cancelado ou aposentado; evento tardio. Interpretação de stream cancelado. |
| `STREAM_LIMIT` | REJECTED | Limite de streams ativos atingido. |
| `SEQUENCE_INVALID` | REJECTED | Sequência anterior ao último aplicado e fora da janela de duplicatas. |
| `SEQUENCE_GAP` | REJECTED | Há sequências faltando. Reenviar as anteriores antes. |
| `STALE_REVISION` | REJECTED | Revisão de interpretação já superada. |
| `CONFLICT` | REJECTED | Mesma sequência ou revisão com conteúdo diferente. |
| `INVALID_CONTENT` | REJECTED | Conteúdo ou transição inválida para o estado atual (ex.: revisão fora de ordem). |
| `INVALID_PAYLOAD` | REJECTED | Payload não interpretado pelo monitor (o IPC já valida a maioria). |
| `STORE_UNAVAILABLE` | NOT_READY | Falha de gravação durável. Nada foi admitido. |
| `BUSY` | NOT_READY | Fila de admissão cheia (seção 11.5) ou teto de aberturas pendentes (D-79). Nada foi processado. |

Uma recusa por `INVALID_CONTENT` consome a sequência do stream. Repetir a mesma mensagem devolve a mesma recusa, nunca `DUPLICATE`.

### 11.4 Retry e idempotência

- Repetir a mesma mensagem (mesmo `message_id`, mesmo conteúdo) é seguro. Admitido antes: `DUPLICATE`. Nunca admitido (`NOT_READY` ou sem ACK): `ACCEPTED` na primeira admissão.
- Perda do ACK: o cliente repete com a mesma identidade e recebe `DUPLICATE` (ou o ACK original, conforme o caso).
- Falha de gravação não deixa estado parcial: a memória de interpretação é desfeita antes do `NOT_READY`.
- Sem ACK em prazo (`ipc.request_timeout_ms`), o IPC responde `ERROR` com `[TIMEOUT]`. O cliente trata como incerto e repete com a mesma identidade.

### 11.5 Backpressure

- Eventos 1.2 de admissão passam por uma fila limitada entre IPC e monitor: `ipc.admission_queue` (padrão 64, faixa 1–4096; variável `KAVO_IPC_ADMISSION_QUEUE`).
- Fila cheia: resposta `NOT_READY BUSY` imediata, sem bloquear a conexão. O cliente aguarda e repete com a mesma identidade.
- Um cliente tem no máximo um evento em voo: envia, lê o ACK e só então envia o próximo. Por isso o ACK sempre volta na ordem do envio.

### 11.6 Exemplos

Evento aceito:

```json
{"type":"ADMISSION_ACK","schema_version":"1.2","message_id":"c1d2e3f4-0000-4000-8000-000000000001","correlation_id":"6f1c9e2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b","session_id":"sessao-1","sender":"kavo-core","timestamp":1761900000.2,"payload":{"event":"STT_RESULT","status":"ACCEPTED","stream_id":"0d6e3c1a-1111-4222-8333-444455556666","sequence":2}}
```

Fila cheia:

```json
{"type":"ADMISSION_ACK","schema_version":"1.2","message_id":"c1d2e3f4-0000-4000-8000-000000000002","correlation_id":"6f1c9e2a-3b4d-4e5f-8a9b-0c1d2e3f4a5c","session_id":"sessao-1","sender":"kavo-core","timestamp":1761900000.3,"payload":{"event":"STT_RESULT","status":"NOT_READY","reason":"BUSY","stream_id":"0d6e3c1a-1111-4222-8333-444455556666","sequence":3}}
```

### 11.7 Compatibilidade

- Clientes 1.1: nenhum ACK. Sessão desconhecida é aberta implicitamente; sessão encerrada é recusada, como no gate da A1 (D-55).
- `ADMISSION_ACK` enviado pelo cliente é recusado com `ERROR`. `CONTROL_RESULT` continua separado: payloads não são intercambiáveis.
- Medidas de latência e vazão: `docs/AGENT_STATE.md`.

### 11.8 Garantias de idempotência e seus limites (auditoria A1/A1.1)

Demonstradas por `harness admission-faults`, `harness admission`, `tests/admission.rs` e E2E:

- Repetir um evento aceito (mesmo `message_id`, mesmo conteúdo) devolve `DUPLICATE` e não reaplica efeito, enquanto o evento estiver na janela de memória do stream (últimos 64 eventos de STT) ou for a última revisão de uma interpretação.
- Após evicção de stream terminal por capacidade, eventos da janela de 64 continuam reconhecidos (`DUPLICATE`, ou a mesma recusa; conteúdo diferente = `CONFLICT`).
- Após fim de stream, a revisão atual de uma interpretação é reconhecida pela durável (`DUPLICATE`), não por `GAP`.
- Recusa por capacidade: o motivo vem do status durável (`REJECTED` → `STREAM_LIMIT`), então o retry devolve o mesmo motivo.
- Falha de gravação não produz ACK positivo nem recusa permanente indevida: a abertura, o terminal e o upsert de interpretação falham com `NOT_READY` e são repetíveis com a mesma identidade.
- Cancelamento e admissão são serializados no monitor. Aceito antes do cancelamento = `DUPLICATE`; depois = `REJECTED STREAM_CLOSED`.
- Sequência ou revisão recusada por conteúdo devolve a mesma recusa em todas as repetições.
- Prazo sem ACK = `ERROR [TIMEOUT]`, nunca ACK. Envio ao monitor também tem prazo: fila de controle cheia não bloqueia a conexão indefinidamente.

Limites conhecidos (não cobertos por esta etapa):

- **Janela:** `DUPLICATE` para um stream é garantido só dentro dos últimos 64 eventos. Mais antigo que isso, a resposta é `REJECTED SEQUENCE_INVALID`. Não há reaplicação, mas o ACK não é `DUPLICATE`. Interpretação guarda só a última revisão: revisão anterior repetida vira `REJECTED STALE_REVISION` (D-76).
- **Reinício:** sessões são só em memória (D-43). Após reinício, 1.2 de sessão anterior recebe `REJECTED SESSION_UNKNOWN` até a sessão ser reaberta. Depois de reaberta, a repetição de interpretação é `DUPLICATE` (durável).
- **Reinício e terminal:** após reinício, a repetição do evento terminal de um stream já encerrado devolve `REJECTED STREAM_CLOSED`, não `DUPLICATE` (o banco não guarda sequência nem digest do terminal). Corrigir exige mudança de esquema (D-75).
- **Commit ambíguo:** se o SQLite reportar erro depois de confirmar uma abertura de stream, o banco pode ter `OPEN` sem a memória. A repetição devolve `REJECTED STREAM_CLOSED`. Caso raro, não reproduzível por falha injetada no `RAISE(ABORT)`.
- **Aberturas pendentes (`pending_open`):** uma abertura gravada que não pôde ser admitida em memória (falha de gravação da recusa por capacidade) fica pendente até ser admitida, recusada, cancelada ou a sessão ser encerrada. Cancelamento e encerramento limpam a pendência depois de gravar. Há teto de `speech.max_streams`: acima dele, nova abertura recebe `NOT_READY BUSY` sem tocar no banco. Pendências órfãs consumiriam o teto de todas as sessões; por isso a limpeza é obrigatória (D-78, D-79).

