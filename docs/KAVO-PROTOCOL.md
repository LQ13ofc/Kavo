# Kavo Protocol — especificação v1.1-draft

Status: **rascunho provisório** (aprovado: framing NDJSON, TOML+Serde; pendente: restante das decisões em `DECISIONS.md`).

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
  - `HEARTBEAT` e `ERROR`: não aceitam payload.
- `error_code` só é permitido em `ERROR`.
- Validação de payload é feita na entrada (IPC) e antes do envio. Payload inválido recebe `ERROR` com `[INVALID_MESSAGE]`.
- Identificadores tipados no payload: `stream_id`, `segment_id`, `interpretation_id` são UUID v4.
- Entrega: `STT_RESULT` e `INTENT_DETECTED` são garantidos, porque perder evento de STT altera a sequência.
