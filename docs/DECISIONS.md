# Decisões do núcleo Kavo

Legenda: **Aprovada** (pelo usuário) · **Provisória** (usada, aguarda revisão) · **Pendente** (não decidida) · **Histórica** (vem da pesquisa, não vigente sem aprovação).

| # | Decisão | Status | Observação |
|---|---|---|---|
| D-01 | Linguagem principal do núcleo: Rust | Aprovada | Python só com justificativa técnica registrada aqui |
| D-02 | Barramento interno em processo, sem Message Bus externo | Aprovada (barramento) / Pendente (externo) | Mensagens garantidas (ERROR, CANCEL) usam filas mpsc por assinante, não `tokio::broadcast` |
| D-03 | Framing NDJSON (uma mensagem por linha) sobre Unix socket | Provisória | Aprovada provisoriamente pelo usuário |
| D-04 | Erro no fio: código 1100 para todo erro não-domínio | Provisória | Taxonomia com classes em `ERROR-TAXONOMY.md` ainda não aprovada |
| D-05 | Identificadores: `message_id`, `correlation_id`, `session_id`, `sender`; `task_id` reservado | Provisória | Remove `cancel_id` (alvo do CANCEL é `correlation_id`) |
| D-06 | Configuração TOML + Serde, overrides `KAVO_*` | Aprovada | Crate `config` da doc não usada |
| D-07 | Protocolo próprio chamado **Kavo Protocol** v1.1-draft; Hermes é referência | Aprovada (nome) / Provisória (versão) | Correção obrigatória do usuário |
| D-08 | Tipos implementados: HEARTBEAT, ERROR, CANCEL | Provisória | Demais tipos da decisão 6.6 são **reservados** (rejeitados com erro explícito) |
| D-09 (local, histórico) | Tipos fora da 6.6 (VOICE_INPUT, STT_PARTIAL, CONNECTION_*, etc.) | Pendente | **Renumerada** em 31/10/2026: o D-09 vigente é o do Notion (linha abaixo). Este item continua pendente com o nome D-09-LOCAL |
| D-18 | `STT_RESULT` e `INTENT_DETECTED` implementados em Kavo Protocol 1.1, com payload tipado | Provisória | Nomes vêm da decisão 6.6; parciais e finais no mesmo tipo, distinguidos por `state` |
| D-19 | Parciais de STT são garantidos (não descartados) | Provisória | Perder evento quebra a sequência; custo de latência a medir |
| D-20 | Contrato de STT agnóstico de fornecedor; nenhum mecanismo real | Provisória | Notion S1/A3 pede backend certificado; escolha pendente |
| D-21 | Só segmento `CONFIRMED` pode ser admitido para execução | Provisória | Notion A3: nada executado por parcial; relaxar para leitura exige decisão |
| D-22 | Intent Engine não concede permissão; `ReadinessGate` só sinaliza prontidão | Provisória | Alinha com D-029 (JEV consultor) e Policy como autoridade |
| D-23 | Chave de execução = segmento+intenção+parâmetros; sem repetição | Provisória | Evita execução duplicada em atualizações sucessivas |
| D-24 | Limiar de confiança sem valor padrão | Provisória | Notion: metas só após baseline medido |
| D-25 | Janela de duplicatas de 64 sequências por stream; lacuna não é aplicada | Provisória | Valor sem base de carga |
| D-26 | Timeout de stream ocioso 10 s; máximo 16 streams | Provisória | Valores sem medição; configuráveis em `[speech]` |
| D-27 | Monitor de contratos é assinante do barramento, sem ações | Provisória | Valida no daemon real antes de existir consumidor de negócio |
| D-28 | Lacuna de sequência não é aplicada e não há sinal de volta ao produtor | Provisória | Limitação conhecida; decidir se 1.1 precisa de `NACK` |
| D-29 | Streams aposentados ficam para sempre no processo (sem limite) | Provisória | Substitui a lista de 256: sem reabertura por limite de memória. Não sobrevive a reinício |
| D-31 | Registro durável de admissões e aposentados (SQLite, D-010) antes de qualquer execução | Implementada; **retenção pendente** | Ver `SPEECH-INTENT-CONTRACTS.md` §9. Não há Execution Engine ainda |
| D-09 | Extensão do Kavo Protocol para sessões e cancelamento de requisições (Notion, 31/10/2026) | **Aprovada** | Implementada em 1.2 (`SESSION_CONTROL`, `CONTROL_RESULT`, `CANCEL` 1.2). Contrato em `KAVO-PROTOCOL.md` seção 10 |
| D-53 | `KavoMessage::new` continua em `schema_version` 1.1 (padrão de compatibilidade); 1.2 é explícito | Provisória | Evita quebrar clientes e suítes 1.1; protocolo passa a `1.2-draft` |
| D-54 | Monitor é a única autoridade de sessões, requisições e streams; IPC só encaminha e espera resposta | Provisória | Um dono de estado, sem Mutex compartilhado; sem duplicação entre Session Manager, monitor e daemon |
| D-55 | Gate de sessão: 1.1 abre sessão desconhecida (legado); 1.2 recusa; encerrada recusa sempre | Provisória | Compatibilidade com clientes 1.1 sem reabrir sessão encerrada |
| D-56 | Requisição = `REQUEST_BEGIN` com `kind: STT_STREAM`; cancelar requisição cancela seu stream vinculado; cancelar stream não altera a requisição | Provisória | Propagação real sem criar tarefas. `kind` restrito a STT_STREAM até decisão própria |
| D-57 | `CONTROL_RESULT`: `outcome` APPLIED/NOOP/REJECTED; catálogo de `reason` | Provisória | Catálogo de motivos é proposta local; expandir exige revisão |
| D-58 | Confirmação de admissão de eventos STT/INTENT 1.2 (`ADMISSION_ACK`: ACCEPTED, DUPLICATE, REJECTED, NOT_READY) | **Aprovada** (Notion, 31/10/2026) | Implementada na A1.1; contrato em `KAVO-PROTOCOL.md` seção 11. Não é ACK de conclusão |
| D-59 | Falha de gravação no encerramento ou cancelamento: fail-closed, sessão segue ativa, operação repetível | Provisória | Streams já gravados como cancelados permanecem; nenhuma mudança em memória sem gravação |
| D-60 | Menu `kavo` sem argumentos abre em TTY (requisito aprovado); só operações de leitura e harness | Provisória | Implementado como interface sobre a CLI; sem regra de negócio |
| D-61 | Erros de controle usam `ERROR` 1100 com prefixo de classe, como D-32 | Provisória | Sem códigos 2xxx novos |
| D-62 | Ponto de admissão: gate de sessão, validação do tracker e gravação durável exigida (abertura, terminal, upsert de interpretação) antes do ACK | Provisória | Define quando ACCEPTED é possível; parciais não gravam |
| D-63 | Readiness e execução avaliadas depois do ACK; falha nelas não altera o status | Provisória | ACK não é conclusão nem execução (D-58) |
| D-64 | `ipc.admission_queue` = 64 (1–4096), `KAVO_IPC_ADMISSION_QUEUE`; fila cheia = `NOT_READY BUSY` imediato | Provisória | Valor sem base de carga em hardware alvo; medido no E2E, a calibrar |
| D-65 | Catálogo de `reason` ampliado (STREAM_CLOSED, SEQUENCE_GAP, STALE_REVISION, CONFLICT, INVALID_CONTENT, BUSY etc.) | Provisória | Motivos estáveis derivados das recusas do tracker; classificação por textos `REJECT_*` testada |
| D-66 | Recusa por conteúdo consome a sequência; repetir devolve a mesma recusa, nunca DUPLICATE | Provisória | Corrige retry que mentiria sobre a admissão |
| D-67 | Falha de gravação de interpretação desfaz a memória (`rollback`) antes do NOT_READY | Provisória | Sem isso, retry viraria DUPLICATE sem gravação durável |
| D-68 | Sem ACK em prazo, IPC responde ERROR `[TIMEOUT]`, não ACK; cliente repete com a mesma identidade | Provisória | Estado incerto para o cliente, resolvido por idempotência |
| D-69 | Cliente 1.2 envia um evento por vez e lê o ACK antes do próximo | Provisória | Preserva ordem do stream; o harness de teste segue essa regra |
| D-70 | Replay idêntico de evento já aceito após evicção de stream devolve `DUPLICATE` (não mais "reaberto"); conteúdo diferente = `CONFLICT`. Testes da Fase 4.2 atualizados | Provisória | Consequência direta de D-58; não reabre nem reaplica. Mudança de expectativa de teste antigo, registrada |
| D-71 | Abertura de stream gravada e não admitida em memória fica em `pending_open`; retry refaz só a decisão de memória, sem reabrir no banco | Provisória | Corrige recusa por capacidade com falha de gravação (ACK divergia do banco) |
| D-72 | Motivo de "stream já gravado" vem do status durável: `REJECTED` → `STREAM_LIMIT`; demais → `STREAM_CLOSED` | Provisória | Retry devolve o mesmo motivo |
| D-73 | Memória de streams guarda digest SHA-256 do conteúdo, não o texto canônico; janela de digests é preservada na evicção (`retired_recent`) | Provisória | Reduz memória e não retém texto transcrito fora do necessário; custo de memória por stream aposentado (D-29) |
| D-74 | Interpretação sem registro em memória é rehidratada da durável antes de decidir | Provisória | Corrige `GAP` indevido após fim de stream |
| D-75 | Idempotência do terminal de stream após reinício exige digest durável (mudança de esquema SQLite) | **Pendente** | Não decidido sem autorização; hoje devolve `STREAM_CLOSED` após reinício |
| D-76 | Histórico de revisões de interpretação para repetir revisões antigas como `DUPLICATE` | **Pendente** | Hoje só a última revisão é reconhecida |
| D-78 | Pendência de abertura (`pending_open`) é removida após gravação durável de cancelamento de stream e de encerramento de sessão; nunca antes da gravação | Provisória | Sem limpeza, entradas órfãs crescem e consomem o teto de todas as sessões; `pending-lifecycle` cobre limpeza, falha na limpeza e repetição |
| D-79 | Teto de aberturas pendentes = `speech.max_streams`; acima dele, nova abertura recebe `NOT_READY BUSY` sem gravação | Provisória | Limita crescimento por clientes que abandonam streams; valor herdado da configuração existente, sem medição própria |
| D-77 | Envio ao monitor (controle e admissão) sempre com prazo do IPC; fila cheia nunca bloqueia indefinidamente | Provisória | Elimina bloqueio indefinido observado na auditoria |
| D-36 | Política de retenção de admissões: nenhuma (imutáveis) | Pendente | Precisa de aprovação antes de crescer sem limite |
| D-37 | Stream interrompido no reinício nunca é retomado | Provisória | Produtor reinicia com `stream_id` novo |
| D-38 | Banco com erro ou esquema desconhecido impede o início | Provisória | Fail-closed |
| D-39 | Só digests SHA-256 de chaves e conteúdo são gravados | Provisória | Privacidade: sem texto nem parâmetros em disco |
| D-40 | Nota de nomenclatura: IDs D-031..D-039 aqui não coincidem com `DECISOES-CONSOLIDADAS.md` (ex.: D-031 = Capability Registry) | Pendente | Renumerar com `KD-` para evitar conflito |
| D-32 | `error_code` restrito à taxonomia aprovada | Provisória | Sem códigos inventados; 2xxx/3xxx só após aprovação |
| D-33 | Identidade de stream e de interpretação é `(session_id, id)` | Provisória | Evita colisão e cancelamento entre sessões |
| D-34 | Admissões de execução são permanentes no processo | Provisória | Impede reexecução por reenvio ou reabertura no mesmo processo |
| D-35 | Sem NACK: produtor recupera por reenvio idêntico após `ERROR` | Pendente | Ver `SPEECH-INTENT-CONTRACTS.md` seção 8 |
| D-30 | `serde_json` com `float_roundtrip` | Provisória | Timestamps precisam de ida e volta exata |
| D-10 | `sender` obrigatório no envelope | Provisória | Motivo: auditoria |
| D-11 | Envelope 1.0 sem campos text/intent/slots/timeout_seconds/transport_version | Provisória | Voltam em versão 1.1 com o componente que os usa |
| D-12 | Erros de protocolo não derrubam o núcleo | Provisória | Afetam só a conexão ou a mensagem |
| D-13 | Socket 0600 em diretório 0700; só remove socket órfão | Provisória | Proteção contra remoção de arquivos do usuário |
| D-14 | Arquitetura híbrida Python/Rust (DA-001 das docs) | **Superada** por D-01 | Mantida como histórico |
| D-15 | Entrega garantida: paralela, prazo único, falha se sem assinante ou falha parcial | Provisória | Revisão independente: evita perda silenciosa |
| D-16 | Conexões: ociosidade 30 s e máximo 64 simultâneas | Provisória | Valores sem base de carga; ajustar após benchmark |
| D-17 | Socket exige diretório pai 0700 | Provisória | Fecha janela entre bind e chmod |
| D-41 | Session Manager (A0) é só ciclo de vida: abrir, encerrar, consultar; não concede permissões | Provisória | Implementado em `session.rs`; não integrado ao IPC (sem tipos de sessão no protocolo) |
| D-42 | Sessão encerrada não reabre; evento de sessão desconhecida ou encerrada é rejeitado | Provisória | Impede que eventos atrasados ressuscitem sessão |
| D-43 | Estado de sessão só em memória; reinício descarta sessões e requisições | Provisória | O que precisa ser durável continua no SQLite (D-31). Recuperação de sessão: pendente |
| D-44 | Cancelamento por requisição: token por `request_id` (= `message_id` aguardado); idempotente; encerrar sessão cancela só as requisições dela | Provisória | Não cancela tarefas (não há Task Manager) nem outras sessões |
| D-45 | Expiração de sessão por inatividade | Pendente | Nenhum prazo aprovado; não implementada |
| D-46 | Harness A0: fonte virtual gera STT sintético (emissor `fonte-virtual`); não simula captura de áudio nem reconhecimento | Provisória | `kavo harness`; cenários contra barramento, monitor e SQLite reais |
| D-47 | Producer envia eventos de um mesmo stream em uma conexão, em ordem | Provisória | Conexões separadas podem ser processadas fora de ordem (achado no E2E real); sem NACK (D-35) |
| D-48 | Falha de gravação é injetada por trigger SQLite (`RAISE(ABORT)`) no cenário `store-fault`, sem alterar código de produção | Provisória | Teste reversível, real no SQLite; reenvio idêntico recupera o estado |
| D-49 | Cenários do harness só embutidos e escolhidos por argumento; cenários por arquivo não implementados | Provisória | Nenhum requisito aprovado pede arquivos de cenário; adiar até haver necessidade |
| D-50 | Integração do Session Manager ao daemon (sessões no IPC). **Implementada em A1 sob D-09** | Provisória | Exige tipos de mensagem de sessão, dependente de D-09; protocolo não alterado |
| D-51 | Logs e artefatos E2E ficam em `target/e2e` (ignorado pelo Git), com XDG próprio | Provisória | Não toca dados do usuário; socket limitado a 108 bytes (`SUN_LEN`) |
| D-52 | Cancelamento de requisição (`correlation_id`) sem alvo no núcleo. **Resolvida por D-09 e D-56** | Resolvida | Não há registro de requisições no daemon; só stream é cancelado pelo socket |

## Reconciliação 03-PROTOCOLOS × 10-CONTRATOS
Detalhada em `KAVO-PROTOCOL.md`, seção 7.

## Pendências para você
1. Aprovar ou ajustar a taxonomia de erros (`ERROR-TAXONOMY.md`) e os códigos 2xxx/3xxx propostos.
2. Aprovar D-05 (remoção de `cancel_id`) e D-10 (`sender` obrigatório).
3. Decidir D-09: quais tipos entram no próximo passo.
4. Decidir se o Message Bus externo (ovos/Mosquitto) entra em alguma fase.
