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
| D-09 | Tipos fora da 6.6 (VOICE_INPUT, STT_PARTIAL, CONNECTION_*, etc.) | Pendente | Só aparecem em 03/10; não implementados |
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
| D-29 | Ids de stream removidos: lista de 256 contra reabertura | Provisória | Limitação: id antigo pode ser reaberto após 256 remoções |
| D-30 | `serde_json` com `float_roundtrip` | Provisória | Timestamps precisam de ida e volta exata |
| D-10 | `sender` obrigatório no envelope | Provisória | Motivo: auditoria |
| D-11 | Envelope 1.0 sem campos text/intent/slots/timeout_seconds/transport_version | Provisória | Voltam em versão 1.1 com o componente que os usa |
| D-12 | Erros de protocolo não derrubam o núcleo | Provisória | Afetam só a conexão ou a mensagem |
| D-13 | Socket 0600 em diretório 0700; só remove socket órfão | Provisória | Proteção contra remoção de arquivos do usuário |
| D-14 | Arquitetura híbrida Python/Rust (DA-001 das docs) | **Superada** por D-01 | Mantida como histórico |
| D-15 | Entrega garantida: paralela, prazo único, falha se sem assinante ou falha parcial | Provisória | Revisão independente: evita perda silenciosa |
| D-16 | Conexões: ociosidade 30 s e máximo 64 simultâneas | Provisória | Valores sem base de carga; ajustar após benchmark |
| D-17 | Socket exige diretório pai 0700 | Provisória | Fecha janela entre bind e chmod |

## Reconciliação 03-PROTOCOLOS × 10-CONTRATOS
Detalhada em `KAVO-PROTOCOL.md`, seção 7.

## Pendências para você
1. Aprovar ou ajustar a taxonomia de erros (`ERROR-TAXONOMY.md`) e os códigos 2xxx/3xxx propostos.
2. Aprovar D-05 (remoção de `cancel_id`) e D-10 (`sender` obrigatório).
3. Decidir D-09: quais tipos entram no próximo passo.
4. Decidir se o Message Bus externo (ovos/Mosquitto) entra em alguma fase.
