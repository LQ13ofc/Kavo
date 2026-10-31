# Taxonomia de erros do Kavo

Status: classes **propostas**. Wire: código `1100` provisório (aprovado pelo usuário).

## 1. Modelo

Cada erro interno (`KavoError`) tem uma classe `ErrorKind` com nome estável. O nome aparece na mensagem de `ERROR` como `[NOME] texto`. O código numérico do fio é `1100` enquanto a taxonomia não for aprovada.

## 2. Classes

| `ErrorKind` | Quando | Código proposto | Recuperação |
|---|---|---|---|
| `INVALID_MESSAGE` | JSON inválido, campo obrigatório ausente, valor fora da regra | 2001 | corrigir emissor |
| `UNSUPPORTED_VERSION` | `schema_version` com major ≠ 1 | 2002 | atualizar emissor |
| `UNSUPPORTED_MESSAGE_TYPE` | tipo reservado ou desconhecido | 2003 | aguardar implementação / corrigir emissor |
| `FRAME_TOO_LARGE` | linha acima de `ipc.max_frame_bytes` | 2004 | conexão é fechada |
| `DELIVERY_FAILED` | mensagem garantida não aceita por um assinante no prazo | 2005 | repetir ou relatar |
| `TIMEOUT` | prazo de conexão, leitura ou escrita excedido | 2006 | repetir |
| `CANCELLED` | operação cancelada por `CANCEL` | 2007 | nenhuma |
| `CONFIG_INVALID` | configuração ou variável `KAVO_*` inválida | 3001 | corrigir configuração |
| `ALREADY_RUNNING` | segunda instância do núcleo | 3002 | nenhuma |
| `COMPONENT_FAILED` | componente terminou com erro ou pânico | 3003 | reiniciar componente |
| `IO` | falha de sistema de arquivos ou socket | 3004 | depende do caso |
| `INTERNAL` | falha não classificada (reservada; nenhum erro atual cai aqui) | 1100 | registrar e investigar |

Códigos 1001–1007 são de domínio (STT, intent, comando, memória, wake word) e não são usados pela fundação.

## 3. Regras

- Erro sem classe conhecida é `INTERNAL`, não `INVALID_MESSAGE`.
- Mensagem ao usuário final não contém o nome da classe; a camada de comunicação traduz (regra 4.1 do projeto).
- Erros de protocolo nunca derrubam o núcleo; afetam só a conexão ou a mensagem.
