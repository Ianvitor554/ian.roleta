# Mega Roulette AI V3.4 - Adaptive Counter Cloud

## Novidades de precisão
- Ensemble adaptativo de 7 modelos.
- Cada modelo é avaliado antes do resultado e recebe peso pelo log loss EMA real.
- Puxadores continuam com validação global e específica do número da rodada.
- IA recebe ajuste por alinhamento com os demais modelos.
- Calibração de temperatura usa apenas previsões da V3.4 quando houver amostra suficiente.
- Consenso dos centros usa os pesos adaptativos dos modelos.

## Contador 0-36
O painel mostra, para cada número:
- quantidade total em todo o SQLite;
- porcentagem;
- atraso atual;
- maior atraso observado;
- frequência nos últimos 20, 50, 100 e 300 giros.

## Histórico integral
`/api/spins` é paginado e lê o banco completo, não apenas a memória da IA.
`/api/spins.csv` exporta todos os giros coletados.
`/api/number-stats` entrega o contador completo.
`/api/model-weights` mostra os pesos adaptativos.

## Coleta com seu celular offline
No Pydroid a coleta para se o processo/celular parar. Para continuar sem você online,
rode no Render pago com `CLOUD_MODE=1` e persistent disk em `/var/data`.
O `render.yaml` desta pasta já está configurado para o plano `0.5c-512mb` e disco persistente.

## Recuperação
- retry/backoff de rede;
- supervisor do collector;
- deep sync periódico;
- até 80 páginas de backfill;
- backups automáticos;
- SQLite WAL.

## Importante
Em uma roleta justa, resultados passados não garantem o próximo giro. O V3.4 tenta
reduzir overfitting e recusar evidência fraca, não prometer acerto certo.
