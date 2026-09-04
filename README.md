# Mega Roulette AI V4 Commander

## Objetivo

V4 muda a arquitetura para que o sinal final seja decidido por um
Meta-Comandante de IA.

Os outros modelos continuam gerando evidência, mas não têm autoridade para
liberar GREEN sozinhos.

## AI Commander 100%

O Commander recebe:
- qualidade da rodada;
- consenso dos sete modelos;
- consenso temporal;
- lift da cobertura;
- lift TOP5;
- skill da IA online;
- skill dos puxadores;
- skill local do último número;
- estabilidade do regime;
- quantidade de modelos maduros que superam a base;
- z-score fora da amostra;
- calibração;
- entropia;
- alinhamento da IA.

Ele aprende online se a cobertura escolhida acertou ou errou.

### Modelo residual

A probabilidade começa na chance-base da cobertura:

`P_base = quantidade_de_números / 37`

O Commander aprende um ajuste no log-odds. Portanto ele não começa
automaticamente acreditando que existe vantagem.

## Cold-start mais rápido

Enquanto ainda tem pouca amostra, usa um prior derivado das evidências
da rodada. À medida que acumula resultados, o modelo aprendido assume
progressivamente o controle.

Isso evita esperar 120 novas rodadas apenas para começar a emitir sinais,
mas o cold-start exige autoridade e edge mais altos.

## Centros mais precisos

A V4 testa todas as combinações de 3 centros entre os 14 melhores
candidatos. São 364 combinações.

A função otimiza:
- massa de probabilidade da cobertura conjunta;
- lift;
- consenso;
- força dos centros;
- puxadores;
- distância física;
- sobreposição entre zonas.

## Velocidade

Backfill antigo não cria uma análise completa em cada giro histórico.

Durante bootstrap:
- treina IA;
- atualiza histórico;
- atualiza modelos;
- grava giros;
- cria somente a previsão atual ao terminar o lote.

Isso reduz bastante o custo inicial.

## Roda Europeia

A roda circular usa a ordem real:

0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8,
23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26

Mostra:
- último número;
- 3 centros;
- cobertura ±2;
- RaceTrack linear.

## Cloud 24/7

Para coleta real mesmo com celular desligado:
- Render Web Service pago;
- plan `0.5c-512mb`;
- persistent disk em `/var/data`;
- `RUN_COLLECTOR=1`;
- `CLOUD_MODE=1`;
- um único Gunicorn worker.

A V4 mantém:
- collector supervisor;
- heartbeat watchdog;
- HTTP retries;
- deep sync;
- backfill;
- recovery do SQLite;
- WAL checkpoint;
- backups validados.

## Endpoints

- `/api/status`
- `/api/commander`
- `/api/collector-status`
- `/api/professional`
- `/api/pullers`
- `/api/number-stats`
- `/api/spins`
- `/api/spins.csv`
- `/api/db-recovery-status`

## Limite real

Uma roleta justa continua aleatória e não existe garantia de prever o
próximo giro apenas com histórico. A V4 melhora disciplina, velocidade,
calibração, aprendizado e seleção de rodada. Ela pode decidir RED mesmo
quando outros modelos parecem otimistas.
