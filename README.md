# Mega Roulette AI V4.0.1 Anti-timeout

O log mostrou que o serviço e o collector estão funcionando:
- 135 giros recuperados;
- +1 giro novo coletado.

O erro foi um WORKER TIMEOUT em `/api/spins`.

## Causa
O collector e as rotas de leitura disputavam o mesmo `self.lock` do SQLite.
Durante bootstrap a página podia ficar esperando até o Gunicorn matar o worker.

## Correção
- read_conn() sem lock global para consultas;
- WAL configurado apenas no startup;
- writes continuam protegidas;
- Gunicorn gthread explícito;
- 1 worker, 4 threads;
- timeout 120 s;
- UI escalona as consultas iniciais;
- query_ms em spins e number-stats.

## Start Command
gunicorn --worker-class gthread --workers 1 --threads 4 --timeout 120 --graceful-timeout 30 --keep-alive 5 --bind 0.0.0.0:$PORT app:app

## Render
Fixe também:
PYTHON_VERSION=3.12.11

O traceback enviado ainda mostrava Python 3.14.

## Mantido
AI Commander, roda europeia, puxadores, coleta 24/7, deep sync,
SQLite recovery, backups, contador e histórico completo.
