# Mega Roulette Precision AI V3.1 Cloud 24/7

Esta versão foi preparada para continuar coletando mesmo quando o seu celular
estiver sem internet, desligado ou com o navegador fechado.

## Como isso funciona

O celular deixa de ser o collector principal.
O collector roda no servidor Render.

Enquanto o servidor estiver ativo:
- consulta Mega Roulette continuamente;
- grava giros no SQLite;
- resolve previsões;
- treina a IA;
- cria novas previsões;
- faz backups periódicos;
- recupera lacunas após falhas da API.

Quando você abrir o painel novamente, os dados já estarão no servidor.

## Importante sobre Render

Para 24/7 real:
- use compute pago;
- mantenha o persistent disk em /var/data.

O Blueprint `render.yaml` já está configurado dessa forma.

Render Free não é adequado para este caso porque serviços web gratuitos podem
entrar em suspensão quando ficam sem tráfego e não suportam persistent disk.

## Recuperação de lacunas

Na inicialização, depois de erro de rede e periodicamente, o collector entra em
Deep Sync. Ele pagina para trás até reencontrar um round que já existe no SQLite.

MAX_BACKFILL_PAGES=80
DEEP_SYNC_INTERVAL=1800

Isso protege contra pequenas quedas e reinicializações do serviço.

## APIs

- /
- /health
- /api/status
- /api/history
- /api/ai
- /api/diagnostics
- /api/cloud-status
- /api/export.csv
- POST /api/backup

## Pydroid

Continua funcionando localmente:

pip install flask requests
python app.py

Mas Pydroid não consegue coletar se o telefone estiver sem internet ou se o
processo for encerrado. Para isso use o deploy cloud.
