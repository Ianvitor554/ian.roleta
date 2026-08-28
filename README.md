# IR Predictor 360 V5 • Render

Pacote preparado para publicar o Flask no Render.

## Arquivos
- `app.py` — aplicação principal.
- `requirements.txt` — dependências Python.
- `.python-version` — usa Python 3.13.
- `render.yaml` — configuração opcional do Render Blueprint.
- `.gitignore` — evita enviar cache e dados locais.

## Configuração manual no Render
- Runtime: `Python 3`
- Build Command: `pip install -r requirements.txt`
- Start Command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120`
- Health Check Path: `/health`

## GitHub
Coloque TODOS estes arquivos na raiz do repositório.

Estrutura esperada:

    ian.roleta/
    ├── app.py
    ├── requirements.txt
    ├── .python-version
    ├── render.yaml
    ├── .gitignore
    └── README.md

Depois faça commit/push e, no Render, use "Manual Deploy" > "Deploy latest commit"
ou deixe o Auto Deploy ativado.

## Dados de aprendizado
Por padrão, os arquivos de histórico e aprendizado são gravados no diretório atual.
Você também pode definir a variável de ambiente `DATA_DIR` para apontar para uma
pasta persistente caso configure um Persistent Disk no Render.

Exemplo:
    DATA_DIR=/var/data

Sem armazenamento persistente, reinícios/redeploys do serviço podem apagar os
arquivos locais do histórico/aprendizado.

## Rodar localmente/Pydroid
    pip install -r requirements.txt
    python app.py

Abra:
    http://127.0.0.1:5000

## Aviso
O painel testa sinais históricos de roleta. Ele não garante resultados futuros.
