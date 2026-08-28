# IR Predictor 360 V7 Professional Cloud

Painel Flask para estudo de resultados da Immersive Roulette, com coleta contínua, estratégias históricas, meta-learner, semáforo e persistência em SQLite.

> O software analisa padrões históricos. Não existe garantia de que resultados passados prevejam o próximo giro de uma roleta equilibrada.

## Principais recursos

- Coletor automático independente do navegador.
- SQLite com WAL e `round_id` único.
- Histórico não é apagado quando o navegador fecha.
- Histórico não é apagado em restart/deploy quando `/var/data` está em um Render Persistent Disk.
- Backup consistente automático a cada 6 horas.
- Retenção configurável dos backups.
- Backup adicional ao encerramento do processo.
- Verificação `PRAGMA integrity_check`.
- Endpoint `/health`.
- Endpoint `/api/storage`.
- Endpoint protegido `POST /api/backup`.
- Migração automática dos JSON antigos.
- Semáforo, Meta-Learner, estratégias e backtest da V5/V6.
- GitHub Actions para validar sintaxe em cada push.

## Estrutura

```text
.
├── app.py
├── requirements.txt
├── render.yaml
├── Dockerfile
├── Procfile
├── .env.example
├── .gitignore
└── .github/
    └── workflows/
        └── python-check.yml
```

## Rodar localmente

Crie um ambiente virtual e instale:

```bash
python -m venv .venv
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows:

```powershell
.venv\Scripts\Activate.ps1
```

Depois:

```bash
pip install -r requirements.txt
python app.py
```

Abra:

```text
http://127.0.0.1:5000
```

## Pydroid3

Para teste local:

```bash
pip install flask requests
python app.py
```

O Gunicorn é para o servidor Linux/Render.

## Persistência no Render

O diretório persistente esperado é:

```text
/var/data
```

Banco:

```text
/var/data/ir_predictor360.sqlite3
```

Backups:

```text
/var/data/backups/
```

**Importante:** o Render precisa ter um Persistent Disk montado em `/var/data`. Sem esse disco, o filesystem normal pode ser descartado em restart/deploy.

## Deploy pelo GitHub + Render

1. Crie um repositório vazio no GitHub.
2. Coloque todos os arquivos deste pacote na raiz do repositório.
3. Faça commit e push.
4. No Render, conecte seu GitHub.
5. Crie o serviço usando o `render.yaml` (Blueprint) ou Web Service manual.
6. Escolha um plano que suporte Persistent Disk.
7. Confirme o disco em `/var/data`.
8. Defina `PANEL_PASSWORD` como Secret.
9. Faça o deploy.
10. Abra `/health` e confirme `"ok": true`.

### Comandos do Render

Build:

```bash
pip install -r requirements.txt
```

Start:

```bash
gunicorn --workers 1 --threads 4 --timeout 120 --bind 0.0.0.0:$PORT app:app
```

Use **1 worker** enquanto o coletor estiver dentro do processo web. Mais workers criariam múltiplos coletores.

## Variáveis

| Variável | Padrão | Uso |
|---|---:|---|
| `DATA_DIR` | `./data` | Diretório de dados |
| `DB_FILE` | `DATA_DIR/ir_predictor360.sqlite3` | Banco SQLite |
| `BACKUP_DIR` | `DATA_DIR/backups` | Backups |
| `RUN_COLLECTOR` | `1` | Liga/desliga coleta |
| `COLLECT_INTERVAL` | `8` | Intervalo da coleta |
| `BACKUP_INTERVAL_SECONDS` | `21600` | Backup automático |
| `BACKUP_KEEP` | `14` | Quantos backups manter |
| `PANEL_USER` | `admin` | Usuário |
| `PANEL_PASSWORD` | vazio | Senha do painel |
| `LOG_LEVEL` | `INFO` | Nível de log |

## Segurança

Nunca envie `.env` para o GitHub. O `.gitignore` já bloqueia esse arquivo.

No Render, guarde `PANEL_PASSWORD` nas Environment Variables/Secrets.

## Endpoints

### Health

```text
GET /health
```

### Estado do armazenamento

```text
GET /api/storage
```

### Criar backup manual

```text
POST /api/backup
```

Esse endpoint exige a mesma autenticação do painel quando `PANEL_PASSWORD` está configurada.

## Backup

O programa usa a API `sqlite3.Connection.backup()`, que gera cópia consistente mesmo com o banco aberto.

Padrão:

- um backup a cada 6 horas;
- mantém os últimos 14;
- faz tentativa de backup no encerramento.

Além disso, o Persistent Disk do Render possui snapshots próprios conforme as regras do provedor.

## O que não é apagado

A tabela `spins` não possui rotina de DELETE. `MAX_HISTORY` limita apenas quantos giros entram na memória do processo para cálculo, não quantos permanecem armazenados no SQLite.

Portanto, mesmo após meses de uso, os giros antigos permanecem no banco enquanto houver espaço no Persistent Disk.

## Próxima arquitetura para escala

Se um dia você quiser múltiplas instâncias ou separar coletor e painel, migre para:

```text
Web Service
    ↓
PostgreSQL
    ↑
Background Worker
```

Para uma única mesa e um único coletor, SQLite + Persistent Disk é mais simples e evita complexidade desnecessária.
