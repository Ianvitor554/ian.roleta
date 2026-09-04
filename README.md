# Mega Roulette AI V3.5.1 - SEM DADOS Fix

Corrige o caso em que o painel ficava em "SEM DADOS" após recuperação de
um SQLite corrompido.

## Causa corrigida

A V3.5 podia entrar em Deep Sync de até 80 páginas com o banco vazio antes de
inserir o primeiro giro. Em um servidor pequeno isso podia deixar o painel
aparentemente sem dados por muito tempo.

## Novo bootstrap

Quando existem menos de 120 giros:

1. usa apenas 5 páginas iniciais;
2. começa a inserir os giros imediatamente;
3. o painel sai de "sem dados" assim que o primeiro giro entra;
4. cria a previsão atual;
5. depois o collector segue normalmente;
6. deep sync grande só ocorre depois que o bootstrap terminou.

## Diagnóstico visível

Enquanto ainda não há histórico o painel mostra:

- fase da sincronização;
- collector ativo/parado;
- páginas lidas;
- registros encontrados;
- erro real da API;
- status da recuperação do banco.

Endpoint:
/api/collector-status

## API Mega Roulette

A fonte continua:
https://api-cs.casino.org/svc-evolution-game-events/api/megaroulette?page=0&size=27&sort=data.settledAt,desc&duration=6&isLightningNumberMatched=false

## Render

RUN_COLLECTOR=1
CLOUD_MODE=1
BOOTSTRAP_PAGES=5
BOOTSTRAP_TARGET_SPINS=120

Mantenha o persistent disk em /var/data.
