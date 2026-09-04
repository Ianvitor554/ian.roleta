# Mega Roulette AI V3.5 Professional Learning

Base: V3.4.1 Recovery.

## IA mais profissional
- AdaGrad por feature
- label smoothing
- gradient clipping
- regularização L2
- memória rápida e lenta
- pesos adaptativos por regime
- validação fora da amostra
- gate profissional que só mantém ou rebaixa sinais

## Sete modelos
1. transição
2. puxadores Bayes
3. frequência curta
4. frequência longa
5. roda física
6. contexto
7. IA online

Cada modelo mantém log loss rápido e lento. O peso depende de desempenho,
estabilidade e regime atual.

## Aprender com o tempo
A IA salva pesos, acumuladores AdaGrad e métricas no SQLite. O tracker salva
memória global e por regime. Depois de reinício, continua de onde parou.

## Gate profissional
O sistema compara Greens resolvidos com a chance-base da própria cobertura.
Calcula observado, esperado, edge e z-score.

Ele pode rebaixar:
GREEN_ELITE -> GREEN_PLUS
GREEN -> YELLOW
GREEN -> RED

quando a evidência fora da amostra não sustenta o sinal.

## Endpoint
/api/professional

## Banco
Mantém toda a recuperação automática da V3.4.1:
- quick_check
- quarentena
- restore de backup saudável
- WAL checkpoint
- backups validados

## Limite real
Uma roleta justa não se torna previsível apenas com histórico. Esta versão
fica mais inteligente no sentido correto: aprende desempenho, reduz
sobreconfiança, penaliza modelos ruins e rejeita rodadas fracas.
