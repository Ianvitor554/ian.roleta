# Mega Roulette Precision AI V3.3 Round Intelligence

Foco: análise específica de cada rodada.

## Dossiê da rodada
Cada novo giro congela uma análise antes do próximo resultado com:
- último e penúltimo número;
- puxadores Bayesianos;
- replay walk-forward específico do número-fonte;
- TOP1/TOP3/TOP5 local;
- log loss e skill local;
- contexto de 2 passos;
- cluster físico na roda europeia;
- consenso de modelos e janelas;
- alinhamento da IA;
- peso dinâmico dos puxadores;
- bloqueios de evidência.

## Peso dinâmico
O número que acabou de sair recebe avaliação própria. Se aquele número-fonte
tem pouco histórico ou desempenho local ruim, o peso dos puxadores cai.
Se há suporte e skill walk-forward, o peso pode subir, sempre limitado.

## Endpoint
/api/round-analysis

## Índice de evidência
FORTE / BOA / MODERADA / FRACA.
É uma nota de evidência, não uma porcentagem de chance.

## Persistência
`round_analysis_json` é salvo junto da previsão, antes do próximo resultado.
A migração SQLite é não destrutiva.

## Nota
Em roleta justa, giros são independentes. “Puxadores” são associações
históricas condicionais, não causalidade física.
