# Level 1 Cases

Этот файл фиксирует текущий Level 1 regression set для публичного baseline.

Он основан на последней кейсовой классификации, которая разделяет:
- стабильные простые кейсы, которые должны оставаться исполнимыми Level 1;
- структурно более сложные кейсы, которые остаются активным engineering backlog Level 2;
- mixed/high-risk cases, которым нужен отдельный Level 3 path.

## Текущий regression set

Level 1 smoke/regression set сейчас такой:

`13, 16, 24, 25, 27, 28, 30`

Эти кейсы следует считать минимальным демонстрационным набором для текущего публичного baseline.

## Список кейсов

| Case | Profile | Почему это Level 1 | Notes |
|---|---|---|---|
| 13 | low/simple: add phrase | стабилен в stress runs; `standard_single` topology | простой append/update path |
| 16 | low: replace + add | стабилен несмотря на две operations | полезный mixed-low regression case |
| 24 | low: repeal | стабилен в stress runs | straightforward repeal |
| 25 | low: repeal | стабилен в stress runs | straightforward repeal |
| 27 | low: replace + exclude | стабильный mixed-low case | покрывает phrase replacement path |
| 28 | simple/no pattern matched | стабилен в обоих stress runs | важный structural smoke case |
| 30 | low: repeal, multi-doc/redaction context | валиден в baseline; поздний rerun failure был классифицирован как infrastructure issue, а не ошибка case logic | keep with infra caveat |


## Что сюда не входит

Не каждый low-complexity или simple-tagged case сейчас является Level 1.

Следующие кейсы пока остаются вне стабильного regression set, потому что все еще упираются в известные gaps в resolver/analyzer/materialization:

`01, 02, 03, 06, 07, 08, 09, 10, 11, 12, 14, 15, 17, 19, 20, 22, 23, 26`

Это Level 2/3 engineering backlog.
