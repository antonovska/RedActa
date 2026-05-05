# Architecture: Production Baseline в текущем виде

## Цель

Собрать пайплайн для автоматической генерации редакций документов: нормативные `.docx`-документы изменений к базовым `.docx`-документам, используя OOXML-структуру и LLM-извлечение намерений изменения.

Текущий публичный baseline использует regex как вспомогательный механизм для:
- подсчета директив в analysis gate;
- нормализации и fallback-извлечения;
- части эвристик в resolver/editor.

## Текущий runtime path

Поддерживаемый runtime path в публичном baseline сейчас такой:

```
cli.py / colab_runner.py
  -> run_case.run_case
     -> case_loader
     -> amendment_analyzer
     -> base_analyzer
     -> skeleton_builder
     -> resolver_v2
     -> editor_v2
     -> final service table insertion
     -> validation_checklist_builder
     -> validator
```

`graph_pipeline.py` присутствует в репозитории как LangGraph-scaffolding, но CLI и Colab demo сейчас исполняют `run_case.run_case` напрямую.

## Текущий capability scope

См. [CAPABILITY_LEVELS.md](CAPABILITY_LEVELS.md) для определения уровней и [LEVEL_1_CASES.md](LEVEL_1_CASES.md) для текущего regression set.

Публичный baseline нацелен на **Level 1** и включает такие runtime operation kinds:
- `replace_point`
- `repeal_point`
- `insert_point`
- `append_section_item`
- `replace_phrase_globally`
- `replace_appendix_block`

Дополнительные фундаментальные элементы уже есть в коде, но еще не подключены end-to-end в runtime:
- `DocumentGraph` в `document_graph.py`
- typed `operation_registry.py`
- `CandidateLedger` в `candidate_ledger.py`
- `post_operation_validation.py`
- schema fields для structured replacements

## Важные runtime-заметки

### Analysis gate

Текущий runtime все еще использует legacy-поведение analysis gate:
1. сравнение количества regex-директив с количеством извлеченных intents;
2. одна repair-попытка через `repair_analyze`;
3. падение run, если coverage все еще недостаточен.


### Service tables

Service tables сейчас вставляются более чем в одном месте:
- во время skeleton build в `skeleton_builder.py`;
- после финального re-analysis в `run_case.py`;
- и логика вставки revision tables также есть в `editor_v2.py`.

Single-pass insertion service tables пока остается cleanup-задачей, а не текущим as-built поведением.

### Validator

`validator.py` сочетает:
1. structural checks для service tables;
2. hard resolution failure logic;
3. deterministic intent checks;
4. optional LLM judge.

Если LLM judge падает, но deterministic checks проходят, validator может повысить результат до valid только при отсутствии hard resolution failure.

## Определение структуры base document

`base_analyzer.py` сейчас определяет:
- верхний document header;
- стандартные appendix headers, начинающиеся с `Приложение`;
- appendix-like headers по форматированию: right-aligned approval block, за которым идет centered title block.


## Demo topology

Текущая Colab demo поддерживает:
- `standard_single`
- последовательный multi-amendment flow для одного base document

Multi-base topology (`special_single_amendment_multi_base`) остается вне demo в процессе доработки.

