# Capability Backlog

Этот файл фиксирует задачи Level 2/3, которые остаются открытыми в текущем публичном baseline.


См. [CAPABILITY_LEVELS.md](CAPABILITY_LEVELS.md) для определения уровней и [LEVEL_1_CASES.md](LEVEL_1_CASES.md) для текущего regression set.

---

## Внедренные элементы

Эти элементы уже есть в кодовой базе:

- appendix-like header detection в `base_analyzer.py`:
  right-aligned approval block + centered title block;
- `DocumentGraph` в `document_graph.py`;
- typed operation registry в `operation_registry.py`;
- `CandidateLedger` в `candidate_ledger.py`;
- `post_operation_validation.py`;
- structured replacement normalization в `amendment_analyzer.py`:
  часть `replace_phrase_globally` intents перекидывается в
  `replace_table_row` / `replace_structured_entry`;
- schema support для `structured_entry_ref`, `table_row_ref`,
  `table_column_ref`, `graph_scope_hint` и operation `metadata`.

Они еще не полностью подключены через resolver, editor, validator и end-to-end tests.

---

## Открытые задачи Level 2

### L2-01: повторяющиеся point numbers в разных scopes
- Pattern: point `5` существует в основном тексте и в одном или нескольких appendices.
- Current blocker: resolver все еще склонен выбирать первый правдоподобный candidate без явной фиксации evidence.
- Needed next: wiring candidate ledger и scope-aware candidate ranking.

### L2-02: nested subpoints с неоднозначным parent scope
- Pattern: amendment target'ит subpoint `а` у point `3`, но point `3` существует в нескольких scopes.
- Current blocker: текущая scope filtering слишком зависит от явно извлеченных appendix/scope hints.
- Needed next: hierarchical candidate search с явным scope evidence.

### L2-03: неоднозначный `old_text`
- Pattern: фраза для замены встречается в нескольких paragraphs или scopes.
- Current blocker: `replace_phrase_globally` пока слишком широк и может менять больше intended location.
- Needed next: occurrence counting перед apply, scoped phrase replacement и post-edit checks на collateral changes.

### L2-04: structured row / entry replacement
- Pattern: директивы вида `строку ... изложить ...` или `позицию "010" изложить ...`.
- Current state:
  - analyzer-side normalization уже есть;
  - operation kinds зарегистрированы;
  - schema fields уже есть;
  - `DocumentGraph.find_table_rows_by_ref` уже есть.
- Missing:
  - resolver routing для `replace_table_row` / `replace_structured_entry`;
  - editor apply methods;
  - end-to-end tests и fixtures.

### L2-05: semantic ranking для ambiguous candidates
- Pattern: lexical candidate set в целом правильный, но слишком широкий.
- Current state: `semantic_embeddings.py` существует, но baseline config держит semantic ranking выключенным.
- Needed next: integration в resolver, controlled thresholds и integration tests.

### L2-06: table-heavy appendix replacement
- Pattern: appendix rewrite, где replacement content содержит tables или row-structured payloads.
- Current blocker: `replace_appendix_block` пока ориентирован на paragraphs.
- Needed next: richer structured payload handling и table-aware replacement strategy.

---

## Открытые задачи Level 3

### L3-01: mixed amendment documents
- Pattern: несколько heterogeneous operation kinds в одном amendment document.
- Current blocker: текущий analyzer/resolver/validator flow пока настроен на более простые и разделимые directives.

### L3-02: конфликтующие amendment chains
- Pattern: более поздний amendment частично отменяет или противоречит более раннему в рамках одного run.
- Current blocker: отсутствует явная conflict detection phase между resolved operations.

### L3-03: images, drawings и formulae как amendment targets
- Pattern: amended content содержит `w:drawing` или эквивалентные embedded structures.
- Current blocker: editor logic ориентирован на text/paragraph/table.

### L3-04: review-oriented orchestration
- Pattern: кейсы, где pipeline должен сохранять partial work и эскалировать, а не hard-fail.
- Current blocker: публичный baseline все еще использует hard analysis gating и не имеет review-state graph route.

---

## Кейсы вне текущего Level 1 regression set

Эти кейсы не отброшены; это активный engineering backlog:

- Level 2 cluster:
  `01, 02, 03, 06, 07, 08, 09, 10, 11, 12, 14, 15, 17, 19, 20, 22, 23, 26`
- Level 3 cluster:
  `04, 05, 18, 21, 29`

Текущий публичный smoke set остается таким:

`13, 16, 24, 25, 27, 28, 30`
