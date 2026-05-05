# Deterministic Intent And Revision Marker Pipeline Report

Дата: 2026-05-05

Ветка: `deterministic-intent-extraction`

Базовая ветка для merge: `master`

## Цель

Проверить архитектурную гипотезу: надежные простые правки можно извлекать и применять детерминированно, обходя LLM на этапе intent extraction, но сохраняя LLM/validator-проверку в конце. Для сложных директив pipeline должен оставлять LLM-компонент, а применение изменений должно оставаться единым общим этапом.

Отдельная гипотеза по служебным таблицам и пометкам: служебные пометки Консультант не должны вставляться во время structural edit, потому что это ломает порядок абзацев, нумерацию и индексы строк. Их нужно вставлять отдельным post-pass после завершения всех структурных изменений.

## Архитектурный результат

Итоговый подход:

1. Amendment analyzer получает текст изменяющего документа.
2. Deterministic extractor предварительно извлекает только надежные `ChangeIntent`.
3. Сложные или неоднозначные директивы остаются для LLM extraction/repair.
4. Resolver собирает общий список `ResolvedOperation`.
5. `EditorV2` применяет все structural edits без вставки служебных пометок.
6. `RevisionMarkerInserter` отдельным post-pass вставляет служебные пометки после финальной структуры документа.
7. Validator выполняет финальную проверку результата.
8. Golden/reference DOCX используется только как dev-only инструмент отладки качества, не как production runtime dependency.

Это не полноценный pipeline graph в смысле графового orchestration engine. Более точное название текущего результата: staged structural amendment pipeline with deterministic intent pre-extraction and revision marker post-pass.

## Что было реализовано

### Deterministic intent extraction

Добавлен deterministic extractor для надежных директив:

- `repeal_point`;
- удаление явно процитированных слов/фраз;
- замена явно процитированных слов/фраз.

Ограничение намеренное: extractor не пытается решать сложные структурные директивы, где есть добавление абзацев, перенумерация, вложенность, несколько объектов изменения или неоднозначная область применения. Такие директивы остаются LLM-компоненту.

### Интеграция deterministic extraction перед LLM

`AmendmentAnalyzer` теперь запускает deterministic extraction до LLM и объединяет результаты. Это снижает риск пропуска простых директив, но не отключает LLM для сложных случаев.

Для case 04 это проявилось так: первичный LLM-анализ стабильно возвращал только 7 intents, затем repair доводил до 14. Deterministic слой помогает закрывать надежные простые операции, но текущий case 04 все еще зависит от repair для полного покрытия сложных директив.

### Deduplication scoped phrase intents

Была найдена проблема: детерминированное извлечение фраз могло создавать дубли для scoped phrase operations. Это приводило к повторным операциям и потенциально повторным пометкам.

Исправление: добавлена дедупликация scoped phrase intents, чтобы одна директива не превращалась в несколько одинаковых structural operations.

### Service-layer revision markers

Добавлен отдельный слой:

- `ConsultantMarkerFormatter`;
- `RevisionMarkerInserter`.

Structural editor больше не вставляет `note_text` и marker-параграфы во время изменения документа. Он возвращает примененные операции и финальные якоря, а marker inserter работает уже по финальному DOCX.

### Consultant marker formatting

Исправлено форматирование пометок:

- замена подпункта: `(пп. "б" в ред. Приказа ...)`;
- добавление подпункта: `(пп. "е(1)" введен Приказом ...)`;
- добавление абзаца: `(абзац введен Приказом ...)`;
- замена отдельного абзаца внутри подпункта: `(в ред. Приказа ...)`;
- `repeal_point` не получает отдельную marker-пометку, потому что `утратил силу. - Приказ ...` является самим текстом структурной правки.

Также formatter перестал доверять legacy `note_text`, потому что LLM/старый resolver могли генерировать не-Консультант-совместимые варианты вроде `(подп. б п. 2 в ред. ...)`.

### Final anchor tracking

Была найдена ключевая причина поломок: операции, примененные раньше, сохраняли индексы абзацев на момент своего применения. После более поздних вставок эти индексы становились stale, и post-pass вставлял пометки не туда.

Исправление:

- `EditorV2` теперь мутирует `paragraph_indices` операции на фактический индекс измененного или вставленного абзаца;
- после поздних вставок/удалений editor пересчитывает anchors уже примененных операций;
- `applied_operations` теперь отражает финальные индексы после всех structural edits, а не промежуточные индексы.

### Разделение нового подпункта и нового абзаца внутри подпункта

На case 04 была найдена общая ошибка: директива `дополнить подпункт "в" абзацем` попадала в тот же путь, что и `дополнить подпунктом "е(1)"`. В результате новый абзац вставлялся в неправильное место и получал неправильную пометку.

Исправление:

- если `append_section_item.new_item_text` начинается с target subpoint marker, это новый подпункт;
- если текст не начинается с marker подпункта, это новый абзац внутри существующего подпункта;
- для второго случая anchor выбирается как конец существующего подпункта, а не конец предыдущего подпункта;
- операция получает `paragraph_ordinal`, чтобы marker formatter выбрал `(абзац введен ...)`.

### Нормализация пунктуации LLM-текста

Были найдены дефекты:

- `;;` в конце замененного подпункта;
- `.".` в конце добавленного абзаца.

Причина: LLM/repair иногда возвращал текст с уже добавленной пунктуацией, а structural edit дополнительно сохранял или усиливал ошибочную концовку.

Исправление: добавлена минимальная нормализация terminal punctuation для item text:

- `;;` -> `;`;
- `.".` -> `.`.

Правило общее, не привязано к case 04.

### Marker placement for phrase replacements

Была найдена ошибка с generic markers для `replace_phrase_globally`: marker вставлялся после конкретного абзаца, где заменена фраза, хотя Консультант в некоторых случаях ставит marker после всего структурного блока.

Исправление:

- для `replace_phrase_globally` marker inserter расширяет anchor до конца структурного блока;
- если несколько phrase replacements дают одну и ту же generic marker-пометку в одном блоке, вставляется одна пометка.

### Служебные таблицы

После совпадения текста оставалось `table_equal=False`.

Причина: `format_revision_reference()` принудительно делал первую букву lower-case:

- было: `(в ред. приказа Минфина России ...)`;
- эталон Консультант: `(в ред. Приказа Минфина России ...)`.

Исправление: удалено принудительное lower-case; добавлен тест на сохранение регистра типа документа.

## Что ломалось на case 04

Исходные наблюдения по сравнению с эталоном:

- structural validation проходила, но DOCX отличался от эталона;
- часть markers вставлялась по stale paragraph indices;
- `repeal_point` получал лишнюю отдельную пометку;
- replacement markers использовали legacy note text;
- append markers для некоторых операций отсутствовали из-за пустых или устаревших anchors;
- у некоторых замен появлялись `;;`;
- добавленный абзац внутри подпункта вставлялся как подпункт/элемент списка не в том месте;
- служебная таблица отличалась регистром `приказа` vs `Приказа`.

## Причины поломок

1. Смешение structural edit и marker insertion.
   Когда служебные пометки вставляются во время structural edit, они меняют paragraph order и сдвигают индексы для следующих операций.

2. Stale anchors.
   Даже после выноса markers в post-pass операции сохраняли индексы на момент применения, а не финальные индексы после всех последующих вставок.

3. Недостаточное различение типов `append_section_item`.
   Pipeline не различал добавление нового подпункта и добавление нового абзаца внутрь существующего подпункта.

4. Доверие legacy `note_text`.
   Старые notes могли быть полезны как fallback, но не гарантировали формат Консультант.

5. LLM/repair punctuation noise.
   LLM мог вернуть текст с лишней терминальной пунктуацией.

6. Неунифицированное форматирование service table references.
   Табличный formatter имел собственное правило lower-case, отличное от эталона Консультант.

## Проверки

Unit/integration suite:

```text
python -m pytest tests
46 passed
```

Case 04 pipeline:

```text
python -m graph_pipeline.cli run-case --case-id 04 ...
Валидация: OK
structural_ok=True
judge_ok=True
is_valid=True
```

Dev-only DOCX comparison against Consultant reference:

```text
diff_blocks=0
result_paragraphs=85
reference_paragraphs=85
table_equal=True
changes=0
```

Эталонный документ использовался только как внутренняя метрика отладки. В production pipeline сравнения с эталоном и golden test нет.

## Измененные компоненты

- `src/graph_pipeline/deterministic_intent_extractor.py`
- `src/graph_pipeline/amendment_analyzer.py`
- `src/graph_pipeline/editor_v2.py`
- `src/graph_pipeline/revision_markers.py`
- `src/graph_pipeline/run_case.py`
- `src/graph_pipeline/graph_pipeline.py`
- `src/graph_pipeline/utils.py`
- `tools/eval_compare_docx.py`

Тесты:

- `tests/unit/test_deterministic_intent_extractor.py`
- `tests/unit/test_revision_markers.py`
- `tests/unit/test_docx_eval_tool.py`
- `tests/unit/test_editor_v2_drift.py`
- `tests/unit/test_service_table_revision_reference.py`

## Коммиты ветки

- `120246e Add deterministic intent extraction experiment`
- `27985f1 Deduplicate scoped phrase intents`
- `47a589a Separate revision marker service layer`
- `b7ebd08 Refine revision marker anchoring`

## Ограничения и дальнейшая работа

1. Deterministic extractor должен оставаться консервативным.
   Его нельзя расширять на сложные директивы без отдельной валидации, иначе он начнет делать уверенные, но неправильные structural assumptions.

2. Case 04 все еще показывает зависимость от analysis repair.
   Первичный LLM extraction возвращал 7 intents, repair доводил до 14. Это отдельная зона улучшения prompts/extraction contract.

3. Marker formatting нужно расширять по корпусу кейсов.
   Текущие правила покрыли case 04 и близкие структурные паттерны, но не исчерпывают все стили Консультант.

4. Service tables должны оставаться отдельной стадией.
   Их нельзя смешивать со structural edit, иначе снова появятся сдвиги paragraph indices.

5. Golden/reference comparison остается dev-only.
   Нельзя вносить case-specific hardcode ради совпадения с эталоном. Допускаются только структурные правила, применимые к аналогичным кейсам, или отдельные prompt rules.
