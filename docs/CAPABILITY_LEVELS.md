# Capability Levels

Пайплайн использует трехуровневую capability-модель. Текущий публичный baseline сфокусирован на **Level 1**, а Level 2/3 находится в разработке под демонстрацию.

См. [LEVEL_1_CASES.md](LEVEL_1_CASES.md) для текущего regression set и [CAPABILITY_BACKLOG.md](CAPABILITY_BACKLOG.md) для открытых задач Level 2/3.

---

## Level 1: текущий публичный baseline

### Runtime-supported operation kinds

| Тип операции | Типичный паттерн директивы |
|---|---|
| `replace_point` | изложить пункт ... в новой редакции |
| `repeal_point` | признать пункт ... утратившим силу |
| `insert_point` | дополнить пунктом ... |
| `append_section_item` | дополнить пункт абзацем / подпунктом |
| `replace_phrase_globally` | слова "..." заменить словами "..." / слова "..." исключить |
| `replace_appendix_block` | изложить приложение ... в редакции приложения ... |

### Практический профиль Level 1

Кейс относится к Level 1, если одновременно верно следующее:
- один base document или последовательная цепочка amendments к одному base;
- нет строк внутри таблицы, structured entries, картинки или формулы как target amendment;
- target scope структурно находим по point reference, номер пункта или однозначному приложению;
- `old_text` либо не требуется для operation, либо проходит через текущую нормализацию;
- кейс входит в фактически подтвержденный regression set, а не только формально подходит под theoretical operation catalog.

### Текущий regression baseline

Текущий стабильный Level 1 regression set:

`13, 16, 24, 25, 27, 28, 30`


### Текущее runtime-поведение

В baseline все еще есть несколько legacy-особенностей, которые важно учитывать:
- analysis gate все еще hard-fail после одной repair-попытки;
- service tables все еще вставляются в нескольких местах runtime path;
- validator может повысить failed LLM-judge до valid только если deterministic checks прошли и нет hard resolution failure;
- post-operation validation существует как helper, но еще не включен как обязательный per-operation runtime gate.

Поэтому Level 1 сейчас означает: **поддерживается и подтвержден regression set**, а не "все структурно простые кейсы уже решены".

### Известные ограничения Level 1

| Ограничение | Статус |
|---|---|
| Multi-base topology в Colab demo | исключено из demo |
| `old_text`, разрезанный сложными OOXML runs | только частичная нормализация |
| Structured replacements по rows/positions | schema и normalization уже есть, runtime wiring неполный |
| Неоднозначные повторяющиеся point scopes | все еще backlog resolver |

---

## Level 2: структурная сложность

Level 2 покрывает паттерны, где основной блокер связан со структурой, scope disambiguation или более богатой моделью target, а не с отсутствием базового пайплайна.

Типичные категории:
- повторяющиеся point numbers в разных scopes;
- nested subpoints с неоднозначным parent scope;
- неоднозначный `old_text` с несколькими occurrences;
- structured replacements строк, позиций или table entries;
- appendix/table-heavy replacements, которым нужен более точный node-level targeting.

В коде уже есть некоторые foundations для Level 2:
- `document_graph.py`
- `operation_registry.py`
- `candidate_ledger.py`
- `post_operation_validation.py`
- structured replacement normalization в `amendment_analyzer.py`

Но все это пока подключено к runtime только частично.

---

## Level 3: mixed / high-risk cases

Level 3 покрывает кейсы, где даже корректного structural targeting недостаточно без более сильной orchestration или явного review.

Типичные категории:
- несколько heterogeneous operations в одном amendment document;
- крупные приложения с переписываемыми таблицами;
- конфликтующие последовательности изменений;
- неполные или внутренне неоднозначные директивы;
- content с медиа (изображения, таблицы, формулы).

Для таких кейсов нужны richer resolver evidence, более сильные post-operation checks и, вероятно, review-oriented graph path.

---

## Capability classifier

Отдельный capability classifier **еще не реализован в текущем репо**, в локальной демостнационной версии через Gradio поведение стабильное на классификации кейса.

Сейчас routing по сути ручной:
- Level 1 использует публичный baseline и regression set;
- Level 2/3 дорабатываются.

Ожидаемый future classifier output:

`level_1 | level_2 | level_3 | unsupported`
