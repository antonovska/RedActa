# Agents SDK Pipeline

Пайплайн для создания документов в формате `.docx`.

Публичная демо-версия рассчитана на Google Colab с GPU T4. Демо использует нативную загрузку и скачивание файлов в Colab.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/appankratova/RedActa/blob/main/notebooks/demo.ipynb)

## Статус

Это production-baseline срез для Colab demo. В него входят core package, CLI, примеры конфигов моделей, scripts, tests и demo notebook.

Текущая демо-версия рассчитана на простые кейсы уровня Level 1. Более сложные структуры документов остаются в активной доработке.

## Требования

- Python 3.11+
- Ollama с локальным OpenAI-compatible endpoint
- Локальная chat-модель, доступная через Ollama
- `python-docx`
- `openai`
- `langgraph`
- `langchain-core`

Установка локальных зависимостей:

```powershell
python -m pip install -e .
```

Или:

```powershell
python -m pip install -r requirements.txt
```

## Конфигурация

Примеры конфигов:

- `config/models.example.json` для локальных запусков.
- `config/models.colab.json` для Colab demo.

Config loader ожидает runtime-поля модели внутри `runtime`:

```json
{
  "runtime": {
    "base_url": "http://127.0.0.1:11434/v1",
    "api_key": "ollama",
    "timeout": 900,
    "semantic_ranking_enabled": false
  },
  "models": {
    "default": "hf.co/ai-sage/GigaChat3.1-10B-A1.8B-GGUF:Q4_K_M",
    "analyst": "hf.co/ai-sage/GigaChat3.1-10B-A1.8B-GGUF:Q4_K_M",
    "resolver_disambiguation": "hf.co/ai-sage/GigaChat3.1-10B-A1.8B-GGUF:Q4_K_M",
    "validator": "hf.co/ai-sage/GigaChat3.1-10B-A1.8B-GGUF:Q4_K_M"
  }
}
```

Semantic ranking по умолчанию отключён в Colab config. Если включать его позже, потребуется отдельный локальный embedding service.

## CLI

Показать справку:

```powershell
python -m graph_pipeline.cli --help
```

Запустить один кейс из workspace, где есть `redactions/<case-id>/`:

```powershell
python -m graph_pipeline.cli run-case --case-id 28 --workspace-root . --models-config config/models.example.json --output-json artifacts/28/result.json
```

Запустить несколько кейсов:

```powershell
python -m graph_pipeline.cli run-batch --case-id 13 --case-id 14 --workspace-root . --models-config config/models.example.json --output-json artifacts/batch.json
```

Также доступны PowerShell wrappers:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_case.ps1 -CaseId 28 -ModelsConfig config/models.example.json -OutputJson artifacts/28/result.json
powershell -ExecutionPolicy Bypass -File scripts/run_batch.ps1 -CaseId 13,14 -ModelsConfig config/models.example.json -OutputJson artifacts/batch.json
```

## Colab Demo

Откройте `notebooks/demo.ipynb` в Google Colab.

Notebook выполняет следующие шаги:

1. Устанавливает зависимости.
2. Клонирует репозиторий.
3. Устанавливает и запускает Ollama.
4. Загружает модель `hf.co/ai-sage/GigaChat3.1-10B-A1.8B-GGUF:Q4_K_M`.
5. Загружает один базовый `.docx` и один документ изменений `.docx`.
6. Запускает пайплайн через `graph_pipeline.colab_runner.run_uploaded_pair`.
7. Скачивает итоговый `.docx`.

Notebook использует только:

- `google.colab.files.upload`
- `google.colab.files.download`
- stdout logs
- optional `ipywidgets` buttons

## Структура входных файлов

Для локальных CLI-запусков workspace должен содержать:

```text
redactions/
  <case-id>/
    <amendment>.docx
    <base>.docx
```

Сгенерированные файлы записываются в:

```text
artifacts/<case-id>/
```
