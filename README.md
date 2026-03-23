# Kaspersky Test Backend

Сервис на FastAPI для асинхронного анализа больших `.txt` файлов:

- приводит слова к леммам (`pymorphy3`);
- считает частоту по документу;
- считает частоту по строкам;
- отдает отчет в `.xlsx`.

## Что реализовано

- `POST /public/report/export` — загрузка `.txt` + `target_lemma`, возврат `analysis_id`.
- `GET /public/report/{analysis_id}`:
  - если задача не завершена: JSON со статусом;
  - если завершена успешно: скачивание `.xlsx`.
- Очередь фоновой обработки с лимитом параллелизма (`MAX_CONCURRENT_JOBS`).
- SQLite-хранилище:
  - метаданные задач (`analyses`);
  - частоты лемм по документу (`word_totals`);
  - частоты лемм по строкам (`word_line_counts`).
- Checkpoint/resume: при перезапуске сервиса подхватываются незавершенные задачи.
- Запись результата строки и checkpoint выполняется атомарно (в одной транзакции).
- Повторная обработка уже сохраненной строки идемпотентна (без задвоения total).
- Retry policy: несколько попыток, затем статус `failed`.
- Автоочистка: после `success` исходный загруженный `.txt` удаляется.
- Логирование HTTP и фоновых задач.
- Swagger UI: `/docs`.
- Postman-коллекция: `postman_collection.json`.

## Структура проекта

`backend/interfaces` — HTTP-слой (роуты FastAPI).  
`backend/application` — orchestration фоновых задач и worker.  
`backend/infrastructure` — SQLite, парсер текста, генерация xlsx.  
`backend/config.py` — конфигурация из env.

## Требования

- Python 3.11+
- Установленные зависимости из `backend/requirements.txt`.

## Установка и запуск

```bash
cd kaspersky
python -m venv .venv
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
pip install -r backend/requirements.txt
uvicorn backend:app --reload --host 127.0.0.1 --port 8000
```

## API

### 1) Загрузка файла

`POST /public/report/export`

`multipart/form-data`:

- `file`: `.txt`
- `target_lemma`: целевая лемма для поиска (например `житель`)

Ответ:

```json
{
  "id": "f1d99ed8-4a1f-4d86-8d7c-0e4f8f7b4d20",
  "target_lemma": "житель"
}
```

Пример `curl`:

```bash
curl -X POST "http://127.0.0.1:8000/public/report/export" ^
  -H "accept: application/json" ^
  -H "Content-Type: multipart/form-data" ^
  -F "file=@sample.txt;type=text/plain" ^
  -F "target_lemma=житель"
```

Примечание: `target_lemma` нормализуется (обрезаются пробелы, приводится к нижнему регистру).

### 2) Получение статуса/отчета

`GET /public/report/{analysis_id}`

- Пока задача выполняется:

```json
{
  "id": "f1d99ed8-4a1f-4d86-8d7c-0e4f8f7b4d20",
  "status": "running",
  "error_message": null,
  "checkpoint_line": 12000,
  "total_lines": null
}
```

- После успеха: ответ — бинарный `.xlsx` (`Content-Disposition: attachment`).

Пример `curl` (скачивание):

```bash
curl -L "http://127.0.0.1:8000/public/report/<analysis_id>" -o report.xlsx
```

## Переменные окружения

- `MAX_CONCURRENT_JOBS` (по умолчанию `2`) — число одновременно выполняемых задач.
- `CHECKPOINT_EVERY_N_LINES` (по умолчанию `2000`) — частота checkpoint.
- `MAX_RETRIES` (по умолчанию `3`) — число повторных попыток обработки.
- `EXCEL_CELL_LIMIT` (по умолчанию `32767`) — лимит символов ячейки Excel.
- `TEXT_ENCODING` (по умолчанию `utf-8`) — кодировка входных txt.

## Формат Excel-отчета

Колонки:

1. `lemma`
2. `total_count`
3. `line_counts`

`line_counts` — строка с количеством по каждой строке документа, например:

`0,11,32,0,0,3`

Если `line_counts` превышает лимит Excel-ячейки, значение разбивается на несколько строк:

- в первой строке: `lemma`, `total_count`, первая часть `line_counts`;
- в последующих строках: `lemma`, пустой `total_count`, продолжение `line_counts`.

## Где хранятся данные

- Входные файлы: `backend/data/uploads/`
- SQLite: `backend/data/analysis.db`

Примечание:

- входные `.txt` сохраняются на диск для retry/resume;
- после успешного завершения анализа исходный `.txt` удаляется автоматически;
- при `paused/failed` файл остается на диске.

## Ограничения текущей версии

- Cleanup старых файлов/записей не автоматизирован.
- `checkpoint_offset` пока не используется как байтовый оффсет (resume идет по номеру строки).

## Быстрая проверка

1. Открыть `http://127.0.0.1:8000/docs`.
2. Загрузить `.txt` через `POST /public/report/export`.
3. Поллить `GET /public/report/{id}` до `success`.
4. Скачать `xlsx` и проверить колонки `lemma`, `total_count`, `line_counts`.

## Запуск тестов

```bash
cd kaspersky
.\.venv\Scripts\python -m pytest tests -q
```

Текущее покрытие: 6 тестов (API-валидации, worker по target_lemma, xlsx-формирование, cleanup файла).

## Проверка

1. Запустить сервис:

```bash
cd kaspersky
.\.venv\Scripts\Activate.ps1
uvicorn backend:app --reload --host 127.0.0.1 --port 8000
```

2. Открыть `http://127.0.0.1:8000/docs`.
3. В `POST /public/report/export` отправить:
   - `file`: `data/test/test_06.txt`
   - `target_lemma`: `житель`
4. Скопировать `id` из ответа.
5. Поллить `GET /public/report/{id}`:
   - пока идет обработка: JSON со статусом;
   - после `success`: скачать `.xlsx`.
6. Проверить отчет
