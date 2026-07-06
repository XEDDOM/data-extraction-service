# 🚀 Описание

Сервис реализует автоматизированный поиск директоров по ФИО или ИНН и извлекает информацию об организациях, которыми они руководят.

## 📦 Запуск проекта

``` bash
docker-compose up -d --build
```

## 🧹 Очистка окружения

``` bash
docker-compose down -v
```

## 🧪 Запуск тестов

``` bash
python -m venv venv
source venv/bin/activate  # Linux/MacOS
venv\Scripts\activate  # Windows
pip install -r requirements.txt
pytest -v
```