FROM python:3.9

# 1. Обновляем и ставим системные зависимости
RUN apt-get update && apt-get install -y wget libpq-dev gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Устанавливаем библиотеки (psycopg2-binary проще для Docker)
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

# 3. Копируем скрипт и SQL (убедитесь, что data.py есть в папке)
COPY data.py . 
COPY create_db.sql .

# 4. Запуск
ENTRYPOINT ["python", "data.py"]

