FROM python:3.10-slim

WORKDIR /app

# Установка системных зависимостей Playwright
RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates fonts-liberation libnss3 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libdrm2 libxcomposite1 libxdamage1 libxrandr2 \
    libxkbcommon0 libxshmfence1 libgbm1 libgtk-3-0 libasound2 libx11-xcb1 libx11-6 \
    libxcb1 libxext6 libxfixes3 libxrender1 libxtst6 libexpat1 libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Установка Python-зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Установка Playwright Chromium
RUN playwright install

# Копируем весь код
COPY . .

CMD ["python", "run_all.py"]
