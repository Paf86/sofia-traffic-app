# Използваме официалния Python имидж на Render
FROM render/python:3

# Инсталираме системния пакет unrar
RUN apt-get update && apt-get install -y unrar

# Задаваме работната директория
WORKDIR /app

# Копираме файла с изискванията
COPY requirements.txt .

# Инсталираме Python библиотеките
RUN pip install -r requirements.txt

# Копираме всички останали файлове от проекта
COPY . .

# Командата за стартиране на сървъра
CMD ["gunicorn", "server:app"]
