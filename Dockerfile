FROM python:3.14-slim-trixie

RUN pip install requests inotify_simple

COPY app.py /app/app.py

CMD ["python", "/app/app.py"]
