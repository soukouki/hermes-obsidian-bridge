FROM python:3.14-slim-trixie

RUN pip install requests inotify_simple sseclient-py pyyaml

COPY app.py /app/app.py

CMD ["python", "/app/app.py"]
