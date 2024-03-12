FROM python:3.9

WORKDIR /app
COPY pyproject.toml .
COPY server.py .
ENV PORT=8080
ENV HOST="0.0.0.0"
EXPOSE 8080

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root
    
CMD python server.py