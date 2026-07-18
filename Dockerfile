FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] pydantic pydantic-settings httpx requests python-multipart \
    sqlalchemy "python-jose[cryptography]" "passlib[bcrypt]" "bcrypt==4.0.1" stripe email-validator psycopg2-binary

COPY main.py .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
