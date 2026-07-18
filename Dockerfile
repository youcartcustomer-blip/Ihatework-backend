FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] pydantic pydantic-settings httpx requests \
    sqlalchemy "python-jose[cryptography]" "passlib[bcrypt]" stripe email-validator

COPY main.py .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
