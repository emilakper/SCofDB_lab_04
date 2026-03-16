"""Idempotency middleware template for LAB 04."""

import hashlib
import json
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Middleware для идемпотентности POST-запросов оплаты.

    Идея:
    - Клиент отправляет `Idempotency-Key` в header.
    - Если запрос с таким ключом уже выполнялся для того же endpoint и payload,
      middleware возвращает кэшированный ответ (без повторного списания).
    """

    def __init__(self, app, ttl_seconds: int = 24 * 60 * 60):
        super().__init__(app)
        self.ttl_seconds = ttl_seconds

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        TODO: Реализовать алгоритм.

        Рекомендуемая логика:
        1) Пропускать только целевые запросы:
        - method == POST
        - path в whitelist для платежей
        2) Читать Idempotency-Key из headers.
        Если ключа нет -> обычный call_next(request)
        3) Считать request_hash (например sha256 от body).
        4) В транзакции:
        - проверить запись в idempotency_keys
        - если completed и hash совпадает -> вернуть кэш (status_code + body)
        - если key есть, но hash другой -> вернуть 409 Conflict
        - если ключа нет -> создать запись processing
        5) Выполнить downstream request через call_next.
        6) Сохранить response в idempotency_keys со статусом completed.
        7) Вернуть response клиенту.

        Дополнительно:
        - обработайте кейс конкурентных одинаковых ключей
        (уникальный индекс + retry/select existing).
        """

        if request.method != "POST":
            return await call_next(request)

        idempotency_key = request.headers.get("Idempotency-Key")

        if not idempotency_key:
            return await call_next(request)

        raw_body = await request.body()
        request_hash = self.build_request_hash(raw_body)

        from app.infrastructure.db import SessionLocal
        from sqlalchemy import text

        async with SessionLocal() as session:

            result = await session.execute(
                text("""
                SELECT status, request_hash, status_code, response_body
                FROM idempotency_keys
                WHERE idempotency_key = :key
                AND request_method = :method
                AND request_path = :path
                """),
                {
                    "key": idempotency_key,
                    "method": request.method,
                    "path": request.url.path,
                },
            )

            row = result.fetchone()

            if row:
                status, stored_hash, status_code, response_body = row

                if stored_hash != request_hash:
                    return Response(
                        content=json.dumps({"error": "Idempotency-Key reused with different payload"}),
                        status_code=409,
                        media_type="application/json",
                    )

                if status == "completed":
                    return Response(
                        content=json.dumps(response_body),
                        status_code=status_code,
                        headers={"X-Idempotency-Replayed": "true"},
                        media_type="application/json",
                    )

            else:
                await session.execute(
                    text("""
                    INSERT INTO idempotency_keys
                    (idempotency_key, request_method, request_path, request_hash, status)
                    VALUES (:key, :method, :path, :hash, 'processing')
                    """),
                    {
                        "key": idempotency_key,
                        "method": request.method,
                        "path": request.url.path,
                        "hash": request_hash,
                    },
                )

                await session.commit()

        response = await call_next(request)

        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        body_json = json.loads(response_body.decode()) if response_body else None

        async with SessionLocal() as session:
            await session.execute(
                text("""
                UPDATE idempotency_keys
                SET
                    status = 'completed',
                    status_code = :status_code,
                    response_body = :response_body
                WHERE idempotency_key = :key
                AND request_method = :method
                AND request_path = :path
                """),
                {
                    "status_code": response.status_code,
                    "response_body": body_json,
                    "key": idempotency_key,
                    "method": request.method,
                    "path": request.url.path,
                },
            )
            await session.commit()

        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

    @staticmethod
    def build_request_hash(raw_body: bytes) -> str:
        """Стабильный хэш тела запроса для проверки reuse ключа с другим payload."""
        return hashlib.sha256(raw_body).hexdigest()

    @staticmethod
    def encode_response_payload(body_obj) -> str:
        """Сериализация response body для сохранения в idempotency_keys."""
        return json.dumps(body_obj, ensure_ascii=False)