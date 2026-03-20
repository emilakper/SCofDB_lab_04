"""Idempotency middleware template for LAB 04."""

import hashlib
import json
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from app.infrastructure.db import SessionLocal
from sqlalchemy import text
import json

class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Middleware для идемпотентности POST-запросов оплаты.
    """

    def __init__(self, app, ttl_seconds: int = 24 * 60 * 60):
        super().__init__(app)
        self.ttl_seconds = ttl_seconds

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.method != "POST" or not request.url.path.startswith("/api/payments/"):
            return await call_next(request)

        idempotency_key = request.headers.get("Idempotency-Key")

        if not idempotency_key:
            return await call_next(request)

        raw_body = await request.body()
        request_hash = self.build_request_hash(raw_body)

        async with SessionLocal() as session:
            try:
                result = await session.execute(
                    text("""
                    SELECT status, request_hash, status_code, response_body
                    FROM idempotency_keys
                    WHERE idempotency_key = :key
                    AND request_method = :method
                    AND request_path = :path
                    FOR UPDATE
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
                        await session.rollback()
                        return Response(
                            content=json.dumps({"error": "Idempotency-Key переиспользуется с другим payload"}),
                            status_code=409,
                            media_type="application/json",
                        )

                    if status == "completed":
                        await session.commit()
                        return Response(
                            content=json.dumps(response_body) if response_body else "{}",
                            status_code=status_code,
                            headers={"X-Idempotency-Replayed": "true"},
                            media_type="application/json",
                        )
                    
                    if status == "processing":
                        await session.commit()
                        return Response(
                            content=json.dumps({"error": "Запрос уже обрабатывается"}),
                            status_code=409,
                            media_type="application/json",
                        )
                else:
                    await session.execute(
                        text("""
                        INSERT INTO idempotency_keys
                        (idempotency_key, request_method, request_path, request_hash, status, expires_at)
                        VALUES (:key, :method, :path, :hash, 'processing', NOW() + INTERVAL '1 day')
                        """),
                        {
                            "key": idempotency_key,
                            "method": request.method,
                            "path": request.url.path,
                            "hash": request_hash,
                        },
                    )
                    await session.commit()

            except Exception:
                await session.rollback()
                return await call_next(request)

        response = await call_next(request)

        try:
            response_body = b""
            async for chunk in response.body_iterator:
                response_body += chunk

            body_json = None
            if response_body:
                try:
                    body_json = json.loads(response_body.decode())
                except:
                    body_json = {"raw": response_body.decode()}

            async with SessionLocal() as update_session:
                await update_session.execute(
                    text("""
                    UPDATE idempotency_keys
                    SET
                        status = 'completed',
                        status_code = :status_code,
                        response_body = CAST(:response_body AS jsonb)
                    WHERE idempotency_key = :key
                    AND request_method = :method
                    AND request_path = :path
                    """),
                    {
                        "status_code": response.status_code,
                        "response_body": json.dumps(body_json) if body_json else None,
                        "key": idempotency_key,
                        "method": request.method,
                        "path": request.url.path,
                    },
                )
                await update_session.commit()

        except Exception:
            pass

        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

    @staticmethod
    def build_request_hash(raw_body: bytes) -> str:
        """Стабильный хэш тела запроса."""
        return hashlib.sha256(raw_body).hexdigest()