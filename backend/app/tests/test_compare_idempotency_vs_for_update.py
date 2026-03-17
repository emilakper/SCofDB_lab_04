"""
LAB 04: Сравнение подходов
1) FOR UPDATE (решение из lab_02)
2) Idempotency-Key + middleware (lab_04)

Цель:
Продемонстрировать различия в поведении двух подходов к защите от повторных платежей.
"""

import pytest
import uuid
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from app.main import app
from app.application.payment_service import PaymentService

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture(scope="module")
async def test_engine():
    """
    Создать AsyncEngine для тестов.
    """
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20
    )
    yield engine
    await engine.dispose()


@pytest.fixture
async def test_order(test_engine):
    """
    Создать тестовый заказ со статусом 'created'.
    """
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()
    
    async with AsyncSession(test_engine) as setup_session:
        async with setup_session.begin():
            await setup_session.execute(
                text("""
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:user_id, :email, :name, NOW())
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "user_id": user_id,
                    "email": f"compare_{order_id}@example.com",
                    "name": "Compare Test"
                }
            )
            
            await setup_session.execute(
                text("""
                    INSERT INTO orders (id, user_id, status, total_amount, created_at)
                    VALUES (:order_id, :user_id, 'created', 100.00, NOW())
                """),
                {"order_id": order_id, "user_id": user_id}
            )

            await setup_session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
                {"order_id": order_id}
            )

    yield order_id

    async with AsyncSession(test_engine) as cleanup_session:
        async with cleanup_session.begin():
            await cleanup_session.execute(
                text("DELETE FROM order_status_history WHERE order_id = :order_id"),
                {"order_id": order_id}
            )
            await cleanup_session.execute(
                text("DELETE FROM orders WHERE id = :order_id"),
                {"order_id": order_id}
            )
            await cleanup_session.execute(
                text("DELETE FROM users WHERE id = :user_id"),
                {"user_id": user_id}
            )


@pytest.fixture
async def cleanup_idempotency_keys(test_engine):
    """
    Очистить таблицу idempotency_keys перед тестом.
    """
    async with AsyncSession(test_engine) as session:
        await session.execute(text("DELETE FROM idempotency_keys"))
        await session.commit()
    yield


@pytest.mark.asyncio
async def test_compare_for_update_and_idempotency_behaviour(test_engine, test_order, cleanup_idempotency_keys):
    """
    Тест сравнивает поведение двух подходов:
    
    1) FOR UPDATE (режим 'for_update') - блокировка на уровне БД
    2) Idempotency-Key (режим 'unsafe' + заголовок) - кэширование на уровне API
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    - FOR UPDATE: второй запрос возвращает ошибку (409 Conflict)
    - Idempotency-Key: второй запрос возвращает кэшированный ответ (200 OK)
    - В обоих случаях заказ оплачен только один раз
    """
    
    order_for_update = test_order
    order_idempotent = await create_separate_order(test_engine, "idempotent")
    
    async with AsyncClient(app=app, base_url="http://test") as client:
        
        print("ТЕСТИРОВАНИЕ FOR UPDATE ПОДХОДА")
        
        payload_for_update = {
            "order_id": str(order_for_update),
            "mode": "for_update"
        }

        response1_for_update = await client.post(
            "/api/payments/retry-demo", 
            json=payload_for_update
        )

        data1 = response1_for_update.json()
        
        assert response1_for_update.status_code == 200
        assert data1.get("success") == True

        response2_for_update = await client.post(
            "/api/payments/retry-demo", 
            json=payload_for_update
        )

        data2 = response2_for_update.json()
        
        assert response2_for_update.status_code == 200
        assert data2.get("success") == False
        assert "already paid" in data2.get("message", "").lower()

        print("ТЕСТИРОВАНИЕ IDEMPOTENCY-KEY ПОДХОДА")

        payload_idempotent = {
            "order_id": str(order_idempotent),
            "mode": "unsafe"
        }

        headers = {"Idempotency-Key": "compare-key-456"}

        response1_idempotent = await client.post(
            "/api/payments/retry-demo",
            json=payload_idempotent,
            headers=headers
        )

        data1_idem = response1_idempotent.json()
        
        assert response1_idempotent.status_code == 200
        assert data1_idem.get("success") == True

        response2_idempotent = await client.post(
            "/api/payments/retry-demo",
            json=payload_idempotent,
            headers=headers
        )

        data2_idem = response2_idempotent.json()
        
        assert response2_idempotent.status_code == 200
        assert data2_idem.get("success") == True
        assert response2_idempotent.headers.get("X-Idempotency-Replayed") == "true"

    async with AsyncSession(test_engine) as session:
        service = PaymentService(session)

        history_for_update = await service.get_payment_history(order_for_update)
        
        history_idempotent = await service.get_payment_history(order_idempotent)

    assert len(history_for_update) == 1, \
        f"FOR UPDATE: ожидалась 1 оплата, получено {len(history_for_update)}"
    assert len(history_idempotent) == 1, \
        f"Idempotency-Key: ожидалась 1 оплата, получено {len(history_idempotent)}"

    print("РЕЗУЛЬТАТЫ СРАВНЕНИЯ")
    
    print("\n   FOR UPDATE подход:")
    print(f"  - Первый запрос: УСПЕХ (success=True)")
    print(f"  - Второй запрос: ОШИБКА (success=False, сообщение: {data2.get('message')})")
    print(f"  - Записей об оплате: {len(history_for_update)}")
    
    print("\n   Idempotency-Key подход:")
    print(f"  - Первый запрос: УСПЕХ (success=True)")
    print(f"  - Второй запрос: УСПЕХ (success=True, из кэша)")
    print(f"  - Заголовок X-Idempotency-Replayed: {response2_idempotent.headers.get('X-Idempotency-Replayed')}")
    print(f"  - Записей об оплате: {len(history_idempotent)}")

    print("\n" + "="*50)
    print("""
    FOR UPDATE (решение из lab_02):
    - Защищает от race condition на уровне БД
    - Блокирует строку при первом запросе
    - Второй запрос видит, что заказ уже оплачен и возвращает бизнес-ошибку
    - HTTP статус 200, но success=False в теле ответа
    - Подходит для защиты от конкурентных запросов

    Idempotency-Key + middleware (lab_04):
    - Защищает от повторных запросов из-за сетевых проблем
    - Кэширует ответ первого запроса
    - Второй запрос возвращает тот же результат без повторной оплаты
    - HTTP статус 200, success=True, заголовок X-Idempotency-Replayed
    - Подходит для защиты от retry клиента

    ВЫВОД:
    Эти подходы решают РАЗНЫЕ задачи и могут использоваться вместе:
    - FOR UPDATE → защита от race condition (конкурентные запросы)
    - Idempotency-Key → защита от сетевых retry (повторные запросы клиента)
    """)


async def create_separate_order(engine, suffix):
    """
    Вспомогательная функция для создания отдельного заказа.
    Используется когда нужно создать второй заказ внутри теста.
    """
    user_id = uuid.uuid4()
    order_id = uuid.uuid4()
    
    async with AsyncSession(engine) as setup_session:
        async with setup_session.begin():
            await setup_session.execute(
                text("""
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:user_id, :email, :name, NOW())
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "user_id": user_id,
                    "email": f"compare_{suffix}_{order_id}@example.com",
                    "name": f"Compare Test {suffix}"
                }
            )
            
            await setup_session.execute(
                text("""
                    INSERT INTO orders (id, user_id, status, total_amount, created_at)
                    VALUES (:order_id, :user_id, 'created', 100.00, NOW())
                """),
                {"order_id": order_id, "user_id": user_id}
            )

            await setup_session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
                {"order_id": order_id}
            )
    
    return order_id


if __name__ == "__main__":
    """
    Запуск теста:
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_compare_approaches.py -v -s

    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    test_compare_for_update_and_idempotency_behaviour PASSED
    """
    pytest.main([__file__, "-v", "-s"])