"""Сервис для демонстрации конкурентных оплат.
Этот модуль содержит два метода оплаты:
pay_order_unsafe() - небезопасная реализация (READ COMMITTED без блокировок)
pay_order_safe() - безопасная реализация (REPEATABLE READ + FOR UPDATE)
"""
import uuid
from typing import Optional
from sqlalchemy import text
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.domain.exceptions import OrderAlreadyPaidError, OrderNotFoundError


class PaymentService:
    """Сервис для обработки платежей с разными уровнями изоляции."""
    
    def __init__(self, session: AsyncSession):
        self.session = session

    async def pay_order_unsafe(self, order_id: uuid.UUID) -> dict:
        """
        НЕБЕЗОПАСНАЯ реализация оплаты заказа.
        Использует READ COMMITTED (по умолчанию) без блокировок.
        ЛОМАЕТСЯ при конкурентных запросах - может привести к двойной оплате!

        Args:
            order_id: ID заказа для оплаты
            
        Returns:
            dict с информацией о заказе после оплаты
            
        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        
        async with self.session.begin():
            result = await self.session.execute(
                text("SELECT status FROM orders WHERE id = :order_id"),
                {"order_id": order_id}
            )
            row = result.fetchone()

            if row is None:
                raise OrderNotFoundError(f"Order {order_id} not found")
            
            status = row[0]
            print(f"[{order_id}] Прочитали статус: {status}")

            await asyncio.sleep(0.1)
            print(f"[{order_id}] Продолжаем после задержки...")

            if status != 'created':
                raise OrderAlreadyPaidError(f"Order {order_id} already paid")

            await self.session.execute(
                text("UPDATE orders SET status = 'paid' WHERE id = :order_id"),
                {"order_id": order_id}
            )
            
            await self.session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
                """),
                {"order_id": order_id}
            )
        
        return {
            "order_id": str(order_id),
            "status": "paid",
            "message": "Order paid successfully (unsafe method)"
        }

    async def pay_order_safe(self, order_id: uuid.UUID) -> dict:
        """
        БЕЗОПАСНАЯ реализация оплаты заказа.
        
        Использует REPEATABLE READ + FOR UPDATE для предотвращения race condition.
        Корректно работает при конкурентных запросах.

        Args:
            order_id: ID заказа для оплаты
            
        Returns:
            dict с информацией о заказе после оплаты
            
        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """

        async with self.session.begin():
            await self.session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )
            print(f"[{order_id}] Isolation level set to REPEATABLE READ inside transaction")

            result = await self.session.execute(
                text("""
                    SELECT status, pg_backend_pid() 
                    FROM orders 
                    WHERE id = :order_id 
                    FOR UPDATE
                """),
                {"order_id": order_id}
            )
            row = result.fetchone()

            if row is None:
                print(f"[{order_id}] Order not found")
                raise OrderNotFoundError(f"Order {order_id} not found")

            status, backend_pid = row

            if status != 'created':
                print(f"[{order_id}] Order already paid → raising exception")
                raise OrderAlreadyPaidError(f"Order {order_id} already paid")

            await self.session.execute(
                text("UPDATE orders SET status = 'paid' WHERE id = :order_id"),
                {"order_id": order_id}
            )

            await self.session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
                """),
                {"order_id": order_id}
            )
        
        return {
            "order_id": str(order_id),
            "status": "paid",
            "message": "Order paid successfully (safe method)"
        }

    async def get_payment_history(self, order_id: uuid.UUID) -> list[dict]:
        """
        Получить историю оплат для заказа.
        Используется для проверки, сколько раз был оплачен заказ.
        
        Args:
            order_id: ID заказа
            
        Returns:
            Список словарей с записями об оплате
        """
        
        result = await self.session.execute(
            text("""
                SELECT id, order_id, status, changed_at
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                ORDER BY changed_at
            """),
            {"order_id": order_id}
        )
        
        rows = result.fetchall()

        history = []
        for row in rows:
            history.append({
                "id": str(row[0]),
                "order_id": str(row[1]),
                "status": row[2],
                "changed_at": row[3]
            })
        
        return history