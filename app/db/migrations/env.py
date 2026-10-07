"""Alembic environment (async).

URL берётся из приложения, не из alembic.ini — чтобы единственный источник
правды для подключения к БД был один.
"""
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.config import get_settings
from app.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Подменяем URL рантайм-настройкой.
config.set_main_option("sqlalchemy.url", get_settings().database_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    # Миграции здесь гоняются на живой таблице до старта uvicorn (см.
    # entrypoint.sh) — ALTER TABLE берёт ACCESS EXCLUSIVE, и без lock_timeout
    # ждёт освобождения лока неограниченно, а следом за ним в очередь встают
    # все читатели/писатели этой таблицы (2026-08-15: ровно так и словил
    # ReadTimeout promote_if_better.py на /admin/metrics/.../compare — не
    # из-за самого запроса, а из-за очереди на лок за миграцией). Короткий
    # lock_timeout — миграция быстро и громко фейлится (entrypoint.sh
    # остановит деплой через set -e), вместо того чтобы тихо держать
    # читателей в очереди до минуты.
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        # ВАЖНО: connection.execute(...) должен идти ПОСЛЕ begin_transaction(),
        # не до него. .execute() на "чистом" соединении триггерит SQLAlchemy
        # autobegin — у транзакции оказывается два независимых владельца
        # (implicit root от autobegin + savepoint, который откроет Alembic
        # внутри begin_transaction()). Alembic на выходе коммитит только свой
        # savepoint; implicit root остаётся открытым и откатывается целиком,
        # когда `async with connectable.connect()` закрывает соединение —
        # вместе с ним откатывается ВЕСЬ результат миграции. Обнаружено
        # 2026-08-16 на локальном чистом деплое: alembic рапортовал успех по
        # всем 11 миграциям, но ни одна таблица физически не появилась в БД.
        # SET (не SET LOCAL) держится на всё соединение вне зависимости от
        # границ транзакций, так что порядок здесь ничего не меняет для
        # самого lock_timeout — только для того, коммitится ли DDL вообще.
        connection.execute(text("SET lock_timeout = '5s'"))
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())