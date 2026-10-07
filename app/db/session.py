"""Database session factory.

Один engine на процесс, новая сессия на запрос. Engine — это пул соединений,
дешёвый объект; AsyncSession — короткоживущий объект на единицу работы.
"""
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings

_settings = get_settings()

engine = create_async_engine(
    _settings.database_url,
    pool_size=_settings.db_pool_size,
    max_overflow=_settings.db_max_overflow,
    pool_pre_ping=True,   # отбрасывает протухшие соединения
    echo=False,
    future=True,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency. Открыть сессию → отдать → закрыть."""
    async with SessionLocal() as session:
        yield session