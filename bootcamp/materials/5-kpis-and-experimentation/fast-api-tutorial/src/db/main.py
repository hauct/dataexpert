import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlmodel import create_engine
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from src.config import Config

engine = create_async_engine(
    url = Config.DATABASE_URL,
    echo = True,
    connect_args={
        'ssl': False
    }
)

async def list_schemas():
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT schema_name FROM information_schema.schemata")
        )
        schemas = result.scalars().all()
        print("Danh sách schemas:")
        for schema in schemas:
            print(f"- {schema}")

import asyncio
asyncio.run(list_schemas())