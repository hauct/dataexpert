import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import asyncio
from sqlmodel import create_engine
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from src.config import Config

engine = create_async_engine(
    url=Config.DATABASE_URL.replace('?sslmode=require', ''),
    echo=True,
    connect_args={
        "ssl": True,
        "server_settings": {
            "application_name": "myapp"
        }
    }
)

async def list_tables():  # Đổi tên hàm
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("""SELECT table_name 
                   FROM information_schema.tables 
                   WHERE table_catalog = current_database()""")  # Đổi query lấy bảng
        )
        tables = result.scalars().all()  # Đổi tên biến
        print("Danh sách các bảng:")
        for table in tables:  # Đổi tên biến
            print(f"- {table}")

asyncio.run(list_tables())  # Đổi tên hàm gọi