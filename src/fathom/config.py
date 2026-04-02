from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./data/signals.db"
    sec_edgar_user_agent: str = "financial-signals user@example.com"
    min_confidence: int = 25
    host: str = "127.0.0.1"
    port: int = 8000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
