from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./data/signals.db"
    sec_edgar_user_agent: str = "financial-signals user@example.com"
    min_confidence: int = 25
    host: str = "127.0.0.1"
    port: int = 8000

    # Pipeline
    price_lookback_days: int = 5
    sector_cache_ttl_days: int = 30

    # Congress.gov API
    congress_api_key: str = ""

    # Scheduler
    edgar_scrape_interval_minutes: int = 15
    congressional_scrape_interval_hours: int = 24
    committee_scrape_interval_hours: int = 168  # weekly
    legislation_scrape_interval_hours: int = 24

    # Dashboard
    dashboard_trade_limit: int = 100
    dashboard_max_days: int = 90

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
