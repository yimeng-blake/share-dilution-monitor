import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    SNOWFLAKE_ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    SNOWFLAKE_USER: str = os.getenv("SNOWFLAKE_USER", "")
    SNOWFLAKE_PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    SNOWFLAKE_DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "DILUTION_MONITOR")
    SNOWFLAKE_SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    SNOWFLAKE_ROLE: str = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    SEC_EDGAR_USER_AGENT: str = os.getenv(
        "SEC_EDGAR_USER_AGENT", "DilutionMonitor admin@example.com"
    )
    SEC_EDGAR_BASE_URL: str = "https://data.sec.gov"
    SEC_EDGAR_RATE_LIMIT: float = 0.15  # seconds between requests (~6.6 req/s)
    SEC_EDGAR_MAX_RETRIES: int = 3
    SEC_EDGAR_REQUEST_TIMEOUT: int = 30


settings = Settings()
