from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str

    USE_RANDOM_DELAY_IN_RUN: bool = False
    RANDOM_DELAY_IN_RUN: list[int] = [0, 600]

    REF_ID: str = '87KR8CV9'
    AUTO_TASK: bool = True

    SLEEP_TIME: list[int] = [10800, 21600]
    USE_PROXY: bool = True

settings = Settings()
