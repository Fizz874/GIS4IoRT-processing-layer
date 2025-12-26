from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    FLINK_URL: str
    KAFKA_BROKER: str
    FLINK_JAR_NAME: str
    GEOFLINK_DB_NAME: str

    model_config = SettingsConfigDict(env_file=".env")



settings = Settings()