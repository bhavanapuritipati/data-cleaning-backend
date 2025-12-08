import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    APP_NAME: str = "Data Cleaning Backend"
    DEBUG: bool = True
    API_V1_STR: str = "/api/v1"
    
    # HuggingFace
    HF_TOKEN: str = ""
    
    # Storage
    UPLOAD_DIR: str = "storage/uploads"
    OUTPUT_DIR: str = "storage/outputs"
    REPORT_DIR: str = "storage/reports"
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()

# Create validation for storage directories
for dir_path in [settings.UPLOAD_DIR, settings.OUTPUT_DIR, settings.REPORT_DIR]:
    os.makedirs(dir_path, exist_ok=True)
