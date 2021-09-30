from pydantic import BaseModel


class Settings(BaseModel):
    base_path: str