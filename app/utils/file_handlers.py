import shutil
import aiofiles
from fastapi import UploadFile
import pandas as pd

async def save_upload_file(upload_file: UploadFile, destination: str) -> None:
    try:
        async with aiofiles.open(destination, 'wb') as out_file:
            while content := await upload_file.read(1024 * 1024):  # Read in chunks
                await out_file.write(content)
    finally:
        await upload_file.close()

def save_dataframe(df: pd.DataFrame, destination: str) -> None:
    df.to_csv(destination, index=False)
