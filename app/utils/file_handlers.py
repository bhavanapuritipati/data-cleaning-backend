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
    """
    Save DataFrame to CSV with UTF-8-SIG encoding for Excel compatibility.
    UTF-8-SIG includes BOM which helps Excel recognize UTF-8 encoding correctly,
    preventing issues with special characters like em dashes (–) and accented letters (é).
    """
    df.to_csv(destination, index=False, encoding='utf-8-sig')
