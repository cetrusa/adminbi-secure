import os
import time
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def clean_old_media_files(hours=4):
    """
    Elimina archivos en la carpeta media/ con extensiones permitidas
    (.xlsx, .db, .zip, .csv, .txt) que tengan más de 'hours' horas de modificados.
    """
    MEDIA_DIR = Path("media")
    EXTENSIONS = {".xlsx", ".db", ".zip", ".csv", ".txt"}
    now = time.time()
    removed = []
    for file in MEDIA_DIR.iterdir():
        if file.is_file() and file.suffix.lower() in EXTENSIONS:
            mtime = file.stat().st_mtime
            age_hours = (now - mtime) / 3600
            if age_hours > hours:
                try:
                    file.unlink()
                    removed.append(str(file))
                    logger.info(
                        f"[clean_old_media_files] Archivo eliminado: {file} (antigüedad: {age_hours:.2f}h)"
                    )
                except Exception as e:
                    logger.error(
                        f"[clean_old_media_files] Error al eliminar {file}: {e}"
                    )
    return removed
