"""
main_preventa.py — Ejecutable standalone para extracción de preventa diaria.

Solo ejecuta los procedimientos:
  - 229  (preventa día, todas las empresas)
  - 223  (Bimbo, solo si está en txProcedureExtrae de la empresa)

Condición: solo se ejecuta si existe la tabla ``fact_preventa_diaria``
en la base de datos BI destino.  De lo contrario, termina sin hacer nada.

Uso:
  - Como ejecutable congelado (PyInstaller):
      empresa/puentedia/main_preventa.exe
      → self.name = carpeta padre (empresa)
      → self.nmDt  = carpeta del .exe  (puentedia)

  - Como script Python:
      python main_preventa.py <database_name>
      → self.nmDt siempre es "puentedia"
"""

import atexit
import ast
import logging
import os
import signal
import sys
import time

from sqlalchemy import text
from unipath import Path

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from scripts.extrae_bi.extrae_bi_insert import ExtraeBiConfig, ExtraeBiExtractor

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    filename="logPreventa.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)

# ---------------------------------------------------------------------------
# Cleanup de conexiones al terminar
# ---------------------------------------------------------------------------
atexit.register(con.dispose_all)


def _signal_cleanup(signum, frame):
    logging.warning("Señal %s recibida. Cerrando conexiones...", signum)
    con.dispose_all()
    sys.exit(1)


signal.signal(signal.SIGTERM, _signal_cleanup)
signal.signal(signal.SIGINT, _signal_cleanup)

# ---------------------------------------------------------------------------
# Procedimientos permitidos
# ---------------------------------------------------------------------------
PROC_PREVENTA = 229  # preventa día (aplica a todas las empresas)
PROC_BIMBO = 223     # específico Bimbo
ALLOWED_PROCS = {PROC_PREVENTA, PROC_BIMBO}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _table_exists(engine, table_name):
    """Verifica si una tabla existe en la BD conectada."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COUNT(*) AS cnt "
                    "FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "  AND TABLE_NAME = :t"
                ),
                {"t": table_name},
            ).scalar()
            return (row or 0) > 0
    except Exception as exc:
        logging.error("Error verificando tabla %s: %s", table_name, exc)
        return False


# ---------------------------------------------------------------------------
# Clase principal
# ---------------------------------------------------------------------------
class InicioPreventa:

    def __init__(self):
        if getattr(sys, "frozen", False):
            # Ejecutable congelado: nombre de empresa y nmDt vienen de la ruta
            self.name = str(
                os.path.split(
                    os.path.dirname(Path(sys.executable).ancestor(1))
                )[-1]
            )
            self.nmDt = str(
                os.path.split(
                    os.path.dirname(Path(sys.executable).ancestor(0))
                )[-1]
            )
        else:
            # Script Python: nombre de empresa por argumento
            self.name = sys.argv[1] if len(sys.argv) > 1 else "compi"
            self.nmDt = "puentedia"

        self._configurar()

    # ------------------------------------------------------------------
    def _configurar(self):
        try:
            self.config_basic = ConfigBasic(self.name)
            self.config = self.config_basic.config
            # Engine BI para verificar existencia de tabla
            self.engine_bi = con.ConexionMariadb3(
                str(self.config["nmUsrIn"]),
                str(self.config["txPassIn"]),
                str(self.config["hostServerIn"]),
                int(self.config["portServerIn"]),
                str(self.config["dbBi"]),
            )
        except Exception as exc:
            logging.error("Error al configurar %s: %s", self.name, exc)
            raise

    # ------------------------------------------------------------------
    def _fetch_date_config(self):
        """Carga fechas desde conf_dt para nmDt."""
        sql = f"SELECT * FROM powerbi_adm.conf_dt WHERE nmDt = '{self.nmDt}';"
        df = self.config_basic.execute_sql_query(sql)
        if df.empty:
            logging.error("No se encontró conf_dt para nmDt='%s'", self.nmDt)
            return False

        ini_df = self.config_basic.execute_sql_query(text(df["txDtIni"].iloc[0]))
        fin_df = self.config_basic.execute_sql_query(text(df["txDtFin"].iloc[0]))
        if ini_df.empty or fin_df.empty:
            logging.error("No se pudo resolver el rango de fechas")
            return False

        self.IdtReporteIni = ini_df["IdtReporteIni"].iloc[0]
        self.IdtReporteFin = fin_df["IdtReporteFin"].iloc[0]
        return True

    # ------------------------------------------------------------------
    def _get_filtered_procedures(self):
        """Retorna solo los procedimientos 223/229 que estén en txProcedureExtrae."""
        procs = self.config.get("txProcedureExtrae", [])
        if isinstance(procs, str):
            procs = ast.literal_eval(procs)
        return [p for p in procs if p in ALLOWED_PROCS]

    # ------------------------------------------------------------------
    def run(self):
        logging.info("=" * 60)
        logging.info("main_preventa: Inicio para '%s' (nmDt=%s)", self.name, self.nmDt)
        logging.info("=" * 60)
        print(f"[{self.name}] Iniciando preventa (nmDt={self.nmDt})")

        # 1. Verificar que la tabla fact_preventa_diaria exista
        if not _table_exists(self.engine_bi, "fact_preventa_diaria"):
            msg = "Tabla fact_preventa_diaria no existe en la BD BI. Fin."
            logging.info(msg)
            print(f"[{self.name}] {msg}")
            return

        # 2. Filtrar procedimientos
        filtered = self._get_filtered_procedures()
        if not filtered:
            msg = "Ningún procedimiento 223/229 configurado en txProcedureExtrae. Fin."
            logging.info(msg)
            print(f"[{self.name}] {msg}")
            return

        logging.info("Procedimientos a ejecutar: %s", filtered)
        print(f"[{self.name}] Procedimientos: {filtered}")

        # 3. Resolver rango de fechas
        if not self._fetch_date_config():
            print(f"[{self.name}] Error: no se encontró config de fecha '{self.nmDt}'")
            return

        logging.info("Período: %s - %s", self.IdtReporteIni, self.IdtReporteFin)
        print(f"[{self.name}] Período: {self.IdtReporteIni} - {self.IdtReporteFin}")

        # 4. Ejecutar extracción con lista filtrada
        start = time.time()
        try:
            ebi_config = ExtraeBiConfig(database_name=self.name)
            ebi_config.config["txProcedureExtrae"] = filtered

            extractor = ExtraeBiExtractor(
                config=ebi_config,
                IdtReporteIni=self.IdtReporteIni,
                IdtReporteFin=self.IdtReporteFin,
            )
            resultado = extractor.run()

            elapsed = time.time() - start

            if resultado.get("success"):
                logging.info(
                    "Completado en %.1f s (%.1f min)", elapsed, elapsed / 60
                )
                print(f"[{self.name}] Completado en {elapsed:.1f}s")

                for err in resultado.get("errores_tablas", []):
                    logging.warning(
                        "  Tabla: %s, Error: %s",
                        err.get("tabla", "N/A"),
                        err.get("error", "N/A"),
                    )
            else:
                logging.error(
                    "Error tras %.1f s: %s", elapsed, resultado.get("message")
                )
                print(f"[{self.name}] Error: {resultado.get('message')}")

        except Exception as exc:
            elapsed = time.time() - start
            logging.error("Error crítico tras %.1f s: %s", elapsed, exc)
            print(f"[{self.name}] Error crítico: {exc}")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    inicio = InicioPreventa()
    inicio.run()
