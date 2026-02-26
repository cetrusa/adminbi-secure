import pandas as pd
from distutils.log import error

# import mariadb
from sqlalchemy.sql import text
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage

# import json
import sqlalchemy
import time
from pathlib import Path, PurePath
####################################################################
import logging

logging.basicConfig(
    filename="log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    filemode="w",
)
####################################################################
logging.info("Inciando Proceso")


class Union_Alpina:
    def __init__(self):
        ConfigBasic()
        # preparamos los ejecutables
        zip7 = Path('7-Zip','7z.exe')
        winscp = Path('winscp','winscp.com')

    def extractor(self):
        ItpReporte = StaticPage.txProcedureExtrae
        for a in ItpReporte:
            with StaticPage.con.connect() as connectionin:
                sql = text(f"SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = {a}")
                result = connectionin.execute(sql)
                df9 = pd.DataFrame(result)
                StaticPage.txTabla = str(df9["txTabla"].values[0])
                StaticPage.nmReporte = str(df9["nmReporte"].values[0])
                StaticPage.nmProcedure_out = str(df9["nmProcedure_out"].values[0])
                StaticPage.nmProcedure_in = str(df9["nmProcedure_in"].values[0])
                StaticPage.txSql = str(df9["txSql"].values[0])
                logging.info(f"Se va a procesar {StaticPage.nmReporte}")
                try:
                    self.procedimiento_a_sql(
                        IdtReporteIni=StaticPage.IdtReporteIni,
                        IdtReporteFin=StaticPage.IdtReporteFin,
                        nmReporte=StaticPage.nmReporte,
                        nmProcedure_out=StaticPage.nmProcedure_out,
                        txTabla=StaticPage.txTabla,
                    )
                    logging.info(
                        f"La información se generó con éxito de {StaticPage.nmReporte}"
                    )
                except Exception as e:
                    logging.info(
                        f"No fue posible extraer la información de {StaticPage.nmReporte} por {e}"
                    )

    def insertar_sql(self, resultado_out, txTabla, max_retries=3):
        with StaticPage.conin3.connect() as connectionin:
            # Establece el nivel de aislamiento de la transacción
            connectionin = connectionin.execution_options(
                isolation_level="READ COMMITTED"
            )

            retry_count = 0

            while retry_count < max_retries:
                try:
                    # Inicia la transacción
                    with connectionin.begin():
                        resultado_out.to_sql(
                            name=txTabla,
                            con=connectionin,
                            if_exists="append",
                            index=False,
                            index_label=None,
                        )

                    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
                    return logging.info("los datos se han insertado correctamente")

                except sqlalchemy.exc.IntegrityError as e:
                    logging.error(f"Error de integridad: {e}")
                    retry_count += 1
                    time.sleep(1)  # Espera antes de reintentar

                except sqlalchemy.exc.ProgrammingError as e:
                    logging.error(f"Error de programación: {e}")
                    retry_count += 1
                    time.sleep(1)  # Espera antes de reintentar

                except Exception as e:
                    logging.error(f"Error desconocido: {e}")
                    retry_count += 1
                    time.sleep(1)  # Espera antes de reintentar

    def consulta_sql_out(
        self, nmProcedure_out, IdtReporteIni, IdtReporteFin, nmReporte,max_retries=3
    ):
        retry_count = 0

        while retry_count < max_retries:
            try:
                with StaticPage.conout.connect() as connectionout:
                    # Establece el nivel de aislamiento de la transacción
                    connectionout = connectionout.execution_options(
                        isolation_level="READ COMMITTED"
                    )

                    # Inicia la transacción y ejecuta la consulta dentro del bloque 'with'
                    with connectionout.begin():
                        sqlout = text(
                            f"CALL {str(nmProcedure_out)}('{str(IdtReporteIni)}','{str(IdtReporteFin)}','{str(nmReporte)}');"
                        )
                        resultado = pd.read_sql_query(sql=sqlout, con=connectionout)

                    return resultado

            except sqlalchemy.exc.IntegrityError as e:
                logging.error(f"Error de integridad: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

            except sqlalchemy.exc.ProgrammingError as e:
                logging.error(f"Error de programación: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

            except Exception as e:
                logging.error(f"Error desconocido: {e}")
                retry_count += 1
                time.sleep(1)  # Espera antes de reintentar

        print("Se han agotado los intentos, no se pudo ejecutar la consulta.")
        logging.error("Se han agotado los intentos, no se pudo ejecutar la consulta.")
        return None

    def consulta_sql_bi(self, IdtReporteIni, IdtReporteFin):
        try:
            with StaticPage.conin.connect() as connectionin:
                # Establece el nivel de aislamiento de la transacción
                connectionin = connectionin.execution_options(
                    isolation_level="READ COMMITTED"
                )

                # Inicia la transacción y ejecuta la consulta dentro del bloque 'with'
                with connectionin.begin():
                    sqldelete = text(StaticPage.txSql)
                    connectionin.execute(
                        sqldelete, {"fi": IdtReporteIni, "ff": IdtReporteFin}
                    )

                logging.info("Datos fueron borrados")

        except Exception as e:
            print(f"Error al ejecutar la consulta: {e}")
            logging.error(f"Error al borrar datos: {e}")
            # Puedes manejar el error aquí, por ejemplo, devolver un resultado vacío, registrar el error, etc.

    def procedimiento_a_sql(
        self,
        IdtReporteIni,
        IdtReporteFin,
        nmReporte,
        nmProcedure_out,
        txTabla,
    ):
        # Iniciamos las conexiones
        EsVacio = True
        con = None

        for intento in range(3):  # Intentar la conexión hasta tres veces
            try:
                if (
                    nmReporte == "update_cubo_bi"
                    or nmReporte == "borra_impactos_bi"
                    or nmReporte == "impactos_bi"
                    or nmReporte == "actualizar_usuarios_periodo"
                    or nmReporte == "actualizar_usuarios_conteo_diario"
                ):
                    try:
                        self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
                        logging.info(f"Se ha realizado el proceso de {txTabla}")
                        logging.info(
                            f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
                        )
                        EsVacio = True
                    except Exception as e:
                        logging.error(f"Error al ejecutar consulta_sql_bi: {e}")
                else:
                    try:
                        StaticPage.resultado_out = self.consulta_sql_out(
                            nmProcedure_out=nmProcedure_out,
                            IdtReporteIni=IdtReporteIni,
                            IdtReporteFin=IdtReporteFin,
                            nmReporte=nmReporte,
                        )
                        logging.info(
                            f"{nmReporte} contiene {len(StaticPage.resultado_out.index)}"
                        )
                        EsVacio = False
                        logging.info(f"el procedimiento {txTabla} sí funcionó")
                    except Exception as e:
                        logging.error(f"Error al ejecutar consulta_sql_out: {e}")

                # Evaluamos si el dataFrame está vacío
                if EsVacio == False and not StaticPage.resultado_out.empty:
                    try:
                        self.consulta_sql_bi(
                            IdtReporteIni=IdtReporteIni, IdtReporteFin=IdtReporteFin
                        )
                        logging.info(f"Se ha realizado el proceso de {txTabla}")
                    except Exception as e:
                        logging.error(f"Error al ejecutar consulta_sql_bi: {e}")

                    try:
                        self.insertar_sql(
                            txTabla=txTabla, resultado_out=StaticPage.resultado_out
                        )
                        logging.info(
                            f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
                        )
                    except Exception as e:
                        logging.error(f"Error al ejecutar insertar_sql: {e}")

                else:
                    logging.info(f"No hay datos para {txTabla} ")
                
                # Si llegamos aquí, todo se ejecutó correctamente
                logging.info("Se ejecutó el procedimiento")
                return

            except Exception as e:
                logging.error(f"Error en procedimiento_a_sql (Intento {intento + 1}/3): {e}")
                if intento < 2:
                    logging.info(f'Reintentando procedimiento (Intento {intento + 1}/3)...')
                    time.sleep(5)  # Esperar 5 segundos antes de reintentar
                else:
                    logging.error('Se agotaron los intentos. No se pudo ejecutar el procedimiento.')
                    break
            finally:
                if con:
                    con.close()
