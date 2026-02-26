import pandas as pd
from distutils.log import error
from django.conf import settings

# import mariadb
from sqlalchemy.sql import text
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage
import json
from django.core.exceptions import ImproperlyConfigured
import os
import re

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


class Cargue_Tsol:
    def __init__(self, database_name):
        ConfigBasic(database_name)

    def cargue(self):
        ItpReporte = StaticPage.txProcedureCargue
        for nb_sql in ItpReporte:
            try:
                with StaticPage.conin.connect() as connectionin:
                    sql = text(f"SELECT * FROM powerbi_adm.conf_sql WHERE nbSql = {nb_sql}")
                    result = connectionin.execute(sql)
                    df9 = pd.DataFrame(result)
                    self.actualizar_static_page(df9)
                    logging.info(f"Se va a procesar {StaticPage.nmReporte}")
                    self.procedimiento_a_sql(
                        IdtReporteIni=StaticPage.IdtReporteIni,
                        IdtReporteFin=StaticPage.IdtReporteFin,
                        nmReporte=StaticPage.nmReporte,
                        nmProcedure_in=StaticPage.nmProcedure_in,
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

    def actualizar_static_page(self, df):
        StaticPage.txTabla = str(df["txTabla"].values[0])
        StaticPage.nmReporte = str(df["nmReporte"].values[0])
        StaticPage.nmProcedure_out = str(df["nmProcedure_out"].values[0])
        StaticPage.nmProcedure_in = str(df["nmProcedure_in"].values[0])
        StaticPage.txDescripcion = str(df["txDescripcion"].values[0])
        StaticPage.txSql = str(df["txSql"].values[0])

    def insertar_sql(self, resultado_out, txTabla):
        try:
            with StaticPage.conin3.connect() as connectionin:
                cursorbi = connectionin.execution_options(
                    isolation_level="READ COMMITTED"
                )
                resultado_out.to_sql(
                    name=txTabla,
                    con=cursorbi,
                    if_exists="append",
                    index=False,
                    index_label=None,
                )
                # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
                return logging.info("los datos se han insertado correctamente")
        except Exception as e:
            logging.error(f"Error al insertar datos en {txTabla}: {e}")

    def mapeo_de_caracteres(self):
        try:
            with StaticPage.conin.connect() as connectionin:
                sql = text(f"SELECT * FROM {StaticPage.database_name}.mapeocaracteres")
                result = connectionin.execute(sql)
                return {
                    row["caracter_original"]: row["caracter_reemplazo"]
                    for row in result
                }
        except Exception as e:
            logging.error(f"Error al obtener mapeo de caracteres: {e}")
            return {}

    def consulta_txt_out(self):
        file_path = os.path.join(settings.MEDIA_ROOT, StaticPage.txDescripcion)
        mapeo_caracteres = self.mapeo_de_caracteres()

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                contenido = file.read()

            for caracter_especial, reemplazo in mapeo_caracteres.items():
                contenido = contenido.replace(caracter_especial, reemplazo)

            contenido = re.sub(
                r"\s+", " ", contenido
            )  # Simplifica espacios múltiples a uno solo
            contenido = contenido.replace('"', "")  # Elimina comillas dobles

            with open(file_path, "w", encoding="utf-8") as file:
                file.write(contenido)

            if StaticPage.txDescripcion == "interinfototal":
                df = pd.read_csv(file_path, delimiter="{", encoding="utf-8")
            else:
                df = pd.read_csv(file_path, delimiter=";", encoding="utf-8")

            return df
        except Exception as e:
            logging.error(f"Error al procesar archivo {file_path}: {e}")
            return pd.DataFrame()

    def consulta_sql_bi(self, IdtReporteIni, IdtReporteFin):
        with StaticPage.conin.connect() as connectionin:
            cursorbi = connectionin
            sqldelete = text(StaticPage.txSql)
            cursorbi.execute(sqldelete, {"fi": IdtReporteIni, "ff": IdtReporteFin})
        return logging.info("Datos fueron borrados")

    def procedimiento_a_sql(
        self,
        IdtReporteIni,
        IdtReporteFin,
        nmReporte,
        nmProcedure_in,
        nmProcedure_out,
        txTabla,
    ):
        # Iniciamos las conexiones
        EsVacio = True
        if (
            nmReporte == "update_cubo_bi"
            or nmReporte == "borra_impactos_bi"
            or nmReporte == "impactos_bi"
        ):
            try:
                self.consulta_sql_bi(IdtReporteIni, IdtReporteFin)
                logging.info(f"Se ha realizado el proceso de {txTabla}")
                logging.info(
                    f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
                )
                EsVacio = True
            except Exception as e:
                logging.error(e)
        else:
            StaticPage.resultado_out = self.consulta_txt_out()
            logging.info(f"{nmReporte} contiene {len(StaticPage.resultado_out.index)}")
            EsVacio = False
            # self.consulta_sql_bi(nmProcedure_in=nmProcedure_in,IdtReporteIni=IdtReporteIni,IdtReporteFin=IdtReporteFin,nmReporte=nmReporte)
            logging.info(f"el procemiento {txTabla} si funciono")

        # Evaluamos si esta vacio el dataFrame
        if EsVacio == False and StaticPage.resultado_out.empty == False:
            logging.info(f"Si hay datos para {txTabla} ")
            self.consulta_sql_bi(
                IdtReporteIni=IdtReporteIni, IdtReporteFin=IdtReporteFin
            )
            logging.info(f"Se ha realizado el proceso de {txTabla}")
            # resultado_out.to_csv(nmReporte.lower()+'.txt',sep='|',index=False,header=False,float_format='%.0f')
            self.insertar_sql(txTabla=txTabla, resultado_out=StaticPage.resultado_out)
            logging.info(
                f"Se han insertado los datos {txTabla}, entre {IdtReporteIni} y {IdtReporteFin}"
            )
        else:
            logging.info(f"No hay datos para {txTabla} ")

        logging.info("Se ejecutó el procedimiento")
