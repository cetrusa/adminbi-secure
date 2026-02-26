# main_cargueinfoventas.py
import logging
from scripts.extrae_bi.cargue_infoventas import CargueInfoVentas

def main():
    database_name = "powerbi_distrijass"
    IdtReporteIni = "2024-06-01"
    IdtReporteFin = "2024-06-30"
    user_id = None  # O el ID del usuario si es necesario

    logging.basicConfig(level=logging.DEBUG)

    cargue_infoventas = CargueInfoVentas(IdtReporteIni, IdtReporteFin, database_name, user_id)
    cargue_infoventas.procesar_cargue_ventas()

if __name__ == "__main__":
    main()