from scripts.costos.costos_bi import CargueHistoricoCostos
from datetime import datetime

def main():
    database_name = 'disay'
    fecha_corte = datetime.now().date()
    cargue_historico_costos = CargueHistoricoCostos(database_name)
    cargue_historico_costos.procesar_todas_las_fechas(fecha_corte)

if __name__ == "__main__":
    main()