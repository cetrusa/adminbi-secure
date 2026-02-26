import datetime
import pandas as pd
import locale

"""
    Clase para calcular los días hábiles de un año dado, incluyendo la posibilidad
    de excluir los sábados y teniendo en cuenta los días festivos según la ley Emiliani
    y otros festivos fijos de un país específico.

    Atributos:
        year (int): El año para el cual se calcularán los días hábiles.

    Métodos:
        calculate_easter: Calcula la fecha de Pascua para el año establecido.
        apply_emiliani_rule: Aplica la regla de Emiliani para mover ciertos festivos a lunes.
        get_dias_festivos: Devuelve un conjunto de los días festivos del año.
        es_dia_habil: Determina si una fecha específica es un día hábil.
        dias_habiles_del_anno: Genera una lista de todos los días hábiles del año.
        dias_habiles_del_anno_df: Genera un DataFrame de pandas con los días hábiles del año.
        obtener_descripcion: Obtiene la descripción de un día festivo si existe.
"""


class CalendarioLaboral:

    def __init__(self, year, incluir_sabados=True):
        """
        Inicializa una nueva instancia de la clase CalendarioLaboral.
        Valida que el año sea un entero y esté dentro de un rango razonable.

        Parámetros:
            year (int): El año para calcular el calendario laboral.
            incluir_sabados (bool): Indica si los sábados deben considerarse días hábiles.
        """
        try:
            locale.setlocale(locale.LC_TIME, 'es_CO.UTF-8')  # 'es_ES' para España, 'es_CO' para Colombia, etc.
        except locale.Error:
            print(
                "No fue posible establecer la localidad a español. Se usará la configuración regional predeterminada."
            )

        if not isinstance(year, int) or year < 1900 or year > 2100:
            raise ValueError(
                "El año debe ser un entero dentro de un rango razonable (1900-2100)."
            )
        self.year = year
        self.incluir_sabados = incluir_sabados
        self.easter_date = (
            self.calculate_easter()
        )  # Guarda la fecha de Pascua para reutilizarla.
        self.dias_festivos = self.get_dias_festivos()

    def calculate_easter(self):
        """
        Utiliza el Algoritmo de Meeus/Jones/Butcher para calcular la fecha de Pascua
        del año especificado en el atributo 'year'.

        Retorna:
            datetime.date: La fecha de Pascua del año correspondiente.
        """
        # Algoritmo para calcular la fecha de Pascua
        a = self.year % 19
        b, c = divmod(self.year, 100)
        d, e = divmod(b, 4)
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i, k = divmod(c, 4)
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1
        return datetime.date(self.year, month, day)

    def apply_emiliani_rule(self, date):
        """
        Aplica la regla de Emiliani para mover ciertos festivos a lunes si caen en sábado o domingo.

        Parámetros:
            date (datetime.date): La fecha del festivo a ajustar.

        Retorna:
            datetime.date: La fecha ajustada según la regla de Emiliani.
        """
        # Código para aplicar la regla de Emiliani
        if date.weekday() == 6:  # Domingo
            return date + datetime.timedelta(days=1)
        elif date.weekday() >= 1 and date.weekday() <= 5:  # De martes a sábado
            return date + datetime.timedelta(days=(8 - date.weekday()))
        return date

    def get_dias_festivos(self):
        """
        Calcula todos los días festivos del año, incluyendo los que dependen de la fecha de Pascua y aquellos que se ajustan
        por la regla de Emiliani cuando caen en domingo.

        Retorna:
            dict: Un diccionario con todas las fechas de días festivos del año como claves y sus descripciones como valores.
        """
        easter_date = self.calculate_easter()
        holidays = {
            # Festivos fijos
            datetime.date(self.year, 1, 1): "Año Nuevo",
            datetime.date(self.year, 5, 1): "Día del Trabajo",
            datetime.date(self.year, 7, 20): "Día de la Independencia",
            datetime.date(self.year, 8, 7): "Batalla de Boyacá",
            datetime.date(self.year, 12, 8): "Día de la Inmaculada Concepción",
            datetime.date(self.year, 12, 25): "Navidad",
        }

        # Festivos que se mueven con la regla de Emiliani
        festivos_emiliani = [
            (1, 6),   # Reyes Magos
            (3, 19),  # San José
            (6, 29),  # San Pedro y San Pablo
            (8, 15),  # Asunción de la Virgen
            (10, 12), # Día de la Raza
            (11, 1),  # Todos los Santos
            (11, 11), # Independencia de Cartagena
        ]
        for mes, dia in festivos_emiliani:
            fecha_festivo = datetime.date(self.year, mes, dia)
            
            if fecha_festivo.weekday() >= 1 and fecha_festivo.weekday() <= 5:
                fecha_festivo = fecha_festivo + datetime.timedelta(days=1)
            holidays[fecha_festivo] = "Festivo movido por regla de Emiliani"

        # Añadir festivos basados en la fecha de Pascua
        holidays[easter_date - datetime.timedelta(days=3)] = "Jueves Santo"
        holidays[easter_date - datetime.timedelta(days=2)] = "Viernes Santo"
        holidays[easter_date + datetime.timedelta(days=43)] = "Ascensión del Señor"
        holidays[easter_date + datetime.timedelta(days=60)] = "Corpus Christi"
        holidays[easter_date + datetime.timedelta(days=68)] = "Sagrado Corazón"

        # Manejar casos especiales para la Independencia y la Batalla de Boyacá
        # Si caen un domingo, se pasan al lunes siguiente y se verifica que el lunes siguiente no sea festivo
        independencia = datetime.date(self.year, 7, 20)
        boyaca = datetime.date(self.year, 8, 7)
        if independencia.weekday() == 6:  # Si cae en domingo
            independencia = independencia + datetime.timedelta(days=1)
            holidays[independencia] = "Día de la Independencia (observado)"
        if boyaca.weekday() == 6:  # Si cae en domingo
            boyaca = boyaca + datetime.timedelta(days=1)
            holidays[boyaca] = "Batalla de Boyacá (observado)"

        # Asegurarse de que no hay dos festivos consecutivos en lunes
        # Por ejemplo, si 1 de enero es un domingo, no se celebra el 2 de enero y el 9 de enero como festivos
        fechas_a_revisar = sorted(holidays.keys())
        for i in range(1, len(fechas_a_revisar)):
            if (
                fechas_a_revisar[i].weekday() == 0 and
                fechas_a_revisar[i] - datetime.timedelta(days=1) == fechas_a_revisar[i-1]
            ):
                # Mover el festivo al próximo lunes
                nuevo_festivo = fechas_a_revisar[i] + datetime.timedelta(days=7)
                holidays[nuevo_festivo] = holidays.pop(fechas_a_revisar[i])
                fechas_a_revisar = sorted(holidays.keys())  # Actualizar la lista de fechas

        return holidays

    def obtener_descripcion(self, fecha):
        """
        Obtiene la descripción del día festivo si la fecha dada es un día festivo en Colombia.

        Parámetros:
            fecha (datetime.date): La fecha del posible día festivo.

        Retorna:
            str: La descripción del día festivo si es un festivo, cadena vacía de lo contrario.
        """
        festivos_descripcion = self.get_dias_festivos()
        return festivos_descripcion.get(fecha, "")

    def es_dia_habil(self, date):
        """
        Determina si una fecha específica es un día hábil, considerando los días festivos y
        la preferencia de inclusión de sábados.

        Parámetros:
            date (datetime.date): La fecha a verificar.

        Retorna:
            bool: True si la fecha es un día hábil, False de lo contrario.
        """
        es_festivo = date in self.dias_festivos
        es_domingo = date.weekday() == 6
        es_sabado = date.weekday() == 5 and not self.incluir_sabados
        return not (es_festivo or es_domingo or es_sabado)

    def dias_habiles_del_anno(self):
        """
        Genera la lista de todos los días hábiles del año, excluyendo domingos, festivos y, si se
        ha configurado así, sábados.

        Retorna:
            list: Una lista de objetos datetime.date que son días hábiles.
        """
        dt_inicio = datetime.date(self.year, 1, 1)
        dt_fin = datetime.date(self.year, 12, 31)
        return [
            dt_inicio + datetime.timedelta(days=i)
            for i in range((dt_fin - dt_inicio).days + 1)
            if self.es_dia_habil(dt_inicio + datetime.timedelta(days=i))
        ]

    def dias_habiles_del_anno_df(self):
        """
        Crea un DataFrame de pandas con los detalles de los días hábiles del año,
        utilizando la configuración de inclusión de sábados definida en la instancia.

        Retorna:
            pd.DataFrame: Un DataFrame con la información de días hábiles.
        """
        dias_semana_espanol = {
            0: 'Lunes',
            1: 'Martes',
            2: 'Miércoles',
            3: 'Jueves',
            4: 'Viernes',
            5: 'Sábado',
            6: 'Domingo',
        }
        dt_inicio = datetime.date(self.year, 1, 1)
        dt_fin = datetime.date(self.year, 12, 31)
        dias = [
            dt_inicio + datetime.timedelta(days=i)
            for i in range((dt_fin - dt_inicio).days + 1)
        ]
        data = [
            {
                "id": idx + 1,
                "ds": "",
                # "nmDia": dia.strftime("%A"), # de esta manera los días de la semana salen con simbolos las tildes
                "nmDia": dias_semana_espanol[dia.weekday()],
                "dtFecha": dia,
                "nbDia": dia.day,
                "nbMes": dia.month,
                "nbAnno": dia.year,
                "txDescripcion": self.obtener_descripcion(dia),
                "boFestivo": 1 if dia in self.dias_festivos or dia.weekday() == 6 else 0,
            }
            for idx, dia in enumerate(dias)
        ]
        df = pd.DataFrame(data)
        return df


# Ejemplo de uso
calendario_con_sabados = CalendarioLaboral(year=2024, incluir_sabados=True)
calendario_sin_sabados = CalendarioLaboral(year=2024, incluir_sabados=False)

print("Con sábados:")
dias_habiles_df = calendario_con_sabados.dias_habiles_del_anno_df()
dias_habiles_df.to_excel("dias_habiles_2024_consabados.xlsx", index=False)
print("Sin sábados:")
dias_habiles_df = calendario_con_sabados.dias_habiles_del_anno_df()
dias_habiles_df.to_excel("dias_habiles_2024_sinsabados.xlsx", index=False)
