class StaticPage:
    con = ""
    cursor = ""
    dir = ""
    dir_actual = ""
    name = ""
    nmDt = ""
    IdtReporteIni = ""
    IdtReporteFin = ""
    hostServerOut = ""
    portServerOut = ""
    nmUsrOut = ""
    txPassOut = ""
    hostServerIn = ""
    portServerIn = ""
    nmUsrIn = ""
    txPassIn = ""
    id = ""
    nmEmpresa = ""
    nbServerSidis = ""
    dbSidis = ""
    nbServerBi = ""
    dbBi = ""
    txProcedureExtrae = ""
    txProcedureCargue = ""
    nmProcedureExcel = ""
    txProcedureExcel = ""
    nmProcedureInterface = ""
    txProcedureInterface = ""
    nmProcedureExcel2 = ""
    txProcedureExcel2 = ""
    nmProcedureCsv = ""
    txProcedureCsv = ""
    nmProcedureCsv2 = ""
    txProcedureCsv2 = ""
    nmProcedureSql = ""
    txProcedureSql = ""
    txDescripcion = ""
    report_id_powerbi = ""
    dataset_id_powerbi = ""
    url_powerbi = ""
    txDtIni = ""
    txDtFin = ""
    IdtReporteIni = ""
    IdtReporteFin = ""
    hostServerOut = ""
    portServerOut = ""
    nbTipo = ""
    nmUsrOut = ""
    txPassOut = ""
    hostServerIn = ""
    portServerIn = ""
    nbTipoBi = ""
    nmUsrIn = ""
    txPassIn = ""
    nmUsrPowerbi = ""
    txPassPowerbi = ""
    txTabla = ""
    nmReporte = ""
    nmProcedure_in = ""
    nmProcedure_out = ""
    conin = ""
    conin2 = ""
    conin3 = ""
    conout = ""
    resultado_out = ""
    txSql = ""
    id_tsol = ""


class DinamicPage:
    valores = {}

    @staticmethod
    def set_valor(clave, valor):
        DinamicPage.valores[clave] = valor

    @staticmethod
    def get_valor(clave):
        return DinamicPage.valores.get(clave)
    
    @staticmethod
    def eliminar_valor(clave):
        if clave in DinamicPage.valores:
            del DinamicPage.valores[clave]


# dinamicpage = DinamicPage()
# dinamicpage.set_atributo('file_path', '/ruta/a/mi/archivo.csv')
# dinamicpage.set_atributo('archivo_cubo_ventas', 'cubo_ventas.csv')

# # Verificar los valores asignados
# print(dinamicpage.file_path)  # Imprime: /ruta/a/mi/archivo.csv
# print(dinamicpage.archivo_cubo_ventas)  # Imprime: cubo_ventas.csv
