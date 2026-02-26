# Interface.py - Fixes
# Este archivo describe los cambios necesarios para solucionar el error:
# "Error fatal en InterfaceContable.run: OperationalError - (sqlite3.OperationalError) no such table: interface_gprs_aws_1_8c77bb7d_COMPRAS"

# Explicación del problema:
# El error ocurre cuando InterfaceContable intenta acceder a una tabla SQLite que no existe
# o que fue eliminada antes de poder ser utilizada. Esto sucede en el procesamiento de las hojas
# cuando se genera el archivo Excel, específicamente para la hoja "COMPRAS".

# Soluciones recomendadas:

# 1. Modificar el código en la función run() para manejar de forma más robusta las tablas
#    Añadir verificación de existencia de tablas y manejo de excepciones en cada iteración
#    de la lista de hojas (txProcedureInterface).

# En el método run() de la clase InterfaceContable, reemplazar el bucle actual por:

"""
for hoja in self.config["txProcedureInterface"]:
    hoja = str(hoja).strip()
    if not hoja:
        continue
    print(
        f"[InterfaceContable] Ejecutando query para extraer datos de la hoja: {hoja}"
    )
    logger.info(
        f"[InterfaceContable] Ejecutando query para extraer datos de la hoja: {hoja}"
    )
    try:
        query = self._generate_sqlout(hoja)
        print(f"[InterfaceContable] Query generado: {query}")
        logger.info(f"[InterfaceContable] Query generado: {query}")
        table_name = f"{self.sqlite_table_name}_{hoja}"

        # Ejecutar consulta y verificar si se creó la tabla
        self._execute_query_to_sqlite(query, table_name)

        # Verificar que la tabla exista antes de continuar
        with self.engine_sqlite.connect() as conn:
            table_exists = conn.execute(
                text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            ).fetchone()

            if not table_exists:
                logger.warning(f"[InterfaceContable] La tabla {table_name} no se creó correctamente. Omitiendo...")
                print(f"[InterfaceContable] La tabla {table_name} no se creó correctamente. Omitiendo...")
                continue

            # Verificar que haya datos en la tabla
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            if row_count == 0:
                logger.warning(f"[InterfaceContable] La tabla {table_name} está vacía. Omitiendo...")
                print(f"[InterfaceContable] La tabla {table_name} está vacía. Omitiendo...")
                self._cleanup_table(table_name)
                continue

        print(
            f"[InterfaceContable] Extracción de datos completada para hoja {hoja}. Total registros procesados: {self.total_records_processed}"
        )
        logger.info(
            f"[InterfaceContable] Extracción de datos completada para hoja {hoja}. Total registros procesados: {self.total_records_processed}"
        )

        # Guardar datos en la hoja correspondiente
        self._guardar_datos_excel_xlsxwriter(table_name, hoja, writer)
        total_global_records += self.total_records_processed

        # Limpiar tabla temporal
        self._cleanup_table(table_name)
    except Exception as e:
        logger.error(f"[InterfaceContable] Error al procesar hoja {hoja}: {str(e)}", exc_info=True)
        print(f"[InterfaceContable] Error al procesar hoja {hoja}: {str(e)}")
        # Intentar limpiar tabla en caso de error
        try:
            self._cleanup_table(f"{self.sqlite_table_name}_{hoja}")
        except:
            pass
        # Continuar con la siguiente hoja
        continue
"""

# 2. Mejorar el método _execute_query_to_sqlite() para verificar si hay datos
#    antes de crear la tabla y manejar mejor los errores
"""
def _execute_query_to_sqlite(self, query, table_name, chunksize=50000):
    stage_name = "Extrayendo datos de MySQL"
    self._update_progress(stage_name, 10)
    total_processed = 0
    start_extract_time = time.time()
    first_chunk = True
    try:
        with self.engine_mysql.connect() as mysql_conn:
            # Primero verificar si la consulta devuelve resultados
            print(f"[InterfaceContable] Ejecutando consulta MySQL para tabla: {table_name}")
            result_proxy = mysql_conn.execute(query)
            columns = result_proxy.keys()
            
            # Obtener el primer chunk para verificar si hay datos
            rows = result_proxy.fetchmany(chunksize)
            if not rows:
                print(f"[InterfaceContable] No hay datos para la tabla: {table_name}")
                logger.warning(f"No hay datos para la tabla: {table_name}")
                self.total_records_processed = 0
                return
            
            # Si hay datos, proceder con la creación de la tabla SQLite
            with self.engine_sqlite.connect() as sqlite_conn, sqlite_conn.begin():
                df_chunk = pd.DataFrame(rows, columns=columns)
                print(f"[InterfaceContable] Creando tabla SQLite '{table_name}' con {len(columns)} columnas y {len(rows)} filas iniciales")
                
                # Crear tabla con el primer chunk
                df_chunk.to_sql(
                    name=table_name,
                    con=sqlite_conn,
                    if_exists="replace",
                    index=False,
                    method=None,
                )
                
                # Actualizar contadores
                total_processed = len(rows)
                self.total_records_processed = total_processed
                
                # Procesar resto de chunks si hay más datos
                while True:
                    rows = result_proxy.fetchmany(chunksize)
                    if not rows:
                        break
                    df_chunk = pd.DataFrame(rows, columns=columns)
                    df_chunk.to_sql(
                        name=table_name,
                        con=sqlite_conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=1000,
                    )
                    total_processed += len(rows)
                    self.total_records_processed = total_processed
                    elapsed_total = time.time() - start_extract_time
                    progress_percent = 10 + (elapsed_total / (elapsed_total + 1)) * 70
                    self._update_progress(stage_name, progress_percent, total_processed)
                    del df_chunk
                    if total_processed % (chunksize * 5) == 0:
                        gc.collect()
            
            # Verificar que la tabla se ha creado y confirmar el recuento final
            with self.engine_sqlite.connect() as sqlite_conn:
                # Verificar que la tabla existe
                table_exists = sqlite_conn.execute(
                    text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                ).fetchone()
                
                if not table_exists:
                    raise ValueError(f"La tabla {table_name} no se creó correctamente en SQLite")
                
                # Confirmar recuento de registros
                final_count = sqlite_conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()
                
                print(f"[InterfaceContable] Tabla {table_name} creada con {final_count} registros")
                
                if final_count != total_processed:
                    logger.warning(f"Discrepancia en conteo para {table_name}: procesados {total_processed}, SQLite tiene {final_count}")
                    self.total_records_processed = final_count
            
        self._update_progress(
            "Datos extraídos a BD temporal", 80, self.total_records_processed
        )
    except SQLAlchemyError as e:
        logger.error(
            f"Error de base de datos durante la extracción para {table_name}: {e}",
            exc_info=True
        )
        print(f"[InterfaceContable] Error de BD para {table_name}: {e}")
        self._update_progress(f"Error BD en {table_name}: {e}", 100)
        # Asegurar que la tabla se elimina si hay error
        try:
            with self.engine_sqlite.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        except:
            pass
        raise
    except Exception as e:
        logger.error(
            f"Error inesperado durante la extracción para {table_name}: {e}",
            exc_info=True
        )
        print(f"[InterfaceContable] Error extraer {table_name}: {e}")
        self._update_progress(f"Error en {table_name}: {e}", 100)
        # Asegurar que la tabla se elimina si hay error
        try:
            with self.engine_sqlite.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        except:
            pass
        raise
"""

# 3. Mejorar el método _guardar_datos_excel_xlsxwriter() para verificar la existencia de la tabla antes de intentar usarla
#    (ya incluye esta verificación, pero se podría robustecer)

# 4. Recomendación general:
#    - Implementar un mecanismo de transacciones más robusto para SQLite
#    - Añadir mejores mensajes de diagnóstico y registro
#    - Considerar añadir un parámetro para continuar/ignorar errores para hojas específicas
