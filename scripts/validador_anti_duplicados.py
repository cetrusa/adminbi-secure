# -*- coding: utf-8 -*-
"""
Validador de cargue inteligente - VERSIÓN ENFOCADA EN DUPLICADOS
Objetivo: Evitar duplicados en _fact/_dev y validar sumas de Vta Neta
"""

import logging
from datetime import datetime, date
from sqlalchemy import text

logger = logging.getLogger(__name__)

class ValidadorAntiDuplicados:
    """
    Validador especializado en prevenir duplicados y verificar consistencia.
    """
    
    def __init__(self, cargador, tolerancia_monto=100.0):
        self.cargador = cargador
        self.tolerancia_monto = tolerancia_monto  # Tolerancia en pesos para diferencias
        
    def conectar(self):
        """Conectar a la BD usando la configuración del cargador."""
        return self.cargador.engine_mysql_bi.connect()
    
    def validar_antes_sincronizar(self, fecha_ini, fecha_fin):
        """
        VALIDACIÓN PRINCIPAL: Prevenir duplicados y verificar sumas.
        
        Proceso:
        1. Comparar suma Vta Neta Excel vs BD actual
        2. Detectar registros duplicados exactos
        3. Decidir acción: continuar, limpiar, o abortar
        
        Returns:
            tuple: (continuar: bool, accion_recomendada: str, detalle: dict)
        """
        print("🔒 INICIANDO VALIDACIÓN ANTI-DUPLICADOS")
        print("="*60)
        logging.info("🔒 Validación anti-duplicados iniciada")
        
        try:
            with self.conectar() as conn:
                # PASO 1: Obtener totales actuales
                totales = self._obtener_totales_completos(conn, fecha_ini, fecha_fin)
                if not totales:
                    return False, "ERROR_CONSULTA", {}
                
                # PASO 2: Detectar duplicados exactos
                duplicados = self._detectar_duplicados_exactos(
                    conn,
                    totales.get('fecha_filtro_ini'),
                    totales.get('fecha_filtro_fin')
                )
                
                # PASO 3: Analizar y decidir
                decision = self._analizar_y_decidir(totales, duplicados)
                
                # PASO 4: Registrar en tabla de control
                self._registrar_validacion(conn, decision, totales, duplicados)
                
                return decision['continuar'], decision['accion'], decision['detalle']
                
        except Exception as e:
            error_msg = f"Error en validación: {e}"
            print(f"❌ {error_msg}")
            logging.error(error_msg)
            return False, "ERROR_VALIDACION", {'error': str(e)}
    
    def _normalizar_fecha(self, valor):
        """Convertir strings o valores inválidos en fechas válidas."""
        if not valor:
            return None

        if isinstance(valor, datetime):
            return valor.date()

        if isinstance(valor, date):
            return valor

        if isinstance(valor, str):
            texto = valor.strip()
            if not texto or texto.startswith("0000"):
                return None
            for formato in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(texto, formato).date()
                except ValueError:
                    continue
            logging.debug(f"Fecha no válida recibida para normalizar: {valor}")
            return None

        return None

    def _obtener_totales_completos(self, conn, fecha_ini, fecha_fin):
        """Obtener totales de staging (Excel) y BD actual (_fact + _dev)."""
        
        try:
            fecha_ini_norm = self._normalizar_fecha(fecha_ini)
            fecha_fin_norm = self._normalizar_fecha(fecha_fin)

            if not fecha_ini_norm or not fecha_fin_norm:
                rango_query = text("""
                    SELECT 
                        MIN(CASE WHEN `Fecha` IS NOT NULL AND `Fecha` != '0000-00-00' THEN `Fecha` END) as fecha_ini,
                        MAX(CASE WHEN `Fecha` IS NOT NULL AND `Fecha` != '0000-00-00' THEN `Fecha` END) as fecha_fin
                    FROM infoventas
                """)
                rango_result = conn.execute(rango_query).fetchone()
                fecha_ini_norm = fecha_ini_norm or rango_result.fecha_ini
                fecha_fin_norm = fecha_fin_norm or rango_result.fecha_fin

            params = {}
            where_clause = ""
            if fecha_ini_norm and fecha_fin_norm:
                params = {'fecha_ini': fecha_ini_norm, 'fecha_fin': fecha_fin_norm}
                where_clause = "WHERE `Fecha` BETWEEN :fecha_ini AND :fecha_fin"
            else:
                logging.warning("Validación ejecutándose sin rango de fechas definido (se evaluará todo el staging)")

            if fecha_ini_norm and fecha_fin_norm:
                print(f"📅 RANGO ANALIZADO: {fecha_ini_norm} → {fecha_fin_norm}")
            else:
                print("📅 RANGO ANALIZADO: Todo el staging (sin filtros por fecha)")

            # Total staging (datos del Excel recién cargados)
            query_staging = text(f"""
                SELECT 
                    COUNT(*) as registros,
                    COALESCE(SUM(`Vta neta`), 0) as suma_vta_neta,
                    MIN(`Fecha`) as fecha_min,
                    MAX(`Fecha`) as fecha_max
                FROM infoventas 
                {where_clause}
            """)
            
            result = conn.execute(query_staging, params).fetchone()
            
            staging = {
                'registros': result.registros,
                'suma_vta_neta': float(result.suma_vta_neta or 0),
                'fecha_min': result.fecha_min,
                'fecha_max': result.fecha_max
            }
            
            # Total _fact (registros Tipo 0 ya sincronizados)
            query_fact = text(f"""
                SELECT 
                    COUNT(*) as registros,
                    COALESCE(SUM(`Vta neta`), 0) as suma_vta_neta
                FROM infoventas_2025_fact 
                {where_clause}
            """)
            
            result = conn.execute(query_fact, params).fetchone()
            
            fact = {
                'registros': result.registros,
                'suma_vta_neta': float(result.suma_vta_neta or 0)
            }
            
            # Total _dev (registros Tipo 1 ya sincronizados)
            query_dev = text(f"""
                SELECT 
                    COUNT(*) as registros,
                    COALESCE(SUM(`Vta neta`), 0) as suma_vta_neta
                FROM infoventas_2025_dev 
                {where_clause}
            """)
            
            result = conn.execute(query_dev, params).fetchone()
            
            dev = {
                'registros': result.registros,
                'suma_vta_neta': float(result.suma_vta_neta or 0)
            }
            
            # ANÁLISIS CRÍTICO POR TIPO
            # Verificar registros staging por tipo que irían a cada tabla
            query_staging_por_tipo = text(f"""
                SELECT 
                    `Tipo`,
                    COUNT(*) as registros,
                    COALESCE(SUM(`Vta neta`), 0) as suma_vta_neta
                FROM infoventas 
                {where_clause}
                GROUP BY `Tipo`
            """)
            
            result_tipos = conn.execute(query_staging_por_tipo, params).fetchall()
            
            staging_tipo_0 = {'registros': 0, 'suma_vta_neta': 0}  # Van a _fact
            staging_tipo_1 = {'registros': 0, 'suma_vta_neta': 0}  # Van a _dev
            
            for row in result_tipos:
                if str(row.Tipo) == '0':
                    staging_tipo_0 = {
                        'registros': row.registros,
                        'suma_vta_neta': float(row.suma_vta_neta or 0)
                    }
                elif str(row.Tipo) == '1':
                    staging_tipo_1 = {
                        'registros': row.registros,
                        'suma_vta_neta': float(row.suma_vta_neta or 0)
                    }
            
            # Calcular totales combinados
            bd_total = {
                'registros': fact['registros'] + dev['registros'],
                'suma_vta_neta': fact['suma_vta_neta'] + dev['suma_vta_neta']
            }
            
            diferencia = staging['suma_vta_neta'] - bd_total['suma_vta_neta']
            
            print(f"📊 ANÁLISIS POR TIPO:")
            print(f"   📄 EXCEL (staging):")
            print(f"      Tipo 0 → _fact: {staging_tipo_0['registros']:,} registros → ${staging_tipo_0['suma_vta_neta']:,.2f}")
            print(f"      Tipo 1 → _dev:  {staging_tipo_1['registros']:,} registros → ${staging_tipo_1['suma_vta_neta']:,.2f}")
            print(f"      TOTAL:          {staging['registros']:,} registros → ${staging['suma_vta_neta']:,.2f}")
            print(f"   🏭 BD ACTUAL:")
            print(f"      _fact: {fact['registros']:,} registros → ${fact['suma_vta_neta']:,.2f}")
            print(f"      _dev:  {dev['registros']:,} registros → ${dev['suma_vta_neta']:,.2f}")
            print(f"      TOTAL: {bd_total['registros']:,} registros → ${bd_total['suma_vta_neta']:,.2f}")
            
            # Diferencias por tipo
            diferencia_fact = staging_tipo_0['suma_vta_neta'] - fact['suma_vta_neta']
            diferencia_dev = staging_tipo_1['suma_vta_neta'] - dev['suma_vta_neta']
            diferencia_total = diferencia_fact + diferencia_dev
            
            print(f"   📊 DIFERENCIAS:")
            print(f"      _fact: ${diferencia_fact:,.2f}")
            print(f"      _dev:  ${diferencia_dev:,.2f}")
            print(f"      TOTAL: ${diferencia_total:,.2f}")
            
            return {
                'staging': staging,
                'staging_tipo_0': staging_tipo_0,
                'staging_tipo_1': staging_tipo_1,
                'fact': fact,
                'dev': dev,
                'bd_total': bd_total,
                'diferencia': diferencia_total,
                'diferencia_fact': diferencia_fact,
                'diferencia_dev': diferencia_dev,
                'fecha_filtro_ini': fecha_ini_norm,
                'fecha_filtro_fin': fecha_fin_norm
            }
            
        except Exception as e:
            logging.error(f"Error obteniendo totales: {e}")
            return None
    
    def _detectar_duplicados_exactos(self, conn, fecha_ini, fecha_fin):
        """Detectar registros en staging que YA existen en _fact (duplicados exactos)."""
        
        try:
            fecha_ini_norm = self._normalizar_fecha(fecha_ini)
            fecha_fin_norm = self._normalizar_fecha(fecha_fin)

            params = {}
            where_clause = ""
            if fecha_ini_norm and fecha_fin_norm:
                params = {'fecha_ini': fecha_ini_norm, 'fecha_fin': fecha_fin_norm}
                where_clause = "AND s.`Fecha` BETWEEN :fecha_ini AND :fecha_fin"
            # Duplicados Tipo 0 (staging vs _fact)
            query_duplicados_fact = text(f"""
                SELECT COUNT(*) as duplicados_fact
                FROM infoventas s
                WHERE s.`Tipo` = '0'
                AND EXISTS (
                    SELECT 1 FROM infoventas_2025_fact f 
                    WHERE f.`Fecha` = s.`Fecha` 
                    AND f.`Cod. cliente` = s.`Cod. cliente`
                    AND f.`Cod. vendedor` = s.`Cod. vendedor`
                    AND f.`Cod. productto` = s.`Cod. productto`
                    AND f.`Fac. numero` = s.`Fac. numero`
                )
                {where_clause}
            """)
            
            duplicados_fact = conn.execute(query_duplicados_fact, params).fetchone().duplicados_fact
            
            # Duplicados Tipo 1 (staging vs _dev)
            query_duplicados_dev = text(f"""
                SELECT COUNT(*) as duplicados_dev
                FROM infoventas s
                WHERE s.`Tipo` = '1'
                AND EXISTS (
                    SELECT 1 FROM infoventas_2025_dev d 
                    WHERE d.`Fecha` = s.`Fecha` 
                    AND d.`Cod. cliente` = s.`Cod. cliente`
                    AND d.`Cod. vendedor` = s.`Cod. vendedor`
                    AND d.`Cod. productto` = s.`Cod. productto`
                    AND d.`Fac. numero` = s.`Fac. numero`
                )
                {where_clause}
            """)
            
            duplicados_dev = conn.execute(query_duplicados_dev, params).fetchone().duplicados_dev
            
            total_duplicados = duplicados_fact + duplicados_dev
            
            print(f"🔍 DUPLICADOS POR TIPO:")
            print(f"   📊 Tipo 0 vs _fact: {duplicados_fact:,}")
            print(f"   📊 Tipo 1 vs _dev:  {duplicados_dev:,}")
            print(f"   📊 TOTAL DUPLICADOS: {total_duplicados:,}")
            
            # Ejemplos de duplicados para análisis
            ejemplos = []

            ejemplos_fact_query = text(f"""
                SELECT 
                    s.`Fecha`,
                    s.`Cod. cliente`, 
                    s.`Cod. productto`,
                    s.`Vta neta` as monto_staging,
                    s.`Tipo`,
                    'fact' as tabla_destino
                FROM infoventas s
                WHERE s.`Tipo` = '0' AND EXISTS (
                    SELECT 1 FROM infoventas_2025_fact f 
                    WHERE f.`Fecha` = s.`Fecha` 
                    AND f.`Cod. cliente` = s.`Cod. cliente`
                    AND f.`Cod. vendedor` = s.`Cod. vendedor`
                    AND f.`Cod. productto` = s.`Cod. productto`
                    AND f.`Fac. numero` = s.`Fac. numero`
                )
                {where_clause}
                LIMIT 3
            """)

            ejemplos_dev_query = text(f"""
                SELECT 
                    s.`Fecha`,
                    s.`Cod. cliente`, 
                    s.`Cod. productto`,
                    s.`Vta neta` as monto_staging,
                    s.`Tipo`,
                    'dev' as tabla_destino
                FROM infoventas s
                WHERE s.`Tipo` = '1' AND EXISTS (
                    SELECT 1 FROM infoventas_2025_dev d 
                    WHERE d.`Fecha` = s.`Fecha` 
                    AND d.`Cod. cliente` = s.`Cod. cliente`
                    AND d.`Cod. vendedor` = s.`Cod. vendedor`
                    AND d.`Cod. productto` = s.`Cod. productto`
                    AND d.`Fac. numero` = s.`Fac. numero`
                )
                {where_clause}
                LIMIT 3
            """)

            ejemplos.extend(conn.execute(ejemplos_fact_query, params).fetchall())
            ejemplos.extend(conn.execute(ejemplos_dev_query, params).fetchall())
            
            return {
                'total': total_duplicados,
                'duplicados_fact': duplicados_fact,
                'duplicados_dev': duplicados_dev,
                'ejemplos': ejemplos[:6]  # Máximo 6 ejemplos
            }
            
        except Exception as e:
            logging.error(f"Error detectando duplicados: {e}")
            return {'total': 0, 'exactos': 0, 'similares': 0, 'ejemplos': []}
    
    def _analizar_y_decidir(self, totales, duplicados):
        """
        Analizar la situación y decidir qué hacer.
        
        NUEVA LÓGICA (Oct 22, 2025):
        - Los duplicados son NORMALES porque se actualiza el mes completo
        - El SP sp_infoventas_full_maintenance() es quien sincroniza los no-duplicados
        - La validación es SOLO una seguridad para verificar Vta neta total
        - SIEMPRE permitir que continúe SI la diferencia ≤ tolerancia
        - SOLO bloquear si diferencia > tolerancia (inconsistencia monetaria real)
        """
        
        diferencia = abs(totales['diferencia'])
        total_duplicados = duplicados['total']
        registros_bd = totales.get('bd_total', {}).get('registros', 0)
        registros_staging = totales.get('staging', {}).get('registros', 0)
        
        print(f"\n🤔 ANÁLISIS DE DECISIÓN:")
        print(f"   💰 Diferencia monto: ${diferencia:,.2f} (tolerancia: ${self.tolerancia_monto:,.2f})")
        print(f"   📊 Duplicados: {total_duplicados:,} (normales en actualización de mes completo)")
        print(f"   📊 Registros BD: {registros_bd:,} | Staging: {registros_staging:,}")
        
        # ============================================================
        # LÓGICA SIMPLIFICADA: Permitir SIEMPRE si Vta neta coincide
        # ============================================================
        
        if diferencia <= self.tolerancia_monto:
            # ✅ CONTINUAR - La diferencia monetaria es aceptable
            # El SP se encargará de sincronizar los no-duplicados
            
            if total_duplicados == 0:
                decision = {
                    'continuar': True,
                    'accion': 'CONTINUAR_NORMAL',
                    'mensaje': f"✅ Sin duplicados, diferencia aceptable (${diferencia:,.2f})",
                    'detalle': totales
                }
            elif total_duplicados > 0 and total_duplicados <= 100:
                decision = {
                    'continuar': True,
                    'accion': 'CONTINUAR_CON_ADVERTENCIA',
                    'mensaje': f"⚠️ {total_duplicados} duplicados (normales en actualización) - Continuando con sincronización",
                    'detalle': {**totales, 'duplicados': duplicados}
                }
            else:  # total_duplicados > 100
                decision = {
                    'continuar': True,
                    'accion': 'CONTINUAR_DUPLICADOS_PREEXISTENTES',
                    'mensaje': (f"ℹ️ {total_duplicados:,} registros iguales en BD (actualización de mes completo). "
                               f"Sumas coinciden (${diferencia:,.2f}). Sincronización delegada al SP."),
                    'detalle': {**totales, 'duplicados': duplicados}
                }
        
        elif diferencia > self.tolerancia_monto:
            decision = {
                'continuar': False,
                'accion': 'REVISION_MANUAL_REQUERIDA',
                'mensaje': f"🚨 Diferencia muy alta: ${diferencia:,.2f} - Revisar manualmente",
                'detalle': {**totales, 'duplicados': duplicados}
            }
        else:
            decision = {
                'continuar': False,
                'accion': 'CASO_NO_CONTEMPLADO',
                'mensaje': "🤷 Situación no contemplada - Revisar manualmente",
                'detalle': {**totales, 'duplicados': duplicados}
            }
        
        print(f"   🎯 DECISIÓN: {decision['accion']}")
        print(f"   💬 {decision['mensaje']}")
        logging.info(f"Validación - Duplicados: {total_duplicados}, Diferencia: ${diferencia:,.2f}, Acción: {decision['accion']}, Continuar: {decision['continuar']}")
        
        return decision
    
    def _registrar_validacion(self, conn, decision, totales, duplicados):
        """Registrar validación en tabla de control."""
        try:
            insert_query = text("""
                INSERT INTO validacion_cargue_diario 
                (fecha_control, mes, anno, 
                 registros_staging, suma_staging,
                 registros_fact, suma_fact,
                 registros_dev, suma_dev,
                 estado_validacion, mensaje_validacion,
                 duplicados_fusionados, accion_tomada)
                VALUES 
                (:fecha_control, :mes, :anno,
                 :registros_staging, :suma_staging,
                 :registros_fact, :suma_fact,
                 :registros_dev, :suma_dev,
                 :estado_validacion, :mensaje_validacion,
                 :duplicados_fusionados, :accion_tomada)
            """)
            
            estado = 'OK' if decision['continuar'] else 'ERROR'

            referencia = totales.get('staging', {}).get('fecha_min') or datetime.now().date()
            referencia = self._normalizar_fecha(referencia) or datetime.now().date()
            
            conn.execute(insert_query, {
                'fecha_control': datetime.now().date(),
                'mes': referencia.month,
                'anno': referencia.year,
                'registros_staging': totales['staging']['registros'],
                'suma_staging': totales['staging']['suma_vta_neta'],
                'registros_fact': totales['fact']['registros'],
                'suma_fact': totales['fact']['suma_vta_neta'],
                'registros_dev': totales['dev']['registros'],
                'suma_dev': totales['dev']['suma_vta_neta'],
                'estado_validacion': estado,
                'mensaje_validacion': decision['mensaje'],
                'duplicados_fusionados': duplicados['total'],
                'accion_tomada': decision['accion']
            })
            conn.commit()
            
            print(f"✅ Validación registrada en tabla de control")
            logging.info(f"Validación registrada: {decision['accion']}")
            
        except Exception as e:
            logging.error(f"Error registrando validación: {e}")


# ============================================================
# FUNCIÓN DE INTEGRACIÓN
# ============================================================

def validar_cargue_antes_sincronizar(cargador, fecha_ini, fecha_fin):
    """
    Función principal para validar antes de sincronizar a _fact/_dev.
    
    Returns:
        bool: True si es seguro continuar, False si requiere atención
    """
    try:
        validador = ValidadorAntiDuplicados(cargador)
        continuar, accion, detalle = validador.validar_antes_sincronizar(fecha_ini, fecha_fin)
        
        if continuar:
            print(f"✅ VALIDACIÓN EXITOSA: {accion}")
            logging.info(f"✅ Validación exitosa: {accion}")
            return True
        else:
            print(f"❌ VALIDACIÓN FALLIDA: {accion}")
            print(f"💡 ACCIÓN REQUERIDA: {detalle.get('mensaje', 'Revisar manualmente')}")
            logging.error(f"❌ Validación fallida: {accion}")
            return False
            
    except Exception as e:
        print(f"❌ Error en validación: {e}")
        logging.error(f"Error en validación: {e}")
        return False