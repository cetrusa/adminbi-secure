#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de diagnóstico de duplicados en _fact y _dev
Detecta y analiza valores duplicados después de sincronización
Fecha: 21 de octubre 2025
"""

import sys
import os
from datetime import datetime
from scripts.conexion import ConexionMariadb3
from scripts.config import get_default_service

# Configuración de logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('D:\\Logs\\DataZenithBI\\diagnostico_duplicados.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def detectar_duplicados_por_campo(conn, tabla: str, campos_clave: list):
    """
    Detecta duplicados en una tabla por campos clave.
    Retorna: (total_registros, registros_unicos, duplicados_encontrados, detalle)
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🔍 Analizando tabla: {tabla}")
    logger.info(f"{'='*80}")
    
    cursor = conn.cursor()
    
    try:
        # Total de registros
        cursor.execute(f"SELECT COUNT(*) as total FROM {tabla}")
        total_registros = cursor.fetchone()[0]
        logger.info(f"📊 Total de registros en {tabla}: {total_registros:,}")
        
        # Registros únicos por campos clave
        campos_sql = ", ".join(campos_clave)
        cursor.execute(f"""
            SELECT COUNT(DISTINCT CONCAT({', '-', '.join([f'CAST({c} AS CHAR)' for c in campos_clave])}) ) as unicos
            FROM {tabla}
        """)
        registros_unicos = cursor.fetchone()[0]
        logger.info(f"📊 Registros únicos (por {campos_sql}): {registros_unicos:,}")
        
        # Detectar duplicados
        cursor.execute(f"""
            SELECT 
                {campos_sql},
                COUNT(*) as repeticiones,
                SUM(monto_venta) as total_monto
            FROM {tabla}
            GROUP BY {campos_sql}
            HAVING COUNT(*) > 1
            LIMIT 50
        """)
        duplicados = cursor.fetchall()
        
        if duplicados:
            logger.warning(f"⚠️ ¡ENCONTRADOS {len(duplicados)} GRUPOS DE DUPLICADOS!")
            logger.warning(f"\nPrimeros duplicados encontrados:")
            for dup in duplicados:
                logger.warning(f"   {dup}")
        else:
            logger.info(f"✅ No se encontraron duplicados en {tabla}")
        
        return total_registros, registros_unicos, len(duplicados), duplicados
        
    finally:
        cursor.close()

def comparar_fact_vs_dev(conn):
    """
    Compara registros entre _fact y _dev para identificar problemas de sincronización.
    """
    logger.info(f"\n{'='*80}")
    logger.info("🔄 COMPARACIÓN: _fact vs _dev")
    logger.info(f"{'='*80}")
    
    cursor = conn.cursor()
    
    try:
        # Tabla temporal con datos agregados
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_fact_summary AS
            SELECT 
                YEAR(fecha_venta) as ano,
                MONTH(fecha_venta) as mes,
                COUNT(*) as registros_fact,
                SUM(monto_venta) as suma_fact
            FROM bi_distrijass.infoventas_2025_fact
            GROUP BY YEAR(fecha_venta), MONTH(fecha_venta)
        """)
        
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_dev_summary AS
            SELECT 
                YEAR(fecha_venta) as ano,
                MONTH(fecha_venta) as mes,
                COUNT(*) as registros_dev,
                SUM(monto_venta) as suma_dev
            FROM bi_distrijass.infoventas_2025_dev
            GROUP BY YEAR(fecha_venta), MONTH(fecha_venta)
        """)
        
        # Comparar
        cursor.execute("""
            SELECT 
                COALESCE(f.ano, d.ano) as ano,
                COALESCE(f.mes, d.mes) as mes,
                f.registros_fact,
                d.registros_dev,
                f.suma_fact,
                d.suma_dev,
                f.registros_fact - COALESCE(d.registros_dev, 0) as diferencia_registros,
                f.suma_fact - COALESCE(d.suma_dev, 0) as diferencia_monto
            FROM temp_fact_summary f
            FULL OUTER JOIN temp_dev_summary d
                ON f.ano = d.ano AND f.mes = d.mes
            ORDER BY ano, mes
        """)
        
        comparacion = cursor.fetchall()
        
        logger.info("\n📊 Comparación FACT vs DEV por período:")
        logger.info(f"{'Año-Mes':<12} {'_fact':<15} {'_dev':<15} {'Dif. Reg.':<12} {'Dif. Monto':<15}")
        logger.info("-" * 70)
        
        hay_diferencias = False
        for row in comparacion:
            ano, mes, reg_fact, reg_dev, suma_fact, suma_dev, dif_reg, dif_monto = row
            
            if dif_reg != 0 or dif_monto != 0:
                hay_diferencias = True
                logger.warning(
                    f"{ano}-{mes:02d}       "
                    f"{reg_fact or 0:<15,} {reg_dev or 0:<15,} "
                    f"{dif_reg or 0:<12,} {dif_monto or 0:.2f}"
                )
            else:
                logger.info(
                    f"{ano}-{mes:02d}       "
                    f"{reg_fact or 0:<15,} {reg_dev or 0:<15,} "
                    f"{dif_reg or 0:<12,} {dif_monto or 0:.2f}"
                )
        
        if hay_diferencias:
            logger.warning("\n⚠️ ¡ENCONTRADAS DIFERENCIAS ENTRE _fact Y _dev!")
        else:
            logger.info("\n✅ Sincronización correcta entre _fact y _dev")
        
        return comparacion
        
    finally:
        cursor.close()

def detectar_duplicados_por_fecha(conn, tabla: str, anno: int = 2025):
    """
    Detecta si hay registros duplicados para la misma fecha y proveedor.
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🔎 Detectando duplicados por FECHA/PROVEEDOR en {tabla}")
    logger.info(f"{'='*80}")
    
    cursor = conn.cursor()
    
    try:
        # Buscar duplicados por fecha y proveedor
        cursor.execute(f"""
            SELECT 
                fecha_venta,
                cod_proveedor,
                COUNT(*) as repeticiones,
                SUM(monto_venta) as suma_monto,
                MIN(id_infoventa) as id_primero,
                MAX(id_infoventa) as id_ultimo
            FROM {tabla}
            WHERE YEAR(fecha_venta) = {anno}
            GROUP BY fecha_venta, cod_proveedor
            HAVING COUNT(*) > 1
            LIMIT 100
        """)
        
        duplicados = cursor.fetchall()
        
        if duplicados:
            logger.warning(f"⚠️ ENCONTRADOS {len(duplicados)} REGISTROS DUPLICADOS!")
            logger.warning(f"\nMuestras de duplicados (primeros 20):")
            logger.warning(f"{'Fecha':<12} {'Proveedor':<15} {'Repeticiones':<12} {'Suma Monto':<15}")
            logger.warning("-" * 55)
            
            for i, dup in enumerate(duplicados[:20]):
                fecha, proveedor, repeticiones, suma_monto, id_primero, id_ultimo = dup
                logger.warning(
                    f"{fecha}  {proveedor:<15} {repeticiones:<12} {suma_monto:>15,.2f}"
                )
                
                # Mostrar detalles de estos registros duplicados
                cursor2 = conn.cursor()
                cursor2.execute(f"""
                    SELECT id_infoventa, fecha_venta, cod_proveedor, monto_venta, fecha_cargue
                    FROM {tabla}
                    WHERE fecha_venta = '{fecha}' AND cod_proveedor = '{proveedor}'
                    ORDER BY id_infoventa
                """)
                registros = cursor2.fetchall()
                for reg in registros:
                    logger.warning(f"       └─ ID:{reg[0]} Fecha:{reg[1]} Monto:{reg[3]:.2f} Cargue:{reg[4]}")
                cursor2.close()
        else:
            logger.info(f"✅ No se encontraron duplicados por fecha/proveedor en {tabla}")
        
        return duplicados
        
    finally:
        cursor.close()

def verificar_sincronizacion_vista(conn):
    """
    Verifica que la vista vw_infoventas está correcta.
    """
    logger.info(f"\n{'='*80}")
    logger.info("🔍 Verificando integridad de la VISTA vw_infoventas")
    logger.info(f"{'='*80}")
    
    cursor = conn.cursor()
    
    try:
        # Contar registros en vista
        cursor.execute("SELECT COUNT(*) FROM bi_distrijass.vw_infoventas")
        total_vista = cursor.fetchone()[0]
        
        # Contar registros en tablas clasificadas
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT * FROM bi_distrijass.infoventas_2023_fact
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2024_fact
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2025_fact
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2026_fact
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2023_dev
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2024_dev
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2025_dev
                UNION ALL
                SELECT * FROM bi_distrijass.infoventas_2026_dev
            ) as todas_tablas
        """)
        total_clasificados = cursor.fetchone()[0]
        
        logger.info(f"📊 Registros en vista: {total_vista:,}")
        logger.info(f"📊 Registros en tablas _fact/_dev: {total_clasificados:,}")
        
        if total_vista == total_clasificados:
            logger.info(f"✅ Vista sincronizada correctamente")
        else:
            diferencia = total_vista - total_clasificados
            logger.warning(f"⚠️ DISCREPANCIA: {diferencia:,} registros no coinciden!")
        
        return total_vista, total_clasificados
        
    finally:
        cursor.close()

def main():
    """Ejecuta diagnóstico completo."""
    logger.info("\n")
    logger.info("╔" + "="*78 + "╗")
    logger.info("║" + " "*20 + "🔍 DIAGNÓSTICO DE DUPLICADOS - 21 OCTUBRE 2025" + " "*12 + "║")
    logger.info("╚" + "="*78 + "╝")
    
    try:
        # Conectar a BD
        logger.info("\n🔗 Conectando a base de datos...")
        config_service = get_default_service()
        config = config_service.get("bi_distrijass", "SYSTEM")
        conn = ConexionMariadb3(
            config['user'],
            config['pass'],
            config['host'],
            int(config['port']),
            config['db']
        ).get_engine()
        
        logger.info("✅ Conexión establecida")
        
        # Análisis 1: Duplicados en tablas
        logger.info("\n" + "="*80)
        logger.info("ANÁLISIS 1: BÚSQUEDA DE DUPLICADOS POR LLAVE PRIMARIA")
        logger.info("="*80)
        
        tablas_revisar = [
            ('bi_distrijass.infoventas_2025_fact', ['fecha_venta', 'cod_proveedor', 'id_infoproducto']),
            ('bi_distrijass.infoventas_2025_dev', ['fecha_venta', 'cod_proveedor', 'id_infoproducto'])
        ]
        
        for tabla, campos_clave in tablas_revisar:
            detectar_duplicados_por_campo(conn, tabla, campos_clave)
        
        # Análisis 2: Duplicados por fecha/proveedor
        logger.info("\n" + "="*80)
        logger.info("ANÁLISIS 2: BÚSQUEDA DE DUPLICADOS POR FECHA/PROVEEDOR")
        logger.info("="*80)
        
        detectar_duplicados_por_fecha(conn, 'bi_distrijass.infoventas_2025_fact')
        detectar_duplicados_por_fecha(conn, 'bi_distrijass.infoventas_2025_dev')
        
        # Análisis 3: Comparación _fact vs _dev
        logger.info("\n" + "="*80)
        logger.info("ANÁLISIS 3: COMPARACIÓN DE SINCRONIZACIÓN")
        logger.info("="*80)
        
        comparar_fact_vs_dev(conn)
        
        # Análisis 4: Integridad de vista
        logger.info("\n" + "="*80)
        logger.info("ANÁLISIS 4: INTEGRIDAD DE VISTA")
        logger.info("="*80)
        
        verificar_sincronizacion_vista(conn)
        
        logger.info("\n" + "="*80)
        logger.info("✅ DIAGNÓSTICO COMPLETADO")
        logger.info("="*80)
        logger.info("\n📄 Resultados guardados en: D:\\Logs\\DataZenithBI\\diagnostico_duplicados.log\n")
        
    except Exception as e:
        logger.error(f"❌ Error durante diagnóstico: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
