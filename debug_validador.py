#!/usr/bin/env python3
"""
Script de diagnóstico para el validador anti-duplicados
"""

import sys
import os
from datetime import date, datetime
sys.path.append('.')

def debug_validador():
    print("🔍 DIAGNÓSTICO DEL VALIDADOR ANTI-DUPLICADOS")
    print("=" * 60)
    
    try:
        # Usar el mismo patrón que el validador existente
        from scripts.cargue_infoventas_insert import CargueInfoVentasInsert
        
        print("📡 Conectando a la base de datos usando el sistema seguro existente...")
        
        # Crear cargador usando el constructor correcto
        cargador = CargueInfoVentasInsert('distrijass', 'SYSTEM')
        
        # Usar la conexión del cargador (mismo patrón que validador)
        conn = cargador.engine_mysql_bi.connect()
        print("✅ Conexión exitosa con engine SQLAlchemy")
        
        # Probar consultas básicas
        cursor = conn.cursor()
        
        print("\n🔍 Verificando tablas necesarias...")
        
        # 1. Verificar tabla infoventas
        cursor.execute("SELECT COUNT(*) FROM infoventas WHERE fecha_venta >= '2025-10-01'")
        count_staging = cursor.fetchone()[0]
        print(f"   📊 infoventas (Oct 2025): {count_staging:,} registros")
        
        # 2. Verificar tabla infoventas_fact
        cursor.execute("SELECT COUNT(*) FROM infoventas_fact WHERE fecha_venta >= '2025-10-01'")
        count_fact = cursor.fetchone()[0]
        print(f"   📊 infoventas_fact (Oct 2025): {count_fact:,} registros")
        
        # 3. Verificar tabla infoventas_dev
        cursor.execute("SELECT COUNT(*) FROM infoventas_dev WHERE fecha_venta >= '2025-10-01'")
        count_dev = cursor.fetchone()[0]
        print(f"   📊 infoventas_dev (Oct 2025): {count_dev:,} registros")
        
        print(f"\n📈 RESUMEN:")
        print(f"   Staging: {count_staging:,}")
        print(f"   Fact:    {count_fact:,}")  
        print(f"   Dev:     {count_dev:,}")
        print(f"   Total sync: {count_fact + count_dev:,}")
        
        if count_staging > 0 and (count_fact > 0 or count_dev > 0):
            print("⚠️ DUPLICADOS DETECTADOS: Staging tiene datos Y las tablas _fact/_dev también")
            print("💡 Esto confirma que necesitamos validación antes de sincronizar")
        
        # 4. Verificar distribución por tipo en staging
        cursor.execute("""
            SELECT tipo, COUNT(*) as registros, SUM(monto_venta) as suma_vta_neta
            FROM infoventas 
            WHERE fecha_venta >= '2025-10-01' AND fecha_venta <= '2025-10-31'
            GROUP BY tipo
        """)
        
        tipos = cursor.fetchall()
        print(f"\n📊 DISTRIBUCIÓN POR TIPO EN STAGING:")
        for tipo, registros, suma in tipos:
            print(f"   Tipo {tipo}: {registros:,} registros, Vta Neta: ${suma:,.2f}")
            
        cursor.close()
        conn.close()
        
        print("\n✅ Diagnóstico completado exitosamente")
        
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_validador()