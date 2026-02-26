#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de migración para corregir la clave única de fact_infoproducto

Este script:
1. Verifica duplicados existentes
2. Hace backup de duplicados (opcional)
3. Elimina duplicados manteniendo el más reciente
4. Elimina la clave única antigua
5. Crea la nueva clave única correcta
6. Verifica la integridad de datos

Uso:
    python migrate_fix_infoproducto_unique_key.py [--dry-run] [--backup]

Opciones:
    --dry-run: Solo muestra lo que haría, sin hacer cambios
    --backup: Crea tabla de backup antes de eliminar duplicados
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Agregar el directorio raíz al path para imports
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# Configurar Django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "adminbi.settings")

import django
django.setup()

from sqlalchemy import create_engine, text
import json
from pathlib import Path


class MigracionInfoProducto:
    """Migración de clave única para fact_infoproducto"""

    def __init__(self, dry_run=False, backup=False):
        self.dry_run = dry_run
        self.backup = backup
        self.engine = None

    def _get_engine(self):
        """Obtener engine de SQLAlchemy para MySQL BI"""
        # Leer configuración desde secret.json
        secret_file = Path(__file__).resolve().parent.parent.parent / "secret.json"
        
        with open(secret_file, 'r') as f:
            config = json.load(f)
        
        # Datos de conexión para BD BI
        user = config.get("nmUsrIn")
        password = config.get("txPassIn")
        host = config.get("hostServerIn")
        port = config.get("portServerIn")
        database = config.get("dbBi")

        connection_string = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            "?charset=utf8mb4"
        )
        return create_engine(connection_string)

    def verificar_duplicados(self):
        """Paso 1: Verificar si existen duplicados con la nueva clave"""
        print("\n" + "="*60)
        print("PASO 1: Verificando duplicados existentes")
        print("="*60)

        query = """
        SELECT 
            fecha_reporte,
            fuente_id,
            codigo_pedido,
            producto_codigo,
            COUNT(*) as total_duplicados,
            GROUP_CONCAT(id ORDER BY id) as ids_duplicados
        FROM fact_infoproducto
        WHERE codigo_pedido IS NOT NULL 
          AND producto_codigo IS NOT NULL
        GROUP BY 
            fecha_reporte,
            fuente_id,
            codigo_pedido,
            producto_codigo
        HAVING COUNT(*) > 1
        ORDER BY total_duplicados DESC
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            duplicados = result.fetchall()

        if not duplicados:
            print("✓ No se encontraron duplicados")
            return 0

        print(f"⚠️  Se encontraron {len(duplicados)} grupos de duplicados:")
        print(f"\n{'Fecha':<12} {'Fuente':<10} {'Pedido':<15} {'Producto':<15} {'Cant':<6} IDs")
        print("-" * 80)
        
        total_registros_duplicados = 0
        for row in duplicados[:10]:  # Mostrar solo primeros 10
            fecha, fuente, pedido, producto, total, ids = row
            total_registros_duplicados += total
            ids_list = ids.split(',')
            print(f"{fecha} {fuente:<10} {pedido:<15} {producto:<15} {total:<6} {', '.join(ids_list[:3])}{'...' if len(ids_list) > 3 else ''}")

        if len(duplicados) > 10:
            print(f"... y {len(duplicados) - 10} grupos más")

        print(f"\n📊 Total de registros duplicados: {total_registros_duplicados}")
        print(f"📊 Registros a eliminar: {total_registros_duplicados - len(duplicados)}")
        print(f"📊 Registros a mantener: {len(duplicados)} (el más reciente de cada grupo)")

        return total_registros_duplicados

    def crear_backup_duplicados(self):
        """Paso 2: Crear backup de duplicados antes de eliminar"""
        if not self.backup:
            print("\n⏭️  Saltando backup (opción --backup no especificada)")
            return

        print("\n" + "="*60)
        print("PASO 2: Creando backup de duplicados")
        print("="*60)

        # Eliminar tabla de backup si existe
        drop_query = "DROP TABLE IF EXISTS fact_infoproducto_duplicados_backup"
        
        # Crear tabla de backup
        create_query = """
        CREATE TABLE fact_infoproducto_duplicados_backup AS
        SELECT t1.*
        FROM fact_infoproducto t1
        INNER JOIN fact_infoproducto t2 
        WHERE 
            t1.fecha_reporte = t2.fecha_reporte
            AND t1.fuente_id = t2.fuente_id
            AND COALESCE(t1.codigo_pedido, '') = COALESCE(t2.codigo_pedido, '')
            AND t1.producto_codigo = t2.producto_codigo
            AND t1.id < t2.id
        """

        if self.dry_run:
            print("🔍 DRY RUN - Se ejecutaría:")
            print(f"   {drop_query}")
            print(f"   {create_query}")
            return

        with self.engine.begin() as conn:
            conn.execute(text(drop_query))
            result = conn.execute(text(create_query))
            
            # Contar registros en backup
            count_query = "SELECT COUNT(*) FROM fact_infoproducto_duplicados_backup"
            count_result = conn.execute(text(count_query))
            count = count_result.scalar()

        print(f"✓ Backup creado: {count} registros duplicados guardados")

    def eliminar_duplicados(self):
        """Paso 3: Eliminar duplicados manteniendo el más reciente"""
        print("\n" + "="*60)
        print("PASO 3: Eliminando duplicados")
        print("="*60)

        delete_query = """
        DELETE t1 FROM fact_infoproducto t1
        INNER JOIN fact_infoproducto t2 
        WHERE 
            t1.fecha_reporte = t2.fecha_reporte
            AND t1.fuente_id = t2.fuente_id
            AND COALESCE(t1.codigo_pedido, '') = COALESCE(t2.codigo_pedido, '')
            AND t1.producto_codigo = t2.producto_codigo
            AND t1.id < t2.id
        """

        if self.dry_run:
            print("🔍 DRY RUN - Se ejecutaría:")
            print(f"   {delete_query}")
            return

        with self.engine.begin() as conn:
            result = conn.execute(text(delete_query))
            rows_deleted = result.rowcount

        print(f"✓ Eliminados {rows_deleted} registros duplicados")

    def verificar_sin_duplicados(self):
        """Paso 4: Verificar que no quedan duplicados"""
        print("\n" + "="*60)
        print("PASO 4: Verificando que no quedan duplicados")
        print("="*60)

        query = """
        SELECT COUNT(*) as duplicados
        FROM (
            SELECT 
                fecha_reporte,
                fuente_id,
                codigo_pedido,
                producto_codigo,
                COUNT(*) as total
            FROM fact_infoproducto
            WHERE codigo_pedido IS NOT NULL 
              AND producto_codigo IS NOT NULL
            GROUP BY 
                fecha_reporte,
                fuente_id,
                codigo_pedido,
                producto_codigo
            HAVING COUNT(*) > 1
        ) sub
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            duplicados = result.scalar()

        if duplicados == 0:
            print("✓ No quedan duplicados - OK para crear nueva clave única")
        else:
            print(f"❌ ERROR: Aún hay {duplicados} grupos de duplicados")
            print("   La migración debe detenerse aquí")
            sys.exit(1)

    def eliminar_clave_antigua(self):
        """Paso 5: Eliminar índice antiguo"""
        print("\n" + "="*60)
        print("PASO 5: Eliminando clave única antigua")
        print("="*60)

        # Verificar qué índices existen
        show_query = """
        SELECT DISTINCT INDEX_NAME 
        FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = DATABASE() 
          AND TABLE_NAME = 'fact_infoproducto'
          AND INDEX_NAME LIKE '%infoproducto%'
          AND INDEX_NAME != 'PRIMARY'
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(show_query))
            indices = [row[0] for row in result.fetchall()]

        if not indices:
            print("⚠️  No se encontró índice antiguo - puede que ya esté eliminado")
            return

        print(f"📋 Índices encontrados: {', '.join(indices)}")

        # Intentar eliminar el índice más probable
        index_name = indices[0]
        drop_query = f"ALTER TABLE fact_infoproducto DROP INDEX {index_name}"

        if self.dry_run:
            print("🔍 DRY RUN - Se ejecutaría:")
            print(f"   {drop_query}")
            return

        with self.engine.begin() as conn:
            conn.execute(text(drop_query))

        print(f"✓ Índice '{index_name}' eliminado")

    def crear_clave_nueva(self):
        """Paso 6: Crear nueva clave única"""
        print("\n" + "="*60)
        print("PASO 6: Creando nueva clave única")
        print("="*60)

        create_query = """
        ALTER TABLE fact_infoproducto 
        ADD UNIQUE INDEX uq_infoproducto (
            fecha_reporte,
            fuente_id,
            codigo_pedido,
            producto_codigo
        )
        """

        if self.dry_run:
            print("🔍 DRY RUN - Se ejecutaría:")
            print(f"   {create_query}")
            return

        try:
            with self.engine.begin() as conn:
                conn.execute(text(create_query))
            print("✓ Nueva clave única creada correctamente")
        except Exception as e:
            print(f"❌ ERROR al crear nueva clave: {e}")
            sys.exit(1)

    def verificar_estructura(self):
        """Paso 7: Verificar nueva estructura"""
        print("\n" + "="*60)
        print("PASO 7: Verificando estructura final")
        print("="*60)

        show_query = "SHOW CREATE TABLE fact_infoproducto"

        with self.engine.connect() as conn:
            result = conn.execute(text(show_query))
            create_table = result.fetchone()[1]

        # Verificar que tiene la clave correcta
        if "UNIQUE KEY `uq_infoproducto` (`fecha_reporte`,`fuente_id`,`codigo_pedido`,`producto_codigo`)" in create_table:
            print("✓ Clave única correcta verificada")
        else:
            print("⚠️  La estructura no muestra la clave esperada")
            print("\nEstructura actual:")
            print(create_table)

    def verificar_integridad(self):
        """Paso 8: Verificar integridad de datos"""
        print("\n" + "="*60)
        print("PASO 8: Verificando integridad de datos")
        print("="*60)

        queries = {
            "Total registros": "SELECT COUNT(*) FROM fact_infoproducto",
            "Fechas distintas": "SELECT COUNT(DISTINCT fecha_reporte) FROM fact_infoproducto",
            "Fuentes distintas": "SELECT COUNT(DISTINCT fuente_id) FROM fact_infoproducto",
            "Pedidos distintos": "SELECT COUNT(DISTINCT codigo_pedido) FROM fact_infoproducto WHERE codigo_pedido IS NOT NULL",
            "NULLs en fecha": "SELECT COUNT(*) FROM fact_infoproducto WHERE fecha_reporte IS NULL",
            "NULLs en fuente": "SELECT COUNT(*) FROM fact_infoproducto WHERE fuente_id IS NULL",
            "NULLs en pedido": "SELECT COUNT(*) FROM fact_infoproducto WHERE codigo_pedido IS NULL",
            "NULLs en producto": "SELECT COUNT(*) FROM fact_infoproducto WHERE producto_codigo IS NULL",
        }

        with self.engine.connect() as conn:
            for label, query in queries.items():
                result = conn.execute(text(query))
                value = result.scalar()
                
                if "NULLs" in label and value > 0:
                    print(f"⚠️  {label}: {value:,}")
                else:
                    print(f"✓ {label}: {value:,}")

    def ejecutar(self):
        """Ejecutar migración completa"""
        print("\n" + "="*70)
        print(" MIGRACIÓN: Corrección de Clave Única - fact_infoproducto")
        print("="*70)
        print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Modo: {'🔍 DRY RUN (simulación)' if self.dry_run else '🚀 EJECUCIÓN REAL'}")
        print(f"Backup: {'✓ Habilitado' if self.backup else '✗ Deshabilitado'}")
        print("="*70)

        self.engine = self._get_engine()

        try:
            # Paso 1: Verificar duplicados
            total_duplicados = self.verificar_duplicados()

            if total_duplicados == 0:
                print("\n⚠️  No hay duplicados, pero la clave puede estar incorrecta")
                respuesta = input("\n¿Continuar con la actualización de la clave? (s/N): ")
                if respuesta.lower() != 's':
                    print("❌ Migración cancelada por el usuario")
                    return

            else:
                # Confirmar antes de continuar
                if not self.dry_run:
                    print(f"\n⚠️  ATENCIÓN: Se eliminarán {total_duplicados - self.verificar_duplicados()} registros duplicados")
                    respuesta = input("\n¿Continuar con la eliminación? (s/N): ")
                    if respuesta.lower() != 's':
                        print("❌ Migración cancelada por el usuario")
                        return

                # Paso 2: Backup (opcional)
                self.crear_backup_duplicados()

                # Paso 3: Eliminar duplicados
                self.eliminar_duplicados()

                # Paso 4: Verificar sin duplicados
                self.verificar_sin_duplicados()

            # Paso 5: Eliminar clave antigua
            self.eliminar_clave_antigua()

            # Paso 6: Crear clave nueva
            self.crear_clave_nueva()

            # Paso 7: Verificar estructura
            self.verificar_estructura()

            # Paso 8: Verificar integridad
            self.verificar_integridad()

            print("\n" + "="*70)
            if self.dry_run:
                print("✓ DRY RUN COMPLETADO - Ningún cambio fue realizado")
            else:
                print("✓ MIGRACIÓN COMPLETADA EXITOSAMENTE")
            print("="*70)

            if not self.dry_run:
                print("\n📋 PRÓXIMOS PASOS:")
                print("1. Probar carga de archivo InfoProducto nuevo")
                print("2. Re-cargar el MISMO archivo para verificar actualización")
                print("3. Verificar que no se incrementa el conteo de registros")
                print("4. Si todo funciona bien después de 1-2 días:")
                print("   DROP TABLE IF EXISTS fact_infoproducto_duplicados_backup;")

        except Exception as e:
            print(f"\n❌ ERROR durante la migración: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            if self.engine:
                self.engine.dispose()


def main():
    parser = argparse.ArgumentParser(
        description="Migración de clave única para fact_infoproducto"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo muestra lo que haría, sin hacer cambios"
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Crea tabla de backup antes de eliminar duplicados"
    )

    args = parser.parse_args()

    migracion = MigracionInfoProducto(dry_run=args.dry_run, backup=args.backup)
    migracion.ejecutar()


if __name__ == "__main__":
    main()
