#!/usr/bin/env python3
"""
Script para probar la ejecución del procedimiento almacenado sp_infoventas_full_maintenance
sin depender del archivo de red UNC.
"""

import sys
import os
import pandas as pd
import openpyxl
from datetime import datetime
import logging

# Agregar las rutas necesarias al sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts'))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logTestSP.txt'),
        logging.StreamHandler()
    ]
)

def crear_archivo_excel_prueba():
    """Crea un archivo Excel de prueba con datos mínimos."""
    print("📁 Creando archivo Excel de prueba...")
    
    # Crear directorio si no existe
    os.makedirs("media/temp", exist_ok=True)
    
    # Crear datos de prueba
    datos_prueba = [
        {
            'Año': 2025,
            'Mes': 10,
            'Codigo': 'TEST001',
            'Descripcion': 'Producto Test 1',
            'Cantidad': 10,
            'ValorUnitario': 100.50,
            'Proveedor': 'Proveedor Test',
            'Fecha': datetime(2025, 10, 15)
        },
        {
            'Año': 2025,
            'Mes': 10,
            'Codigo': 'TEST002',
            'Descripcion': 'Producto Test 2',
            'Cantidad': 5,
            'ValorUnitario': 75.25,
            'Proveedor': 'Proveedor Test',
            'Fecha': datetime(2025, 10, 20)
        }
    ]
    
    df = pd.DataFrame(datos_prueba)
    
    # Crear archivo Excel con la hoja 'infoventas'
    archivo_test = "media/temp/info proveedores.xlsx"
    with pd.ExcelWriter(archivo_test, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='infoventas', index=False)
    
    print(f"✅ Archivo creado: {archivo_test}")
    print(f"📊 Registros de prueba: {len(df)}")
    return archivo_test

def main():
    """Función principal para probar el procedimiento almacenado."""
    try:
        print("🧪 === INICIANDO PRUEBA DE PROCEDIMIENTO ALMACENADO ===")
        
        # Crear archivo de prueba
        archivo_test = crear_archivo_excel_prueba()
        
        # Importar y ejecutar el cargue
        from cargue_infoventas_main import run_cargue
        
        print("🚀 Ejecutando proceso de cargue con archivo de prueba...")
        run_cargue("distrijass", archivo_test, "TEST_USER")
        
        print("✅ Prueba completada exitosamente")
        
    except Exception as e:
        print(f"❌ Error en la prueba: {e}")
        logging.error(f"Error en la prueba: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()