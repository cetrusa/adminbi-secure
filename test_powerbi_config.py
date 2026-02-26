#!/usr/bin/env python
"""
Script de prueba para verificar la configuración de URL de Power BI
"""
import os
import sys
import django

# Configurar Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'adminbi.settings.local')
django.setup()

from scripts.config import ConfigBasic

def test_powerbi_config():
    """Prueba la configuración de Power BI para diferentes empresas"""
    
    # Lista de empresas de prueba (puedes cambiar estos nombres por los reales)
    test_databases = ["puente1dia", "amtech", "comercializadoracomputadores"]  # Ajusta según tus empresas
    
    print("=== Prueba de Configuración Power BI ===\n")
    
    for db_name in test_databases:
        print(f"Probando empresa: {db_name}")
        try:
            config = ConfigBasic(db_name)
            
            # Información básica
            print(f"  - Nombre empresa: {config.config.get('nm_empresa', 'N/A')}")
            print(f"  - URL Power BI: {config.config.get('url_powerbi', 'NO CONFIGURADA')}")
            print(f"  - Report ID: {config.config.get('report_id_powerbi', 'N/A')}")
            print(f"  - Group ID: {config.config.get('group_id_powerbi', 'N/A')}")
            
            # Verificar si la URL está disponible
            url_powerbi = config.config.get('url_powerbi')
            if url_powerbi:
                print(f"  ✅ URL configurada correctamente")
            else:
                print(f"  ❌ URL NO configurada")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        print("-" * 50)

if __name__ == "__main__":
    test_powerbi_config()