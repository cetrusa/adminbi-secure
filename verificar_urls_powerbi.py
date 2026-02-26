#!/usr/bin/env python
"""
Script para verificar y actualizar URLs de Power BI
"""
import os
import sys
import django
import requests
from urllib.parse import urlparse

# Configurar Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'adminbi.settings.local')
django.setup()

from scripts.config import ConfigBasic

def test_powerbi_url(url):
    """Prueba si una URL de Power BI es accesible"""
    if not url:
        return False, "URL vacía"
    
    try:
        # Hacer una petición HEAD para verificar si la URL responde
        response = requests.head(url, timeout=10, allow_redirects=True)
        if response.status_code == 200:
            return True, "URL accesible"
        elif response.status_code == 403:
            return False, "Acceso denegado - Verificar permisos"
        elif response.status_code == 404:
            return False, "URL no encontrada - Posiblemente vencida"
        else:
            return False, f"Error HTTP {response.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout - URL no responde"
    except requests.exceptions.RequestException as e:
        return False, f"Error de conexión: {str(e)}"

def verify_all_powerbi_urls():
    """Verifica todas las URLs de Power BI configuradas"""
    print("=== VERIFICACIÓN DE URLs DE POWER BI ===\n")
    
    # Lista de empresas para verificar
    empresas_muestra = [
        'cima_aws', 'distrijass', 'disay', 'victor_alvarez', 
        'sidimat', 'compi', 'test'
    ]
    
    for empresa in empresas_muestra:
        print(f"🏢 Empresa: {empresa.upper()}")
        print("-" * 50)
        
        try:
            # Configurar la empresa
            config = ConfigBasic(empresa)
            url_powerbi = config.get("url_powerbi")
            
            if url_powerbi:
                print(f"URL encontrada: {url_powerbi}")
                # Verificar si la URL es accesible
                is_valid, message = test_powerbi_url(url_powerbi)
                
                if is_valid:
                    print("✅ Estado: URL VÁLIDA")
                else:
                    print(f"❌ Estado: URL INVÁLIDA - {message}")
            else:
                print("⚠️  No se encontró URL de Power BI configurada")
                
        except Exception as e:
            print(f"❌ Error al verificar {empresa}: {str(e)}")
        
        print()

def show_sample_urls():
    """Muestra ejemplos de URLs válidas de Power BI"""
    print("=== FORMATO DE URLs VÁLIDAS DE POWER BI ===\n")
    print("Las URLs de Power BI deben tener este formato:")
    print("https://app.powerbi.com/view?r=EMBED_CODE")
    print()
    print("Ejemplo:")
    print("https://app.powerbi.com/view?r=eyJrIjoiMTIzNDU2NzgtYWJjZC1lZmdoLWlqa2wtbW5vcHFyc3R1dnd4IiwidCI6IjEyMzQ1Njc4LWFiY2QtZWZnaC1pamtsLW1ub3BxcnN0dXZ3eCJ9")
    print()
    print("Para obtener una URL pública válida:")
    print("1. Ir a Power BI Service (app.powerbi.com)")
    print("2. Abrir el reporte")
    print("3. Clic en 'Archivo' > 'Insertar reporte' > 'Sitio web o portal'")
    print("4. Seleccionar 'Público (no requiere inicio de sesión)'")
    print("5. Copiar la URL generada")
    print()

if __name__ == "__main__":
    print("🔍 DIAGNÓSTICO DE URLs DE POWER BI")
    print("=" * 60)
    
    # Verificar URLs actuales
    verify_all_powerbi_urls()
    
    # Mostrar información sobre URLs válidas
    show_sample_urls()
    
    print("💡 RECOMENDACIONES:")
    print("1. Las URLs inválidas necesitan ser actualizadas en la base de datos")
    print("2. Verificar que los reportes tengan permisos públicos en Power BI")
    print("3. Generar nuevas URLs públicas para reportes vencidos")
    print("4. Contactar al administrador de Power BI si persisten los problemas")