from scripts.config import ConfigBasic

# Lista de empresas de prueba
test_databases = ["puente1dia"]

print("=== Prueba de Configuración Power BI ===\n")

for db_name in test_databases:
    print(f"Probando empresa: {db_name}")
    try:
        config = ConfigBasic(db_name)
        
        # Información básica
        print(f"  - Nombre empresa: {config.config.get('nm_empresa', 'N/A')}")
        print(f"  - URL Power BI: {config.config.get('url_powerbi', 'NO CONFIGURADA')}")
        
        # Verificar si la URL está disponible
        url_powerbi = config.config.get('url_powerbi')
        if url_powerbi:
            print(f"  ✅ URL configurada correctamente")
        else:
            print(f"  ❌ URL NO configurada")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
    
    print("-" * 50)