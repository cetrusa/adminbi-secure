from scripts.config import ConfigBasic

# Verificar qué URL se está obteniendo
database_name = "puente1dia"  # Cambia por tu empresa

try:
    config = ConfigBasic(database_name)
    url_powerbi = config.config.get("url_powerbi")
    
    print(f"Database: {database_name}")
    print(f"URL Power BI: {url_powerbi}")
    print(f"Tipo: {type(url_powerbi)}")
    
    if url_powerbi:
        print(f"URL completa que se está enviando: {url_powerbi}?embed=true&chromeless=true")
    else:
        print("❌ URL NO configurada")
        
except Exception as e:
    print(f"Error: {e}")