#!/usr/bin/env python3
"""
Script para crear la tabla de validación de cargue diario
"""

def crear_tabla_validacion():
    """Crear tabla validacion_cargue_diario usando pymysql directo."""
    
    try:
        print("🔧 Creando tabla validacion_cargue_diario...")
        
        # Usar la conexión existente del sistema
        from scripts.conexion import Conexion
        from scripts.config import ConfigBasic
        
        config = ConfigBasic('distrijass')
        conexion = Conexion(config, 'distrijass')
        conn = conexion.conectar()
        
        if not conn:
            raise Exception("No se pudo conectar a la base de datos")
            
        cursor = conn.cursor()
        print("✅ Conectado a la BD usando el sistema existente")
        
        # SQL para crear la tabla
        sql = """
        CREATE TABLE IF NOT EXISTS `validacion_cargue_diario` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `fecha_control` date NOT NULL COMMENT 'Fecha cuando se ejecutó la validación',
          `mes` int(11) NOT NULL COMMENT 'Mes del rango validado',
          `anno` int(11) NOT NULL COMMENT 'Año del rango validado',
          `registros_staging` int(11) DEFAULT 0 COMMENT 'Registros en tabla staging',
          `suma_staging` decimal(15,2) DEFAULT 0.00 COMMENT 'Suma Vta neta en staging',
          `registros_fact` int(11) DEFAULT 0 COMMENT 'Registros en _fact',
          `suma_fact` decimal(15,2) DEFAULT 0.00 COMMENT 'Suma Vta neta en _fact',
          `registros_dev` int(11) DEFAULT 0 COMMENT 'Registros en _dev', 
          `suma_dev` decimal(15,2) DEFAULT 0.00 COMMENT 'Suma Vta neta en _dev',
          `estado_validacion` varchar(20) DEFAULT 'PENDIENTE' COMMENT 'OK, ERROR, ADVERTENCIA',
          `mensaje_validacion` text COMMENT 'Mensaje descriptivo',
          `duplicados_fusionados` int(11) DEFAULT 0 COMMENT 'Total duplicados detectados',
          `accion_tomada` varchar(50) DEFAULT 'NINGUNA' COMMENT 'Acción recomendada',
          `timestamp` datetime DEFAULT CURRENT_TIMESTAMP COMMENT 'Momento validación',
          PRIMARY KEY (`id`),
          INDEX `idx_fecha_control` (`fecha_control`),
          INDEX `idx_mes_anno` (`mes`, `anno`),
          INDEX `idx_estado` (`estado_validacion`),
          INDEX `idx_timestamp` (`timestamp`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
        COMMENT='Tabla de auditoría para validaciones de cargue diario'
        """
        
        # Ejecutar creación
        cursor.execute(sql)
        conn.commit()
        print("✅ Tabla validacion_cargue_diario creada exitosamente")
        
        # Verificar que se creó
        cursor.execute("SHOW TABLES LIKE 'validacion_cargue_diario'")
        result = cursor.fetchone()
        if result:
            print("✅ Verificación exitosa: La tabla existe")
            
            # Mostrar estructura
            cursor.execute("DESCRIBE validacion_cargue_diario")
            columns = cursor.fetchall()
            print("� Estructura de la tabla:")
            for col in columns[:5]:  # Primeras 5 columnas
                print(f"   {col[0]} - {col[1]}")
        else:
            print("❌ Error: La tabla no se encontró")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    crear_tabla_validacion()
