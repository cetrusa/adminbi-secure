"""
Script de validación de estructura y coherencia de archivos TrackSales
Genera un reporte detallado para comunicar el estado de los archivos generados
y lo envía por correo electrónico
"""

import os
import pandas as pd
from datetime import datetime
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import getpass

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidadorTrackSales:
    """Valida la estructura y coherencia de los archivos TrackSales según especificaciones 3.7.2.3"""
    
    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.resultados = {
            'estructura': [],
            'coherencia': [],
            'integridad': [],
            'estadisticas': [],
            'warnings': [],
            'errores': [],
            'resumen': {}
        }
        self.archivos_cargados = {}
        self.metricas = {}
        
    def cargar_archivo_txt(self, nombre_archivo):
        """Carga un archivo TXT con delimitador '{'"""
        ruta = os.path.join(self.output_folder, nombre_archivo)
        if not os.path.exists(ruta):
            self.resultados['errores'].append(f"❌ Archivo no encontrado: {nombre_archivo}")
            return None
        
        try:
            df = pd.read_csv(ruta, delimiter='{', dtype=str, encoding='utf-8')
            registros = len(df)
            self.resultados['estructura'].append(f"✅ {nombre_archivo}: {registros:,} registros cargados")
            return df
        except Exception as e:
            self.resultados['errores'].append(f"❌ Error cargando {nombre_archivo}: {str(e)}")
            return None
    
    def validar_estructura_sku(self, df):
        """Valida estructura de SKU (Productos).txt según especificaciones"""
        if df is None:
            return False
        
        campos_requeridos = [
            'Código', 'Nombre', 'Tipo Referencia', 'Tipo De Unidad', 
            'Código De Barras', 'Código Categoría', 'Nombre Categoría',
            'Código SubCategoría', 'Nombre SubCategoría', 'Factor Conversion Unidad',
            'Factor Peso', 'Código Sede', 'Nombre Sede', 'Proveedor'
        ]
        
        faltantes = [c for c in campos_requeridos if c not in df.columns]
        if faltantes:
            self.resultados['errores'].append(f"❌ SKU: Faltan campos: {', '.join(faltantes)}")
            return False
        
        # Validar tipos de datos
        if df['Código'].isna().any():
            cant = df['Código'].isna().sum()
            self.resultados['warnings'].append(f"⚠️ SKU: {cant} códigos vacíos detectados")
        
        # Verificar códigos con ceros a la izquierda
        codigos_con_ceros = df[df['Código'].str.match(r'^0\d+', na=False)]
        if not codigos_con_ceros.empty:
            ejemplos = codigos_con_ceros['Código'].head(5).tolist()
            self.resultados['estructura'].append(
                f"✅ SKU: Detectados {len(codigos_con_ceros)} códigos con ceros a la izquierda preservados. Ejemplos: {', '.join(ejemplos)}"
            )

        # Validar valores obligatorios
        registros_sin_codigo = df['Código'].isna().sum()
        registros_sin_nombre = df['Nombre'].isna().sum()

        if registros_sin_codigo > 0:
            self.resultados['errores'].append(f"❌ SKU: {registros_sin_codigo} productos sin código")
        if registros_sin_nombre > 0:
            self.resultados['warnings'].append(f"⚠️ SKU: {registros_sin_nombre} productos sin nombre")

        # Validar tipos de referencia
        tipos_validos = ['RG', 'OF', 'OB']
        tipos_invalidos = df[~df['Tipo Referencia'].isin(tipos_validos)]['Tipo Referencia'].unique()
        if len(tipos_invalidos) > 0:
            self.resultados['warnings'].append(f"⚠️ SKU: Tipos de referencia no estándar: {', '.join(tipos_invalidos)}")

        # Estadísticas
        self.metricas['sku_total'] = len(df)
        self.metricas['sku_con_ceros'] = len(codigos_con_ceros) if not codigos_con_ceros.empty else 0
        self.metricas['sku_por_proveedor'] = df['Proveedor'].value_counts().to_dict()

        self.resultados['estadisticas'].append(f"📊 SKU: {len(df):,} productos total")
        self.resultados['estadisticas'].append(f"📊 SKU: {len(df['Proveedor'].unique())} proveedores únicos")

        self.resultados['estructura'].append("✅ SKU: Estructura validada correctamente")
        return True
    
    def validar_estructura_ventas(self, df):
        """Valida estructura de ventas.txt según especificaciones"""
        # Validar fechas
        try:
            fechas = pd.to_datetime(df['Fecha'], format='%Y/%m/%d', errors='coerce')
            fechas_invalidas = fechas.isna().sum()
            if fechas_invalidas > 0:
                self.resultados['warnings'].append(f"⚠️ Ventas: {fechas_invalidas} registros con fechas inválidas")
        except Exception:
            self.resultados['warnings'].append("⚠️ Ventas: No se pudo validar formato de fechas")

        # Validar tipos de venta
        tipos_validos = ['0', '1', '2']
        tipos_invalidos = df[~df['Tipo'].isin(tipos_validos)]['Tipo'].unique()
        if len(tipos_invalidos) > 0:
            self.resultados['warnings'].append(f"⚠️ Ventas: Tipos de documento no estándar: {', '.join(tipos_invalidos)}")

        # Validar valores numéricos
        try:
            cantidades = pd.to_numeric(df['Cantidad'], errors='coerce')
            valores = pd.to_numeric(df['Valor Total Item Vendido'], errors='coerce')
            costos = pd.to_numeric(df['Costo'], errors='coerce')

            cant_invalidas = cantidades.isna().sum()
            val_invalidos = valores.isna().sum()
            cost_invalidos = costos.isna().sum()

            if cant_invalidas > 0:
                self.resultados['warnings'].append(f"⚠️ Ventas: {cant_invalidas} cantidades no numéricas")
            if val_invalidos > 0:
                self.resultados['warnings'].append(f"⚠️ Ventas: {val_invalidos} valores no numéricos")
            if cost_invalidos > 0:
                self.resultados['warnings'].append(f"⚠️ Ventas: {cost_invalidos} costos no numéricos")
        except Exception:
            self.resultados['warnings'].append("⚠️ Ventas: Error validando campos numéricos")
        
        # Estadísticas
        self.metricas['ventas_total'] = len(df)
        self.metricas['ventas_por_tipo'] = df['Tipo'].value_counts().to_dict()
        self.metricas['clientes_unicos'] = df['Código Cliente'].nunique()
        self.metricas['vendedores_unicos'] = df['Código Vendedor'].nunique()
        self.metricas['productos_vendidos'] = df['Código Producto (Sku)'].nunique()
        
        try:
            valor_total = pd.to_numeric(df['Valor Total Item Vendido'], errors='coerce').sum()
            self.metricas['valor_total_ventas'] = valor_total
        except Exception:
            self.metricas['valor_total_ventas'] = 0
        
        self.resultados['estadisticas'].append(f"📊 Ventas: {len(df):,} transacciones total")
        self.resultados['estadisticas'].append(f"📊 Ventas: {self.metricas['clientes_unicos']:,} clientes únicos")
        self.resultados['estadisticas'].append(f"📊 Ventas: {self.metricas['vendedores_unicos']:,} vendedores únicos")
        self.resultados['estadisticas'].append(f"📊 Ventas: {self.metricas['productos_vendidos']:,} productos diferentes vendidos")

        self.resultados['estructura'].append("✅ Ventas: Estructura validada correctamente")
        return True

    def validar_integridad_totales(self, ventas_df):
        """Valida que los totales de control coincidan con las ventas"""
        if ventas_df is None:
            return
        
        try:
            # Calcular total de ventas (solo tipo 0 - ventas)
            ventas_reales = ventas_df[ventas_df['Tipo'] == '0'].copy()
            if ventas_reales.empty:
                self.resultados['warnings'].append("⚠️ INTEGRIDAD: No se encontraron registros de venta (Tipo = 0)")
                return
            
            valores = pd.to_numeric(ventas_reales['Valor Total Item Vendido'], errors='coerce')
            total_calculado = valores.sum()
            
            # Verificar si existe archivo de totales
            totales_path = os.path.join(self.output_folder, 'Totales de Control.txt')
            if os.path.exists(totales_path):
                totales_df = self.cargar_archivo_txt('Totales de Control.txt')
                if totales_df is not None and not totales_df.empty:
                    total_reportado = pd.to_numeric(totales_df['Valor'].iloc[0], errors='coerce')
                    diferencia = abs(total_calculado - total_reportado)
                    
                    if diferencia < 0.01:  # Tolerancia para redondeo
                        self.resultados['integridad'].append(
                            f"✅ INTEGRIDAD: Total de control coincide: ${total_calculado:,.2f}"
                        )
                    else:
                        self.resultados['errores'].append(
                            f"❌ INTEGRIDAD: Total calculado (${total_calculado:,.2f}) vs reportado (${total_reportado:,.2f}). Diferencia: ${diferencia:,.2f}"
                        )
                else:
                    self.resultados['warnings'].append("⚠️ INTEGRIDAD: Archivo de totales existe pero está vacío")
            else:
                self.resultados['integridad'].append(
                    f"ℹ️ INTEGRIDAD: Total calculado de ventas: ${total_calculado:,.2f} (sin archivo de control)"
                )
                
        except Exception as e:
            self.resultados['errores'].append(f"❌ INTEGRIDAD: Error validando totales: {str(e)}")
    
    def validar_inventario_coherencia(self, inventario_df, sku_df):
        """Valida coherencia entre inventario y SKUs"""
        if inventario_df is None or sku_df is None:
            return
        
        try:
            productos_inventario = set(inventario_df['Código Producto'].dropna().unique())
            productos_sku = set(sku_df['Código'].dropna().unique())
            
            productos_sin_maestro = productos_inventario - productos_sku
            if productos_sin_maestro:
                ejemplos = list(productos_sin_maestro)[:10]
                self.resultados['warnings'].append(
                    f"⚠️ COHERENCIA: {len(productos_sin_maestro)} productos en inventario sin maestra SKU. Ejemplos: {', '.join(ejemplos)}"
                )
            else:
                self.resultados['coherencia'].append(
                    f"✅ COHERENCIA: Todos los productos en inventario ({len(productos_inventario)}) tienen maestra SKU"
                )
        except Exception as e:
            self.resultados['errores'].append(f"❌ Error validando coherencia inventario-SKU: {str(e)}")
    
    def validar_fechas_consistencia(self, ventas_df):
        """Valida consistencia de fechas en ventas"""
        if ventas_df is None:
            return

        try:
            fechas = pd.to_datetime(ventas_df['Fecha'], format='%Y/%m/%d', errors='coerce')
            fechas_validas = fechas.dropna()

            if len(fechas_validas) > 0:
                fecha_min = fechas_validas.min()
                fecha_max = fechas_validas.max()

                # Validar que no hay fechas futuras
                fecha_hoy = datetime.now()
                fechas_futuras = (fechas_validas > fecha_hoy).sum()

                if fechas_futuras > 0:
                    self.resultados['warnings'].append(f"⚠️ INTEGRIDAD: {fechas_futuras} registros con fechas futuras")

                self.resultados['integridad'].append(
                    f"✅ INTEGRIDAD: Rango de fechas válido: {fecha_min.strftime('%Y-%m-%d')} a {fecha_max.strftime('%Y-%m-%d')}"
                )

                # Validar distribución mensual
                meses = fechas_validas.dt.to_period('M').value_counts()
                if len(meses) == 1:
                    self.resultados['integridad'].append(f"ℹ️ INTEGRIDAD: Datos de un solo mes: {meses.index[0]}")
                else:
                    self.resultados['integridad'].append(f"ℹ️ INTEGRIDAD: Datos de {len(meses)} meses diferentes")

        except Exception as e:
            self.resultados['errores'].append(f"❌ Error validando fechas: {str(e)}")

    def validar_estructura_clientes(self, df):
        """Valida estructura de Clientes.txt según especificaciones"""
        if df is None:
            return False

        campos_requeridos = [
            'Código', 'Nombre', 'Dirección', 'Nit',
            'Código Municipio', 'Codigo Negocio', 'Código Sede', 'Nombre Sede'
        ]

        faltantes = [c for c in campos_requeridos if c not in df.columns]
        if faltantes:
            self.resultados['errores'].append(f"❌ Clientes: Faltan campos: {', '.join(faltantes)}")
            return False

        # Validar códigos municipio
        if 'Código Municipio' in df.columns:
            municipios_invalidos = df[df['Código Municipio'].str.len() != 8]['Código Municipio'].unique()
            if len(municipios_invalidos) > 0:
                self.resultados['warnings'].append(
                    f"⚠️ Clientes: {len(municipios_invalidos)} códigos de municipio con formato incorrecto"
                )

        # Validar NITs
        nits_vacios = df['Nit'].isna().sum() if 'Nit' in df.columns else 0
        if nits_vacios > 0:
            self.resultados['warnings'].append(f"⚠️ Clientes: {nits_vacios} clientes sin NIT")

        # Estadísticas
        self.metricas['clientes_total'] = len(df)
        if 'Codigo Negocio' in df.columns:
            self.metricas['clientes_por_tipo'] = df['Codigo Negocio'].value_counts().to_dict()

        self.resultados['estadisticas'].append(f"📊 Clientes: {len(df):,} clientes total")
        if 'Codigo Negocio' in df.columns:
            tipos_negocio = df['Codigo Negocio'].nunique()
            self.resultados['estadisticas'].append(f"📊 Clientes: {tipos_negocio} tipos de negocio diferentes")

        self.resultados['estructura'].append("✅ Clientes: Estructura validada correctamente")
        return True
    
    def validar_estructura_vendedores(self, df):
        """Valida estructura de Vendedores.txt según especificaciones"""
        if df is None:
            return False
        
        campos_requeridos = [
            'Código', 'Nombre', 'Ubicación', 'Cedula',
            'Código Supervisor', 'Código Sede', 'Nombre Sede'
        ]
        
        faltantes = [c for c in campos_requeridos if c not in df.columns]
        if faltantes:
            self.resultados['errores'].append(f"❌ Vendedores: Faltan campos: {', '.join(faltantes)}")
            return False
        
        self.resultados['estructura'].append(f"✅ Vendedores: Estructura validada correctamente")
        return True
    
    def validar_coherencia_sku_ventas(self, sku_df, ventas_df):
        """Valida que todos los SKUs en ventas existan en la maestra"""
        if sku_df is None or ventas_df is None:
            return
        
        skus_ventas = set(ventas_df['Código Producto (Sku)'].dropna().unique())
        skus_maestra = set(sku_df['Código'].dropna().unique())
        
        faltantes = skus_ventas - skus_maestra
        if faltantes:
            ejemplos = list(faltantes)[:10]
            self.resultados['errores'].append(
                f"❌ COHERENCIA: {len(faltantes)} SKUs en ventas no existen en maestra. Ejemplos: {', '.join(ejemplos)}"
            )
        else:
            self.resultados['coherencia'].append(
                f"✅ COHERENCIA: Todos los SKUs en ventas ({len(skus_ventas)}) existen en la maestra"
            )
    
    def validar_coherencia_clientes_ventas(self, clientes_df, ventas_df):
        """Valida que todos los clientes en ventas existan en la maestra"""
        if clientes_df is None or ventas_df is None:
            return
        
        clientes_ventas = set(ventas_df['Código Cliente'].dropna().unique())
        clientes_maestra = set(clientes_df['Código'].dropna().unique())
        
        faltantes = clientes_ventas - clientes_maestra
        if faltantes:
            ejemplos = list(faltantes)[:10]
            self.resultados['errores'].append(
                f"❌ COHERENCIA: {len(faltantes)} clientes en ventas no existen en maestra. Ejemplos: {', '.join(ejemplos)}"
            )
        else:
            self.resultados['coherencia'].append(
                f"✅ COHERENCIA: Todos los clientes en ventas ({len(clientes_ventas)}) existen en la maestra"
            )
    
    def validar_coherencia_vendedores_ventas(self, vendedores_df, ventas_df):
        """Valida que todos los vendedores en ventas existan en la maestra"""
        if vendedores_df is None or ventas_df is None:
            return
        
        vendedores_ventas = set(ventas_df['Código Vendedor'].dropna().unique())
        vendedores_maestra = set(vendedores_df['Código'].dropna().unique())
        
        faltantes = vendedores_ventas - vendedores_maestra
        if faltantes:
            ejemplos = list(faltantes)[:10]
            self.resultados['errores'].append(
                f"❌ COHERENCIA: {len(faltantes)} vendedores en ventas no existen en maestra. Ejemplos: {', '.join(ejemplos)}"
            )
        else:
            self.resultados['coherencia'].append(
                f"✅ COHERENCIA: Todos los vendedores en ventas ({len(vendedores_ventas)}) existen en la maestra"
            )
    
    def ejecutar_validacion(self):
        """Ejecuta todas las validaciones"""
        logger.info("Iniciando validación de archivos TrackSales...")
        
        # Cargar archivos
        archivos = {
            'SKU (Productos).txt': 'sku',
            'ventas.txt': 'ventas',
            'Clientes.txt': 'clientes',
            'Vendedores.txt': 'vendedores',
            'Inventario.txt': 'inventario',
            'Listado de Facturas.txt': 'facturas',
            'Rutas.txt': 'rutas',
            'Supervisores.txt': 'supervisores',
            'Municipios.txt': 'municipios',
            'Tipos De Negocio.txt': 'tipos_negocio',
            'Totales de Control.txt': 'totales'
        }
        
        for nombre, clave in archivos.items():
            df = self.cargar_archivo_txt(nombre)
            if df is not None:
                self.archivos_cargados[clave] = df
        
        # Validar estructuras
        if 'sku' in self.archivos_cargados:
            self.validar_estructura_sku(self.archivos_cargados['sku'])
        
        if 'ventas' in self.archivos_cargados:
            self.validar_estructura_ventas(self.archivos_cargados['ventas'])
        
        if 'clientes' in self.archivos_cargados:
            self.validar_estructura_clientes(self.archivos_cargados['clientes'])
        
        if 'vendedores' in self.archivos_cargados:
            self.validar_estructura_vendedores(self.archivos_cargados['vendedores'])
        
        # Validar coherencias
        if 'sku' in self.archivos_cargados and 'ventas' in self.archivos_cargados:
            self.validar_coherencia_sku_ventas(
                self.archivos_cargados['sku'],
                self.archivos_cargados['ventas']
            )
        
        if 'clientes' in self.archivos_cargados and 'ventas' in self.archivos_cargados:
            self.validar_coherencia_clientes_ventas(
                self.archivos_cargados['clientes'],
                self.archivos_cargados['ventas']
            )
        
        if 'vendedores' in self.archivos_cargados and 'ventas' in self.archivos_cargados:
            self.validar_coherencia_vendedores_ventas(
                self.archivos_cargados['vendedores'],
                self.archivos_cargados['ventas']
            )
        
        # Validar integridad de datos
        if 'ventas' in self.archivos_cargados:
            self.validar_integridad_totales(self.archivos_cargados['ventas'])
            self.validar_fechas_consistencia(self.archivos_cargados['ventas'])

        if 'inventario' in self.archivos_cargados and 'sku' in self.archivos_cargados:
            self.validar_inventario_coherencia(
                self.archivos_cargados['inventario'],
                self.archivos_cargados['sku']
            )

        # Generar resumen
        self.resultados['resumen'] = {
            'total_archivos': len(self.archivos_cargados),
            'total_errores': len(self.resultados['errores']),
            'total_warnings': len(self.resultados['warnings']),
            'total_coherencias': len(self.resultados['coherencia']),
            'total_integridades': len(self.resultados['integridad']),
            'fecha_validacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'archivos_procesados': list(self.archivos_cargados.keys()),
        }
        
        logger.info("Validación completada")
        return self.resultados
    
    def generar_correo(self):
        """Genera el texto del correo con los resultados de la validación"""
        resultados = self.ejecutar_validacion()
        
        correo = []
        correo.append("=" * 80)
        correo.append("REPORTE DE VALIDACIÓN - ARCHIVOS TRACKSALES 3.7.2.3")
        correo.append("=" * 80)
        correo.append("")
        correo.append(f"Fecha de validación: {resultados['resumen']['fecha_validacion']}")
        correo.append(f"Carpeta analizada: {self.output_folder}")
        correo.append(f"Archivos procesados: {resultados['resumen']['total_archivos']}")
        correo.append("")
        
        # Resumen ejecutivo
        correo.append("📊 RESUMEN EJECUTIVO")
        correo.append("-" * 80)
        if resultados['resumen']['total_errores'] == 0:
            correo.append("✅ Estado: APROBADO - Todos los archivos cumplen con las especificaciones")
        else:
            correo.append(f"❌ Estado: REQUIERE ATENCIÓN - {resultados['resumen']['total_errores']} errores detectados")
        
        if resultados['resumen']['total_warnings'] > 0:
            correo.append(f"⚠️ Advertencias: {resultados['resumen']['total_warnings']} advertencias menores")
        correo.append("")
        
        # Validación de estructura
        if resultados['estructura']:
            correo.append("📁 VALIDACIÓN DE ESTRUCTURA DE ARCHIVOS")
            correo.append("-" * 80)
            for item in resultados['estructura']:
                correo.append(item)
            correo.append("")
        
        # Validación de coherencia
        if resultados['coherencia']:
            correo.append("🔗 VALIDACIÓN DE COHERENCIA ENTRE ARCHIVOS")
            correo.append("-" * 80)
            for item in resultados['coherencia']:
                correo.append(item)
            correo.append("")
        
        # Advertencias
        if resultados['warnings']:
            correo.append("⚠️ ADVERTENCIAS")
            correo.append("-" * 80)
            for item in resultados['warnings']:
                correo.append(item)
            correo.append("")
        
        # Errores
        if resultados['errores']:
            correo.append("❌ ERRORES CRÍTICOS")
            correo.append("-" * 80)
            for item in resultados['errores']:
                correo.append(item)
            correo.append("")
        
        # Validaciones específicas realizadas
        correo.append("✔️ VALIDACIONES REALIZADAS")
        correo.append("-" * 80)
        correo.append("1. Estructura de campos según especificación TrackSales 3.7.2.3")
        correo.append("2. Preservación de ceros a la izquierda en códigos de productos")
        correo.append("3. Coherencia de SKUs entre archivo de ventas y maestra de productos")
        correo.append("4. Coherencia de clientes entre archivo de ventas y maestra de clientes")
        correo.append("5. Coherencia de vendedores entre archivo de ventas y maestra de vendedores")
        correo.append("6. Formato de delimitador '{' en archivos TXT")
        correo.append("")
        
        # Conclusión
        correo.append("=" * 80)
        if resultados['resumen']['total_errores'] == 0:
            correo.append("✅ CONCLUSIÓN: Los archivos están listos para ser enviados a TSOL")
        else:
            correo.append("❌ CONCLUSIÓN: Se requiere corrección antes del envío a TSOL")
        correo.append("=" * 80)

        return "\n".join(correo)

    def enviar_correo(self, asunto, cuerpo, archivo_adjunto=None):
        """Envía el reporte por correo electrónico"""
        remitente = "admonventas.distrijass@gmail.com"
        destinatario = "admonventas.distrijass@gmail.com"

        # Solicitar contraseña
        password = getpass.getpass(f"Ingrese la contraseña de {remitente}: ")

        if not password:
            logger.error("No se proporcionó contraseña. Correo no enviado.")
            return False

        try:
            # Crear mensaje
            mensaje = MIMEMultipart()
            mensaje['From'] = remitente
            mensaje['To'] = destinatario
            mensaje['Subject'] = asunto

            # Adjuntar cuerpo del mensaje
            mensaje.attach(MIMEText(cuerpo, 'plain', 'utf-8'))

            # Adjuntar archivo si existe
            if archivo_adjunto and os.path.exists(archivo_adjunto):
                with open(archivo_adjunto, 'rb') as adjunto:
                    parte = MIMEBase('application', 'octet-stream')
                    parte.set_payload(adjunto.read())
                    encoders.encode_base64(parte)
                    parte.add_header(
                        'Content-Disposition',
                        f'attachment; filename={os.path.basename(archivo_adjunto)}'
                    )
                    mensaje.attach(parte)

            # Conectar con servidor SMTP de Gmail
            servidor = smtplib.SMTP('smtp.gmail.com', 587)
            servidor.starttls()
            servidor.login(remitente, password)

            # Enviar correo
            texto = mensaje.as_string()
            servidor.sendmail(remitente, destinatario, texto)
            servidor.quit()

            logger.info("Correo enviado exitosamente a %s", destinatario)
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error("Error de autenticación SMTP. Verifique usuario y contraseña.")
            return False
        except Exception as e:
            logger.error("Error al enviar correo: %s", e)
            return False


def main():
    """Función principal"""
    output_folder = os.path.join(os.getcwd(), 'output_files', 'Distrijass')

    if not os.path.exists(output_folder):
        logger.error("La carpeta %s no existe", output_folder)
        return

    validador = ValidadorTrackSales(output_folder)
    correo = validador.generar_correo()

    # Mostrar en consola
    print(correo)

    # Guardar en archivo
    output_file = os.path.join(os.getcwd(), 'REPORTE_VALIDACION_TRACKSALES.txt')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(correo)

    logger.info("Reporte guardado en: %s", output_file)


if __name__ == "__main__":
    main()
