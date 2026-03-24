#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de Archivos Planos Master Foods V3.0
Genera archivos delimitados por pipe (|) para Master Foods Colombia
Separados por empresa (Distrijass/Eje) y códigos de vendedor especiales

Códigos de distribuidor:
- 17645695: Distrijass con vendedores MA01, MA02, M1013 + BODEGA MASTER SPT
- 10359935: Distrijass sin esos vendedores (excluye BODEGA MASTER SPT)
- 17664540: Eje con vendedores MA01, MA02, M1013 (sin BODEGA MASTER SPT)
- 17636889: Eje sin esos vendedores (sin BODEGA MASTER SPT)
"""

import pandas as pd
import os
import json
import logging
import warnings
from datetime import datetime

# Configuración
warnings.filterwarnings('ignore', category=DeprecationWarning)

class MasterFoodsProcessor:
    def __init__(self, config_path='config_masterfoods.json'):
        """Inicializa el procesador Master Foods."""
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = self._cargar_configuracion(config_path)
        
        # Configuración del negocio
        self.proveedor = '006-MASTERFOODS COLOMBIA LTDA'
        self.vendedores_especiales = ['MA01', 'MA02', 'M1013','EMA01','EMA03']
        self.bodega_especial = 'BODEGA MASTER SPT'
        self.codigo_pais = 'CO'
        self.fecha_actual = datetime.now().strftime('%Y%m%d')
        
        # Mapeo de códigos distribuidor
        self.codigos_distribuidor = {
            'Distrijass_con_vendedores': '17645695',
            'Distrijass_sin_vendedores': '10359935', 
            'Eje_con_vendedores': '17664540',
            'Eje_sin_vendedores': '17636889'
        }
        
        # Configurar logging
        self._configurar_logging()
        
        # Configurar directorios
        self.output_folder = self.config.get('output_folder', 'output_masterfoods')
        if not os.path.isabs(self.output_folder):
            self.output_folder = os.path.join(self.base_dir, self.output_folder)
        self._crear_directorios()

    def _cargar_configuracion(self, config_path):
        """Carga la configuración desde JSON."""
        try:
            config_full_path = os.path.join(self.base_dir, config_path)
            with open(config_full_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error al cargar configuración: {e}")
            raise

    def _configurar_logging(self):
        """Configura el sistema de logging."""
        log_dir = os.path.join(self.base_dir, 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        logging.basicConfig(
            filename=os.path.join(log_dir, 'masterfoods_processor.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='w'  # Sobrescribir log en cada ejecución
        )
        self.logger = logging.getLogger()

    def _crear_directorios(self):
        """Crea los directorios necesarios."""
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    def cargar_datos(self):
        """Carga los datos desde los archivos Excel, forzando columnas clave como texto para mantener ceros a la izquierda."""
        try:
            self.logger.info("=== INICIANDO CARGA DE DATOS ===")

            # Cargar ventas (forzar Cod. productto como str)
            self.logger.info(f"Cargando ventas desde: {self.config['ventas_path']}")
            self.ventas_df = pd.read_excel(
                self.config['ventas_path'],
                sheet_name='infoventas',
                dtype={
                    'Cod. productto': str
                }
            )
            self.logger.info(f"✓ Ventas cargadas: {len(self.ventas_df)} registros")

            # Cargar clientes  
            self.logger.info(f"Cargando clientes desde: {self.config['clientes_path']}")
            self.clientes_df = pd.read_excel(self.config['clientes_path'], sheet_name='CLIENTES')
            self.logger.info(f"✓ Clientes cargados: {len(self.clientes_df)} registros")

            # Cargar inventario (forzar Codigo articulo como str)
            self.logger.info(f"Cargando inventario desde: {self.config['inventario_path']}")
            self.inventario_df = pd.read_excel(
                self.config['inventario_path'],
                sheet_name='Informe',
                dtype={
                    'Codigo articulo': str
                }
            )
            self.logger.info(f"✓ Inventario cargado: {len(self.inventario_df)} registros")

            self._verificar_datos_cargados()

        except Exception as e:
            self.logger.error(f"Error al cargar datos: {e}")
            raise

    def _verificar_datos_cargados(self):
        """Verifica que los datos cargados tengan la estructura esperada."""
        # Verificar vendedores especiales
        if 'Cod. vendedor' in self.ventas_df.columns:
            vendedores_unicos = self.ventas_df['Cod. vendedor'].dropna().unique()
            vendedores_encontrados = [v for v in self.vendedores_especiales if v in vendedores_unicos]
            self.logger.info(f"Vendedores especiales encontrados: {vendedores_encontrados}")
        
        # Verificar bodega especial
        if 'Nombre bodega' in self.inventario_df.columns:
            if self.bodega_especial in self.inventario_df['Nombre bodega'].values:
                self.logger.info(f"✓ {self.bodega_especial} encontrada en inventario")
            else:
                self.logger.warning(f"⚠ {self.bodega_especial} NO encontrada")

    def filtrar_por_proveedor(self):
        """Filtra todos los datos por el proveedor Master Foods."""
        try:
            self.logger.info("=== FILTRANDO POR PROVEEDOR MASTER FOODS ===")
            
            # Filtrar ventas
            ventas_original = len(self.ventas_df)
            self.ventas_df = self.ventas_df[
                self.ventas_df['Proveedor'].astype(str).str.contains(self.proveedor, case=False, na=False)
            ]
            self.logger.info(f"Ventas: {ventas_original} → {len(self.ventas_df)} registros")
            
            # Filtrar inventario
            inventario_original = len(self.inventario_df)
            self.inventario_df = self.inventario_df[
                self.inventario_df['Proveedor'].astype(str).str.contains(self.proveedor, case=False, na=False)
            ]
            self.logger.info(f"Inventario: {inventario_original} → {len(self.inventario_df)} registros")
            
        except Exception as e:
            self.logger.error(f"Error al filtrar por proveedor: {e}")
            raise

    def separar_por_empresa_vendedor(self):
        """Separa los datos por empresa y códigos de vendedor especiales."""
        try:
            self.logger.info("=== SEPARANDO POR EMPRESA Y VENDEDORES ===")
            self.datasets = {}
            
            for empresa in ['Distrijass', 'Eje']:
                self.logger.info(f"\n--- Procesando {empresa} ---")
                
                # Filtrar ventas por empresa
                ventas_empresa = self.ventas_df[
                    self.ventas_df['Empresa'].str.contains(empresa, case=False, na=False)
                ]
                self.logger.info(f"{empresa} - Total ventas: {len(ventas_empresa)}")
                
                # Separar por vendedores especiales
                ventas_con_vendedores = ventas_empresa[
                    ventas_empresa['Cod. vendedor'].isin(self.vendedores_especiales)
                ]
                ventas_sin_vendedores = ventas_empresa[
                    ~ventas_empresa['Cod. vendedor'].isin(self.vendedores_especiales)
                ]
                
                self.logger.info(f"  Con vendedores especiales: {len(ventas_con_vendedores)}")
                self.logger.info(f"  Sin vendedores especiales: {len(ventas_sin_vendedores)}")
                  # Filtrar inventario por empresa (SIN FILTRAR POR VENDEDORES)
                inventario_empresa = self.inventario_df[
                    self.inventario_df['Empresa'].str.contains(empresa, case=False, na=False)
                ]
                self.logger.info(f"  Total inventario {empresa}: {len(inventario_empresa)}")
                
                # LÓGICA CORREGIDA PARA INVENTARIO:
                # Con vendedores especiales: INCLUYE bodegas que contengan SPT (ambas empresas)
                inventario_con = inventario_empresa[
                    inventario_empresa['Nombre bodega'].str.contains('SPT', case=False, na=False)
                ]
                
                # Sin vendedores especiales: EXCLUYE bodegas que contengan SPT (ambas empresas)  
                inventario_sin = inventario_empresa[
                    ~inventario_empresa['Nombre bodega'].str.contains('SPT', case=False, na=False)
                ]
                
                spt_count = len(inventario_con)
                no_spt_count = len(inventario_sin)
                self.logger.info(f"  Inventario {empresa} CON SPT: {spt_count} registros")
                self.logger.info(f"  Inventario {empresa} SIN SPT: {no_spt_count} registros")
                
                # Guardar datasets
                key_con = f"{empresa}_con_vendedores"
                key_sin = f"{empresa}_sin_vendedores"
                
                self.datasets[key_con] = {
                    'ventas': ventas_con_vendedores,
                    'inventario': inventario_con,
                    'codigo': self.codigos_distribuidor[key_con]
                }
                
                self.datasets[key_sin] = {
                    'ventas': ventas_sin_vendedores, 
                    'inventario': inventario_sin,
                    'codigo': self.codigos_distribuidor[key_sin]
                }
                
                self.logger.info(f"✓ {key_con} (#{self.codigos_distribuidor[key_con]}): {len(ventas_con_vendedores)} ventas, {len(inventario_con)} inventario")
                self.logger.info(f"✓ {key_sin} (#{self.codigos_distribuidor[key_sin]}): {len(ventas_sin_vendedores)} ventas, {len(inventario_sin)} inventario")
            
        except Exception as e:
            self.logger.error(f"Error al separar datos: {e}")
            raise

    def generar_archivo_clientes(self, dataset_key):
        """Genera el archivo de clientes."""
        try:
            dataset = self.datasets[dataset_key]
            ventas = dataset['ventas']
            codigo = dataset['codigo']

            if len(ventas) == 0:
                self.logger.warning(f"No hay ventas para {dataset_key}, saltando clientes")
                return None

            # Obtener la fecha máxima de ventas para el nombre del archivo
            if 'Fecha' in ventas.columns and not ventas['Fecha'].isnull().all():
                max_fecha = pd.to_datetime(ventas['Fecha'], errors='coerce').max()
                if pd.notnull(max_fecha):
                    fecha_str = max_fecha.strftime('%Y%m%d')
                else:
                    fecha_str = self.fecha_actual
            else:
                fecha_str = self.fecha_actual

            # Obtener clientes únicos con ventas
            clientes_con_venta = ventas['Cod. cliente'].dropna().unique()
            clientes_filtrados = self.clientes_df[
                self.clientes_df['Cod. Cliente'].astype(str).isin(clientes_con_venta.astype(str))
            ].copy()

            if len(clientes_filtrados) == 0:
                self.logger.warning(f"No se encontraron clientes para {dataset_key}")
                return None
            # Crear estructura de salida con valores fijos correctos
            num_registros = len(clientes_filtrados)
            output = pd.DataFrame()

            # CAMPOS CRÍTICOS - Forzar como string
            output['CODPAÍS'] = [self.codigo_pais] * num_registros
            output['CODIGO DISTRIBUIDOR'] = [str(codigo)] * num_registros

            self.logger.info(f"  Asignando CODPAÍS='{self.codigo_pais}' y CODIGO DISTRIBUIDOR='{codigo}' a {num_registros} registros")

            output['CODCLIENTE DISTRIBUIDOR'] = clientes_filtrados['Cod. Cliente'].astype(str).values
            output['RAZON SOCIAL'] = clientes_filtrados['Nom. Cliente'].values
            output['DIRECCION'] = clientes_filtrados['Direccion'].fillna('').values
            output['NOMBRE FANTASIA'] = [''] * num_registros
            output['CONTACTO'] = [''] * num_registros
            output['TELEFONOS'] = clientes_filtrados['Telefono'].fillna('').values
            output['CODIGO AGENTE VENDEDOR'] = [''] * num_registros
            output['NOMBRE AGENTE VENDEDOR'] = [''] * num_registros
            output['DEPARTAMENTO_COD'] = ['VALLE DEL CAUCA'] * num_registros
            output['CIUDAD_COD'] = clientes_filtrados['Codigo Municipio'].fillna('76001').values
            output['BARRIO_COD'] = clientes_filtrados['Barrio'].fillna('').values
            output['CODIGO TIPOPUNTOVENTA'] = clientes_filtrados['Codigo Negocio'].fillna('1').values
            output['CODIGO TIPOPUNTOVENTA 2'] = [''] * num_registros
            output['CODIGO ZONAVENTA'] = [''] * num_registros
            output['RUTA'] = [''] * num_registros
            output['ESTADO'] = ['1'] * num_registros
            output['NIT'] = clientes_filtrados['Nit'].fillna('').values
            output['LONGITUD'] = clientes_filtrados['Longitud'].fillna('').values
            output['LATITUD'] = clientes_filtrados['Latitud'].fillna('').values
            output['DEPARTAMENTO_NOM'] = ['VALLE DEL CAUCA'] * num_registros
            output['CIUDAD_NOM'] = ['CALI'] * num_registros
            output['BARRIO_NOM'] = output['BARRIO_COD'].values
            output['NOMBRE TIPO PUNTO DE VENTA'] = ['TIENDAS'] * num_registros
            output['NOMBRE TIPO PUNTO DE VENTA 2'] = [''] * num_registros
            output['Nombre ZonaVenta'] = [''] * num_registros
            output['Ruta_NOM'] = [''] * num_registros
            # Verificar campos críticos antes de guardar
            if not self.verificar_campos_criticos(output, 'CLIENTES', codigo):
                self.logger.error(f"❌ Verificación falló para Clientes_{codigo}")

            # Guardar archivo con la fecha máxima de ventas
            filename = f'Clientes_{codigo}_{fecha_str}.txt'
            filepath = os.path.join(self.output_folder, filename)

            output.to_csv(filepath, sep='|', index=False, header=False, encoding='utf-8')

            self.logger.info(f"✓ {filename}: {len(output)} registros")

            return filepath

        except Exception as e:
            self.logger.error(f"Error generando clientes para {dataset_key}: {e}")
            return None

    def generar_archivo_ventas(self, dataset_key):
        """Genera el archivo de ventas."""
        try:
            dataset = self.datasets[dataset_key]
            ventas = dataset['ventas']
            codigo = dataset['codigo']

            if len(ventas) == 0:
                self.logger.warning(f"No hay ventas para {dataset_key}")
                return None

            # Obtener la fecha máxima de ventas para el nombre del archivo
            if 'Fecha' in ventas.columns and not ventas['Fecha'].isnull().all():
                max_fecha = pd.to_datetime(ventas['Fecha'], errors='coerce').max()
                if pd.notnull(max_fecha):
                    fecha_str = max_fecha.strftime('%Y%m%d')
                else:
                    fecha_str = self.fecha_actual
            else:
                fecha_str = self.fecha_actual

            # Crear mapeos desde clientes
            clientes_dict = dict(zip(
                self.clientes_df['Cod. Cliente'].astype(str),
                self.clientes_df['Nom. Cliente']
            ))
            direccion_dict = dict(zip(
                self.clientes_df['Cod. Cliente'].astype(str),
                self.clientes_df['Direccion'].fillna('')
            ))
            telefono_dict = dict(zip(
                self.clientes_df['Cod. Cliente'].astype(str),
                self.clientes_df['Telefono'].fillna('')
            ))
            # Crear estructura de salida con valores fijos correctos
            num_registros = len(ventas)
            output = pd.DataFrame()

            # CAMPOS CRÍTICOS - Forzar como string
            output['CODPAÍS'] = [self.codigo_pais] * num_registros
            output['Cod.DISTRIB.'] = [str(codigo)] * num_registros

            self.logger.info(f"  Asignando CODPAÍS='{self.codigo_pais}' y Cod.DISTRIB.='{codigo}' a {num_registros} registros")

            output['CODCLIENTEDISTRIBUIDOR'] = ventas['Cod. cliente'].astype(str).values
            output['NOMBRECLIENTEDISTRIBUIDOR'] = ventas['Cod. cliente'].astype(str).map(clientes_dict).fillna('').values
            output['DIRECCIÓN'] = ventas['Cod. cliente'].astype(str).map(direccion_dict).values
            output['TELÉFONO'] = ventas['Cod. cliente'].astype(str).map(telefono_dict).values
            output['Contacto'] = [''] * num_registros
            output['CODIGO DE AGENTE VENDEDOR'] = ventas['Cod. vendedor'].astype(str).values
            output['NOMBRE AGENTE VENDEDOR'] = ventas['Nombre'].fillna('').values
            output['CÓDIGO PRODUCTO INTERNO DISTRIBUIDOR'] = ventas['Cod. productto'].astype(str).values
            output['NOMBRE PRODUCTO'] = ventas['Descripción'].fillna('').values
            output['UNID.VEND.'] = ventas['Cantidad'].astype(int).values
            output['MONTOVENTA'] = ventas['Vta neta'].round(2).values
            output['MONTOBRUTO'] = ventas['Vta neta'].round(2).values
            output['FACTOR CONVERSION'] = ['1'] * num_registros
            output['FECHA(DD/MM/AAAA)'] = pd.to_datetime(ventas['Fecha']).dt.strftime('%d/%m/%Y').values
            output['FECHA ENTREGA (DD/MM/AAAA)'] = output['FECHA(DD/MM/AAAA)'].values
            output['DEPARTAMENTO'] = ['VALLE DEL CAUCA'] * num_registros
            output['CIUDAD'] = ventas['Cod. cliente'].astype(str).map(
                dict(zip(self.clientes_df['Cod. Cliente'].astype(str), 
                        self.clientes_df['Codigo Municipio'].fillna('76001')))
            ).values
            output['BARRIO'] = ventas['Cod. cliente'].astype(str).map(
                dict(zip(self.clientes_df['Cod. Cliente'].astype(str), 
                        self.clientes_df['Barrio'].fillna('')))
            ).values
            output['CODIGO TIPOPUNTOVENTA'] = ['13'] * num_registros
            output['CÓDIGO ZONA VENTA'] = ventas['Área'].fillna('').values
            output['RUTA'] = [''] * num_registros
            output['DOCUMENTO'] = ventas['Fac. numero'].astype(str).values
            output['Tipo de Documento'] = ['F' if len(ventas) > 0 and ventas['Tipo'].iloc[0] == 0 else 'D'] * num_registros
            output['TIPO VENTA'] = ['V'] * num_registros
            # Verificar campos críticos antes de guardar
            if not self.verificar_campos_criticos(output, 'VENTAS', codigo):
                self.logger.error(f"❌ Verificación falló para VentaDia_{codigo}")

            # Guardar archivo con la fecha máxima de ventas
            filename = f'VentaDia_{codigo}_{fecha_str}.txt'
            filepath = os.path.join(self.output_folder, filename)

            output.to_csv(filepath, sep='|', index=False, header=False, encoding='utf-8')

            self.logger.info(f"✓ {filename}: {len(output)} registros")

            return filepath

        except Exception as e:
            self.logger.error(f"Error generando ventas para {dataset_key}: {e}")
            return None

    def generar_archivo_inventario(self, dataset_key):
        """Genera el archivo de inventario."""
        try:
            dataset = self.datasets[dataset_key]
            inventario = dataset['inventario']
            codigo = dataset['codigo']

            if len(inventario) == 0:
                self.logger.warning(f"No hay inventario para {dataset_key}")
                return None

            # Obtener la fecha máxima de ventas para el nombre del archivo
            ventas = dataset.get('ventas')
            if ventas is not None and 'Fecha' in ventas.columns and not ventas['Fecha'].isnull().all():
                max_fecha = pd.to_datetime(ventas['Fecha'], errors='coerce').max()
                if pd.notnull(max_fecha):
                    fecha_str = max_fecha.strftime('%Y%m%d')
                else:
                    fecha_str = self.fecha_actual
            else:
                fecha_str = self.fecha_actual

            # Crear estructura de salida con valores fijos correctos
            num_registros = len(inventario)
            output = pd.DataFrame()

            # CAMPOS CRÍTICOS - Forzar como string
            output['CODPAÍS'] = [self.codigo_pais] * num_registros
            output['CODIGO DISTRIBUIDOR'] = [str(codigo)] * num_registros

            self.logger.info(f"  Asignando CODPAÍS='{self.codigo_pais}' y CODIGO DISTRIBUIDOR='{codigo}' a {num_registros} registros")

            output['CODIGO ARTICULO DISTRIBUIDOR'] = inventario['Codigo articulo'].astype(str).values
            output['DESCRIPCION ARTICULO'] = inventario['Nombre articulo'].fillna('').values
            output['BODEGA'] = inventario['Nombre bodega'].fillna('').values
            output['UNID'] = inventario['Unidades'].astype(int).values
            output['COSTO INVENT.'] = inventario['Valor'].round(2).values
            output['FECHA'] = [datetime.now().strftime('%d/%m/%Y')] * num_registros
            # Verificar campos críticos antes de guardar
            if not self.verificar_campos_criticos(output, 'INVENTARIO', codigo):
                self.logger.error(f"❌ Verificación falló para Inventario_{codigo}")

            # Guardar archivo con la fecha máxima de ventas
            filename = f'Inventario_{codigo}_{fecha_str}.txt'
            filepath = os.path.join(self.output_folder, filename)

            output.to_csv(filepath, sep='|', index=False, header=False, encoding='utf-8')

            self.logger.info(f"✓ {filename}: {len(output)} registros")

            return filepath

        except Exception as e:
            self.logger.error(f"Error generando inventario para {dataset_key}: {e}")
            return None

    def verificar_campos_criticos(self, output, tipo_archivo, codigo_distribuidor):
        """Verifica que los campos críticos estén correctamente llenados en el archivo."""
        try:
            # Verificar que el DataFrame no esté vacío
            if len(output) == 0:
                self.logger.warning(f"  ⚠️ {tipo_archivo}: DataFrame vacío")
                return False
            
            # Verificar CODPAÍS
            valores_pais = output.iloc[:, 0].unique()  # Primera columna
            if len(valores_pais) != 1 or valores_pais[0] != self.codigo_pais:
                self.logger.error(f"  ❌ {tipo_archivo}: CODPAÍS incorrecto - {valores_pais}")
                return False
            
            # Verificar código distribuidor  
            valores_codigo = output.iloc[:, 1].unique()  # Segunda columna
            if len(valores_codigo) != 1 or str(valores_codigo[0]) != str(codigo_distribuidor):
                self.logger.error(f"  ❌ {tipo_archivo}: Código distribuidor incorrecto - {valores_codigo}")
                return False
            
            self.logger.info(f"  ✅ {tipo_archivo}: Campos críticos OK - {self.codigo_pais}|{codigo_distribuidor}")
            
            # Mostrar primeras 2 columnas de la primera fila como ejemplo
            ejemplo = f"{output.iloc[0, 0]}|{output.iloc[0, 1]}"
            self.logger.info(f"  📝 Ejemplo primera fila: {ejemplo}|...")            
            return True
            
        except Exception as e:
            self.logger.error(f"  ❌ Error verificando {tipo_archivo}: {e}")
            return False

    def enviar_por_sftp(self, archivos):
        """Envía archivos por SFTP con logging detallado."""
        if not self.config.get('enviar_sftp', False):
            self.logger.info("Envío SFTP deshabilitado")
            return
        
        archivos_validos = [a for a in archivos if a is not None]
        archivos_enviados = []
        archivos_fallidos = []
        
        self.logger.info("=== INICIANDO ENVÍO SFTP ===")
        self.logger.info(f"Total archivos a enviar: {len(archivos_validos)}")
        
        if len(archivos_validos) == 0:
            self.logger.warning("⚠️ No hay archivos válidos para enviar")
            return
        
        ssh = None
        sftp = None
        
        try:
            import paramiko
            
            # Configuración SFTP
            hostname = self.config.get('sftp_host', 'mars.cdtbigdata.com')
            port = self.config.get('sftp_port', 22)
            username = self.config.get('sftp_user', 'mars.distrijass')
            password = self.config.get('sftp_pass', '')
            
            self.logger.info(f"🔗 Conectando a {hostname}:{port} como {username}")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname, port, username, password)
            
            self.logger.info("🔑 Autenticación exitosa")
            
            sftp = ssh.open_sftp()
            self.logger.info("📁 Canal SFTP abierto exitosamente")
            
            # Enviar cada archivo individualmente con logging detallado
            for i, archivo in enumerate(archivos_validos, 1):
                try:
                    nombre = os.path.basename(archivo)
                    tamaño = os.path.getsize(archivo)
                    tamaño_mb = tamaño / (1024 * 1024)
                    
                    self.logger.info(f"📤 [{i}/{len(archivos_validos)}] Enviando {nombre} ({tamaño_mb:.2f} MB)")
                    
                    # Intentar envío del archivo
                    sftp.put(archivo, nombre)
                    
                    # Verificar que el archivo fue enviado correctamente
                    try:
                        remote_stat = sftp.stat(nombre)
                        if remote_stat.st_size == tamaño:
                            self.logger.info(f"✅ {nombre} enviado exitosamente (verificado)")
                            archivos_enviados.append({
                                'archivo': nombre,
                                'tamaño_local': tamaño,
                                'tamaño_remoto': remote_stat.st_size,
                                'estado': 'EXITOSO'
                            })
                        else:
                            self.logger.warning(f"⚠️ {nombre} tamaño no coincide: local={tamaño}, remoto={remote_stat.st_size}")
                            archivos_fallidos.append({
                                'archivo': nombre,
                                'error': f'Tamaño no coincide: local={tamaño}, remoto={remote_stat.st_size}',
                                'estado': 'FALLIDO'
                            })
                    except Exception as verify_error:
                        self.logger.warning(f"⚠️ No se pudo verificar {nombre}: {verify_error}")
                        archivos_enviados.append({
                            'archivo': nombre,
                            'tamaño_local': tamaño,
                            'tamaño_remoto': 'No verificado',
                            'estado': 'ENVIADO_SIN_VERIFICAR'
                        })
                        
                except Exception as file_error:
                    self.logger.error(f"❌ Error enviando {nombre}: {file_error}")
                    archivos_fallidos.append({
                        'archivo': nombre,
                        'error': str(file_error),
                        'estado': 'FALLIDO'
                    })
            
        except Exception as e:
            self.logger.error(f"❌ Error de conexión SFTP: {e}")
            # Marcar todos los archivos como fallidos si falla la conexión
            for archivo in archivos_validos:
                nombre = os.path.basename(archivo)
                archivos_fallidos.append({
                    'archivo': nombre,
                    'error': f'Error de conexión: {e}',
                    'estado': 'FALLIDO'
                })
            raise
            
        finally:
            # Cerrar conexiones
            if sftp:
                try:
                    sftp.close()
                    self.logger.info("📁 Canal SFTP cerrado")
                except Exception:
                    pass
            
            if ssh:
                try:
                    ssh.close()
                    self.logger.info("🔗 Conexión SSH cerrada")
                except Exception:
                    pass
            
            # Log del resumen final
            self._log_resumen_sftp(archivos_enviados, archivos_fallidos)

    def _log_resumen_sftp(self, archivos_enviados, archivos_fallidos):
        """Genera un resumen detallado del envío SFTP."""
        self.logger.info("=== RESUMEN ENVÍO SFTP ===")
        
        total_archivos = len(archivos_enviados) + len(archivos_fallidos)
        total_exitosos = len([a for a in archivos_enviados if a['estado'] == 'EXITOSO'])
        total_sin_verificar = len([a for a in archivos_enviados if a['estado'] == 'ENVIADO_SIN_VERIFICAR'])
        total_fallidos = len(archivos_fallidos)
        
        self.logger.info(f"� Total archivos procesados: {total_archivos}")
        self.logger.info(f"✅ Enviados exitosamente: {total_exitosos}")
        self.logger.info(f"⚠️ Enviados sin verificar: {total_sin_verificar}")
        self.logger.info(f"❌ Fallidos: {total_fallidos}")
        
        if archivos_enviados:
            self.logger.info("--- ARCHIVOS ENVIADOS ---")
            for archivo in archivos_enviados:
                self.logger.info(f"  ✓ {archivo['archivo']} - {archivo['estado']}")
                if archivo['tamaño_remoto'] != 'No verificado':
                    self.logger.info(f"    Tamaño: {archivo['tamaño_local']} bytes")
        
        if archivos_fallidos:
            self.logger.info("--- ARCHIVOS FALLIDOS ---")
            for archivo in archivos_fallidos:
                self.logger.info(f"  ❌ {archivo['archivo']} - {archivo['error']}")
        
        # Determinar estado general
        if total_fallidos == 0:
            if total_sin_verificar == 0:
                self.logger.info("🎉 ENVÍO COMPLETAMENTE EXITOSO")
            else:
                self.logger.info("✅ ENVÍO COMPLETADO CON ADVERTENCIAS")
        else:
            if total_exitosos > 0:
                self.logger.info("⚠️ ENVÍO PARCIALMENTE EXITOSO")
            else:
                self.logger.info("❌ ENVÍO COMPLETAMENTE FALLIDO")
        
        self.logger.info("=== FIN RESUMEN SFTP ===")
        
        # Crear archivo de resumen adicional
        self._crear_log_sftp_detallado(archivos_enviados, archivos_fallidos)

    def _crear_log_sftp_detallado(self, archivos_enviados, archivos_fallidos):
        """Crea un archivo de log específico para el envío SFTP."""
        try:
            log_sftp_path = os.path.join(self.base_dir, 'logs', f'sftp_envio_{self.fecha_actual}.log')
            
            with open(log_sftp_path, 'w', encoding='utf-8') as f:
                f.write(f"REPORTE DETALLADO ENVÍO SFTP - {self.fecha_actual}\n")
                f.write(f"{'='*60}\n")
                f.write(f"Fecha y hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Servidor SFTP: {self.config.get('sftp_host', 'mars.cdtbigdata.com')}\n")
                f.write(f"Usuario: {self.config.get('sftp_user', 'mars.distrijass')}\n\n")
                
                # Resumen ejecutivo
                total_archivos = len(archivos_enviados) + len(archivos_fallidos)
                total_exitosos = len([a for a in archivos_enviados if a['estado'] == 'EXITOSO'])
                total_sin_verificar = len([a for a in archivos_enviados if a['estado'] == 'ENVIADO_SIN_VERIFICAR'])
                total_fallidos = len(archivos_fallidos)
                
                f.write("RESUMEN EJECUTIVO\n")
                f.write("-" * 30 + "\n")
                f.write(f"Total archivos procesados: {total_archivos}\n")
                f.write(f"Enviados exitosamente: {total_exitosos}\n")
                f.write(f"Enviados sin verificar: {total_sin_verificar}\n")
                f.write(f"Fallidos: {total_fallidos}\n")
                f.write(f"Tasa de éxito: {(total_exitosos/total_archivos*100):.1f}%\n\n" if total_archivos > 0 else "Tasa de éxito: 0.0%\n\n")
                
                # Detalle de archivos enviados
                if archivos_enviados:
                    f.write("ARCHIVOS ENVIADOS EXITOSAMENTE\n")
                    f.write("-" * 40 + "\n")
                    for archivo in archivos_enviados:
                        f.write(f"Archivo: {archivo['archivo']}\n")
                        f.write(f"Estado: {archivo['estado']}\n")
                        f.write(f"Tamaño local: {archivo['tamaño_local']:,} bytes\n")
                        if archivo['tamaño_remoto'] != 'No verificado':
                            f.write(f"Tamaño remoto: {archivo['tamaño_remoto']:,} bytes\n")
                        f.write("-" * 20 + "\n")
                    f.write("\n")
                
                # Detalle de archivos fallidos
                if archivos_fallidos:
                    f.write("ARCHIVOS FALLIDOS\n")
                    f.write("-" * 20 + "\n")
                    for archivo in archivos_fallidos:
                        f.write(f"Archivo: {archivo['archivo']}\n")
                        f.write(f"Estado: {archivo['estado']}\n")
                        f.write(f"Error: {archivo['error']}\n")
                        f.write("-" * 20 + "\n")
                    f.write("\n")
                
                # Recomendaciones
                f.write("RECOMENDACIONES\n")
                f.write("-" * 20 + "\n")
                if total_fallidos == 0:
                    f.write("✅ Todos los archivos fueron enviados exitosamente.\n")
                    f.write("   No se requieren acciones adicionales.\n")
                else:
                    f.write("⚠️ Se detectaron archivos fallidos.\n")
                    f.write("   Recomendaciones:\n")
                    f.write("   1. Verificar la conectividad al servidor SFTP\n")
                    f.write("   2. Validar las credenciales de acceso\n")
                    f.write("   3. Confirmar que los archivos locales no estén corruptos\n")
                    f.write("   4. Reintentar el envío de los archivos fallidos\n")
            
            self.logger.info(f"📋 Log detallado SFTP creado: {log_sftp_path}")
            
        except Exception as e:
            self.logger.error(f"Error creando log detallado SFTP: {e}")

    def generar_resumen_ejecutivo(self):
        """Genera un resumen ejecutivo en Excel con estadísticas por distribuidor y fecha máxima de facturación."""
        try:
            self.logger.info("=== GENERANDO RESUMEN EJECUTIVO ===")

            # Preparar datos para el resumen
            resumen_inventario = []
            resumen_ventas = []
            max_fecha_facturacion = None

            for dataset_key, dataset in self.datasets.items():
                if len(dataset['ventas']) > 0 or len(dataset['inventario']) > 0:
                    codigo = dataset['codigo']
                    ventas = dataset['ventas']
                    inventario = dataset['inventario']

                    # Buscar la fecha máxima de facturación en ventas
                    if 'Fecha' in ventas.columns and not ventas['Fecha'].isnull().all():
                        fechas_validas = pd.to_datetime(ventas['Fecha'], errors='coerce')
                        max_fecha_dataset = fechas_validas.max()
                        if pd.notnull(max_fecha_dataset):
                            if max_fecha_facturacion is None or max_fecha_dataset > max_fecha_facturacion:
                                max_fecha_facturacion = max_fecha_dataset

                    # Determinar empresa y tipo
                    if 'Distrijass' in dataset_key:
                        empresa = 'Distrijass'
                    else:
                        empresa = 'Eje'

                    if 'con_vendedores' in dataset_key:
                        tipo = 'Con vendedores especiales'
                    else:
                        tipo = 'Sin vendedores especiales'

                    # RESUMEN DE INVENTARIO
                    if len(inventario) > 0:
                        total_unidades = inventario['Unidades'].sum()
                        total_valor = inventario['Valor'].sum()

                        resumen_inventario.append({
                            'Código Distribuidor': codigo,
                            'Empresa': empresa,
                            'Tipo': tipo,
                            'Total Unidades': total_unidades,
                            'Total Valor (COP)': total_valor,
                            'Cantidad de SKUs': len(inventario)
                        })

                    # RESUMEN DE VENTAS
                    if len(ventas) > 0:
                        total_unidades_vendidas = ventas['Cantidad'].sum()
                        total_venta_neta = ventas['Vta neta'].sum()
                        
                        # Calcular clientes únicos con venta total > 0
                        # Agrupar por cliente y sumar ventas, luego filtrar los que tienen venta > 0
                        ventas_por_cliente = ventas.groupby('Cod. cliente')['Vta neta'].sum().reset_index()
                        clientes_con_venta = ventas_por_cliente[
                            pd.to_numeric(ventas_por_cliente['Vta neta'], errors='coerce').fillna(0) > 0
                        ]
                        clientes_unicos = len(clientes_con_venta)
                        
                        # Log detallado para debugging
                        self.logger.info(f"  📊 Análisis de clientes para {dataset_key}:")
                        self.logger.info(f"    Total registros de ventas: {len(ventas)}")
                        self.logger.info(f"    Clientes únicos en datos: {ventas['Cod. cliente'].nunique()}")
                        self.logger.info(f"    Clientes con venta total > 0: {clientes_unicos}")
                        self.logger.info(f"    Venta neta total: {total_venta_neta:,.2f}")
                        
                        # Mostrar algunos ejemplos de clientes y sus totales
                        if len(ventas_por_cliente) > 0:
                            ejemplos = ventas_por_cliente.head(5)
                            self.logger.info(f"    Ejemplos de ventas por cliente:")
                            for _, row in ejemplos.iterrows():
                                self.logger.info(f"      Cliente {row['Cod. cliente']}: {row['Vta neta']:,.2f}")
                        
                        total_facturas = ventas['Fac. numero'].nunique()

                        resumen_ventas.append({
                            'Código Distribuidor': codigo,
                            'Empresa': empresa,
                            'Tipo': tipo,
                            'Unidades Rotadas': total_unidades_vendidas,
                            'Venta Neta (COP)': total_venta_neta,
                            'Clientes Únicos (Venta > 0)': clientes_unicos,
                            'Total Facturas': total_facturas,
                            'Cantidad de SKUs': len(ventas['Cod. productto'].unique())
                        })

            # Crear DataFrames
            df_inventario = pd.DataFrame(resumen_inventario)
            df_ventas = pd.DataFrame(resumen_ventas)

            # Determinar la fecha para el nombre del archivo
            if max_fecha_facturacion is not None:
                fecha_str = max_fecha_facturacion.strftime('%Y%m%d')
            else:
                fecha_str = self.fecha_actual

            # Crear archivo Excel
            filename = f'Resumen_Ejecutivo_MasterFoods_{fecha_str}.xlsx'
            filepath = os.path.join(self.output_folder, filename)

            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Hoja de resumen de inventario
                if not df_inventario.empty:
                    df_inventario.to_excel(writer, sheet_name='Resumen Inventario', index=False)

                    # Formatear la hoja de inventario
                    worksheet = writer.sheets['Resumen Inventario']

                    # Ajustar ancho de columnas
                    for column in worksheet.columns:
                        max_length = 0
                        column = [cell for cell in column]
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except (TypeError, AttributeError):
                                pass
                        adjusted_width = (max_length + 2)
                        worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

                # Hoja de resumen de ventas
                if not df_ventas.empty:
                    df_ventas.to_excel(writer, sheet_name='Resumen Ventas', index=False)

                    # Formatear la hoja de ventas
                    worksheet = writer.sheets['Resumen Ventas']

                    # Ajustar ancho de columnas
                    for column in worksheet.columns:
                        max_length = 0
                        column = [cell for cell in column]
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except (TypeError, AttributeError):
                                pass
                        adjusted_width = (max_length + 2)
                        worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

                # Hoja de totales consolidados
                self._crear_hoja_totales(writer, df_inventario, df_ventas)

            self.logger.info(f"✓ Resumen ejecutivo generado: {filename}")
            self.logger.info(f"  📊 Resumen inventario: {len(df_inventario)} distribuidores")
            self.logger.info(f"  📈 Resumen ventas: {len(df_ventas)} distribuidores")

            return filepath

        except Exception as e:
            self.logger.error(f"Error generando resumen ejecutivo: {e}")
            return None

    def _crear_hoja_totales(self, writer, df_inventario, df_ventas):
        """Crea una hoja con totales consolidados."""
        try:
            totales = []
            
            # Totales por empresa
            for empresa in ['Distrijass', 'Eje']:
                # Totales de inventario por empresa
                inv_empresa = df_inventario[df_inventario['Empresa'] == empresa] if not df_inventario.empty else pd.DataFrame()
                ven_empresa = df_ventas[df_ventas['Empresa'] == empresa] if not df_ventas.empty else pd.DataFrame()
                
                if not inv_empresa.empty or not ven_empresa.empty:
                    total_unidades_inv = inv_empresa['Total Unidades'].sum() if not inv_empresa.empty else 0
                    total_valor_inv = inv_empresa['Total Valor (COP)'].sum() if not inv_empresa.empty else 0
                    total_unidades_ven = ven_empresa['Unidades Rotadas'].sum() if not ven_empresa.empty else 0
                    total_valor_ven = ven_empresa['Venta Neta (COP)'].sum() if not ven_empresa.empty else 0
                    total_clientes = ven_empresa['Clientes Únicos (Venta > 0)'].sum() if not ven_empresa.empty else 0
                    
                    totales.append({
                        'Empresa': empresa,
                        'Total Unidades Inventario': total_unidades_inv,
                        'Total Valor Inventario (COP)': total_valor_inv,
                        'Total Unidades Vendidas': total_unidades_ven,
                        'Total Venta Neta (COP)': total_valor_ven,
                        'Total Clientes Únicos': total_clientes,
                        'Rotación (%)': round((total_unidades_ven / total_unidades_inv * 100) if total_unidades_inv > 0 else 0, 2)
                    })
            
            # Gran total
            if totales:
                gran_total = {
                    'Empresa': 'GRAN TOTAL',
                    'Total Unidades Inventario': sum(t['Total Unidades Inventario'] for t in totales),
                    'Total Valor Inventario (COP)': sum(t['Total Valor Inventario (COP)'] for t in totales),
                    'Total Unidades Vendidas': sum(t['Total Unidades Vendidas'] for t in totales),
                    'Total Venta Neta (COP)': sum(t['Total Venta Neta (COP)'] for t in totales),
                    'Total Clientes Únicos': sum(t['Total Clientes Únicos'] for t in totales),
                    'Rotación (%)': round((sum(t['Total Unidades Vendidas'] for t in totales) / 
                                         sum(t['Total Unidades Inventario'] for t in totales) * 100) 
                                        if sum(t['Total Unidades Inventario'] for t in totales) > 0 else 0, 2)
                }
                totales.append(gran_total)
            
            df_totales = pd.DataFrame(totales)
            df_totales.to_excel(writer, sheet_name='Totales Consolidados', index=False)
            
            # Formatear la hoja de totales
            worksheet = writer.sheets['Totales Consolidados']
            
            # Ajustar ancho de columnas
            for column in worksheet.columns:
                max_length = 0
                column = [cell for cell in column]
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except (TypeError, AttributeError):
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
            
        except Exception as e:
            self.logger.error(f"Error creando hoja de totales: {e}")

    def procesar(self):
        """Ejecuta el procesamiento completo."""
        try:
            self.logger.info("🚀 INICIANDO PROCESAMIENTO MASTER FOODS V3.0")
            self.logger.info(f"Fecha de procesamiento: {self.fecha_actual}")
            
            # Paso 1: Cargar datos
            self.cargar_datos()
            
            # Paso 2: Filtrar por proveedor
            self.filtrar_por_proveedor()
            
            # Paso 3: Separar por empresa y vendedores
            self.separar_por_empresa_vendedor()
            
            # Paso 4: Generar archivos
            self.logger.info("=== GENERANDO ARCHIVOS ===")
            archivos_generados = []
            
            for dataset_key, dataset in self.datasets.items():
                if len(dataset['ventas']) > 0:
                    self.logger.info(f"\n--- Generando {dataset_key} (#{dataset['codigo']}) ---")
                    
                    # Generar los 3 tipos de archivo
                    archivo_clientes = self.generar_archivo_clientes(dataset_key)
                    archivo_ventas = self.generar_archivo_ventas(dataset_key)
                    archivo_inventario = self.generar_archivo_inventario(dataset_key)
                    
                    archivos_generados.extend([archivo_clientes, archivo_ventas, archivo_inventario])
                else:
                    self.logger.info(f"Saltando {dataset_key} - sin ventas")
            
            # Filtrar archivos válidos
            archivos_validos = [a for a in archivos_generados if a is not None]
            
            # Paso 5: Generar resumen ejecutivo (NO se envía por SFTP)
            self.logger.info("=== GENERANDO RESUMEN EJECUTIVO ===")
            resumen_ejecutivo = self.generar_resumen_ejecutivo()
            
            # Paso 6: Enviar por SFTP (solo archivos planos, NO el resumen)
            self.enviar_por_sftp(archivos_validos)
            
            # Resumen final
            self.logger.info("=== PROCESAMIENTO COMPLETADO ===")
            self.logger.info(f"Archivos generados: {len(archivos_validos)}")
            for archivo in archivos_validos:
                self.logger.info(f"  ✓ {os.path.basename(archivo)}")
            
            print(f"✅ Procesamiento completado exitosamente")
            print(f"📁 Archivos generados: {len(archivos_validos)}")
            print(f"📂 Ubicación: {self.output_folder}")
            print(f"📝 Log: logs/masterfoods_processor.log")
            if resumen_ejecutivo:
                print(f"📊 Resumen ejecutivo: {os.path.basename(resumen_ejecutivo)}")
            else:
                print(f"⚠️ No se pudo generar el resumen ejecutivo")
            
        except Exception as e:
            self.logger.error(f"❌ Error durante el procesamiento: {e}")
            print(f"❌ Error: {e}")
            print("📝 Revisar log para más detalles")
            raise

if __name__ == '__main__':
    try:
        processor = MasterFoodsProcessor()
        processor.procesar()
    except KeyboardInterrupt:
        print("\n⚠️ Procesamiento cancelado por el usuario")
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        exit(1)
