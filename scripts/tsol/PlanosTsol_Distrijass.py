# PlanosTsol_Distrijass.py
# Generador de archivos TSOL para DISTRIJASS CALI (NIT 211688)
# Basado en PlanosTsol_Colgate.py - Integrado con PROVEE-TSOL.xlsx

import pandas as pd
import csv
import os
from datetime import datetime
import logging
import re
import json
import zipfile
import ftplib
import calendar
import shutil


# Asegurar carpeta de logs para no ensuciar el root del proyecto
os.makedirs('logs', exist_ok=True)


# Configuración del logging
logging.basicConfig(
    filename=os.path.join('logs', 'distrijass_cali.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

class VentaProcessor:
    BODEGA_TO_CODE = {
        'CALI': '01',
        'CALI/PALMIRA': '01',
        'PALMIRA': '01',
        'PALMIRA/CALI': '01',
        'SUCURSAL CALI': '01',
        'TULUA': '02',
        'TULUÁ': '02',
        'BUGA': '03',
        'POPAYAN': '04',
        'POPAYÁN': '04',
        'BUENAVENTURA': '05',
        'B/VENTURA': '05',
        'PEREIRA': '06'
    }
    CODE_TO_SEDE = {
        '01': 'PALMIRA/CALI',
        '02': 'TULUÁ',
        '03': 'BUGA',
        '04': 'POPAYÁN',
        '05': 'BUENAVENTURA',
        '06': 'PEREIRA'
    }
    DEFAULT_SEDE_CODE = '01'
    DEFAULT_SEDE_NAME = 'PALMIRA/CALI'
    ALLOWED_SEDES = {'01', '04', '06'}

    def __init__(self, config_path):
        self.config = self._cargar_configuracion(config_path)
        # Usar configuración de empresa 'distrijass'
        self.company_config = self.config['companies']['distrijass']
        
        self.ventas_path = self._obtener_ruta_archivo('ventas')
        self.output_folder = os.path.join(
            self.config.get('output_folder', 'output_files'),
            self.company_config['output_subfolder']
        )
        self.catalogo_principal = self.config['files']['catalogo_principal']
        
        # Cargar proveedores desde archivo proveedores.txt
        self.proveedores = self._cargar_proveedores_desde_archivo()
        
        # mes y ano se determinarán dinámicamente desde los datos del Excel
        self.mes = None
        self.ano = None
        self.filtered_data = None
        self.period_data = None
        self.quality_issues = []
        self._crear_carpeta_salida()

    def _cargar_configuracion(self, config_path):
        """Carga la configuración desde un archivo JSON."""
        if not os.path.isfile(config_path):
            logger.error(f"Archivo de configuración no encontrado: {config_path}")
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = json.load(file)
                logger.info("Configuración cargada exitosamente.")
                return config
        except Exception as e:
            logger.error(f"Error al cargar la configuración: {e}")
            raise

    def _crear_carpeta_salida(self):
        """Crea la carpeta de salida si no existe."""
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            logger.info(f"Carpeta de salida creada: {self.output_folder}")
        else:
            logger.info(f"Carpeta de salida ya existe: {self.output_folder}")

    def _cargar_proveedores_desde_archivo(self):
        """Carga la lista de proveedores desde el archivo proveedores.txt."""
        try:
            providers_path = self.config['files'].get('providers', 'proveedores.txt')
            if not os.path.isfile(providers_path):
                logger.warning(f"Archivo de proveedores no encontrado: {providers_path}. Usando configuración por defecto.")
                return self.company_config.get('filtro_proveedores', {}).get('criterios', [])
            
            proveedores = []
            with open(providers_path, 'r', encoding='utf-8') as file:
                for line in file:
                    proveedor = line.strip()
                    if proveedor:  # Ignorar líneas vacías
                        proveedores.append(proveedor)
            
            logger.info(f"Cargados {len(proveedores)} proveedores desde {providers_path}")
            return proveedores
        except Exception as e:
            logger.error(f"Error al cargar proveedores desde archivo: {e}")
            # Fallback a configuración por defecto
            return self.company_config.get('filtro_proveedores', {}).get('criterios', [])

    def _obtener_ruta_archivo(self, clave):
        """Obtiene rutas priorizando la configuracion de la empresa."""
        company_files = self.company_config.get('files', {})
        ruta = company_files.get(clave)

        if not ruta:
            ruta = self.config.get('files', {}).get(clave)

        if not ruta:
            mensaje = f"No se definió la ruta del archivo '{clave}' en config.json"
            logger.error(mensaje)
            raise ValueError(mensaje)

        return ruta

    def _inferir_codigo_sede(self, nombre_bodega):
        """Devuelve el código TSOL de la sede a partir del nombre de bodega recibido."""
        if nombre_bodega is None or pd.isna(nombre_bodega):
            return self.DEFAULT_SEDE_CODE
        normalizado = str(nombre_bodega).strip().upper()
        if normalizado in self.BODEGA_TO_CODE:
            return self.BODEGA_TO_CODE[normalizado]
        for etiqueta, codigo in self.BODEGA_TO_CODE.items():
            if etiqueta in normalizado:
                return codigo
        return self.DEFAULT_SEDE_CODE

    def _obtener_nombre_sede(self, codigo_sede):
        """Devuelve el nombre de sede a partir del código TSOL."""
        return self.CODE_TO_SEDE.get(str(codigo_sede), self.DEFAULT_SEDE_NAME)

    def _normalizar_codigo_bodega(self, valor):
        """Acepta nombres o códigos de bodega y retorna el código TSOL estandarizado."""
        if valor is None or pd.isna(valor):
            return self.DEFAULT_SEDE_CODE
        texto = str(valor).strip()
        if not texto:
            return self.DEFAULT_SEDE_CODE
        if texto in self.CODE_TO_SEDE:
            return texto
        return self._inferir_codigo_sede(texto)

    def _filtrar_sedes_permitidas(self, df, columna_codigo, contexto):
        """Restringe los registros a las sedes autorizadas y documenta los descartes."""
        if columna_codigo not in df.columns:
            return df
        df[columna_codigo] = df[columna_codigo].apply(self._normalizar_codigo_bodega)
        mask = df[columna_codigo].isin(self.ALLOWED_SEDES)
        descartados = df.loc[~mask, columna_codigo].unique()
        if descartados.size > 0:
            self._registrar_inconsistencia(
                'Bodegas',
                f"Se excluyen {len(df) - mask.sum()} registros de {contexto} por sedes no autorizadas.",
                detalle=', '.join(sorted(str(x) for x in descartados)),
                accion='Limitar a PALMIRA/CALI y POPAYÁN'
            )
        return df.loc[mask].copy()

    def _buscar_columna(self, df, posibles_nombres):
        """Encuentra la primera columna disponible dentro de una lista de opciones."""
        for posible in posibles_nombres:
            if posible in df.columns:
                return posible
        return None

    def _detectar_delimitador(self, primera_linea):
        """Detecta el delimitador más probable en un archivo de texto."""
        if not primera_linea:
            return '{'
        for delim in [';', '{', '|', '\t']:
            if delim in primera_linea:
                return delim
        return '{'

    def _leer_txt_delimitado(self, path, nombres, encoding='latin1'):
        """Lee TXT con delimitador variable y/o encabezado, normalizando columnas."""
        with open(path, 'r', encoding=encoding, errors='replace') as file:
            primera_linea = file.readline()

        delimitador = self._detectar_delimitador(primera_linea)
        tokens = [t.strip().lower() for t in primera_linea.strip().split(delimitador) if t.strip()]
        nombres_lower = [n.strip().lower() for n in nombres]
        tiene_encabezado = any(token in nombres_lower for token in tokens)

        try:
            csv.field_size_limit(1024 * 1024 * 10)
        except (OverflowError, ValueError):
            csv.field_size_limit(1024 * 1024)

        df = pd.read_csv(
            path,
            sep=delimitador,
            engine='python',
            encoding=encoding,
            encoding_errors='replace',
            header=0 if tiene_encabezado else None,
            dtype=str,
            keep_default_na=False,
            on_bad_lines='skip',
            quoting=csv.QUOTE_NONE,
            escapechar='\\',
            doublequote=False
        )

        if not tiene_encabezado:
            df.columns = nombres
        else:
            # Normalizar nombres de columnas a los esperados
            col_map = {}
            for col in df.columns:
                norm = str(col).strip().lower()
                for esperado in nombres:
                    if norm == esperado.strip().lower():
                        col_map[col] = esperado
                        break
            df = df.rename(columns=col_map)
            if not any(col in df.columns for col in nombres) and len(df.columns) >= len(nombres):
                df = df.iloc[:, :len(nombres)]
                df.columns = nombres

        return df

    def _registrar_inconsistencia(self, tipo, descripcion, detalle=None, severidad='warning', accion=''):
        """Centraliza el registro de hallazgos en el reporte de calidad."""
        logger.log(logging.ERROR if severidad == 'error' else logging.WARNING, descripcion)
        self.quality_issues.append({
            'Tipo': tipo,
            'Descripción': descripcion,
            'Detalle': detalle if detalle is not None else '',
            'Severidad': severidad,
            'Acción': accion
        })

    def _formatear_codigo_producto(self, codigo):
        """Normaliza códigos de producto preservando ceros a la izquierda y limpiando decimales residuales."""
        if codigo is None or pd.isna(codigo):
            return ''
        texto = str(codigo).strip().replace('"', '').replace("'", '')
        if not texto:
            return ''
        if re.match(r'^\d+\.0+$', texto):
            texto = texto.split('.')[0]
        return texto

    def _normalizar_codigo_vendedor(self, codigo):
        """Normaliza códigos de vendedor y corrige errores conocidos (p.ej. ROVEEDOR)."""
        if codigo is None or pd.isna(codigo):
            return ''
        texto = str(codigo).strip().replace('.0', '')
        if texto.upper() == 'ROVEEDOR':
            return 'PROVEEDOR'
        return texto

    def _limpiar_texto(self, valor, default=''):
        """Convierte valores NaN/None en cadenas limpias con un valor por defecto."""
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return default
        texto = str(valor).strip()
        return texto if texto and texto.lower() not in ['nan', 'none'] else default

    def _autocompletar_vendedores(self, interasesor_data):
        """Crea registros auxiliares de vendedores ausentes usando la información de infoventas."""
        if self.period_data is None or self.period_data.empty:
            return interasesor_data
        if not hasattr(self, 'filtered_data_total'):
            return interasesor_data

        codigos_ventas = {
            str(codigo).strip()
            for codigo in self.filtered_data_total['Código Vendedor']
            if pd.notna(codigo)
        }
        codigos_maestra = {
            str(codigo).strip()
            for codigo in interasesor_data['Codigo']
            if pd.notna(codigo)
        }
        faltantes = sorted(codigo for codigo in codigos_ventas - codigos_maestra if codigo)
        if not faltantes:
            return interasesor_data

        codigo_col = self._buscar_columna(self.period_data, ['Cod. vendedor', 'Código Vendedor', 'Cod. Vendedor'])
        if not codigo_col:
            self._registrar_inconsistencia(
                'Vendedores',
                'Existen códigos de vendedor sin maestro pero no hay columnas de referencia para autocompletar.',
                detalle=', '.join(faltantes[:10]),
                severidad='error'
            )
            return interasesor_data

        nombre_col = self._buscar_columna(self.period_data, ['Nom. vendedor', 'Nombre vendedor', 'Vendedor'])
        doc_col = self._buscar_columna(self.period_data, ['Documento vendedor', 'Cedula vendedor', 'Cédula vendedor'])
        dir_col = self._buscar_columna(self.period_data, ['Direccion vendedor', 'Dirección vendedor'])
        supervisor_col = self._buscar_columna(self.period_data, ['Cod. supervisor', 'Código supervisor'])
        bodega_col = self._buscar_columna(self.period_data, ['Bodega', 'Nombre bodega'])

        nuevos_registros = []
        for codigo in faltantes:
            subset = self.period_data[self.period_data[codigo_col].astype(str).str.strip() == codigo]
            if subset.empty:
                continue

            registro = {col: '' for col in interasesor_data.columns}
            registro['Codigo'] = codigo
            documento = self._limpiar_texto(subset[doc_col].iloc[0] if doc_col else None, '0')
            registro['Documento'] = documento
            nombre = self._limpiar_texto(subset[nombre_col].iloc[0] if nombre_col else None, f'VEND-{codigo}')
            registro['Nombre'] = nombre if nombre else f'VEND-{codigo}'
            registro['Apellido'] = ''
            registro['Telefono'] = ''
            direccion = self._limpiar_texto(subset[dir_col].iloc[0] if dir_col else None, 'SIN DIRECCION')
            registro['Direccion'] = direccion if direccion else 'SIN DIRECCION'
            registro['Cargo'] = 'VENDEDOR AUTO'
            registro['Portafolio'] = 'DEFAULT'
            registro['Estado'] = 'Activado Auto'
            supervisor = str(subset[supervisor_col].iloc[0]).strip() if supervisor_col else '0000'
            registro['Codigo supervisor'] = supervisor if supervisor else '0000'
            bodega_val = subset[bodega_col].iloc[0] if bodega_col else self.DEFAULT_SEDE_NAME
            registro['Codigo bodega'] = self._inferir_codigo_sede(bodega_val)
            nuevos_registros.append(registro)

        if nuevos_registros:
            interasesor_data = pd.concat([interasesor_data, pd.DataFrame(nuevos_registros)], ignore_index=True)
            self._registrar_inconsistencia(
                'Vendedores',
                f'Se autocompletan {len(nuevos_registros)} vendedores faltantes usando infoventas.',
                detalle=', '.join(faltantes[:15]),
                accion='Agregar registro automático'
            )

        return interasesor_data

    def _autocompletar_supervisores(self, intersupervisor_data, codigos_requeridos):
        """Crea supervisores auxiliares cuando falten en la maestra oficial."""
        if self.period_data is None or self.period_data.empty:
            return intersupervisor_data

        existentes = {
            str(codigo).strip()
            for codigo in intersupervisor_data['Codigo']
            if pd.notna(codigo)
        }
        faltantes = sorted({str(c).strip() for c in codigos_requeridos if str(c).strip()} - existentes)
        if not faltantes:
            return intersupervisor_data

        codigo_col = self._buscar_columna(self.period_data, ['Cod. supervisor', 'Código supervisor'])
        nombre_col = self._buscar_columna(self.period_data, ['Nom. supervisor', 'Nombre supervisor', 'Supervisor'])
        bodega_col = self._buscar_columna(self.period_data, ['Bodega supervisor', 'Bodega', 'Nombre bodega'])

        if not codigo_col:
            # Crear registros mínimos cuando no hay columnas de referencia
            nuevos_minimos = []
            for codigo in faltantes:
                registro = {col: '' for col in intersupervisor_data.columns}
                registro['Codigo'] = codigo
                registro['Documento'] = '0'
                registro['Nombre'] = f'SUP-{codigo}'
                registro['Apellido'] = ''
                registro['Telefono'] = ''
                registro['Direccion'] = ''
                registro['Cargo'] = 'SUPERVISOR AUTO'
                registro['Portafolio'] = 'DEFAULT'
                registro['Estado'] = 'Activado Auto'
                registro['Codigo bodega'] = self.DEFAULT_SEDE_CODE
                nuevos_minimos.append(registro)

            if nuevos_minimos:
                intersupervisor_data = pd.concat([intersupervisor_data, pd.DataFrame(nuevos_minimos)], ignore_index=True)
                self._registrar_inconsistencia(
                    'Supervisores',
                    f'Se autocompletan {len(nuevos_minimos)} supervisores con datos mínimos por falta de columnas.',
                    detalle=', '.join(faltantes[:10]),
                    accion='Agregar registro automático'
                )
            return intersupervisor_data

        nuevos = []
        for codigo in faltantes:
            subset = self.period_data[self.period_data[codigo_col].astype(str).str.strip() == codigo]
            if subset.empty:
                continue
            registro = {col: '' for col in intersupervisor_data.columns}
            registro['Codigo'] = codigo
            registro['Documento'] = '0'
            registro['Nombre'] = self._limpiar_texto(subset[nombre_col].iloc[0] if nombre_col else None, f'SUP-{codigo}')
            registro['Apellido'] = ''
            registro['Telefono'] = ''
            registro['Direccion'] = ''
            registro['Cargo'] = 'SUPERVISOR AUTO'
            registro['Portafolio'] = 'DEFAULT'
            registro['Estado'] = 'Activado Auto'
            bodega_val = subset[bodega_col].iloc[0] if bodega_col else self.DEFAULT_SEDE_NAME
            registro['Codigo bodega'] = self._inferir_codigo_sede(bodega_val)
            nuevos.append(registro)

        if nuevos:
            intersupervisor_data = pd.concat([intersupervisor_data, pd.DataFrame(nuevos)], ignore_index=True)
            self._registrar_inconsistencia(
                'Supervisores',
                f'Se autocompletan {len(nuevos)} supervisores faltantes usando infoventas.',
                detalle=', '.join(faltantes[:15]),
                accion='Agregar registro automático'
            )
        return intersupervisor_data

    def _autocompletar_clientes(self, intercliente_data, clientes_objetivo):
        """Agrega clientes faltantes en la maestra con datos mínimos a partir de infoventas."""
        if self.period_data is None or self.period_data.empty:
            return intercliente_data

        codigos_existentes = {
            str(codigo).strip()
            for codigo in intercliente_data['Código']
            if pd.notna(codigo)
        }
        clientes_objetivo = {str(c).strip() for c in clientes_objetivo if str(c).strip()}
        faltantes = sorted(clientes_objetivo - codigos_existentes)
        if not faltantes:
            return intercliente_data

        codigo_col = self._buscar_columna(self.period_data, ['Cod. cliente', 'Código Cliente'])
        if not codigo_col:
            self._registrar_inconsistencia(
                'Clientes',
                'No fue posible autocompletar clientes por ausencia de la columna de código en infoventas.',
                detalle=', '.join(faltantes[:10]),
                severidad='error'
            )
            return intercliente_data

        def obtener_valor(subset, candidatos, default=''):
            columna = self._buscar_columna(subset, candidatos)
            if columna:
                return self._limpiar_texto(subset[columna].iloc[0], default)
            return default

        nuevos = []
        for codigo in faltantes:
            subset = self.period_data[
                self.period_data[codigo_col].astype(str).str.strip().str.replace('-', '999', regex=False) == codigo
            ]
            if subset.empty:
                continue
            registro = {col: '' for col in intercliente_data.columns}
            registro['Código'] = codigo
            registro['Nombre'] = obtener_valor(subset, ['Nom. cliente', 'Nombre Cliente', 'Cliente'], f'CLIENTE {codigo}')
            fecha_val = obtener_valor(subset, ['Fecha'], datetime.now().strftime('%Y/%m/%d'))
            registro['Fecha Ingreso'] = fecha_val
            registro['Nit'] = obtener_valor(subset, ['Nit', 'Documento cliente', 'Identificación Cliente'], '0')
            registro['Dirección'] = obtener_valor(subset, ['Direccion', 'Dirección cliente'], 'SIN DIRECCION')
            registro['Teléfono'] = obtener_valor(subset, ['Telefono', 'Teléfono cliente'], '0')
            registro['Representante Legal'] = obtener_valor(subset, ['Representante Legal'], 'NA')
            registro['Código Municipio'] = obtener_valor(subset, ['Codigo Municipio', 'Código Municipio', 'Municipio'], '76001000')
            registro['Codigo Negocio'] = obtener_valor(subset, ['Codigo Negocio'], '0')
            registro['Tipo Negocio'] = obtener_valor(subset, ['Tipo Negocio', 'Tipo Cliente'], 'SINCLAS')
            registro['Estrato'] = '4'
            registro['Barrio'] = obtener_valor(subset, ['Barrio'], 'SIN BARRIO')
            nuevos.append(registro)

        if nuevos:
            intercliente_data = pd.concat([intercliente_data, pd.DataFrame(nuevos)], ignore_index=True)
            self._registrar_inconsistencia(
                'Clientes',
                f'Se autocompletan {len(nuevos)} clientes faltantes usando infoventas.',
                detalle=', '.join(faltantes[:15]),
                accion='Agregar registro automático'
            )
        return intercliente_data

    def _transformar_catalogo_productos(self, productos_df, prod_config):
        """Normaliza el catálogo de productos al formato requerido por TSOL."""
        columnas_cfg = prod_config.get('columnas', {})
        col_codigo = columnas_cfg.get('codigo', 'Codigo SAP')
        col_nombre = columnas_cfg.get('nombre', 'Nombre')
        col_barras = columnas_cfg.get('codigo_barras', 'Codigo de barras')
        col_proveedor = columnas_cfg.get('proveedor', 'Proveedor')
        col_proveedor2 = columnas_cfg.get('proveedor2', 'PROVEE 2')
        col_categoria = columnas_cfg.get('categoria', 'Categoría')
        col_tipo_producto = columnas_cfg.get('tipo_producto', 'Tipo Prod')
        col_contenido = columnas_cfg.get('contenido', 'Contenido')

        columnas_base = [col for col in [col_codigo, col_nombre, col_barras, col_proveedor] if col in productos_df.columns]
        if not columnas_base:
            raise KeyError('El catálogo de productos no contiene las columnas mínimas requeridas.')

        columnas_a_usar = columnas_base.copy()
        for extra in [col_proveedor2, col_categoria, col_tipo_producto, col_contenido]:
            if extra in productos_df.columns and extra not in columnas_a_usar:
                columnas_a_usar.append(extra)

        productos_final = productos_df[columnas_a_usar].copy()

        rename_dict = {
            col_codigo: 'Código',
            col_nombre: 'Nombre'
        }
        if col_barras in productos_final.columns:
            rename_dict[col_barras] = 'Código De Barras'
        if col_proveedor in productos_final.columns:
            rename_dict[col_proveedor] = 'Proveedor'
        if col_proveedor2 in productos_final.columns:
            rename_dict[col_proveedor2] = 'PROVEE 2'
        if col_categoria in productos_final.columns:
            rename_dict[col_categoria] = 'temp_categoria'
        if col_tipo_producto in productos_final.columns:
            rename_dict[col_tipo_producto] = 'temp_tipo_producto'
        if col_contenido in productos_final.columns:
            rename_dict[col_contenido] = 'Factor Conversion Unidad'

        productos_final = productos_final.rename(columns=rename_dict)

        productos_final['Código'] = productos_final['Código'].apply(self._formatear_codigo_producto)
        if 'Código De Barras' in productos_final.columns:
            productos_final['Código De Barras'] = productos_final['Código De Barras'].astype(str).str.strip()
        else:
            productos_final['Código De Barras'] = ''

        productos_final['Proveedor'] = productos_final['Proveedor'].astype(str).str.strip() if 'Proveedor' in productos_final.columns else 'TM - LO NUESTRO'
        productos_final['Tipo Referencia'] = 'RG'
        productos_final['Tipo De Unidad'] = 'UND'

        if 'temp_categoria' in productos_final.columns:
            productos_final['Código Categoría'] = productos_final['temp_categoria'].astype(str).str.strip()
            productos_final['Nombre Categoría'] = productos_final['temp_categoria'].astype(str).str.strip()
            productos_final = productos_final.drop('temp_categoria', axis=1)
        else:
            productos_final['Código Categoría'] = '001'
            productos_final['Nombre Categoría'] = 'GENERAL'

        if 'temp_tipo_producto' in productos_final.columns:
            productos_final['Código SubCategoría'] = productos_final['temp_tipo_producto'].astype(str).str.strip()
            productos_final['Nombre SubCategoría'] = productos_final['temp_tipo_producto'].astype(str).str.strip()
            productos_final = productos_final.drop('temp_tipo_producto', axis=1)
        else:
            productos_final['Código SubCategoría'] = '001'
            productos_final['Nombre SubCategoría'] = 'GENERAL'

        if 'Factor Conversion Unidad' in productos_final.columns:
            productos_final['Factor Conversion Unidad'] = pd.to_numeric(productos_final['Factor Conversion Unidad'], errors='coerce').fillna(1)
        else:
            productos_final['Factor Conversion Unidad'] = 1

        productos_final['Factor Peso'] = 1

        if 'PROVEE 2' in productos_final.columns:
            def asignar_proveedor(row):
                proveedor = str(row['Proveedor']).strip()
                if proveedor in ['0', '', 'nan', 'None']:
                    return 'TM - LO NUESTRO'
                if proveedor == 'TM - LO NUESTRO':
                    provee2 = str(row['PROVEE 2']).strip()
                    return provee2 if provee2 not in ['0', '', 'nan', 'None'] else 'TM - LO NUESTRO'
                return proveedor
            productos_final['Proveedor'] = productos_final.apply(asignar_proveedor, axis=1)
            productos_final = productos_final.drop('PROVEE 2', axis=1)
        else:
            productos_final['Proveedor'] = productos_final['Proveedor'].apply(
                lambda x: 'TM - LO NUESTRO' if str(x).strip() in ['0', '', 'nan', 'None'] else str(x).strip()
            )

        productos_final['Código Sede'] = self.DEFAULT_SEDE_CODE
        productos_final['Nombre Sede'] = self.DEFAULT_SEDE_NAME

        columnas_finales = [
            'Código', 'Nombre', 'Tipo Referencia', 'Tipo De Unidad', 'Código De Barras',
            'Código Categoría', 'Nombre Categoría', 'Código SubCategoría', 'Nombre SubCategoría',
            'Factor Conversion Unidad', 'Factor Peso', 'Código Sede', 'Nombre Sede', 'Proveedor'
        ]
        return productos_final[columnas_finales]

    @staticmethod
    def verificar_archivo(archivo):
        """Verifica que el archivo exista y sea accesible."""
        if not os.path.isfile(archivo):
            logger.error(f"Archivo no encontrado: {archivo}")
            raise FileNotFoundError(f"Archivo no encontrado: {archivo}")
        logger.info(f"Archivo encontrado: {archivo}")
        return archivo

    def cargar_y_filtrar_datos_por_periodo(self):
        """Carga los datos y filtra por el período especificado y proveedores."""
        self.verificar_archivo(self.ventas_path)

        try:
            # Cargar todos los datos primero para determinar la fecha más reciente
            dtype_map = {
                'Cod. cliente': str,
                'Cod. vendedor': str,
                'Cod. productto': str,
                'Fac. numero': str,
                'Pedido': str,
                'Bodega': str,
                'Proveedor': str,
                'Empresa': str,
                'Unidad': str,
                'Tipo': str
            }
            all_data = pd.read_excel(
                self.ventas_path,
                sheet_name='infoventas',
                dtype=dtype_map
            )
            
            # Convertir explícitamente la columna Fecha a datetime
            all_data['Fecha'] = pd.to_datetime(all_data['Fecha'], errors='coerce')
            
            # FILTRO CRÍTICO: Solo datos de Distrijass (excluir Eje y Elite)
            if 'Empresa' in all_data.columns:
                all_data = all_data[all_data['Empresa'] == 'Distrijass']
                logger.info(f"Datos filtrados por empresa: Solo Distrijass. Registros resultantes: {len(all_data):,}")
            else:
                logger.warning("Columna 'Empresa' no encontrada. Procesando todos los datos.")
            
            # Encontrar la fecha más reciente en los datos
            if all_data.empty or 'Fecha' not in all_data.columns:
                logger.error("No se encontraron datos o la columna 'Fecha' no existe")
                raise ValueError("No se encontraron datos válidos en el archivo Excel")
            
            # Convertir a datetime antes de trabajar con la columna
            fecha_serie = pd.to_datetime(all_data['Fecha'], errors='coerce')
            fecha_maxima = fecha_serie.max()
            self.mes = fecha_maxima.month
            self.ano = fecha_maxima.year
            
            logger.info(f"Fecha más reciente encontrada: {fecha_maxima}")
            logger.info(f"Mes y año determinados: Mes {self.mes}, Año {self.ano}")
            
            # Ahora filtrar por el período determinado
            # Asegurar que la columna Fecha esté en formato datetime
            all_data['Fecha'] = fecha_serie
            period_data = all_data[
                (fecha_serie.dt.month == self.mes) &
                (fecha_serie.dt.year == self.ano)
            ]
            self.period_data = period_data.copy()
            self.filtered_data = period_data.copy()
            logger.info(f"Datos filtrados por período: Mes {self.mes}, Año {self.ano}.")

            # Filtrar por proveedores si están definidos
            if self.proveedores:
                regex_pattern = '|'.join([re.escape(proveedor) for proveedor in self.proveedores])
                self.filtered_data = self.filtered_data[self.filtered_data['Proveedor'].str.contains(regex_pattern, case=False, na=False)]
                logger.info(f"Datos filtrados por proveedores: {self.proveedores}")
            else:
                logger.warning("No se especificaron proveedores para filtrar.")
        except Exception as e:
            logger.error(f"Error al cargar y filtrar los datos: {e}")
            raise

    def procesar_datos(self):
        """Procesa los datos para preparar los campos necesarios según las especificaciones."""
        if self.filtered_data is None:
            raise ValueError("Los datos no están cargados o filtrados. Ejecute 'cargar_y_filtrar_datos_por_periodo' primero.")

        try:
            columnas_requeridas = [
                'Cod. cliente', 'Cod. vendedor', 'Cod. productto',
                'Fecha', 'Fac. numero', 'Cantidad', 'Vta neta',
                'Tipo', 'Costo', 'Unidad', 'Pedido', 'Bodega'
            ]

            # Validar columnas requeridas
            for columna in columnas_requeridas:
                if columna not in self.filtered_data.columns:
                    logger.error(f"Columna requerida no encontrada: {columna}")
                    raise KeyError(f"Columna requerida no encontrada: {columna}")

            # Filtrar y renombrar columnas
            self.filtered_data = self.filtered_data[columnas_requeridas].rename(columns={
                'Cod. cliente': 'Código Cliente',
                'Cod. vendedor': 'Código Vendedor',
                'Cod. productto': 'Código Producto (Sku)',
                'Fecha': 'Fecha',
                'Fac. numero': 'Numero Documento',
                'Cantidad': 'Cantidad',
                'Vta neta': 'Valor Total Item Vendido',
                'Tipo': 'Tipo',
                'Costo': 'Costo',
                'Unidad': 'Unidad de Medida',
                'Pedido': 'Numero Único de Pedido',
                'Bodega': 'Codigo bodega'
            })

            # Aplicar mapeo de bodega/código de sede centralizado
            self.filtered_data = self._filtrar_sedes_permitidas(self.filtered_data, 'Codigo bodega', 'Ventas')
            self.filtered_data['Codigo bodega'] = self.filtered_data['Codigo bodega'].astype(str)
            self.filtered_data['Nombre Sede'] = self.filtered_data['Codigo bodega'].apply(self._obtener_nombre_sede)
            
            logger.info("Aplicado mapeo de nombres de bodega a códigos de sede y nombres de sede")

            # Convertir tipos y ajustar formato
            self.filtered_data['Código Vendedor'] = self.filtered_data['Código Vendedor'].apply(
                self._normalizar_codigo_vendedor
            )
            self.filtered_data['Código Producto (Sku)'] = self.filtered_data['Código Producto (Sku)'].apply(
                lambda x: self._formatear_codigo_producto(x).upper()
            )
            # Mantener Fecha como datetime para procesamiento posterior
            # self.filtered_data['Fecha'] = self.filtered_data['Fecha'].dt.strftime('%Y/%m/%d')
            self.filtered_data['Numero Documento'] = self.filtered_data['Numero Documento'].astype(str)
            self.filtered_data['Tipo'] = self.filtered_data['Tipo'].astype(str)
            self.filtered_data['Cantidad'] = self.filtered_data['Cantidad'].astype(int)
            self.filtered_data['Valor Total Item Vendido'] = pd.to_numeric(self.filtered_data['Valor Total Item Vendido'], errors='coerce').round(2)
            self.filtered_data['Costo'] = pd.to_numeric(self.filtered_data['Costo'], errors='coerce').round(2)
            # Reemplazar guiones en Código Cliente con "999"
            self.filtered_data['Código Cliente'] = self.filtered_data['Código Cliente'].apply(
                lambda x: str(x).replace('-', '999')
            )
            self.filtered_data_total = self.filtered_data.copy()
            self.filtered_data_total['Código Vendedor'] = self.filtered_data_total['Código Vendedor'].apply(
                self._normalizar_codigo_vendedor
            )
            # Limpieza de la columna 'Código Cliente'
            self.filtered_data_total['Código Cliente'] = (
                self.filtered_data_total['Código Cliente']
                .astype(str)
                .str.strip()
                .str.replace('-', '999')
                .str.replace('"', '')
                .str.replace("'", '')
            )


            # Limpieza de la columna 'Código Producto (Sku)'
            self.filtered_data_total['Código Producto (Sku)'] = self.filtered_data_total['Código Producto (Sku)'].apply(
                lambda x: self._formatear_codigo_producto(x).upper()
            )
            # Alternativa: Multiplicar por -1 para garantizar que los valores sean positivos cuando Tipo == 1
            mask = self.filtered_data['Tipo'] == '1'
            self.filtered_data.loc[mask, 'Cantidad'] = self.filtered_data.loc[mask, 'Cantidad'].apply(lambda x: x * -1 if x < 0 else x)
            self.filtered_data.loc[mask, 'Valor Total Item Vendido'] = self.filtered_data.loc[mask, 'Valor Total Item Vendido'].apply(lambda x: x * -1 if x < 0 else x)
            self.filtered_data.loc[mask, 'Costo'] = self.filtered_data.loc[mask, 'Costo'].apply(lambda x: x * -1 if x < 0 else x)
            

            logger.info("Datos procesados exitosamente.")
        except Exception as e:
            logger.error(f"Error al procesar los datos: {e}")
            raise

    def guardar_archivo_ventas(self):
        """Guarda los datos procesados en archivos delimitados por '{' y en formato Excel."""
        if self.filtered_data is None:
            raise ValueError("Los datos no están procesados. Ejecute 'procesar_datos' primero.")

        try:
            # Ruta para el archivo TXT
            output_path_txt = os.path.join(self.output_folder, 'ventas.txt')
            # Ruta para el archivo Excel
            output_path_excel = os.path.join(self.output_folder, 'ventas.xlsx')

            # Guardar el archivo TXT
            txt_data = self.filtered_data.copy()
            # Convertir Fecha a string solo para el archivo de salida
            txt_data['Fecha'] = pd.to_datetime(txt_data['Fecha']).dt.strftime('%Y/%m/%d')
            txt_data['Valor Total Item Vendido'] = txt_data['Valor Total Item Vendido'].map(
                lambda x: f"{x:.2f}".replace('.', ',')
            )
            txt_data['Costo'] = txt_data['Costo'].map(
                lambda x: f"{x:.2f}".replace('.', ',')
            )
            txt_columns = [
                'Código Cliente', 'Código Vendedor', 'Código Producto (Sku)',
                'Fecha', 'Numero Documento', 'Cantidad',
                'Valor Total Item Vendido', 'Tipo', 'Costo', 'Unidad de Medida', 'Codigo bodega', 'Nombre Sede'
            ]
            encabezado = '{'.join(txt_columns)
            with open(output_path_txt, 'w', encoding='utf-8') as file:
                file.write(encabezado + '\n')
                for _, row in txt_data[txt_columns].iterrows():
                    file.write('{'.join(row.astype(str)) + '\n')
            logger.info(f"Archivo TXT guardado exitosamente en: {output_path_txt}")

            # Guardar el archivo Excel
            # excel_data = self.filtered_data.copy()
            # excel_data.to_excel(output_path_excel, index=False, sheet_name='Ventas', engine='openpyxl')
            # logger.info(f"Archivo Excel guardado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al guardar los archivos: {e}")
            raise
        
        
    def generar_listado_facturas(self):
        """Genera el archivo 'Listado de Facturas' en formato TXT y Excel."""
        if self.filtered_data_total is None:
            raise ValueError("Los datos no están procesados. Ejecute 'cargar_y_filtrar_datos_por_periodo' y 'procesar_datos' primero.")

        try:
            # Validar columnas necesarias
            required_columns = ['Código Cliente', 'Código Vendedor', 'Fecha', 'Numero Documento', 'Valor Total Item Vendido', 'Costo']
            missing_columns = [col for col in required_columns if col not in self.filtered_data_total.columns]
            if missing_columns:
                raise KeyError(f"Las siguientes columnas están ausentes: {', '.join(missing_columns)}")

            # Agrupar datos por las columnas requeridas
            facturas_resumen = self.filtered_data_total.groupby(
                ['Código Cliente', 'Código Vendedor', 'Fecha', 'Numero Documento']
            ).agg(
                Valor_Total_Factura=('Valor Total Item Vendido', 'sum'),
                Valor_Facturado_Casa_Comercial = ('Valor Total Item Vendido', 'sum')
            ).reset_index()
            
            # Convertir Fecha a string
            facturas_resumen['Fecha'] = pd.to_datetime(facturas_resumen['Fecha']).dt.strftime('%Y/%m/%d')

            # Convertir valores a formato con dos decimales
            facturas_resumen['Valor_Total_Factura'] = facturas_resumen['Valor_Total_Factura'].round(2)
            facturas_resumen['Valor_Facturado_Casa_Comercial'] = facturas_resumen['Valor_Facturado_Casa_Comercial'].round(2)

            # Ruta para los archivos de salida
            output_txt = os.path.join(self.output_folder, 'Listado de Facturas.txt')
            output_excel = os.path.join(self.output_folder, 'Listado de Facturas.xlsx')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(facturas_resumen.columns)
            with open(output_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in facturas_resumen.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_txt}")

            # Guardar archivo Excel
            # facturas_resumen.to_excel(output_excel, index=False, sheet_name='Listado Facturas', engine='openpyxl')
            # logger.info(f"Archivo Excel generado: {output_excel}")

        except Exception as e:
            logger.error(f"Error al generar el listado de facturas: {e}")
            raise

    def generar_totales_de_control(self):
        """Genera el archivo 'Totales de Control' en formato TXT y Excel."""
        if self.filtered_data is None:
            raise ValueError("Los datos no están procesados. Ejecute 'procesar_datos' primero.")

        try:
            # Calcular el total correctamente: Ventas (Tipo 0) - Devoluciones (Tipo 1)
            ventas_tipo_0 = self.filtered_data[self.filtered_data['Tipo'] == '0']['Valor Total Item Vendido'].sum()
            devoluciones_tipo_1 = self.filtered_data[self.filtered_data['Tipo'] == '1']['Valor Total Item Vendido'].sum()
            
            # Total neto = Ventas - Devoluciones
            total_valor_venta_neto = ventas_tipo_0 - devoluciones_tipo_1
            
            logger.info(f"Cálculo de totales: Ventas (Tipo 0): ${ventas_tipo_0:,.2f}, Devoluciones (Tipo 1): ${devoluciones_tipo_1:,.2f}, Total Neto: ${total_valor_venta_neto:,.2f}")

            # Crear el DataFrame con los resultados
            totales_control = pd.DataFrame({
                'Descriptor Total': ['TotalValorVenta'],
                'Valor': [round(total_valor_venta_neto, 2)]
            })

            # Ruta para los archivos de salida
            output_txt = os.path.join(self.output_folder, 'Totales de Control.txt')
            output_excel = os.path.join(self.output_folder, 'Totales de Control.xlsx')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(totales_control.columns)
            with open(output_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in totales_control.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_txt}")

            # Guardar archivo Excel
            # totales_control.to_excel(output_excel, index=False, sheet_name='Totales de Control', engine='openpyxl')
            # logger.info(f"Archivo Excel generado: {output_excel}")

        except Exception as e:
            logger.error(f"Error al generar los totales de control: {e}")
            raise

    def generar_vendedores(self):
        """Genera el archivo 'Vendedores' cruzando los datos de ventas con interasesor.txt."""
        try:
            # Ruta del archivo interasesor desde config
            interasesor_path = self.company_config['paths']['interasesor']
            
            # Verificar que el archivo exista
            self.verificar_archivo(interasesor_path)

            # Cargar los datos de interasesor.txt (delimitador variable)
            interasesor_data = self._leer_txt_delimitado(
                interasesor_path,
                nombres=["Codigo", "Documento", "Nombre", "Apellido", "Telefono", "Direccion",
                         "Cargo", "Portafolio", "Estado", "Codigo supervisor", "Codigo bodega"],
                encoding='latin1'
            )

            interasesor_data['Codigo'] = (
                interasesor_data['Codigo']
                .astype(str)
                .str.strip()
                .str.replace('.0', '', regex=False)
            )

            interasesor_data = self._autocompletar_vendedores(interasesor_data)

            # Filtrar solo los vendedores activos
            interasesor_data = interasesor_data[interasesor_data['Estado'].str.contains("Activado", na=False)]

            # Cruzar con los vendedores que tienen ventas
            vendedores_con_venta = interasesor_data[interasesor_data['Codigo'].isin(self.filtered_data_total['Código Vendedor'])]

            # Seleccionar y renombrar columnas requeridas (incluyendo campos de sede)
            vendedores_final = vendedores_con_venta[
                ['Codigo', 'Nombre', 'Direccion', 'Documento', 'Codigo supervisor', 'Codigo bodega']
            ].rename(columns={
                'Codigo': 'Código',
                'Nombre': 'Nombre',
                'Direccion': 'Ubicación',
                'Documento': 'Cédula',
                'Codigo supervisor': 'Código Supervisor',
                'Codigo bodega': 'Código Sede'
            })
            
            # Agregar el campo "Nombre Sede" basado en el código de bodega/sede
            vendedores_final = self._filtrar_sedes_permitidas(vendedores_final, 'Código Sede', 'Vendedores')
            vendedores_final['Nombre Sede'] = vendedores_final['Código Sede'].apply(self._obtener_nombre_sede)

            # Ordenar por Código
            self.vendedores_final = vendedores_final.sort_values(by='Código')

            # Ruta para guardar el archivo de excel
            output_path = os.path.join(self.output_folder, 'Vendedores.xlsx')
            
            # Ruta para guardar el archivo txt
            output_txt = os.path.join(self.output_folder, 'Vendedores.txt')
            
            # Guardar archivo TXT
            encabezado_txt = '{'.join(self.vendedores_final.columns)
            with open(output_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in self.vendedores_final.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_txt}")

            # Guardar archivo Excel
            # self.vendedores_final.to_excel(output_path, index=False, sheet_name='Vendedores', engine='openpyxl')
            # logger.info(f"Archivo 'Vendedores' generado exitosamente en: {output_path}")

        except Exception as e:
            logger.error(f"Error al generar el archivo 'Vendedores': {e}")
            raise

    def generar_supervisores(self):
        """Genera los archivos 'Supervisores.txt' y 'Supervisores.xlsx' cruzando los datos con intersupervisor.txt."""
        try:
            # Ruta del archivo intersupervisor desde config
            intersupervisor_path = self.company_config['paths']['intersupervisor']
            
            # Verificar que el archivo exista
            self.verificar_archivo(intersupervisor_path)

            # Cargar los datos de intersupervisor.txt (delimitador variable)
            intersupervisor_data = self._leer_txt_delimitado(
                intersupervisor_path,
                nombres=["Codigo", "Documento", "Nombre", "Apellido", "Telefono", "Direccion",
                         "Cargo", "Portafolio", "Estado", "Codigo bodega"],
                encoding='latin1'
            )

            # Obtener los códigos de supervisor del archivo de vendedores
            supervisores_codigo = self.vendedores_final['Código Supervisor'].unique()

            # Filtrar supervisores: activos o referenciados por vendedores
            if 'Estado' in intersupervisor_data.columns:
                intersupervisor_data = intersupervisor_data[
                    intersupervisor_data['Estado'].str.contains("Activado", na=False)
                    | intersupervisor_data['Codigo'].isin(supervisores_codigo)
                ]

            intersupervisor_data = self._autocompletar_supervisores(intersupervisor_data, supervisores_codigo)

            # Filtrar los supervisores en base a los códigos de supervisor
            supervisores_final = intersupervisor_data[intersupervisor_data['Codigo'].isin(supervisores_codigo)]

            # Seleccionar y renombrar columnas requeridas (incluyendo campos de sede)
            supervisores_final = supervisores_final[['Codigo', 'Nombre', 'Codigo bodega']].rename(columns={
                'Codigo': 'Código',
                'Nombre': 'Nombre',
                'Codigo bodega': 'Código Sede'
            })
            
            # Agregar el campo "Nombre Sede" basado en el código de bodega/sede
            supervisores_final = self._filtrar_sedes_permitidas(supervisores_final, 'Código Sede', 'Supervisores')
            supervisores_final['Nombre Sede'] = supervisores_final['Código Sede'].apply(self._obtener_nombre_sede)

            # Ordenar supervisores por 'Código'
            supervisores_final = supervisores_final.sort_values(by='Código')

            self.supervisores_final = supervisores_final.copy()

            # Ruta para guardar el archivo de Excel
            output_path_excel = os.path.join(self.output_folder, 'Supervisores.xlsx')
            
            # Ruta para guardar el archivo TXT
            output_path_txt = os.path.join(self.output_folder, 'Supervisores.txt')
            
            # Guardar archivo TXT
            encabezado_txt = '{'.join(supervisores_final.columns)
            with open(output_path_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in supervisores_final.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_path_txt}")

            # Guardar archivo Excel
            # supervisores_final.to_excel(output_path_excel, index=False, sheet_name='Supervisores', engine='openpyxl')
            # logger.info(f"Archivo 'Supervisores' generado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al generar el archivo 'Supervisores': {e}")
            raise

    def _normalizar_texto(self, texto):
        """Normaliza texto eliminando tildes y caracteres especiales para matching."""
        if pd.isna(texto):
            return ""
        texto = str(texto).strip()
        # Eliminar tildes
        texto = texto.replace('á', 'a').replace('é', 'e').replace('í', 'i')
        texto = texto.replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')
        texto = texto.replace('Á', 'A').replace('É', 'E').replace('Í', 'I')
        texto = texto.replace('Ó', 'O').replace('Ú', 'U').replace('Ñ', 'N')
        return texto

    def cargar_tipologia_negocio(self):
        """Carga la tipología de negocio desde PROVEE-TSOL.xlsx hoja TIPOLOGIA."""
        try:
            self.verificar_archivo(self.catalogo_principal)
            
            # Configuración de tipología desde config
            tip_config = self.company_config.get('tipologia_negocio', {})
            hoja = tip_config.get('hoja_excel', 'TIPOLOGIA')
            col_codigo = tip_config.get('columnas', {}).get('codigo', 'Cod. necesidad')
            col_descripcion = tip_config.get('columnas', {}).get('descripcion', 'Nom. necesidad')
            
            # Leer hoja TIPOLOGIA
            tipologia_df = pd.read_excel(self.catalogo_principal, sheet_name=hoja)
            
            # Normalizar códigos para matching (eliminar tildes)
            tipologia_df[col_codigo] = tipologia_df[col_codigo].apply(self._normalizar_texto)
            
            # Crear diccionario de tipología: código normalizado -> código original
            self.tipologia_map = dict(zip(
                tipologia_df[col_codigo],
                tipologia_df[col_codigo]
            ))
            
            logger.info(f"Tipología cargada: {len(self.tipologia_map)} registros desde {hoja}")
            return tipologia_df[[col_codigo, col_descripcion]]
            
        except Exception as e:
            logger.error(f"Error al cargar tipología de negocio: {e}")
            raise

    def generar_tipos_de_negocio(self):
        """Genera los archivos 'Tipos De Negocio.txt' desde PROVEE-TSOL TIPOLOGIA."""
        try:
            # Cargar tipología desde PROVEE-TSOL
            tip_config = self.company_config.get('tipologia_negocio', {})
            col_codigo = tip_config.get('columnas', {}).get('codigo', 'Cod. necesidad')
            col_descripcion = tip_config.get('columnas', {}).get('descripcion', 'Nom. necesidad')
            
            tipologia_df = self.cargar_tipologia_negocio()
            
            # Renombrar columnas para salida
            tipos_negocio = tipologia_df.rename(columns={
                col_codigo: 'Código',
                col_descripcion: 'Nombre'
            })

            # Ruta para guardar el archivo Excel
            output_path_excel = os.path.join(self.output_folder, 'Tipos De Negocio.xlsx')
            
            # Ruta para guardar el archivo TXT
            output_path_txt = os.path.join(self.output_folder, 'Tipos De Negocio.txt')
            
            # Guardar archivo TXT
            encabezado_txt = '{'.join(tipos_negocio.columns)
            with open(output_path_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in tipos_negocio.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_path_txt}")

            # Guardar archivo Excel
            # tipos_negocio.to_excel(output_path_excel, index=False, sheet_name='Tipos De Negocio', engine='openpyxl')
            # logger.info(f"Archivo 'Tipos De Negocio' generado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al generar los archivos 'Tipos De Negocio': {e}")
            raise

    def generar_sku_productos(self):
        """Genera los archivos 'SKU (Productos).txt' filtrando desde PROVEE-TSOL por proveedores."""
        try:
            self.verificar_archivo(self.catalogo_principal)
            
            # Configuración de productos desde config
            prod_config = self.company_config.get('filtros_productos', {})
            hoja = prod_config.get('hoja_excel', 'PRODUCTO')
            columnas_cfg = prod_config.get('columnas', {})
            col_proveedor = columnas_cfg.get('proveedor', 'Proveedor')

            catalogo_completo = pd.read_excel(self.catalogo_principal, sheet_name=hoja)
            productos_filtrados = catalogo_completo.copy()

            if self.proveedores and col_proveedor in productos_filtrados.columns:
                regex_pattern = '|'.join([re.escape(proveedor) for proveedor in self.proveedores])
                productos_filtrados = productos_filtrados[
                    productos_filtrados[col_proveedor].astype(str).str.contains(regex_pattern, case=False, na=False)
                ]
                logger.info(f"Productos filtrados por proveedores: {len(productos_filtrados)} registros")
            elif self.proveedores:
                logger.warning('No se encontró la columna de proveedor en el catálogo. Se omite el filtro por proveedores.')

            productos_final = self._transformar_catalogo_productos(productos_filtrados, prod_config)

            # Asegurar que todos los SKU presentes en ventas existan en la maestra
            if hasattr(self, 'filtered_data_total'):
                skus_ventas = {
                    self._formatear_codigo_producto(codigo)
                    for codigo in self.filtered_data_total['Código Producto (Sku)']
                    if pd.notna(codigo)
                }
                skus_maestra = set(productos_final['Código'])
                faltantes = sorted(sku for sku in skus_ventas - skus_maestra if sku)
                if faltantes:
                    col_codigo = columnas_cfg.get('codigo', 'Codigo SAP')
                    catalogo_faltantes = catalogo_completo[
                        catalogo_completo[col_codigo].apply(self._formatear_codigo_producto).isin(faltantes)
                    ] if col_codigo in catalogo_completo.columns else pd.DataFrame()

                    if not catalogo_faltantes.empty:
                        productos_completados = self._transformar_catalogo_productos(catalogo_faltantes, prod_config)
                        productos_final = pd.concat([productos_final, productos_completados], ignore_index=True)
                        productos_final = productos_final.sort_values('Código').drop_duplicates('Código').reset_index(drop=True)
                        self._registrar_inconsistencia(
                            'SKU',
                            f'Se autocompletan {len(productos_completados)} SKU faltantes desde PROVEE-TSOL.',
                            detalle=', '.join(faltantes[:20]),
                            accion='Agregar registro automático'
                        )
                    else:
                        self._registrar_inconsistencia(
                            'SKU',
                            'Hay SKU presentes en ventas que no existen en PROVEE-TSOL. Revisar proveedores.',
                            detalle=', '.join(faltantes[:20]),
                            severidad='error'
                        )

            self.sku_maestra = productos_final

            # Ruta para guardar el archivo Excel
            output_path_excel = os.path.join(self.output_folder, 'SKU (Productos).xlsx')
            
            # Ruta para guardar el archivo TXT
            output_path_txt = os.path.join(self.output_folder, 'SKU (Productos).txt')
            
            # Guardar archivo TXT
            encabezado_txt = '{'.join(productos_final.columns)
            with open(output_path_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in productos_final.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_path_txt}")

            # Guardar archivo Excel
            # productos_final.to_excel(output_path_excel, index=False, sheet_name='SKU (Productos)', engine='openpyxl')
            # logger.info(f"Archivo 'SKU (Productos)' generado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al generar los archivos 'SKU (Productos)': {e}")
            raise


    def generar_clientes(self):
        """Genera los archivos 'Clientes.txt' cruzando datos con intercliente.txt y PROVEE-TSOL TIPOLOGIA."""
        try:
            # Rutas de los archivos desde config
            intercliente_path = self.company_config['paths']['intercliente']
            
            # Verificar archivos
            self.verificar_archivo(intercliente_path)
            self.verificar_archivo(self.catalogo_principal)

            # Cargar datos con delimitador variable
            intercliente_data = self._leer_txt_delimitado(
                intercliente_path,
                nombres=["Cod. Cliente", "Nom. Cliente", "Fecha Ingreso", "Nit", "Direccion",
                         "Telefono", "Representante Legal", "Codigo Municipio",
                         "Codigo Negocio", "Tipo Negocio", "Estracto", "Barrio"],
                encoding='Windows-1252'
            )

            # Limpieza básica de comillas en columnas de texto
            for columna in intercliente_data.columns:
                intercliente_data[columna] = (
                    intercliente_data[columna]
                    .astype(str)
                    .str.strip()
                    .str.strip('"')
                    .str.strip('“')
                    .str.strip('”')
                    .str.strip("'")
                    .str.strip('`')
                    .str.replace(r'^"|"$', '', regex=True)
                )

            # Renombrar y limpiar columnas
            intercliente_data.rename(columns={
                "Cod. Cliente": "Código",
                "Nom. Cliente": "Nombre",
                "Direccion": "Dirección",
                "Estracto": "Estrato",
                "Codigo Municipio": "Código Municipio",
                "Telefono": "Teléfono"
            }, inplace=True)

            # Normalizar códigos
            intercliente_data['Código'] = (
                intercliente_data['Código']
                .astype(str)
                .str.strip()
                .str.replace('-', '999')
            )
            # Normalizar códigos
            intercliente_data['Código'] = (
                intercliente_data['Código']
                .astype(str)
                .str.strip()
                .str.replace('-', '999')
                .str.replace('"', '')
            )

            # Cargar tipología desde PROVEE-TSOL
            self.cargar_tipologia_negocio()

            # Normalizar códigos de clientes únicos del DataFrame de ventas
            clientes_unicos = (
                self.filtered_data_total['Código Cliente']
                .astype(str)
                .str.strip()
                .str.replace('-', '999')
                .unique()
            )

            intercliente_data = self._autocompletar_clientes(intercliente_data, clientes_unicos)

            # Filtrar clientes presentes en intercliente.txt
            intercliente_data['Código'] = intercliente_data['Código'].str.strip()
            clientes_final = intercliente_data[intercliente_data['Código'].isin(clientes_unicos)].copy()

            # Usar directamente la columna 'Tipo Negocio' como 'Código Tipo Negocio'
            clientes_final['Código Tipo Negocio'] = clientes_final['Tipo Negocio'].astype(str).str.strip()

            # Agregar campos obligatorios según especificaciones TSOL
            clientes_final['Código Sede'] = '01'  # Sede principal por defecto
            clientes_final['Nombre Sede'] = 'PALMIRA/CALI'  # Sede principal por defecto
            clientes_final['Ubicación longitud'] = -76.300000  # Coordenada aproximada Cali
            clientes_final['Ubicación latitud'] = 3.450000   # Coordenada aproximada Cali
            clientes_final['Identificador de sucursal'] = '001'  # Sucursal principal por defecto

            # Seleccionar y ordenar las columnas según especificaciones TSOL (15 campos)
            columnas_finales = [
                'Código', 'Nombre', 'Fecha Ingreso', 'Nit', 'Dirección', 'Teléfono',
                'Representante Legal', 'Código Municipio', 'Código Tipo Negocio',
                'Estrato', 'Código Sede', 'Nombre Sede', 'Ubicación longitud', 
                'Ubicación latitud', 'Identificador de sucursal'
            ]
            clientes_final = clientes_final[columnas_finales]

            self.clientes_final = clientes_final.copy()

            # Rutas para los archivos de salida
            output_path_txt = os.path.join(self.output_folder, 'Clientes.txt')
            output_path_excel = os.path.join(self.output_folder, 'Clientes.xlsx')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(columnas_finales)
            with open(output_path_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in clientes_final.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_path_txt}")

            # Guardar archivo Excel
            # clientes_final.to_excel(output_path_excel, index=False, sheet_name='Clientes', engine='openpyxl')
            # logger.info(f"Archivo 'Clientes' generado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al generar los archivos de clientes: {e}")
            raise




    def generar_inventario(self):
        """Genera los archivos 'Inventario.txt' filtrando por productos de la maestra SKU y proveedores."""
        try:
            # Ruta del archivo Consolidado.xlsx desde config
            inventario_path = self._obtener_ruta_archivo('inventario')
            
            # Verificar que el archivo exista
            self.verificar_archivo(inventario_path)

            # Cargar los datos del archivo de inventario
            inventario_data = pd.read_excel(inventario_path, sheet_name='Informe')

            # Filtrar por proveedores definidos
            if not self.proveedores:
                raise ValueError("No se encontraron proveedores para filtrar el inventario.")

            regex_pattern = '|'.join([re.escape(proveedor) for proveedor in self.proveedores])
            inventario_data = inventario_data[inventario_data['Proveedor'].str.contains(regex_pattern, case=False, na=False)]

            # Normalizar los códigos en inventario
            inventario_data['Codigo articulo'] = inventario_data['Codigo articulo'].astype(str).str.strip().str.split('.').str[0]

            bodega_col = self._buscar_columna(inventario_data, ['Codigo bodega', 'Código bodega', 'Bodega', 'Nombre bodega'])
            if bodega_col:
                inventario_data['Codigo bodega'] = inventario_data[bodega_col].apply(self._normalizar_codigo_bodega)
            else:
                inventario_data['Codigo bodega'] = self.DEFAULT_SEDE_CODE

            # Filtrar los productos que están en la maestra SKU
            if hasattr(self, 'sku_maestra'):
                inventario_data = inventario_data[inventario_data['Codigo articulo'].isin(self.sku_maestra['Código'])]

            # Crear DataFrame con las columnas requeridas
            inventario_final = inventario_data[['Codigo articulo', 'Unidades', 'Codigo bodega']].rename(columns={
                'Codigo articulo': 'Código Producto',
                'Unidades': 'Cantidad',
                'Codigo bodega': 'Código de bodega'
            })
            
            # Agregar columnas obligatorias según especificaciones TSOL
            inventario_final['Fecha'] = datetime.now().strftime('%Y/%m/%d')  # Fecha actual
            inventario_final['Unidad de Medida'] = 'UND'
            inventario_final['Código de bodega'] = inventario_final['Código de bodega'].apply(self._normalizar_codigo_bodega)
            inventario_final = self._filtrar_sedes_permitidas(inventario_final, 'Código de bodega', 'Inventario')
            inventario_final['Código de bodega'] = inventario_final['Código de bodega'].astype(str).str.zfill(2)
            inventario_final['Código Sede'] = inventario_final['Código de bodega']
            inventario_final['Nombre Sede'] = inventario_final['Código Sede'].apply(self._obtener_nombre_sede)

            # Seleccionar el orden de columnas según especificaciones (7 campos)
            columnas_finales = [
                'Fecha', 'Código Producto', 'Cantidad', 'Unidad de Medida', 
                'Código de bodega', 'Código Sede', 'Nombre Sede'
            ]
            inventario_final = inventario_final[columnas_finales]

            # Agrupar por código de producto y sumar las cantidades
            inventario_final = inventario_final.groupby(
                ['Fecha', 'Código Producto', 'Unidad de Medida', 'Código de bodega', 'Código Sede', 'Nombre Sede'], 
                as_index=False
            ).agg({'Cantidad': 'sum'})
            
            # Reordenar columnas después del groupby
            inventario_final = inventario_final[columnas_finales]
            
            self.inventario_final = inventario_final.copy()

            # Rutas para guardar los archivos
            output_path_txt = os.path.join(self.output_folder, 'Inventario.txt')
            output_path_excel = os.path.join(self.output_folder, 'Inventario.xlsx')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(inventario_final.columns)
            with open(output_path_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in inventario_final.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_path_txt}")

            # Guardar archivo Excel
            # inventario_final.to_excel(output_path_excel, index=False, sheet_name='Inventario', engine='openpyxl')
            # logger.info(f"Archivo 'Inventario' generado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al generar los archivos de inventario: {e}")
            raise



    def generar_municipios(self):
        """Genera el archivo de Municipios en formato TXT."""
        try:
            # Ruta del archivo interciudad desde config
            interciudad_path = self.company_config['paths']['interciudad']
            
            # Verificar que el archivo exista
            self.verificar_archivo(interciudad_path)

            # Cargar los datos del archivo interciudad.txt (delimitador variable)
            interciudad_data = self._leer_txt_delimitado(
                interciudad_path,
                nombres=["Código", "Nombre"],
                encoding='latin1'
            )

            # Extraer los municipios únicos del DataFrame de clientes
            municipios_clientes = self.clientes_final['Código Municipio'].dropna().unique()

            # Filtrar los municipios en interciudad que aparecen en el DataFrame de clientes
            municipios_final = interciudad_data[interciudad_data['Código'].isin(municipios_clientes)].drop_duplicates()

            # Ruta para guardar el archivo de Excel
            output_excel = os.path.join(self.output_folder, 'Municipios.xlsx')

            # Ruta para guardar el archivo TXT
            output_txt = os.path.join(self.output_folder, 'Municipios.txt')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(municipios_final.columns)
            with open(output_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in municipios_final.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_txt}")

            # Guardar archivo Excel
            # municipios_final.to_excel(output_excel, index=False, sheet_name='Municipios', engine='openpyxl')
            # logger.info(f"Archivo Excel generado: {output_excel}")

        except Exception as e:
            logger.error(f"Error al generar el archivo 'Municipios': {e}")
            raise

    def generar_barrios(self):
        """Genera el archivo de Barrios en formato TXT y Excel."""
        try:
            # Asegurarse de que el DataFrame de clientes exista
            if not hasattr(self, 'clientes_final'):
                raise AttributeError("El DataFrame 'clientes_final' no está definido. Asegúrate de ejecutar el método correspondiente.")

            # Filtrar y crear los datos únicos de Barrios
            barrios_df = self.clientes_final[['Código Municipio', 'Barrio']].drop_duplicates()

            # Crear la columna de Código usando el mismo valor que Nombre
            barrios_df = barrios_df.rename(columns={'Barrio': 'Nombre'})
            barrios_df['Código'] = barrios_df['Nombre']

            # Ordenar por Código Municipio y Nombre
            barrios_df = barrios_df.sort_values(by=['Código Municipio', 'Nombre']).reset_index(drop=True)

            # Reorganizar las columnas
            barrios_df = barrios_df[['Código', 'Nombre', 'Código Municipio']]

            # Ruta para guardar el archivo de Excel
            output_excel = os.path.join(self.output_folder, 'Barrios.xlsx')

            # Ruta para guardar el archivo TXT
            output_txt = os.path.join(self.output_folder, 'Barrios.txt')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(barrios_df.columns)
            with open(output_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in barrios_df.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_txt}")

            # Guardar archivo Excel
            # barrios_df.to_excel(output_excel, index=False, sheet_name='Barrios', engine='openpyxl')
            # logger.info(f"Archivo Excel generado: {output_excel}")

        except Exception as e:
            logger.error(f"Error al generar el archivo 'Barrios': {e}")
            raise

    def generar_rutas(self):
        """Genera el archivo 'Rutas' cruzando datos de ventas con un archivo de rutas existente."""
        try:
            # Ruta del archivo rutero desde config
            rutas_path = self._obtener_ruta_archivo('rutero')
            
            # Verificar que el archivo exista
            self.verificar_archivo(rutas_path)
            
            # Cargar datos del archivo rutero
            rutas_df = pd.read_excel(rutas_path, sheet_name='Informe')

            # Asegurarse de que las columnas necesarias existan
            rutas_df = rutas_df.rename(columns={'Codigo': 'Código Cliente', 'Cod. Asesor': 'Código Vendedor'})
            ventas_rutas = self.filtered_data_total[['Código Cliente', 'Código Vendedor', 'Codigo bodega', 'Nombre Sede']].drop_duplicates()

            rutas_data = pd.merge(
                ventas_rutas,
                rutas_df[['Código Cliente', 'Código Vendedor']],
                on=['Código Cliente', 'Código Vendedor'],
                how='inner'
            ).drop_duplicates()

            rutas_data = self._filtrar_sedes_permitidas(rutas_data, 'Codigo bodega', 'Rutas')

            # Agregar columnas requeridas según especificaciones
            rutas_data['Mes'] = int(self.mes)
            rutas_data['Dia Semana'] = 1  # Lunes por defecto
            rutas_data['Frecuencia'] = 4  # Semanal por defecto
            
            rutas_data['Código Sede'] = rutas_data['Codigo bodega'].astype(str).str.zfill(2)
            rutas_data['Nombre Sede'] = rutas_data['Código Sede'].apply(self._obtener_nombre_sede)
            rutas_data['Identificador de sucursal'] = rutas_data['Código Sede']

            # Aplicar el reemplazo en el código del cliente
            rutas_data['Código Cliente'] = rutas_data['Código Cliente'].apply(lambda x: str(x).replace('-', '999'))
            
            # Reordenar columnas según especificaciones (Código Vendedor primero)
            columnas_finales = [
                'Código Vendedor', 'Código Cliente', 'Mes', 'Dia Semana', 
                'Frecuencia', 'Código Sede', 'Nombre Sede', 'Identificador de sucursal'
            ]
            rutas_data = rutas_data[columnas_finales]

            # Guardar archivos
            output_path_txt = os.path.join(self.output_folder, 'Rutas.txt')
            output_path_excel = os.path.join(self.output_folder, 'Rutas.xlsx')

            # Guardar archivo TXT
            encabezado_txt = '{'.join(rutas_data.columns)
            with open(output_path_txt, 'w', encoding='utf-8') as txt_file:
                txt_file.write(encabezado_txt + '\n')
                for _, row in rutas_data.iterrows():
                    txt_file.write('{'.join(map(str, row)) + '\n')
            logger.info(f"Archivo TXT generado: {output_path_txt}")

            # Guardar archivo Excel
            # rutas_data.to_excel(output_path_excel, index=False, sheet_name='Rutas', engine='openpyxl')
            # logger.info(f"Archivo 'Rutas' generado exitosamente en: {output_path_excel}")

        except Exception as e:
            logger.error(f"Error al generar el archivo 'Rutas': {e}")
            raise
        
    def validar_inconsistencias(self):
        """Valida las inconsistencias entre las maestras y genera un reporte."""
        try:
            inconsistencias = []

            # Validar códigos de clientes
            if hasattr(self, 'filtered_data_total') and hasattr(self, 'clientes_final'):
                clientes_ventas = set(self.filtered_data_total['Código Cliente'])
                clientes_maestra = set(self.clientes_final['Código'])
                clientes_faltantes = clientes_ventas - clientes_maestra
                if clientes_faltantes:
                    inconsistencias.append({
                        'Maestra': 'Clientes',
                        'Códigos faltantes': list(clientes_faltantes)
                    })
                    logger.warning(f"Códigos de clientes faltantes en la maestra: {clientes_faltantes}")
                    self._registrar_inconsistencia(
                        'Clientes',
                        f'Persisten {len(clientes_faltantes)} clientes sin maestro tras el autocompletado.',
                        detalle=', '.join(list(clientes_faltantes)[:20]),
                        severidad='error'
                    )

            # Validar códigos de productos (SKU)
            if hasattr(self, 'filtered_data_total'):
                if hasattr(self, 'inventario_final'):
                    productos_inventario = set(self.inventario_final['Código Producto'])
                    if hasattr(self, 'sku_maestra'):
                        productos_maestra = set(self.sku_maestra['Código'])
                        productos_faltantes = productos_inventario - productos_maestra
                        if productos_faltantes:
                            inconsistencias.append({
                                'Maestra': 'SKU',
                                'Códigos faltantes': list(productos_faltantes)
                            })
                            logger.warning(f"Códigos de productos faltantes en la maestra SKU: {productos_faltantes}")
                            self._registrar_inconsistencia(
                                'SKU',
                                f'Inventario con {len(productos_faltantes)} SKU sin definición en catálogo.',
                                detalle=', '.join(list(productos_faltantes)[:20]),
                                severidad='error'
                            )

            # Validar códigos de vendedor
            if hasattr(self, 'vendedores_final') and hasattr(self, 'filtered_data_total'):
                codigos_vendedores = set(self.filtered_data_total['Código Vendedor'])
                codigos_maestra_vend = set(self.vendedores_final['Código'])
                vendedores_faltantes = codigos_vendedores - codigos_maestra_vend
                if vendedores_faltantes:
                    inconsistencias.append({
                        'Maestra': 'Vendedores',
                        'Códigos faltantes': list(vendedores_faltantes)
                    })
                    self._registrar_inconsistencia(
                        'Vendedores',
                        f'{len(vendedores_faltantes)} vendedores del período no cuentan con maestro.',
                        detalle=', '.join(list(vendedores_faltantes)[:20]),
                        severidad='error'
                    )

            # Validar códigos de supervisor
            if hasattr(self, 'supervisores_final') and hasattr(self, 'vendedores_final'):
                codigos_super_requeridos = set(self.vendedores_final['Código Supervisor'])
                codigos_super_maestra = set(self.supervisores_final['Código'])
                supervisores_faltantes = codigos_super_requeridos - codigos_super_maestra
                if supervisores_faltantes:
                    inconsistencias.append({
                        'Maestra': 'Supervisores',
                        'Códigos faltantes': list(supervisores_faltantes)
                    })
                    self._registrar_inconsistencia(
                        'Supervisores',
                        f'{len(supervisores_faltantes)} supervisores asignados no existen en la maestra.',
                        detalle=', '.join(list(supervisores_faltantes)[:20]),
                        severidad='error'
                    )

            # Validar filas con costo mayor a la venta
            if hasattr(self, 'filtered_data_total'):
                costos_altos = self.filtered_data_total[
                    self.filtered_data_total['Costo'] > self.filtered_data_total['Valor Total Item Vendido']
                ]
                if not costos_altos.empty:
                    diferencia = (costos_altos['Costo'] - costos_altos['Valor Total Item Vendido']).sum()
                    inconsistencias.append({
                        'Maestra': 'Ventas (Costo)',
                        'Códigos faltantes': list(costos_altos['Código Producto (Sku)'].unique())
                    })
                    alerta_path = os.path.join(self.output_folder, 'alerta_costos_mayores.csv')
                    costos_altos.to_csv(alerta_path, index=False)
                    self._registrar_inconsistencia(
                        'Ventas',
                        f'Se detectan {len(costos_altos)} registros con costo mayor a la venta neta.',
                        detalle=f'Diferencia acumulada ${diferencia:,.2f}. Detalle en {alerta_path}',
                        severidad='warning',
                        accion='Prepacks/combos: revisar márgenes en infoventas si aplica'
                    )

            # Validar cobertura de bodegas
            if self.period_data is not None and not self.period_data.empty and hasattr(self, 'filtered_data_total'):
                if 'Bodega' in self.period_data.columns:
                    bodegas_reportadas = {
                        self._normalizar_codigo_bodega(bodega)
                        for bodega in self.period_data['Bodega'].dropna().unique()
                    }
                    bodegas_en_salidas = set(self.filtered_data_total['Codigo bodega'].unique())
                    bodegas_faltantes = bodegas_reportadas - bodegas_en_salidas
                    if bodegas_faltantes:
                        inconsistencias.append({
                            'Maestra': 'Bodegas',
                            'Códigos faltantes': list(bodegas_faltantes)
                        })
                        self._registrar_inconsistencia(
                            'Bodegas',
                            'No llegaron todas las bodegas presentes en infoventas a los archivos finales.',
                            detalle=', '.join(sorted(bodegas_faltantes)),
                            accion='Verificar mapeo de sedes y filtros de proveedores'
                        )

            # Generar reporte de inconsistencias
            if inconsistencias or self.quality_issues:
                output_path_excel = os.path.join(self.output_folder, 'Reporte de Inconsistencias.xlsx')
                with pd.ExcelWriter(output_path_excel, engine='openpyxl') as writer:
                    if inconsistencias:
                        pd.DataFrame(inconsistencias).to_excel(writer, index=False, sheet_name='Inconsistencias')
                    if self.quality_issues:
                        pd.DataFrame(self.quality_issues).to_excel(writer, index=False, sheet_name='Calidad')
                logger.info(f"Reporte de inconsistencias generado: {output_path_excel}")
            else:
                logger.info("No se encontraron inconsistencias.")

        except Exception as e:
            logger.error(f"Error al validar las inconsistencias: {e}")
            raise

    def comprimir_archivos(self):
        """
        Comprime todos los archivos TXT generados en un archivo ZIP con el formato requerido.
        Elimina los archivos TXT originales y guarda el ZIP en la carpeta 'historico'.
        Utiliza la fecha del último día de venta reportado para el nombre del archivo.
        """
        
        try:
            # Obtener la última fecha de venta reportada
            if hasattr(self, 'filtered_data_total') and not self.filtered_data_total.empty:
                # Verificar si la fecha ya está en formato string o es datetime
                if isinstance(self.filtered_data_total['Fecha'].iloc[0], str):
                    # Convertir de string a datetime para poder encontrar el máximo
                    fechas = pd.to_datetime(self.filtered_data_total['Fecha'])
                    ultima_fecha = fechas.max()
                else:
                    # Si es datetime, simplemente encontrar el máximo
                    ultima_fecha = self.filtered_data_total['Fecha'].max()
                    
                # Extraer día, mes y año de la última fecha
                try:
                    if isinstance(ultima_fecha, str):
                        # Si es string en formato 'YYYY/MM/DD'
                        partes = ultima_fecha.split('/')
                        dia = int(partes[2])
                        mes = int(partes[1])
                        ano = int(partes[0])
                    else:
                        # Si es datetime
                        dia = ultima_fecha.day
                        mes = ultima_fecha.month
                        ano = ultima_fecha.year
                except Exception:
                    # En caso de error, usar el último día del mes como fallback
                    logger.warning("No se pudo determinar la última fecha de venta. Usando último día del mes.")
                    dia = calendar.monthrange(int(self.ano), int(self.mes))[1]
                    mes = int(self.mes)
                    ano = int(self.ano)
            else:
                dia = calendar.monthrange(int(self.ano), int(self.mes))[1]
                mes = int(self.mes)
                ano = int(self.ano)
                
            # Crear el nombre del archivo ZIP - formato: {CODIGO}_{ano}6{mes:02d}{dia:02d}.zip
            zip_filename = f"{self.company_config['codigo']}_{ano}6{mes:02d}{dia:02d}.zip"
            zip_path = os.path.join(self.output_folder, zip_filename)
            zip_path = os.path.join(self.output_folder, zip_filename)            # Resto del método se mantiene igual...
            # Crear la carpeta de histórico si no existe
            historico_folder = os.path.join(self.output_folder, "historico")
            if not os.path.exists(historico_folder):
                os.makedirs(historico_folder)
                logger.info(f"Carpeta de histórico creada: {historico_folder}")
            
            # Crear el archivo ZIP solo con archivos TXT
            txt_files = []
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(self.output_folder):
                    for file in files:
                        # Solo incluir archivos TXT y excluir la carpeta histórico
                        if file.endswith('.txt') and 'historico' not in root:
                            file_path = os.path.join(root, file)
                            arcname = os.path.basename(file_path)
                            zipf.write(file_path, arcname)
                            txt_files.append(file_path)
                            logger.info(f"Archivo TXT añadido al ZIP: {file}")
            
            # Mover el ZIP a la carpeta de histórico
            historico_zip_path = os.path.join(historico_folder, zip_filename)
            shutil.move(zip_path, historico_zip_path)
            logger.info(f"Archivo ZIP movido a histórico: {historico_zip_path}")
            
            # Eliminar los archivos TXT originales
            for txt_file in txt_files:
                os.remove(txt_file)
                logger.info(f"Archivo TXT eliminado: {txt_file}")
            
            logger.info(f"Proceso de compresión completado. ZIP guardado en: {historico_zip_path}")
            return historico_zip_path
        
        except Exception as e:
            logger.error(f"Error al comprimir los archivos: {e}")
            raise

    def enviar_por_ftp(self, zip_path):
        """Envía el archivo ZIP a un servidor FTP usando configuración del company_config."""
        try:
            # Verificar si FTP está habilitado
            if not self.company_config.get('ftp_enabled', False):
                logger.info("FTP no habilitado para esta empresa.")
                return False
                
            if not os.path.exists(zip_path):
                raise FileNotFoundError(f"El archivo ZIP no existe: {zip_path}")
            
            # Obtener configuración FTP
            ftp_config = self.config.get('ftp', {})
            ftp_host = ftp_config.get('host', 'apps.grupobit.net')
            ftp_port = ftp_config.get('port', 21)
            
            company_ftp = self.company_config.get('ftp', {})
            ftp_user = company_ftp.get('user')
            ftp_pass = company_ftp.get('password')
            
            if not ftp_user or not ftp_pass:
                logger.warning("Credenciales FTP no configuradas")
                return False
            
            print(f"Conectando al servidor FTP: {ftp_host}:{ftp_port}")
            logger.info(f"Conectando al servidor FTP: {ftp_host}:{ftp_port}")
            
            # Crear conexión FTP
            ftp = ftplib.FTP()
            ftp.connect(ftp_host, ftp_port, timeout=30)
            print(f"Conexión establecida con {ftp_host}")
            logger.info(f"Conexión establecida con {ftp_host}")
            
            # Login
            print(f"Iniciando sesión como: {ftp_user}")
            logger.info(f"Iniciando sesión como: {ftp_user}")
            ftp.login(ftp_user, ftp_pass)
            print(f"Sesión iniciada correctamente - Directorio actual: {ftp.pwd()}")
            
            # Subir archivo
            print(f"Subiendo archivo: {os.path.basename(zip_path)} ({os.path.getsize(zip_path)/1024/1024:.2f} MB)")
            logger.info(f"Subiendo archivo: {os.path.basename(zip_path)} ({os.path.getsize(zip_path)/1024/1024:.2f} MB)")
            with open(zip_path, 'rb') as file:
                remote_filename = os.path.basename(zip_path)
                ftp.storbinary(f'STOR {remote_filename}', file, blocksize=262144)
            
            print("Archivo subido correctamente")
            logger.info("Archivo subido correctamente")
            
            # Verificar que el archivo se subió
            print("Verificando archivos en el servidor:")
            logger.info("Verificando archivos en el servidor:")
            files = []
            ftp.dir(files.append)
            for file_info in files:
                print(f"  {file_info}")
            
            # Cerrar conexión
            ftp.quit()
            print("Conexión FTP cerrada")
            logger.info("Conexión FTP cerrada")
            return True
            
        except Exception as e:
            print(f"Error en la transferencia FTP: {e.__class__.__name__}: {e}")
            logger.error(f"Error al enviar el archivo por FTP: {e}")
            return False

    # Ejecución del script
if __name__ == '__main__':
    config_path = 'config.json'  # Ruta del archivo de configuración

    processor = VentaProcessor(config_path)

    # Cargar y filtrar los datos
    processor.cargar_y_filtrar_datos_por_periodo()

    # Procesar los datos
    processor.procesar_datos()

    # Guardar los resultados
    processor.guardar_archivo_ventas()
    
    # Generar el listado de facturas
    processor.generar_listado_facturas()
    
    # Generar los totales de control
    processor.generar_totales_de_control()
    
    # Generar el archivo de vendedores
    processor.generar_vendedores()

    # Generar el archivo de supervisores
    processor.generar_supervisores()
    
    # Generar el archivo de Tipos De Negocio
    processor.generar_tipos_de_negocio()
    
    # Generar el archivo SKU (Productos)
    processor.generar_sku_productos()
    
    # Generar los archivos de clientes
    processor.generar_clientes()
    
    # Generar el archivo de municipios
    processor.generar_municipios()
    
    # Generar el archivo de inventario
    processor.generar_inventario()
    
    # Generar el archivo de barrios (comentado temporalmente - no en especificaciones TSOL)
    # processor.generar_barrios()
    
    # Generar rutas
    processor.generar_rutas()
    
    # Validar inconsistencias
    processor.validar_inconsistencias()
    
    # Comprimir archivos
    zip_path = processor.comprimir_archivos()
    print(f"Archivos TXT comprimidos y guardados en: {zip_path}")
    
    # Enviar por FTP
    if processor.enviar_por_ftp(zip_path):
        print(f"Archivo enviado exitosamente al servidor FTP")
    else:
        print("No se envió el archivo por FTP (deshabilitado o error)")
