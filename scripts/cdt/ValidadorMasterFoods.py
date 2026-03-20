#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validador Master Foods V3.0
Valida la estructura y contenido de los archivos fuente antes del procesamiento
"""

import pandas as pd
import os
import json
from datetime import datetime

class ValidadorMasterFoods:
    def __init__(self, config_path='config_masterfoods.json'):
        """Inicializa el validador."""
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config = self._cargar_configuracion(config_path)
        self.proveedor = '006-MASTERFOODS COLOMBIA LTDA'
        self.vendedores_especiales = ['MA01', 'MA02', 'M1013']
        self.bodega_especial = 'BODEGA MASTER SPT'
        self.errores = []
        self.advertencias = []

    def _cargar_configuracion(self, config_path):
        """Carga la configuración."""
        try:
            with open(os.path.join(self.base_dir, config_path), 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Error cargando configuración: {e}")

    def validar_archivos_fuente(self):
        """Valida que los archivos fuente existan y sean accesibles."""
        print("🔍 Validando archivos fuente...")
        
        archivos = [
            ('Ventas', self.config['ventas_path']),
            ('Clientes', self.config['clientes_path']),
            ('Inventario', self.config['inventario_path'])
        ]
        
        for nombre, ruta in archivos:
            if not os.path.exists(ruta):
                self.errores.append(f"❌ {nombre}: Archivo no encontrado - {ruta}")
            else:
                try:
                    # Intentar leer el archivo
                    if nombre == 'Ventas':
                        pd.read_excel(ruta, sheet_name='infoventas', nrows=1)
                    elif nombre == 'Clientes':
                        pd.read_excel(ruta, sheet_name='CLIENTES', nrows=1)
                    else:  # Inventario
                        pd.read_excel(ruta, sheet_name='Informe', nrows=1)
                    
                    print(f"  ✅ {nombre}: OK")
                except Exception as e:
                    self.errores.append(f"❌ {nombre}: Error al leer archivo - {e}")

    def validar_datos_ventas(self):
        """Valida la estructura de datos de ventas."""
        print("\n🔍 Validando datos de ventas...")
        
        try:
            df = pd.read_excel(self.config['ventas_path'], sheet_name='infoventas')
            
            # Columnas requeridas
            columnas_requeridas = [
                'Empresa', 'Cod. vendedor', 'Proveedor', 'Cod. cliente',
                'Descripción', 'Cantidad', 'Vta neta', 'Fecha'
            ]
            
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            if columnas_faltantes:
                self.errores.append(f"❌ Ventas: Columnas faltantes - {columnas_faltantes}")
            else:
                print("  ✅ Estructura de columnas: OK")
            
            # Filtrar por proveedor Master Foods
            df_filtrado = df[df['Proveedor'].astype(str).str.contains(self.proveedor, case=False, na=False)]
            
            if len(df_filtrado) == 0:
                self.errores.append(f"❌ Ventas: No se encontraron registros para {self.proveedor}")
            else:
                print(f"  ✅ Registros Master Foods: {len(df_filtrado)}")
                
                # Verificar vendedores especiales
                if 'Cod. vendedor' in df.columns:
                    vendedores_unicos = df_filtrado['Cod. vendedor'].dropna().unique()
                    vendedores_encontrados = [v for v in self.vendedores_especiales if v in vendedores_unicos]
                    
                    if vendedores_encontrados:
                        print(f"  ✅ Vendedores especiales encontrados: {vendedores_encontrados}")
                    else:
                        self.advertencias.append(f"⚠️ Ventas: No se encontraron vendedores especiales {self.vendedores_especiales}")
                
                # Verificar empresas
                if 'Empresa' in df.columns:
                    empresas = df_filtrado['Empresa'].dropna().unique()
                    print(f"  ✅ Empresas encontradas: {list(empresas)}")
                    
                    if not any('Distrijass' in str(emp) for emp in empresas):
                        self.advertencias.append("⚠️ Ventas: No se encontró empresa Distrijass")
                    if not any('Eje' in str(emp) for emp in empresas):
                        self.advertencias.append("⚠️ Ventas: No se encontró empresa Eje")
        
        except Exception as e:
            self.errores.append(f"❌ Ventas: Error al validar - {e}")

    def validar_datos_clientes(self):
        """Valida la estructura de datos de clientes."""
        print("\n🔍 Validando datos de clientes...")
        
        try:
            df = pd.read_excel(self.config['clientes_path'], sheet_name='CLIENTES')
            
            columnas_requeridas = ['Cod. Cliente', 'Nom. Cliente', 'Direccion']
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            
            if columnas_faltantes:
                self.errores.append(f"❌ Clientes: Columnas faltantes - {columnas_faltantes}")
            else:
                print("  ✅ Estructura de columnas: OK")
                print(f"  ✅ Total clientes: {len(df)}")
        
        except Exception as e:
            self.errores.append(f"❌ Clientes: Error al validar - {e}")

    def validar_datos_inventario(self):
        """Valida la estructura de datos de inventario."""
        print("\n🔍 Validando datos de inventario...")
        
        try:
            df = pd.read_excel(self.config['inventario_path'], sheet_name='Informe')
            
            columnas_requeridas = [
                'Empresa', 'Proveedor', 'Nombre bodega',
                'Codigo articulo', 'Nombre articulo', 'Unidades', 'Valor'
            ]
            
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            if columnas_faltantes:
                self.errores.append(f"❌ Inventario: Columnas faltantes - {columnas_faltantes}")
            else:
                print("  ✅ Estructura de columnas: OK")
            
            # Filtrar por proveedor
            df_filtrado = df[df['Proveedor'].astype(str).str.contains(self.proveedor, case=False, na=False)]
            
            if len(df_filtrado) == 0:
                self.errores.append(f"❌ Inventario: No se encontraron registros para {self.proveedor}")
            else:
                print(f"  ✅ Registros Master Foods: {len(df_filtrado)}")
                
                # Verificar bodega especial
                if 'Nombre bodega' in df.columns:
                    if self.bodega_especial in df_filtrado['Nombre bodega'].values:
                        spt_count = len(df_filtrado[df_filtrado['Nombre bodega'] == self.bodega_especial])
                        print(f"  ✅ {self.bodega_especial}: {spt_count} registros")
                    else:
                        self.advertencias.append(f"⚠️ Inventario: No se encontró {self.bodega_especial}")
                
                # Verificar empresas en inventario
                empresas = df_filtrado['Empresa'].dropna().unique()
                print(f"  ✅ Empresas en inventario: {list(empresas)}")
        
        except Exception as e:
            self.errores.append(f"❌ Inventario: Error al validar - {e}")

    def validar_configuracion(self):
        """Valida la configuración del sistema."""
        print("\n🔍 Validando configuración...")
        
        try:
            # Verificar rutas en configuración
            rutas_requeridas = ['ventas_path', 'clientes_path', 'inventario_path']
            for ruta in rutas_requeridas:
                if ruta not in self.config:
                    self.errores.append(f"❌ Configuración: Falta {ruta}")
            
            # Verificar directorio de salida
            output_folder = self.config.get('output_folder', 'output_masterfoods')
            if not os.path.isabs(output_folder):
                output_folder = os.path.join(self.base_dir, output_folder)
            
            if not os.path.exists(output_folder):
                print(f"  ⚠️ Directorio de salida será creado: {output_folder}")
            else:
                print(f"  ✅ Directorio de salida: {output_folder}")
            
            # Verificar configuración SFTP
            if self.config.get('enviar_sftp', False):
                sftp_params = ['sftp_host', 'sftp_user', 'sftp_pass']
                sftp_faltantes = [p for p in sftp_params if not self.config.get(p)]
                if sftp_faltantes:
                    self.advertencias.append(f"⚠️ SFTP habilitado pero faltan parámetros: {sftp_faltantes}")
                else:
                    print("  ✅ Configuración SFTP: OK")
            else:
                print("  ℹ️ Envío SFTP: Deshabilitado")
        
        except Exception as e:
            self.errores.append(f"❌ Configuración: Error al validar - {e}")

    def ejecutar_validacion(self):
        """Ejecuta la validación completa."""
        print("=" * 50)
        print("🔍 VALIDADOR MASTER FOODS V3.0")
        print("=" * 50)
        
        # Ejecutar todas las validaciones
        self.validar_archivos_fuente()
        self.validar_datos_ventas()
        self.validar_datos_clientes()
        self.validar_datos_inventario()
        self.validar_configuracion()
        
        # Mostrar resumen
        print("\n" + "=" * 50)
        print("📋 RESUMEN DE VALIDACIÓN")
        print("=" * 50)
        
        if self.errores:
            print(f"\n❌ ERRORES CRÍTICOS ({len(self.errores)}):")
            for error in self.errores:
                print(f"  {error}")
        
        if self.advertencias:
            print(f"\n⚠️ ADVERTENCIAS ({len(self.advertencias)}):")
            for advertencia in self.advertencias:
                print(f"  {advertencia}")
        
        if not self.errores and not self.advertencias:
            print("\n✅ VALIDACIÓN EXITOSA")
            print("🚀 El sistema está listo para procesar")
            return True
        elif not self.errores:
            print("\n⚠️ VALIDACIÓN CON ADVERTENCIAS")
            print("🚀 El sistema puede procesar, pero revise las advertencias")
            return True
        else:
            print("\n❌ VALIDACIÓN FALLIDA")
            print("🛑 Corrija los errores antes de procesar")
            return False

if __name__ == '__main__':
    try:
        validador = ValidadorMasterFoods()
        exito = validador.ejecutar_validacion()
        exit(0 if exito else 1)
    except Exception as e:
        print(f"❌ Error fatal en validación: {e}")
        exit(1)
