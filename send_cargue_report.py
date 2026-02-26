#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SEND_CARGUE_REPORT.PY
Script de utilidad para enviar reportes de cargue por correo

Uso:
  python send_cargue_report.py --log "D:\\Logs\\DataZenithBI\\cargue_distrijass.log" --email "admin@distrijass.com"
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Agregar scripts al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

from email_reporter import EmailReporter, obtener_estadisticas_tablas


def parsear_log(archivo_log):
    """
    Parsea el archivo de log para extraer estadísticas
    
    Returns:
        dict con datos extraídos del log
    """
    datos = {
        'fecha_ejecucion': datetime.now(),
        'duracion_segundos': 0,
        'registros_procesados': 0,
        'registros_insertados': 0,
        'registros_actualizados': 0,
        'registros_preservados': 0,
        'fecha_inicio': 'N/A',
        'fecha_fin': 'N/A',
        'registros_fact': 0,
        'registros_dev': 0,
        'registros_staging': 0,
        'status': 'DESCONOCIDO',
        'detalles_tablas': []
    }
    
    try:
        with open(archivo_log, 'r', encoding='utf-8', errors='replace') as f:
            lineas = f.readlines()
        
        # Procesar líneas
        for linea in lineas:
            linea = linea.strip()
            
            # Detectar fechas
            if 'Rango de fechas detectado' in linea and '→' in linea:
                try:
                    partes = linea.split('→')
                    datos['fecha_inicio'] = partes[0].split(':')[-1].strip()
                    datos['fecha_fin'] = partes[1].strip()
                except:
                    pass
            
            # Detectar registros procesados
            if 'Registros procesados:' in linea:
                try:
                    valor = linea.split(':')[-1].strip()
                    datos['registros_procesados'] = int(valor.replace(',', ''))
                except:
                    pass
            
            # Detectar registros insertados
            if 'Registros insertados:' in linea:
                try:
                    valor = linea.split(':')[-1].strip()
                    datos['registros_insertados'] = int(valor.replace(',', ''))
                except:
                    pass
            
            # Detectar registros actualizados
            if 'Registros actualizados:' in linea:
                try:
                    valor = linea.split(':')[-1].strip()
                    datos['registros_actualizados'] = int(valor.replace(',', ''))
                except:
                    pass
            
            # Detectar registros preservados
            if 'Registros preservados:' in linea:
                try:
                    valor = linea.split(':')[-1].strip()
                    datos['registros_preservados'] = int(valor.replace(',', ''))
                except:
                    pass
            
            # Detectar registros en _fact
            if 'Registros en _fact:' in linea:
                try:
                    valor = linea.split(':')[-1].strip()
                    datos['registros_fact'] = int(valor.replace(',', '').split()[0])
                except:
                    pass
            
            # Detectar registros en _dev
            if 'Registros en _dev:' in linea:
                try:
                    valor = linea.split(':')[-1].strip()
                    datos['registros_dev'] = int(valor.replace(',', '').split()[0])
                except:
                    pass
            
            # Detectar duración
            if 'PROCESO COMPLETADO EXITOSAMENTE en' in linea:
                try:
                    valor = linea.split('en')[-1].split('segundos')[0].strip()
                    datos['duracion_segundos'] = float(valor)
                    datos['status'] = 'EXITOSO'
                except:
                    pass
            
            # Detectar errores
            if 'ERROR CRÍTICO' in linea:
                datos['status'] = 'ERROR'
        
        return datos
    
    except Exception as e:
        logging.error(f"Error parseando log: {e}")
        return datos


def cargar_config_email():
    """Carga la configuración de email"""
    config_path = os.path.join(os.path.dirname(__file__), 'config_email.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"No se pudo cargar config_email.json: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Envía reportes de cargue por correo")
    parser.add_argument("--log", required=True, help="Ruta del archivo de log")
    parser.add_argument("--email", help="Email destinatario")
    parser.add_argument("--usuario", help="Usuario Gmail para envío")
    parser.add_argument("--contrasena", help="Contraseña de aplicación Gmail")
    parser.add_argument("--asunto", default="[CARGUE BI] Reporte de Ejecución", help="Asunto del correo")
    
    args = parser.parse_args()
    
    # Verificar que el log exista
    if not os.path.exists(args.log):
        print(f"❌ Archivo de log no encontrado: {args.log}")
        return 1
    
    # Cargar configuración
    config = cargar_config_email()
    
    # Determinar email y credenciales
    email_dest = args.email
    usuario_gmail = args.usuario
    contrasena_gmail = args.contrasena
    
    if not email_dest:
        if config and 'destinatarios' in config:
            email_dest = config['destinatarios'].get('reportes_exito', [])[0] if config['destinatarios'].get('reportes_exito') else None
    
    if not usuario_gmail and config and 'credenciales' in config:
        usuario_gmail = config['credenciales'].get('usuario')
        contrasena_gmail = config['credenciales'].get('contrasena')
    
    if not email_dest or not usuario_gmail or not contrasena_gmail:
        print("❌ Faltan parámetros requeridos: email, usuario y contraseña")
        print("   Use --email, --usuario, --contrasena o configure config_email.json")
        return 1
    
    print(f"📧 Enviando reporte a: {email_dest}")
    print(f"📄 Log: {args.log}")
    
    # Parsear log
    print("📖 Parseando archivo de log...")
    datos_cargue = parsear_log(args.log)
    
    print(f"   ✓ Registros procesados: {datos_cargue['registros_procesados']:,}")
    print(f"   ✓ Rango: {datos_cargue['fecha_inicio']} → {datos_cargue['fecha_fin']}")
    print(f"   ✓ Status: {datos_cargue['status']}")
    
    # Crear reporter y enviar
    print("📨 Conectando al servidor de correo...")
    reporter = EmailReporter(
        smtp_server="smtp.gmail.com",
        smtp_port=587,
        username=usuario_gmail,
        password=contrasena_gmail
    )
    
    exito, mensaje = reporter.enviar_reporte(
        destinatarios=email_dest,
        asunto=args.asunto,
        datos_cargue=datos_cargue,
        remitente=usuario_gmail
    )
    
    if exito:
        print(f"✅ {mensaje}")
        return 0
    else:
        print(f"❌ {mensaje}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
