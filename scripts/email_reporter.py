# -*- coding: utf-8 -*-
"""
MÓDULO: email_reporter.py
Descripción: Envía reportes de cargue por correo electrónico con estadísticas detalladas
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging

class EmailReporter:
    """Genera y envía reportes de cargue por correo"""
    
    def __init__(self, smtp_server="smtp.gmail.com", smtp_port=587, username=None, password=None):
        """
        Inicializa el reporter de email
        
        Args:
            smtp_server: Servidor SMTP (default: Gmail)
            smtp_port: Puerto SMTP (default: 587)
            username: Usuario de Gmail
            password: Contraseña de Gmail o contraseña de aplicación
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.logger = logging.getLogger(__name__)
    
    def generar_reporte_html(self, datos_cargue):
        """
        Genera reporte HTML con estadísticas de cargue
        
        Args:
            datos_cargue: Diccionario con estadísticas:
                - fecha_ejecucion: datetime
                - duracion_segundos: float
                - registros_procesados: int
                - registros_insertados: int
                - registros_actualizados: int
                - registros_preservados: int
                - fecha_inicio: date
                - fecha_fin: date
                - tabla_fact: str (nombre tabla)
                - registros_fact: int
                - tabla_dev: str (nombre tabla)
                - registros_dev: int
                - tabla_staging: str (nombre tabla)
                - registros_staging: int
                - status: str ('EXITOSO' o 'ERROR')
                - detalles_tablas: list de dicts con {tabla, registros, tipo}
        """
        
        fecha_exec = datos_cargue.get('fecha_ejecucion', datetime.now())
        duracion = datos_cargue.get('duracion_segundos', 0)
        registros_proc = datos_cargue.get('registros_procesados', 0)
        registros_ins = datos_cargue.get('registros_insertados', 0)
        registros_act = datos_cargue.get('registros_actualizados', 0)
        registros_pres = datos_cargue.get('registros_preservados', 0)
        fecha_ini = datos_cargue.get('fecha_inicio', '')
        fecha_fin = datos_cargue.get('fecha_fin', '')
        registros_fact = datos_cargue.get('registros_fact', 0)
        registros_dev = datos_cargue.get('registros_dev', 0)
        registros_staging = datos_cargue.get('registros_staging', 0)
        status = datos_cargue.get('status', 'DESCONOCIDO')
        detalles_tablas = datos_cargue.get('detalles_tablas', [])
        
        # Colores según status
        color_status = '#28a745' if status == 'EXITOSO' else '#dc3545'
        
        # Generar filas de tabla para detalles
        filas_detalles = ''
        for tabla in detalles_tablas:
            tipo = tabla.get('tipo', '')
            registros = tabla.get('registros', 0)
            nombre_tabla = tabla.get('tabla', '')
            color_tipo = '#007bff' if tipo == '_fact' else '#6f42c1' if tipo == '_dev' else '#6c757d'
            
            filas_detalles += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{nombre_tabla}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">
                    <span style="background-color: {color_tipo}; color: white; padding: 4px 8px; border-radius: 3px; font-size: 12px;">
                        {tipo}
                    </span>
                </td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">
                    <strong>{registros:,}</strong>
                </td>
            </tr>
            """
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 900px; margin: 0 auto; background-color: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow: hidden; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 28px; }}
        .header p {{ margin: 10px 0 0 0; opacity: 0.9; }}
        .status-badge {{ 
            display: inline-block; 
            background-color: {color_status}; 
            color: white; 
            padding: 8px 16px; 
            border-radius: 20px; 
            font-weight: bold;
            margin-top: 10px;
        }}
        .section {{ padding: 20px; border-bottom: 1px solid #e0e0e0; }}
        .section:last-child {{ border-bottom: none; }}
        .section-title {{ font-size: 16px; font-weight: bold; color: #333; margin-bottom: 15px; display: flex; align-items: center; }}
        .section-title::before {{ content: ""; display: inline-block; width: 4px; height: 20px; background-color: #667eea; margin-right: 10px; }}
        .stats-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 15px; margin-bottom: 15px; }}
        .stat-card {{ background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #667eea; }}
        .stat-label {{ font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #333; margin-top: 5px; }}
        .stat-card.accent-fact {{ border-left-color: #007bff; }}
        .stat-card.accent-fact .stat-value {{ color: #007bff; }}
        .stat-card.accent-dev {{ border-left-color: #6f42c1; }}
        .stat-card.accent-dev .stat-value {{ color: #6f42c1; }}
        .stat-card.accent-time {{ border-left-color: #28a745; }}
        .stat-card.accent-time .stat-value {{ color: #28a745; }}
        .date-range {{ background: #f0f4ff; padding: 15px; border-radius: 6px; margin: 10px 0; border-left: 4px solid #667eea; }}
        .date-range strong {{ color: #333; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        table th {{ background-color: #f8f9fa; padding: 12px; text-align: left; font-weight: 600; color: #333; border-bottom: 2px solid #ddd; }}
        table td {{ padding: 10px 12px; border-bottom: 1px solid #ddd; }}
        table tr:hover {{ background-color: #f8f9fa; }}
        .timestamp {{ font-size: 12px; color: #666; margin-top: 15px; padding-top: 15px; border-top: 1px solid #ddd; }}
        .alert {{ padding: 12px; border-radius: 4px; margin: 10px 0; }}
        .alert-info {{ background-color: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }}
        .alert-success {{ background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }}
        .alert-danger {{ background-color: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }}
    </style>
</head>
<body>
    <div class="container">
        <!-- HEADER -->
        <div class="header">
            <h1>📊 Reporte de Cargue InfoVentas</h1>
            <p>DataZenith BI - Distrijass</p>
            <div class="status-badge">{status}</div>
        </div>

        <!-- RESUMEN RÁPIDO -->
        <div class="section">
            <div class="section-title">📈 Resumen de Procesamiento</div>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Registros Procesados</div>
                    <div class="stat-value">{registros_proc:,}</div>
                </div>
                <div class="stat-card accent-fact">
                    <div class="stat-label">Cargados en _fact</div>
                    <div class="stat-value">{registros_fact:,}</div>
                </div>
                <div class="stat-card accent-dev">
                    <div class="stat-label">Cargados en _dev</div>
                    <div class="stat-value">{registros_dev:,}</div>
                </div>
                <div class="stat-card accent-time">
                    <div class="stat-label">Duración Total</div>
                    <div class="stat-value">{duracion:.1f}s</div>
                </div>
            </div>

            <!-- RANGO DE FECHAS -->
            <div class="date-range">
                📅 <strong>Rango de Fechas Procesadas:</strong><br>
                Desde: <strong>{fecha_ini}</strong> Hasta: <strong>{fecha_fin}</strong>
            </div>

            <!-- ALERTA DE STATUS -->
            {f'<div class="alert alert-success">✅ Cargue completado sin errores</div>' if status == 'EXITOSO' else f'<div class="alert alert-danger">❌ Se detectaron errores durante el cargue</div>'}
        </div>

        <!-- DETALLES DE INSERCIÓN -->
        <div class="section">
            <div class="section-title">📝 Detalles de Operaciones</div>
            <table>
                <tr>
                    <th>Operación</th>
                    <th>Cantidad</th>
                </tr>
                <tr>
                    <td>Registros Insertados</td>
                    <td><strong>{registros_ins:,}</strong></td>
                </tr>
                <tr>
                    <td>Registros Actualizados</td>
                    <td><strong>{registros_act:,}</strong></td>
                </tr>
                <tr>
                    <td>Registros Preservados</td>
                    <td><strong>{registros_pres:,}</strong></td>
                </tr>
                <tr>
                    <td>Registros en Staging (POST-cargue)</td>
                    <td><strong>{registros_staging:,}</strong></td>
                </tr>
            </table>
        </div>

        <!-- DETALLES POR TABLA -->
        <div class="section">
            <div class="section-title">📦 Distribución por Tabla Clasificada</div>
            <table>
                <tr>
                    <th>Tabla</th>
                    <th>Tipo</th>
                    <th>Registros</th>
                </tr>
                {filas_detalles if filas_detalles else '<tr><td colspan="3" style="text-align: center; color: #999;">Cargando detalles...</td></tr>'}
            </table>
        </div>

        <!-- PIE DE PÁGINA -->
        <div class="section">
            <div class="timestamp">
                <strong>Generado:</strong> {fecha_exec.strftime('%Y-%m-%d %H:%M:%S')}<br>
                <strong>Sistema:</strong> DataZenith BI v2.1<br>
                <strong>Base de datos:</strong> bi_distrijass
            </div>
        </div>
    </div>
</body>
</html>
        """
        return html
    
    def enviar_reporte(self, destinatarios, asunto, datos_cargue, remitente=None):
        """
        Envía el reporte por correo
        
        Args:
            destinatarios: Lista de emails o email único (string)
            asunto: Asunto del correo
            datos_cargue: Diccionario con estadísticas
            remitente: Email del remitente (default: username)
        
        Returns:
            tuple (éxito: bool, mensaje: str)
        """
        try:
            if isinstance(destinatarios, str):
                destinatarios = [destinatarios]
            
            if not remitente:
                remitente = self.username
            
            # Generar HTML
            html_content = self.generar_reporte_html(datos_cargue)
            
            # Crear mensaje
            mensaje = MIMEMultipart('alternative')
            mensaje['Subject'] = asunto
            mensaje['From'] = remitente
            mensaje['To'] = ', '.join(destinatarios)
            
            # Versión texto simple como fallback
            texto_simple = f"""
REPORTE DE CARGUE - DataZenith BI
==================================

Fecha de ejecución: {datos_cargue.get('fecha_ejecucion', datetime.now()).strftime('%Y-%m-%d %H:%M:%S')}
Status: {datos_cargue.get('status', 'DESCONOCIDO')}

ESTADÍSTICAS:
- Registros procesados: {datos_cargue.get('registros_procesados', 0):,}
- Registros en _fact: {datos_cargue.get('registros_fact', 0):,}
- Registros en _dev: {datos_cargue.get('registros_dev', 0):,}
- Duración: {datos_cargue.get('duracion_segundos', 0):.1f} segundos

RANGO DE FECHAS:
- Desde: {datos_cargue.get('fecha_inicio', 'N/A')}
- Hasta: {datos_cargue.get('fecha_fin', 'N/A')}

Reporte completo en versión HTML adjunta.
            """
            
            # Adjuntar partes
            parte_texto = MIMEText(texto_simple, 'plain', 'utf-8')
            parte_html = MIMEText(html_content, 'html', 'utf-8')
            
            mensaje.attach(parte_texto)
            mensaje.attach(parte_html)
            
            # Enviar
            if self.username and self.password:
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as servidor:
                    servidor.starttls()
                    servidor.login(self.username, self.password)
                    servidor.send_message(mensaje)
                
                self.logger.info(f"✅ Reporte enviado a {', '.join(destinatarios)}")
                return True, f"Reporte enviado exitosamente a {len(destinatarios)} destinatario(s)"
            else:
                self.logger.warning("⚠️ Credenciales SMTP no configuradas. Reporte no enviado.")
                return False, "Credenciales SMTP no configuradas"
        
        except Exception as e:
            self.logger.error(f"❌ Error enviando reporte: {e}")
            return False, f"Error enviando reporte: {str(e)}"


def obtener_estadisticas_tablas(cargador):
    """
    Obtiene estadísticas detalladas de tablas _fact y _dev
    
    Args:
        cargador: Instancia de CargueInfoVentasInsert
    
    Returns:
        dict con estadísticas
    """
    estadisticas = {
        'registros_fact': 0,
        'registros_dev': 0,
        'registros_staging': 0,
        'detalles_tablas': []
    }
    
    try:
        conn = cargador.engine_mysql_bi.raw_connection()
        try:
            cursor = conn.cursor()
            try:
                # Obtener listado de tablas clasificadas
                cursor.execute("""
                    SELECT TABLE_NAME, TABLE_ROWS 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'bi_distrijass' 
                    AND (TABLE_NAME LIKE '%_fact' OR TABLE_NAME LIKE '%_dev')
                    ORDER BY TABLE_NAME
                """)
                
                tablas = cursor.fetchall()
                total_fact = 0
                total_dev = 0
                
                for tabla_nombre, num_filas in tablas:
                    if tabla_nombre.endswith('_fact'):
                        total_fact += num_filas or 0
                        tipo = '_fact'
                    else:
                        total_dev += num_filas or 0
                        tipo = '_dev'
                    
                    estadisticas['detalles_tablas'].append({
                        'tabla': tabla_nombre,
                        'tipo': tipo,
                        'registros': num_filas or 0
                    })
                
                estadisticas['registros_fact'] = total_fact
                estadisticas['registros_dev'] = total_dev
                
                # Registros en staging
                cursor.execute("SELECT COUNT(*) FROM infoventas;")
                estadisticas['registros_staging'] = cursor.fetchone()[0] or 0
                
            finally:
                cursor.close()
        finally:
            conn.close()
    
    except Exception as e:
        logging.error(f"❌ Error obteniendo estadísticas de tablas: {e}")
    
    return estadisticas
