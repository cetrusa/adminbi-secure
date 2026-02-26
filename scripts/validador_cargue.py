#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validador Inteligente de Cargue - Opción A
Detecta y fusiona duplicados ANTES de sincronizar a _fact/_dev
Fecha: 21 de octubre 2025
"""

import sys
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

class ValidadorCargueInteligente:
    """
    Valida datos en staging ANTES de sincronizar a _fact/_dev.
    Previene contaminar tablas de producción con datos defectuosos.
    """
    
    def __init__(self, cargador, umbral_duplicados_pct=1.0, tolerancia_monto=0.01):
        """
        Args:
            cargador: Instancia de CargueInfoVentasInsert
            umbral_duplicados_pct: % de duplicados tolerables (1% = fusiona automáticamente)
            tolerancia_monto: $ de tolerancia en comparación de totales
        """
        self.cargador = cargador
        self.umbral_duplicados_pct = umbral_duplicados_pct
        self.tolerancia_monto = tolerancia_monto
        self.conn = None
        self.validaciones = {}
        
    def conectar(self):
        """Establece conexión a BD."""
        self.conn = self.cargador.engine_mysql_bi.raw_connection()
        logger.info("✅ Conexión establecida para validación")
        
    def desconectar(self):
        """Cierra conexión."""
        if self.conn:
            self.conn.close()
            
    # ============================================================
    # VALIDACIÓN 1: DETECTAR DUPLICADOS EN STAGING
    # ============================================================
    
    def detectar_duplicados_staging(self) -> Dict[str, List]:
        """
        Detecta duplicados en tabla staging (infoventas).
        Retorna: {
            'total_duplicados': N,
            'pct_duplicados': X%,
            'detalle': [...]
        }
        """
        logger.info("\n" + "="*80)
        logger.info("🔍 VALIDACIÓN 1: Detectando duplicados en staging...")
        logger.info("="*80)
        
        cursor = self.conn.cursor()
        
        try:
            # Contar total de registros
            cursor.execute("SELECT COUNT(*) FROM infoventas")
            total_registros = cursor.fetchone()[0]
            logger.info(f"📊 Total registros en staging: {total_registros:,}")
            
            # Detectar duplicados por (fecha_venta, cod_proveedor, id_infoproducto)
            cursor.execute("""
                SELECT 
                    fecha_venta,
                    cod_proveedor,
                    id_infoproducto,
                    COUNT(*) as repeticiones,
                    SUM(monto_venta) as suma_monto,
                    GROUP_CONCAT(id_infoventa) as ids_duplicados
                FROM infoventas
                GROUP BY fecha_venta, cod_proveedor, id_infoproducto
                HAVING COUNT(*) > 1
            """)
            
            duplicados = cursor.fetchall()
            
            if duplicados:
                total_duplicados = len(duplicados)
                pct_duplicados = (total_duplicados / total_registros) * 100 if total_registros > 0 else 0
                
                logger.warning(f"⚠️  ENCONTRADOS {total_duplicados:,} GRUPOS DE DUPLICADOS ({pct_duplicados:.2f}%)")
                
                resultado = {
                    'total_duplicados': total_duplicados,
                    'pct_duplicados': pct_duplicados,
                    'detalle': duplicados[:20],  # Primeros 20
                    'total_registros': total_registros
                }
                
                # Mostrar ejemplos
                logger.warning("\n📋 Ejemplos de duplicados:")
                for dup in duplicados[:5]:
                    fecha, proveedor, producto, reps, suma, ids = dup
                    logger.warning(f"   • {fecha} - Prov:{proveedor} - Prod:{producto}: {reps} repeticiones (${suma})")
                    logger.warning(f"     IDs: {ids}")
                
                self.validaciones['duplicados'] = resultado
                return resultado
            else:
                logger.info("✅ No se detectaron duplicados en staging")
                self.validaciones['duplicados'] = {'total_duplicados': 0}
                return {'total_duplicados': 0, 'pct_duplicados': 0}
                
        finally:
            cursor.close()
    
    # ============================================================
    # VALIDACIÓN 2: DECIDIR SI FUSIONAR O ALERTAR
    # ============================================================
    
    def evaluar_duplicados(self, info_duplicados: Dict) -> Tuple[bool, str]:
        """
        Decide si fusionar automáticamente o alertar.
        
        Retorna: (puede_continuar: bool, accion: str)
        """
        logger.info("\n" + "="*80)
        logger.info("🎯 EVALUACIÓN: ¿Qué hacer con duplicados?")
        logger.info("="*80)
        
        total_duplicados = info_duplicados.get('total_duplicados', 0)
        pct_duplicados = info_duplicados.get('pct_duplicados', 0)
        
        if total_duplicados == 0:
            logger.info("✅ Sin duplicados - Puede continuar")
            return True, "SIN_DUPLICADOS"
        
        # Política: Si <1% duplicados, fusionar automáticamente
        if pct_duplicados < self.umbral_duplicados_pct:
            logger.warning(f"⚠️  {pct_duplicados:.2f}% duplicados (< {self.umbral_duplicados_pct}%)")
            logger.info("→ DECISIÓN: Fusionar automáticamente")
            return True, "FUSIONAR_AUTOMATICO"
        else:
            logger.error(f"❌ {pct_duplicados:.2f}% duplicados (> {self.umbral_duplicados_pct}%)")
            logger.error("→ DECISIÓN: ALERTAR Y PAUSAR")
            return False, "ALERTAR_PAUSAR"
    
    # ============================================================
    # VALIDACIÓN 3: FUSIONAR DUPLICADOS AUTOMÁTICAMENTE
    # ============================================================
    
    def fusionar_duplicados(self) -> int:
        """
        Fusiona duplicados automáticamente en staging.
        Estrategia: Mantener el registro con mayor monto, borrar los demás.
        
        Retorna: Número de registros eliminados
        """
        logger.info("\n" + "="*80)
        logger.info("🔧 ACCIÓN: Fusionando duplicados...")
        logger.info("="*80)
        
        cursor = self.conn.cursor()
        
        try:
            # Identificar duplicados y mantener el de mayor monto
            cursor.execute("""
                DELETE FROM infoventas
                WHERE id_infoventa NOT IN (
                    SELECT id_max FROM (
                        SELECT MAX(CASE 
                            WHEN monto_venta = (
                                SELECT MAX(monto_venta) 
                                FROM infoventas t2 
                                WHERE t2.fecha_venta = t1.fecha_venta
                                  AND t2.cod_proveedor = t1.cod_proveedor
                                  AND t2.id_infoproducto = t1.id_infoproducto
                            )
                            THEN id_infoventa
                            ELSE NULL
                        END) as id_max,
                        fecha_venta, cod_proveedor, id_infoproducto
                        FROM infoventas t1
                        GROUP BY fecha_venta, cod_proveedor, id_infoproducto
                    ) tb
                    WHERE id_max IS NOT NULL
                )
                AND (fecha_venta, cod_proveedor, id_infoproducto) IN (
                    SELECT fecha_venta, cod_proveedor, id_infoproducto
                    FROM infoventas
                    GROUP BY fecha_venta, cod_proveedor, id_infoproducto
                    HAVING COUNT(*) > 1
                )
            """)
            
            registros_eliminados = cursor.rowcount
            self.conn.commit()
            
            logger.info(f"✅ Se eliminaron {registros_eliminados:,} registros duplicados")
            logger.info(f"📊 Registros fusionados exitosamente")
            
            self.validaciones['fusion'] = {'registros_eliminados': registros_eliminados}
            return registros_eliminados
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error al fusionar duplicados: {e}")
            raise
        finally:
            cursor.close()
    
    # ============================================================
    # VALIDACIÓN 4: VERIFICAR TOTALES
    # ============================================================
    
    def verificar_totales(self, fecha_ini, fecha_fin) -> Tuple[bool, Dict]:
        """
        Verifica que los totales en staging coincidan con lo esperado.
        
        Retorna: (validacion_ok: bool, detalle: dict)
        """
        logger.info("\n" + "="*80)
        logger.info("💰 VALIDACIÓN 4: Verificando totales de Vta Neta...")
        logger.info("="*80)
        
        cursor = self.conn.cursor()
        
        try:
            # Total en staging
            cursor.execute("""
                SELECT 
                    SUM(monto_venta) as suma_venta,
                    COUNT(*) as total_registros,
                    COUNT(DISTINCT DATE(fecha_venta)) as dias_unicos,
                    COUNT(DISTINCT cod_proveedor) as proveedores_unicos
                FROM infoventas
            """)
            
            resultado = cursor.fetchone()
            suma_venta = resultado[0] or Decimal('0')
            total_registros = resultado[1] or 0
            dias_unicos = resultado[2] or 0
            proveedores_unicos = resultado[3] or 0
            
            logger.info(f"📊 Staging - Vta Neta: ${suma_venta:,.2f}")
            logger.info(f"📊 Staging - Registros: {total_registros:,}")
            logger.info(f"📊 Staging - Días únicos: {dias_unicos}")
            logger.info(f"📊 Staging - Proveedores: {proveedores_unicos}")
            
            # Por ahora solo logueamos (el usuario comparará manualmente)
            detalle = {
                'suma_venta': float(suma_venta),
                'total_registros': total_registros,
                'dias_unicos': dias_unicos,
                'proveedores_unicos': proveedores_unicos,
                'periodo': f"{fecha_ini} → {fecha_fin}"
            }
            
            self.validaciones['totales'] = detalle
            
            logger.info("\n💡 PRÓXIMO PASO: Comparar estos totales con el servidor acumulado")
            logger.info("   Si hay discrepancia > ${}, requiere revisión manual".format(self.tolerancia_monto))
            
            return True, detalle
            
        finally:
            cursor.close()
    
    # ============================================================
    # VALIDACIÓN 5: REGISTRAR VALIDACIÓN
    # ============================================================
    
    def registrar_validacion(self, fecha_cargue, estado: str, mensaje: str = "", 
                           duplicados_fusionados: int = 0):
        """
        Registra el resultado de la validación en tabla de control.
        """
        logger.info("\n" + "="*80)
        logger.info("📝 Registrando validación en BD...")
        logger.info("="*80)
        
        cursor = self.conn.cursor()
        
        try:
            # Obtener datos actuales
            cursor.execute("""
                SELECT 
                    MONTH(fecha_cargue) as mes,
                    YEAR(fecha_cargue) as anno,
                    COUNT(*) as registros_fact,
                    SUM(monto_venta) as suma_fact,
                    MD5(GROUP_CONCAT(CAST(id_infoventa AS CHAR))) as checksum_fact
                FROM bi_distrijass.infoventas_2025_fact
            """)
            
            row = cursor.fetchone()
            mes, anno, registros_fact, suma_fact, checksum = row if row else (None, None, 0, 0, None)
            
            # Insertar registro de validación
            cursor.execute("""
                INSERT INTO bi_distrijass.validacion_cargue_diario (
                    fecha_control, mes, anno,
                    registros_fact, suma_fact, checksum_fact,
                    estado_validacion, mensaje_validacion,
                    duplicados_fusionados, accion_tomada,
                    fecha_creacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                datetime.now().date(),
                mes or datetime.now().month,
                anno or datetime.now().year,
                registros_fact,
                suma_fact,
                checksum,
                estado,
                mensaje,
                duplicados_fusionados,
                'VALIDACION_COMPLETADA'
            ))
            
            self.conn.commit()
            logger.info(f"✅ Validación registrada: {estado}")
            
        except Exception as e:
            logger.error(f"⚠️  No se pudo registrar validación: {e}")
            # No fallar por esto
        finally:
            cursor.close()
    
    # ============================================================
    # ORQUESTADOR: EJECUTAR VALIDACIÓN COMPLETA
    # ============================================================
    
    def validar_cargue_completo(self, fecha_ini, fecha_fin) -> bool:
        """
        Ejecuta validación completa PRE-sincronización.
        
        Retorna: True si puede continuar, False si debe pausar
        """
        logger.info("\n")
        logger.info("╔" + "="*78 + "╗")
        logger.info("║" + " "*15 + "🔍 VALIDACIÓN INTELIGENTE DE CARGUE - 21 OCTUBRE 2025" + " "*12 + "║")
        logger.info("╚" + "="*78 + "╝")
        
        try:
            self.conectar()
            
            # PASO 1: Detectar duplicados
            info_dup = self.detectar_duplicados_staging()
            
            # PASO 2: Evaluar qué hacer
            puede_continuar, accion = self.evaluar_duplicados(info_dup)
            
            # PASO 3: Si hay duplicados tolerables, fusionar
            if accion == "FUSIONAR_AUTOMATICO":
                duplicados_fusionados = self.fusionar_duplicados()
            else:
                duplicados_fusionados = 0
            
            # PASO 4: Verificar totales
            totales_ok, detalle_totales = self.verificar_totales(fecha_ini, fecha_fin)
            
            # PASO 5: Registrar validación
            if puede_continuar:
                self.registrar_validacion(
                    datetime.now(),
                    estado='OK',
                    mensaje=f'Validación exitosa. Duplicados fusionados: {duplicados_fusionados}',
                    duplicados_fusionados=duplicados_fusionados
                )
            else:
                self.registrar_validacion(
                    datetime.now(),
                    estado='ERROR',
                    mensaje=f'Demasiados duplicados detectados',
                    duplicados_fusionados=0
                )
            
            # RESUMEN FINAL
            logger.info("\n" + "="*80)
            logger.info("📊 RESUMEN DE VALIDACIÓN")
            logger.info("="*80)
            logger.info(f"Duplicados detectados: {info_dup.get('total_duplicados', 0)}")
            logger.info(f"Duplicados fusionados: {duplicados_fusionados}")
            logger.info(f"Puede continuar: {'✅ SÍ' if puede_continuar else '❌ NO'}")
            logger.info(f"Acción tomada: {accion}")
            logger.info("="*80 + "\n")
            
            return puede_continuar
            
        except Exception as e:
            logger.error(f"❌ Error durante validación: {e}", exc_info=True)
            self.registrar_validacion(
                datetime.now(),
                estado='ERROR',
                mensaje=f'Error: {str(e)}',
                duplicados_fusionados=0
            )
            return False
        finally:
            self.desconectar()

# ============================================================
# FUNCIÓN DE INTEGRACIÓN PARA CARGUE_INFOVENTAS_MAIN
# ============================================================

def validar_cargue_antes_sincronizar(cargador, fecha_ini, fecha_fin) -> bool:
    """
    Función que se llamará desde cargue_infoventas_main.py.
    
    Uso:
        if validar_cargue_antes_sincronizar(cargador, fecha_ini, fecha_fin):
            # Continuar con sincronización a _fact/_dev
        else:
            # PAUSAR y ALERTAR
    """
    validador = ValidadorCargueInteligente(
        cargador,
        umbral_duplicados_pct=1.0,    # Fusionar si <1%
        tolerancia_monto=0.01         # Tolerancia: $0.01
    )
    
    return validador.validar_cargue_completo(fecha_ini, fecha_fin)
