"""
HomologacionUpdater: Actualiza equivalencias PENDIENTES.

Patrón idéntico a extrae_bi_insert.py:
  ExtraeBiConfig  → HomologacionConfig
  ExtraeBiExtractor → HomologacionUpdater

HomologacionConfig(database_name):
  ConfigBasic(database_name) → config
  DataBaseConnection → engine_mysql_bi

HomologacionUpdater(config).run():
  1. Carga pendientes de bi_equivalencias
  2. Intenta auto-match contra bi_productos_canonico
  3. Actualiza tipo_asignacion + codigo_canonico
  4. Retorna métricas
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from scripts.bimbo.repositories.bimbo_repository import BimboRepository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config — igual que ExtraeBiConfig
# ---------------------------------------------------------------------------

class HomologacionConfig:
    """
    Configuración de conexión para el proceso de homologación.

    Sigue EXACTAMENTE el patrón de ExtraeBiConfig:
      ConfigBasic(database_name) → config
      engine_mysql_bi → servidor BI (powerbi_bimbo)

    Solo necesita engine_mysql_bi (todo está en powerbi_bimbo).
    No requiere engine_mysql_out (no consulta SIDIS).
    """

    def __init__(self, database_name: str):
        self.database_name = database_name

        # Patrón ExtraeBiConfig
        self.config_basic = ConfigBasic(database_name)
        self.config = self.config_basic.config

        # Solo BI — igual que ExtraeBiConfig._create_engine_mysql_bi
        c = self.config
        self.engine_mysql_bi = con.ConexionMariadb3(
            str(c.get("nmUsrIn")),
            str(c.get("txPassIn")),
            str(c.get("hostServerIn")),
            int(c.get("portServerIn")),
            str(c.get("dbBi")),
        )


# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------

@dataclass
class HomologacionMetrics:
    """Resultado del proceso de homologación."""
    total_pendientes: int = 0
    auto_exacto: int = 0       # idhml_original coincide exacto con codigo_bimbo
    sin_match: int = 0         # No se encontró equivalente
    errores: int = 0
    duracion_seg: float = 0.0

    @property
    def cobertura_pct(self) -> float:
        if self.total_pendientes == 0:
            return 0.0
        return round(self.auto_exacto / self.total_pendientes * 100, 2)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total_pendientes": self.total_pendientes,
            "auto_exacto": self.auto_exacto,
            "sin_match": self.sin_match,
            "errores": self.errores,
            "cobertura_pct": self.cobertura_pct,
            "duracion_seg": round(self.duracion_seg, 1),
        }


# ---------------------------------------------------------------------------
# Updater — igual que ExtraeBiExtractor
# ---------------------------------------------------------------------------

class HomologacionUpdater:
    """
    Actualiza equivalencias PENDIENTES contra el catálogo canónico.

    Patrón:
      config = HomologacionConfig(database_name)
      updater = HomologacionUpdater(config)
      resultado = updater.run()

    Igual que:
      config = ExtraeBiConfig(database_name)
      extractor = ExtraeBiExtractor(config, ...)
      resultado = extractor.run()
    """

    def __init__(
        self,
        config: HomologacionConfig,
        id_agencia: Optional[int] = None,
        progress_callback=None,
    ):
        self.config = config
        self.id_agencia = id_agencia  # None = todas las agencias del usuario
        self._progress = progress_callback or (lambda *a, **kw: None)
        self._repo = BimboRepository(config.engine_mysql_bi)

    # ── Entry point ────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el proceso completo de homologación.
        Retorna métricas igual que ExtraeBiExtractor.run().
        """
        inicio = time.time()
        self._progress("Cargando catálogo canónico", 5)

        # 1. Cargar catálogo canónico completo
        codigos_canonicos = self._cargar_catalogo()
        logger.info("Catálogo: %d códigos disponibles", len(codigos_canonicos))
        self._progress("Catálogo cargado", 15)

        # 2. Cargar pendientes
        pendientes = self._cargar_pendientes()
        metrics = HomologacionMetrics(total_pendientes=len(pendientes))
        logger.info("Pendientes: %d equivalencias sin homologar", len(pendientes))

        if not pendientes:
            self._progress("Sin pendientes", 100)
            metrics.duracion_seg = time.time() - inicio
            return {"success": True, "message": "Sin equivalencias pendientes", **metrics.as_dict()}

        self._progress(f"Procesando {len(pendientes)} pendientes", 20)

        # 3. Procesar cada pendiente
        chunk = max(1, len(pendientes) // 10)
        for idx, row in enumerate(pendientes):
            try:
                self._procesar_pendiente(row, codigos_canonicos, metrics)
            except Exception as exc:
                logger.error("Error procesando id=%s: %s", row.get("id"), exc)
                metrics.errores += 1

            if (idx + 1) % chunk == 0:
                pct = 20 + int((idx + 1) / len(pendientes) * 75)
                self._progress(f"Procesados {idx+1}/{len(pendientes)}", pct)

        metrics.duracion_seg = time.time() - inicio
        self._progress("Completado", 100)

        logger.info(
            "Homologación completada: %d auto_exacto | %d sin_match | %d errores | %.1fs",
            metrics.auto_exacto, metrics.sin_match, metrics.errores, metrics.duracion_seg,
        )

        return {
            "success": metrics.errores == 0,
            "message": (
                f"Homologados: {metrics.auto_exacto} | "
                f"Sin match: {metrics.sin_match} | "
                f"Errores: {metrics.errores}"
            ),
            **metrics.as_dict(),
        }

    # ── Catalogo ───────────────────────────────────────────────────────

    def _cargar_catalogo(self) -> Dict[str, str]:
        """
        Carga catálogo canónico: {codigo_bimbo: codigo_bimbo}.
        También indexa por nombre_corto para fuzzy lookup.
        """
        sql = text("""
            SELECT codigo_bimbo, nombre_corto
            FROM powerbi_bimbo.bi_productos_canonico
            WHERE estado = 'Disponible'
        """)
        with self.config.engine_mysql_bi.connect() as conn:
            rows = conn.execute(sql).mappings().all()

        # Index: codigo_bimbo (uppercase stripped) → codigo_bimbo original
        return {
            str(r["codigo_bimbo"]).strip().upper(): str(r["codigo_bimbo"]).strip()
            for r in rows
            if r["codigo_bimbo"]
        }

    # ── Pendientes ─────────────────────────────────────────────────────

    def _cargar_pendientes(self) -> List[Dict[str, Any]]:
        """
        Carga bi_equivalencias con tipo_asignacion = 'PENDIENTE'
        y dt_fin IS NULL (vigentes), filtrado por id_agencia si se pasó.
        """
        where = "WHERE tipo_asignacion = 'PENDIENTE' AND dt_fin IS NULL"
        params: Dict[str, Any] = {}
        if self.id_agencia:
            where += " AND id_agencia = :id_agencia"
            params["id_agencia"] = self.id_agencia

        sql = text(f"""
            SELECT id, id_agencia, nbProducto, nmProducto, idhml_original
            FROM powerbi_bimbo.bi_equivalencias
            {where}
            ORDER BY id_agencia, nbProducto
        """)
        with self.config.engine_mysql_bi.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]

    # ── Procesamiento ──────────────────────────────────────────────────

    def _procesar_pendiente(
        self,
        row: Dict[str, Any],
        catalogo: Dict[str, str],
        metrics: HomologacionMetrics,
    ) -> None:
        """
        Intenta auto-match por idhml_original exacto contra catálogo.
        Si match → actualiza tipo_asignacion = 'AUTO_EXACTO' + codigo_canonico.
        """
        idhml = str(row.get("idhml_original") or "").strip()
        idhml_key = idhml.upper()

        if idhml_key and idhml_key in catalogo:
            codigo_canon = catalogo[idhml_key]
            self._actualizar_equivalencia(
                id_equiv=row["id"],
                codigo_canonico=codigo_canon,
                tipo_asignacion="AUTO_EXACTO",
                motivo=f"Auto-match exacto: idhml '{idhml}' = codigo '{codigo_canon}'",
            )
            metrics.auto_exacto += 1
            logger.debug("  Match: %s → %s", row["nbProducto"], codigo_canon)
        else:
            metrics.sin_match += 1
            logger.debug("  Sin match: nbProducto=%s idhml='%s'", row["nbProducto"], idhml)

    def _actualizar_equivalencia(
        self,
        id_equiv: int,
        codigo_canonico: str,
        tipo_asignacion: str,
        motivo: str,
        usuario: str = "JOB_HOMOLOGACION",
    ) -> None:
        sql = text("""
            UPDATE powerbi_bimbo.bi_equivalencias
            SET codigo_canonico = :canon,
                tipo_asignacion = :tipo,
                estado_sync     = 'NO_REQUIERE',
                usuario_cambio  = :usr,
                motivo_cambio   = :motivo,
                fecha_modificacion = NOW()
            WHERE id = :id AND dt_fin IS NULL
        """)
        with self.config.engine_mysql_bi.connect() as conn:
            conn.execute(sql, {
                "canon": codigo_canonico,
                "tipo": tipo_asignacion,
                "motivo": motivo,
                "usr": usuario,
                "id": id_equiv,
            })
            conn.commit()
