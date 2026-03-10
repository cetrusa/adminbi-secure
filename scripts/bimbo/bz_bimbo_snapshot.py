"""
bz_bimbo_snapshot: Snapshot diario de productos BIMBO (multi-proveedor).

Itera sobre TODOS los proveedores confirmados en proveedores_agencia_bimbo,
extrae productos de mproductos para cada uno y gestiona SCD2.

Patrón idéntico a main.py / ExtraeBiConfig:
  ConfigBasic(database_name) -> config dict
  DataBaseConnection(config) -> engine_mysql_bi + engine_mysql_out

El database_name viene de agencias_bimbo.db_powerbi = conf_empresas.name.

Uso desde task (igual que extrae_bi_task(database_name, ...)):
    resultado = run_bimbo_snapshot(database_name="olpar_ibague")
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import date
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic
from scripts.bimbo.repositories.bimbo_repository import AgenciaBimbo, BimboRepository
from scripts.bimbo.services.provider_discovery import ProviderDiscovery
from scripts.bimbo.services.product_snapshot import ProductSnapshot, SnapshotMetrics

logger = logging.getLogger(__name__)


class BimboSnapshotConfig:
    """
    Configuracion de conexiones por agencia.

    Sigue el patron de ExtraeBiConfig y main.py Inicio.configurar:
      ConfigBasic(database_name) -> config
      engine_mysql_bi  -> servidor BI  (hostServerIn) -> dbBi
      engine_mysql_out -> servidor SIDIS (hostServerOut) -> dbSidis
    """

    def __init__(self, database_name: str):
        self.name = database_name
        self.config_basic = ConfigBasic(database_name)
        self.config = self.config_basic.config
        self.engine_mysql_bi = self._create_engine_bi()
        self.engine_mysql_out = self._create_engine_out()

    def _create_engine_bi(self) -> Engine:
        c = self.config
        return con.ConexionMariadb3(
            str(c.get("nmUsrIn")),
            str(c.get("txPassIn")),
            str(c.get("hostServerIn")),
            int(c.get("portServerIn")),
            str(c.get("dbBi")),
        )

    def _create_engine_out(self) -> Engine:
        c = self.config
        return con.ConexionMariadb3(
            str(c.get("nmUsrOut")),
            str(c.get("txPassOut")),
            str(c.get("hostServerOut")),
            int(c.get("portServerOut")),
            str(c.get("dbSidis")),
        )

    @property
    def db_sidis(self) -> str:
        return str(self.config.get("dbSidis", ""))


class BimboSnapshot:
    """
    Job de snapshot BIMBO para una agencia (multi-proveedor).

    Recibe database_name (conf_empresas.name) igual que todos
    los scripts de DataZenith.
    """

    def __init__(self, database_name: str, job_id: Optional[str] = None):
        self.database_name = database_name
        self.job_id = job_id or f"bimbo_snap_{uuid.uuid4().hex[:8]}"

        self.ag_config = BimboSnapshotConfig(database_name)
        self.engine_bi = self.ag_config.engine_mysql_bi
        self.engine_sidis = self.ag_config.engine_mysql_out

        self.repo = BimboRepository(self.engine_bi)
        self.discovery = ProviderDiscovery(self.repo)
        self.snapshot_svc = ProductSnapshot(self.repo)

    # -- Lock advisory --------------------------------------------------------

    def _adquirir_lock(self, id_agencia: int, timeout: int = 10) -> bool:
        try:
            with self.engine_bi.connect() as conn:
                row = conn.execute(
                    text("SELECT GET_LOCK(:name, :timeout) AS locked"),
                    {"name": f"bimbo_snap_{id_agencia}", "timeout": timeout},
                ).first()
                return row[0] == 1 if row else False
        except Exception:
            return False

    def _liberar_lock(self, id_agencia: int) -> None:
        try:
            with self.engine_bi.connect() as conn:
                conn.execute(
                    text("SELECT RELEASE_LOCK(:name)"),
                    {"name": f"bimbo_snap_{id_agencia}"},
                )
        except Exception:
            pass

    # -- Ejecucion ------------------------------------------------------------

    def ejecutar(self, solo_discovery: bool = False) -> Dict[str, Any]:
        """
        Ejecuta el snapshot para la agencia configurada en database_name.
        Itera sobre TODOS los proveedores confirmados en proveedores_agencia_bimbo.
        """
        inicio = time.time()
        logger.info("=" * 60)
        logger.info("BIMBO SNAPSHOT — db=%s job=%s", self.database_name, self.job_id)
        logger.info("=" * 60)

        resultado: Dict[str, Any] = {
            "database_name": self.database_name,
            "estado": "ERROR",
        }

        # Obtener agencia desde powerbi_bimbo
        agencia = self._get_agencia()
        if not agencia:
            resultado.update({"estado": "SKIP", "motivo": f"Agencia '{self.database_name}' no en agencias_bimbo"})
            return resultado

        db_sidis = self.ag_config.db_sidis
        if not db_sidis:
            resultado.update({"estado": "SKIP", "motivo": "Sin dbSidis en conf_empresas"})
            return resultado

        # Obtener proveedores desde proveedores_agencia_bimbo (multi-proveedor)
        proveedores = self.repo.get_proveedores_agencia(agencia.id)
        if not proveedores and agencia.id_proveedor_bimbo:
            # Fallback: usar campo legacy si no hay nada en junction table
            logger.warning("  Sin proveedores en junction table, usando campo legacy id_proveedor_bimbo=%s",
                           agencia.id_proveedor_bimbo)
            proveedores = [{"id": None, "id_proveedor_sidis": agencia.id_proveedor_bimbo,
                            "nm_proveedor_sidis": "LEGACY", "nb_documento": None}]

        if not proveedores:
            resultado.update({"estado": "SKIP", "motivo": "Sin proveedores configurados (ejecutar discovery primero)"})
            logger.warning("  Sin proveedores -> SKIP")
            return resultado

        prov_ids = [p["id_proveedor_sidis"] for p in proveedores]
        logger.info("  Proveedores: %s", ", ".join(prov_ids))

        if not self._adquirir_lock(agencia.id):
            resultado.update({"estado": "SKIP", "motivo": "Lock no disponible"})
            return resultado

        try:
            # -- FASE A: Validar proveedores contra mproveedores --
            proveedores_validados = self.discovery.validar_proveedores_bimbo(
                self.engine_sidis, db_sidis, agencia
            )
            if not proveedores_validados:
                resultado.update({
                    "estado": "SKIP",
                    "motivo": f"Proveedores {prov_ids} no validados en {db_sidis}.mproveedores",
                })
                return resultado

            fvp = self.discovery.obtener_id_agente(self.engine_sidis, db_sidis, agencia)

            # Registrar cada proveedor validado
            prov_agencia_ids: Dict[str, Optional[int]] = {}
            for id_prov, nm_prov, nb_doc in proveedores_validados:
                pa_id = self.discovery.registrar_proveedor(agencia, id_prov, nm_prov, nb_doc)
                prov_agencia_ids[id_prov] = pa_id

            if solo_discovery:
                resultado.update({
                    "estado": "OK",
                    "proveedores_bimbo": [p[0] for p in proveedores_validados],
                    "total_proveedores": len(proveedores_validados),
                    "id_proveedor_fvp": fvp,
                })
                return resultado

            # -- FASE B: Snapshot Productos (por cada proveedor) --
            palabras_descarte = self.repo.get_palabras_descarte()
            codigos_canonicos = self.repo.get_catalogo_codigos()

            metrics_total = SnapshotMetrics()
            for id_prov, nm_prov, nb_doc in proveedores_validados:
                logger.info("  Extrayendo %s.mproductos (proveedor=%s '%s')...",
                            db_sidis, id_prov, nm_prov)

                productos = self.snapshot_svc.extraer_productos_sidis(
                    self.engine_sidis, db_sidis, id_prov
                )

                if not productos:
                    logger.warning("    0 productos para proveedor %s", id_prov)
                    continue

                # -- FASES C-E: Descarte + SCD2 + Auto-Match --
                metrics = self.snapshot_svc.procesar_snapshot(
                    id_agencia=agencia.id,
                    id_proveedor_agencia=prov_agencia_ids.get(id_prov),
                    productos=productos,
                    palabras_descarte=palabras_descarte,
                    codigos_canonicos=codigos_canonicos,
                )

                logger.info("    Proveedor %s: total=%d nuevos=%d auto=%d desc=%d",
                            id_prov, metrics.total, metrics.nuevos,
                            metrics.auto_asignados, metrics.descartados)

                # Acumular metricas
                metrics_total.total += metrics.total
                metrics_total.nuevos += metrics.nuevos
                metrics_total.actualizados += metrics.actualizados
                metrics_total.sin_cambio += metrics.sin_cambio
                metrics_total.descartados += metrics.descartados
                metrics_total.auto_asignados += metrics.auto_asignados
                metrics_total.errores += metrics.errores

            # -- FASE F: Metricas consolidadas --
            total_reales = metrics_total.total - metrics_total.descartados
            cobertura = round(metrics_total.auto_asignados / total_reales * 100, 2) if total_reales > 0 else 0
            duracion = round(time.time() - inicio, 1)

            self.repo.insertar_snapshot(
                id_agencia=agencia.id,
                fecha=date.today().isoformat(),
                metricas={
                    "total": metrics_total.total,
                    "nuevos": metrics_total.nuevos,
                    "auto_asignados": metrics_total.auto_asignados,
                    "descartados": metrics_total.descartados,
                    "pendientes": metrics_total.total - metrics_total.auto_asignados - metrics_total.descartados - metrics_total.sin_cambio,
                    "cobertura_pct": cobertura,
                    "estado_job": "OK" if metrics_total.errores == 0 else "PARCIAL",
                    "detalle_error": f"{metrics_total.errores} errores" if metrics_total.errores > 0 else None,
                    "duracion_seg": int(duracion),
                },
                job_id=self.job_id,
            )
            self.repo.actualizar_ultimo_snapshot(agencia.id)

            logger.info("  CONSOLIDADO: Total=%d | Nuevos=%d | Auto=%d | Desc=%d | %.1f%% | %d proveedores",
                         metrics_total.total, metrics_total.nuevos, metrics_total.auto_asignados,
                         metrics_total.descartados, cobertura, len(proveedores_validados))

            resultado.update({
                "estado": "OK",
                "total": metrics_total.total,
                "nuevos": metrics_total.nuevos,
                "auto_asignados": metrics_total.auto_asignados,
                "descartados": metrics_total.descartados,
                "sin_cambio": metrics_total.sin_cambio,
                "errores": metrics_total.errores,
                "cobertura_pct": cobertura,
                "duracion_seg": duracion,
                "proveedores_procesados": len(proveedores_validados),
            })

        except Exception as exc:
            logger.exception("Error snapshot '%s': %s", self.database_name, exc)
            resultado["motivo"] = str(exc)
        finally:
            self._liberar_lock(agencia.id if agencia else 0)

        return resultado

    def _get_agencia(self) -> Optional[AgenciaBimbo]:
        """Obtiene la agencia desde powerbi_bimbo usando db_powerbi = self.database_name."""
        agencias = [
            a for a in self.repo.get_todas_agencias()
            if a.db_powerbi == self.database_name
        ]
        return agencias[0] if agencias else None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_bimbo_snapshot(
    database_name: str,
    solo_discovery: bool = False,
) -> Dict[str, Any]:
    """
    Funcion de entrada para DataZenith (RQ task).

    Args:
        database_name: conf_empresas.name de la agencia (= agencias_bimbo.db_powerbi)
                       Ej: 'olpar_ibague', 'cima_b'
        solo_discovery: Si True, solo valida proveedor + FVP.
    """
    job = BimboSnapshot(database_name)
    return job.ejecutar(solo_discovery=solo_discovery)
