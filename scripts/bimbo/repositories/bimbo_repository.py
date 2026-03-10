"""
Repositorio BIMBO: Acceso a datos en powerbi_bimbo.

Centraliza queries hacia las tablas de homologación BIMBO.
Usa SQLAlchemy text() para queries parametrizadas.

Correcciones v2:
- idProveedorBimbo desde agencias_bimbo (NO LIKE)
- idProveedorFVP desde mempresa per SIDIS
- ConfigBasic per agencia
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class AgenciaBimbo:
    """Agencia BIMBO con datos de conexión SIDIS."""
    id: int
    id_agente: int
    nombre: str
    db_powerbi: Optional[str]
    estado: str
    id_proveedor_bimbo: Optional[str] = None   # LEGACY: primer proveedor
    id_proveedor_fvp: Optional[str] = None
    # Datos de conf_empresas (via ConfigBasic)
    db_sidis: Optional[str] = None
    # Multi-proveedor: lista de IDs (poblada desde proveedores_agencia_bimbo)
    proveedores_ids: List[str] = field(default_factory=list)


@dataclass
class ProductoSidis:
    """Producto extraído de mproductos."""
    nbProducto: str
    nmProducto: Optional[str]
    idhmlProdProv: Optional[str]


@dataclass
class ReglaBimbo:
    """Regla de configuración."""
    tipo_regla: str
    clave: str
    valor: str


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class BimboRepository:
    """Acceso a datos BIMBO en powerbi_bimbo."""

    def __init__(self, engine_bimbo: Engine):
        self._engine = engine_bimbo

    # -- Agencias ----------------------------------------------------------

    def get_agencias_activas(self) -> List[AgenciaBimbo]:
        """Obtiene agencias ACTIVAS con id_proveedor_bimbo."""
        sql = text("""
            SELECT id, id_agente, Nombre, db_powerbi, estado,
                   id_proveedor_bimbo, id_proveedor_fvp
            FROM powerbi_bimbo.agencias_bimbo
            WHERE estado = 'ACTIVO'
            ORDER BY id
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()
        return [self._map_agencia(r) for r in rows]

    def get_todas_agencias(self) -> List[AgenciaBimbo]:
        """Obtiene TODAS las agencias."""
        sql = text("""
            SELECT id, id_agente, Nombre, db_powerbi, estado,
                   id_proveedor_bimbo, id_proveedor_fvp
            FROM powerbi_bimbo.agencias_bimbo
            ORDER BY id
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(sql).mappings().all()
        return [self._map_agencia(r) for r in rows]

    @staticmethod
    def _map_agencia(r) -> AgenciaBimbo:
        return AgenciaBimbo(
            id=r["id"],
            id_agente=r["id_agente"],
            nombre=r["Nombre"],
            db_powerbi=r["db_powerbi"],
            estado=r["estado"],
            id_proveedor_bimbo=r.get("id_proveedor_bimbo"),
            id_proveedor_fvp=r.get("id_proveedor_fvp"),
        )

    def actualizar_proveedor_fvp(self, id_agencia: int, id_proveedor_fvp: str) -> None:
        """Guarda el idProveedorFVP obtenido de mempresa."""
        sql = text(
            "UPDATE powerbi_bimbo.agencias_bimbo "
            "SET id_proveedor_fvp = :fvp WHERE id = :id"
        )
        with self._engine.connect() as conn:
            conn.execute(sql, {"fvp": id_proveedor_fvp, "id": id_agencia})
            conn.commit()

    def actualizar_proveedor_bimbo(self, id_agencia: int, id_prov: str) -> None:
        """Guarda el idProveedorBimbo descubierto."""
        sql = text(
            "UPDATE powerbi_bimbo.agencias_bimbo "
            "SET id_proveedor_bimbo = :prov WHERE id = :id"
        )
        with self._engine.connect() as conn:
            conn.execute(sql, {"prov": id_prov, "id": id_agencia})
            conn.commit()

    def actualizar_ultimo_snapshot(self, id_agencia: int) -> None:
        sql = text(
            "UPDATE powerbi_bimbo.agencias_bimbo "
            "SET fecha_ultimo_snapshot = NOW() WHERE id = :id"
        )
        with self._engine.connect() as conn:
            conn.execute(sql, {"id": id_agencia})
            conn.commit()

    # -- Reglas -------------------------------------------------------------

    def get_reglas(self, tipo_regla: Optional[str] = None) -> List[ReglaBimbo]:
        if tipo_regla:
            sql = text(
                "SELECT tipo_regla, clave, valor "
                "FROM powerbi_bimbo.reglas_bimbo "
                "WHERE activo = 1 AND tipo_regla = :tipo ORDER BY id"
            )
            params = {"tipo": tipo_regla}
        else:
            sql = text(
                "SELECT tipo_regla, clave, valor "
                "FROM powerbi_bimbo.reglas_bimbo "
                "WHERE activo = 1 ORDER BY tipo_regla, id"
            )
            params = {}
        with self._engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()
        return [ReglaBimbo(r["tipo_regla"], r["clave"], r["valor"]) for r in rows]

    def get_palabras_descarte(self) -> List[str]:
        return [r.valor for r in self.get_reglas("DESCARTE")]

    def get_umbral(self, clave: str, default: str = "0") -> str:
        reglas = [r for r in self.get_reglas("UMBRAL") if r.clave == clave]
        return reglas[0].valor if reglas else default

    # -- Proveedores --------------------------------------------------------

    def get_proveedores_agencia(self, id_agencia: int) -> List[Dict[str, Any]]:
        """Obtiene TODOS los proveedores confirmados de una agencia."""
        sql = text("""
            SELECT id, id_proveedor_sidis, nm_proveedor_sidis, nb_documento
            FROM powerbi_bimbo.proveedores_agencia_bimbo
            WHERE id_agencia = :id_ag AND es_confirmado = 1
            ORDER BY id_proveedor_sidis
        """)
        with self._engine.connect() as conn:
            return [dict(r) for r in conn.execute(sql, {"id_ag": id_agencia}).mappings().all()]

    def get_proveedores_ids(self, id_agencia: int) -> List[str]:
        """Retorna lista de id_proveedor_sidis confirmados para la agencia."""
        provs = self.get_proveedores_agencia(id_agencia)
        return [p["id_proveedor_sidis"] for p in provs]

    def get_proveedor_agencia(self, id_agencia: int, id_prov_sidis: str) -> Optional[int]:
        """Obtiene el id de proveedores_agencia_bimbo para un proveedor SIDIS."""
        sql = text("""
            SELECT id FROM powerbi_bimbo.proveedores_agencia_bimbo
            WHERE id_agencia = :id_ag AND id_proveedor_sidis = :id_prov
            LIMIT 1
        """)
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"id_ag": id_agencia, "id_prov": id_prov_sidis}).first()
            return row[0] if row else None

    def insertar_proveedor_confirmado(
        self, id_agencia: int, id_prov_sidis: str,
        nm_proveedor: str, nb_documento: Optional[str],
    ) -> int:
        """Inserta proveedor confirmado en proveedores_agencia_bimbo (ON DUPLICATE KEY UPDATE)."""
        sql = text("""
            INSERT INTO powerbi_bimbo.proveedores_agencia_bimbo
                (id_agencia, id_proveedor_sidis, nm_proveedor_sidis,
                 nb_documento, es_confirmado, fecha_confirmacion, confirmado_por)
            VALUES (:id_ag, :id_prov, :nm, :doc, 1, NOW(), 'JOB_DISCOVERY')
            ON DUPLICATE KEY UPDATE
                nm_proveedor_sidis = VALUES(nm_proveedor_sidis),
                nb_documento = VALUES(nb_documento)
        """)
        with self._engine.connect() as conn:
            result = conn.execute(sql, {
                "id_ag": id_agencia, "id_prov": id_prov_sidis,
                "nm": nm_proveedor, "doc": nb_documento,
            })
            conn.commit()
            return result.lastrowid

    # -- Equivalencias SCD2 -------------------------------------------------

    def get_equivalencia_vigente(self, id_agencia: int, nb_producto: str) -> Optional[Dict[str, Any]]:
        sql = text("""
            SELECT id, idhml_original, codigo_canonico, tipo_asignacion, estado_sync
            FROM powerbi_bimbo.bi_equivalencias
            WHERE id_agencia = :id_ag AND nbProducto = :nb AND dt_fin IS NULL
            LIMIT 1
        """)
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"id_ag": id_agencia, "nb": nb_producto}).mappings().first()
            return dict(row) if row else None

    def cerrar_vigencia(self, id_equivalencia: int, motivo: str, usuario: str = "JOB_SNAPSHOT") -> None:
        sql = text("UPDATE powerbi_bimbo.bi_equivalencias SET dt_fin = NOW() WHERE id = :id AND dt_fin IS NULL")
        with self._engine.connect() as conn:
            conn.execute(sql, {"id": id_equivalencia})
            conn.commit()
        self._log_cambio_equivalencia(id_equivalencia, None, "dt_fin", None, "NOW()", usuario, motivo)

    def insertar_equivalencia(
        self, id_agencia: int, id_proveedor_agencia: Optional[int],
        producto: ProductoSidis, tipo_asignacion: str = "PENDIENTE",
        codigo_canonico: Optional[str] = None, estado_sync: str = "NO_REQUIERE",
        es_producto_real: int = 1, usuario: str = "JOB_SNAPSHOT", motivo: str = "Nuevo producto",
    ) -> int:
        sql = text("""
            INSERT INTO powerbi_bimbo.bi_equivalencias
                (id_agencia, id_proveedor_agencia, nbProducto, nmProducto,
                 idhml_original, codigo_canonico, tipo_asignacion, estado_sync,
                 es_producto_real, dt_inicio, dt_fin, usuario_cambio, motivo_cambio)
            VALUES (:id_ag, :id_prov, :nb, :nm, :idhml, :canon, :tipo, :sync,
                    :real, NOW(), NULL, :usr, :motivo)
        """)
        with self._engine.connect() as conn:
            result = conn.execute(sql, {
                "id_ag": id_agencia, "id_prov": id_proveedor_agencia,
                "nb": producto.nbProducto, "nm": producto.nmProducto,
                "idhml": producto.idhmlProdProv, "canon": codigo_canonico,
                "tipo": tipo_asignacion, "sync": estado_sync, "real": es_producto_real,
                "usr": usuario, "motivo": motivo,
            })
            conn.commit()
            return result.lastrowid

    def get_catalogo_codigos(self) -> Set[str]:
        sql = text("SELECT codigo_bimbo FROM powerbi_bimbo.bi_productos_canonico WHERE estado = 'Disponible'")
        with self._engine.connect() as conn:
            return {r[0] for r in conn.execute(sql).all()}

    def marcar_requiere_update(self, id_agencia: int) -> int:
        sql = text("""
            UPDATE powerbi_bimbo.bi_equivalencias
            SET estado_sync = 'REQUIERE_UPDATE'
            WHERE id_agencia = :id AND dt_fin IS NULL
              AND tipo_asignacion IN ('AUTO_EXACTO', 'MANUAL')
              AND codigo_canonico IS NOT NULL
              AND idhml_original != codigo_canonico
              AND estado_sync = 'NO_REQUIERE'
        """)
        with self._engine.connect() as conn:
            result = conn.execute(sql, {"id": id_agencia})
            conn.commit()
            return result.rowcount

    # -- Descartados --------------------------------------------------------

    def insertar_descartado(
        self, id_agencia: int, id_proveedor_agencia: Optional[int],
        producto: ProductoSidis, motivo: str, regla: str,
    ) -> bool:
        sql = text("""
            INSERT IGNORE INTO powerbi_bimbo.bi_productos_descartados
                (id_agencia, id_proveedor_agencia, nbProducto, nmProducto,
                 idhml_original, motivo_descarte, regla_aplicada)
            VALUES (:id_ag, :id_prov, :nb, :nm, :idhml, :motivo, :regla)
        """)
        with self._engine.connect() as conn:
            result = conn.execute(sql, {
                "id_ag": id_agencia, "id_prov": id_proveedor_agencia,
                "nb": producto.nbProducto, "nm": producto.nmProducto,
                "idhml": producto.idhmlProdProv, "motivo": motivo, "regla": regla,
            })
            conn.commit()
            return result.rowcount > 0

    # -- Snapshots / Logs ---------------------------------------------------

    def insertar_snapshot(self, id_agencia: int, fecha: str, metricas: Dict[str, Any], job_id: Optional[str] = None) -> None:
        sql = text("""
            INSERT INTO powerbi_bimbo.snapshots_diarios
                (id_agencia, fecha_snapshot, total_productos_sidis, nuevos_detectados,
                 auto_asignados, descartados, pendientes_acumulados, cobertura_pct,
                 job_id, estado_job, detalle_error, duracion_seg)
            VALUES (:id_ag, :fecha, :total, :nuevos, :auto, :desc, :pend, :cob,
                    :job, :estado, :error, :dur)
            ON DUPLICATE KEY UPDATE
                total_productos_sidis = VALUES(total_productos_sidis),
                nuevos_detectados = VALUES(nuevos_detectados),
                auto_asignados = VALUES(auto_asignados),
                descartados = VALUES(descartados),
                pendientes_acumulados = VALUES(pendientes_acumulados),
                cobertura_pct = VALUES(cobertura_pct),
                job_id = VALUES(job_id), estado_job = VALUES(estado_job),
                detalle_error = VALUES(detalle_error), duracion_seg = VALUES(duracion_seg)
        """)
        with self._engine.connect() as conn:
            conn.execute(sql, {
                "id_ag": id_agencia, "fecha": fecha,
                "total": metricas.get("total", 0), "nuevos": metricas.get("nuevos", 0),
                "auto": metricas.get("auto_asignados", 0), "desc": metricas.get("descartados", 0),
                "pend": metricas.get("pendientes", 0), "cob": metricas.get("cobertura_pct", 0),
                "job": job_id, "estado": metricas.get("estado_job", "OK"),
                "error": metricas.get("detalle_error"), "dur": metricas.get("duracion_seg", 0),
            })
            conn.commit()

    def _log_cambio_equivalencia(self, id_equiv, id_equiv_nueva, campo, anterior, nuevo, usuario, motivo):
        sql = text("""
            INSERT INTO powerbi_bimbo.log_cambios_equivalencia
                (id_equivalencia, id_equivalencia_nueva, campo_modificado,
                 valor_anterior, valor_nuevo, modificado_por, motivo)
            VALUES (:id, :id_new, :campo, :ant, :nuevo, :usr, :motivo)
        """)
        with self._engine.connect() as conn:
            conn.execute(sql, {
                "id": id_equiv, "id_new": id_equiv_nueva, "campo": campo,
                "ant": anterior, "nuevo": nuevo, "usr": usuario, "motivo": motivo,
            })
            conn.commit()
