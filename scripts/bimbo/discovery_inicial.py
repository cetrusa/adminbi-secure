"""
Discovery inicial BIMBO — multi-proveedor.

Busca TODOS los proveedores cuyo NIT = 830002366 en mproveedores
e inserta cada uno en proveedores_agencia_bimbo.

Patrón idéntico a main.py / ExtraeBiConfig:
  ConfigBasic(database_name) → config dict
  DataBaseConnection(config) → engine_mysql_bi + engine_mysql_out

El database_name viene de agencias_bimbo.db_powerbi que es
el mismo conf_empresas.name, igual que self.name en main.py.

Uso como clase (desde task):
    service = BimboDiscovery(database_name="olpar_ibague")
    resultado = service.run()

Uso como batch (todas las agencias):
    resultado = run_discovery_todas()
"""

from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy import text

from scripts.conexion import Conexion as con
from scripts.config import ConfigBasic

logger = logging.getLogger(__name__)

# NIT oficial de BIMBO Colombia
NIT_BIMBO = "830002366"


class BimboDiscovery:
    """
    Discovery de proveedores BIMBO para una agencia (multi-proveedor).

    Sigue el patrón de main.py Inicio.configurar:
      ConfigBasic(database_name) → config
      DataBaseConnection(config) → engine_mysql_bi + engine_mysql_out

    engine_mysql_bi  → servidor BI  (hostServerIn) → powerbi_bimbo
    engine_mysql_out → servidor SIDIS (hostServerOut) → dbamovil_*
    """

    def __init__(self, database_name: str):
        """
        Args:
            database_name: Nombre de empresa en conf_empresas (= agencias_bimbo.db_powerbi)
                           Ej: 'olpar_ibague', 'cima_b', 'sidimat_bimbo'
        """
        self.name = database_name

        # Patrón main.py: ConfigBasic → config dict completo
        self.config_basic = ConfigBasic(database_name)
        self.config = self.config_basic.config

        # engine_mysql_bi  → servidor BI  (dbBi)
        # engine_mysql_out → servidor SIDIS (dbSidis)
        self.engine_mysql_bi = self._create_engine_bi()
        self.engine_mysql_out = self._create_engine_out()

        self.db_sidis = str(self.config.get("dbSidis", ""))

    def _create_engine_bi(self):
        c = self.config
        return con.ConexionMariadb3(
            str(c.get("nmUsrIn")),
            str(c.get("txPassIn")),
            str(c.get("hostServerIn")),
            int(c.get("portServerIn")),
            str(c.get("dbBi")),
        )

    def _create_engine_out(self):
        c = self.config
        return con.ConexionMariadb3(
            str(c.get("nmUsrOut")),
            str(c.get("txPassOut")),
            str(c.get("hostServerOut")),
            int(c.get("portServerOut")),
            str(c.get("dbSidis")),
        )

    def run(self) -> Dict[str, Any]:
        """
        Busca TODOS los proveedores BIMBO por NIT en SIDIS,
        registra cada uno en proveedores_agencia_bimbo,
        y obtiene idProveedorFVP.
        """
        logger.info("Discovery BIMBO para '%s' (dbSidis=%s)", self.name, self.db_sidis)

        if not self.db_sidis:
            logger.warning("  Sin dbSidis en conf_empresas -> SKIP")
            return {"estado": "SKIP", "motivo": "Sin dbSidis", "name": self.name}

        # Buscar TODOS los proveedores BIMBO por NIT en SIDIS
        proveedores = self._buscar_proveedores_bimbo()

        # Obtener idProveedorFVP desde mempresa
        id_fvp = self._obtener_fvp()

        # Obtener id de la agencia en agencias_bimbo
        id_agencia = self._get_id_agencia()

        if id_agencia and proveedores:
            # Registrar CADA proveedor en proveedores_agencia_bimbo
            for id_prov, nm_prov, nb_doc in proveedores:
                self._registrar_proveedor(id_agencia, id_prov, nm_prov, nb_doc)

            # Actualizar campo legacy id_proveedor_bimbo con el primero
            primer_prov = proveedores[0][0]
            self._actualizar_agencia(primer_prov, id_fvp)
        elif id_agencia:
            self._actualizar_agencia(None, id_fvp)

        ids_proveedores = [p[0] for p in proveedores]
        return {
            "estado": "OK" if proveedores else "SIN_PROVEEDOR",
            "name": self.name,
            "db_sidis": self.db_sidis,
            "proveedores_bimbo": ids_proveedores,
            # Backward compat: primer proveedor
            "id_proveedor_bimbo": ids_proveedores[0] if ids_proveedores else None,
            "nm_proveedor": proveedores[0][1] if proveedores else None,
            "id_proveedor_fvp": id_fvp,
            "total_proveedores": len(proveedores),
        }

    def _buscar_proveedores_bimbo(self) -> List[Tuple[str, str, Optional[str]]]:
        """
        Busca TODOS los proveedores con NIT BIMBO en mproveedores.
        Retorna lista de (idProveedor, nmProveedor, nbDocumento).
        """
        sql = text(f"""
            SELECT idProveedor, nmProveedor, nbDocumento
            FROM {self.db_sidis}.mproveedores
            WHERE nbDocumento = :nit
            ORDER BY idProveedor
        """)
        try:
            with self.engine_mysql_out.connect() as conn:
                rows = conn.execute(sql, {"nit": NIT_BIMBO}).mappings().all()
            if rows:
                proveedores = [
                    (str(r["idProveedor"]), r["nmProveedor"] or "", r.get("nbDocumento"))
                    for r in rows
                ]
                logger.info(
                    "  %d proveedor(es) BIMBO encontrados: %s",
                    len(proveedores),
                    ", ".join(f"id={p[0]} ({p[1]})" for p in proveedores),
                )
                return proveedores
            logger.warning("  NIT %s no encontrado en %s.mproveedores", NIT_BIMBO, self.db_sidis)
        except Exception as exc:
            logger.error("  Error buscando proveedores: %s", exc)
        return []

    def _obtener_fvp(self) -> Optional[str]:
        sql = text(f"SELECT idProveedorFVP FROM {self.db_sidis}.mempresa LIMIT 1")
        try:
            with self.engine_mysql_out.connect() as conn:
                row = conn.execute(sql).first()
            if row and row[0]:
                fvp = str(row[0])
                logger.info("  idProveedorFVP=%s", fvp)
                return fvp
            logger.warning("  mempresa sin idProveedorFVP")
        except Exception as exc:
            logger.error("  Error consultando mempresa: %s", exc)
        return None

    def _get_id_agencia(self) -> Optional[int]:
        """Obtiene agencias_bimbo.id usando db_powerbi = self.name."""
        sql = text(
            "SELECT id FROM powerbi_bimbo.agencias_bimbo WHERE db_powerbi = :name LIMIT 1"
        )
        try:
            with self.engine_mysql_bi.connect() as conn:
                row = conn.execute(sql, {"name": self.name}).first()
                return row[0] if row else None
        except Exception as exc:
            logger.error("  Error obteniendo id_agencia: %s", exc)
            return None

    def _registrar_proveedor(
        self, id_agencia: int, id_prov: str, nm_prov: str, nb_doc: Optional[str]
    ):
        """Inserta/actualiza proveedor en proveedores_agencia_bimbo."""
        sql = text("""
            INSERT INTO powerbi_bimbo.proveedores_agencia_bimbo
                (id_agencia, id_proveedor_sidis, nm_proveedor_sidis,
                 nb_documento, es_confirmado, fecha_confirmacion, confirmado_por)
            VALUES (:id_ag, :id_prov, :nm, :doc, 1, NOW(), 'JOB_DISCOVERY')
            ON DUPLICATE KEY UPDATE
                nm_proveedor_sidis = VALUES(nm_proveedor_sidis),
                nb_documento = VALUES(nb_documento)
        """)
        try:
            with self.engine_mysql_bi.connect() as conn:
                conn.execute(sql, {
                    "id_ag": id_agencia, "id_prov": id_prov,
                    "nm": nm_prov, "doc": nb_doc,
                })
                conn.commit()
            logger.info("  Proveedor %s registrado en proveedores_agencia_bimbo", id_prov)
        except Exception as exc:
            logger.error("  Error registrando proveedor %s: %s", id_prov, exc)

    def _actualizar_agencia(self, id_prov: Optional[str], id_fvp: Optional[str]):
        """UPDATE campo legacy id_proveedor_bimbo y id_proveedor_fvp."""
        updates = []
        params: Dict[str, Any] = {"name": self.name}
        if id_prov:
            updates.append("id_proveedor_bimbo = :prov")
            params["prov"] = id_prov
        if id_fvp:
            updates.append("id_proveedor_fvp = :fvp")
            params["fvp"] = id_fvp
        if not updates:
            return
        sql = text(
            f"UPDATE powerbi_bimbo.agencias_bimbo SET {', '.join(updates)} WHERE db_powerbi = :name"
        )
        with self.engine_mysql_bi.connect() as conn:
            conn.execute(sql, params)
            conn.commit()
        logger.info("  agencias_bimbo actualizada (db_powerbi=%s)", self.name)


def run_discovery_todas() -> list:
    """
    Ejecuta BimboDiscovery para todas las agencias BIMBO.
    Obtiene la lista desde powerbi_adm.conf_empresas WHERE es_bimbo = 1.
    """
    from scripts.config import default_repository_factory

    repo = default_repository_factory()
    filas = repo.run_query(text(
        "SELECT name FROM powerbi_adm.conf_empresas "
        "WHERE es_bimbo = 1 ORDER BY name"
    ))

    if not filas:
        logger.warning("No se encontraron empresas con es_bimbo=1 en conf_empresas")
        return []

    logger.info("=" * 60)
    logger.info("DISCOVERY TODAS — %d agencias BIMBO", len(filas))
    logger.info("=" * 60)

    resultados = []
    for fila in filas:
        db_name = fila["name"]
        try:
            service = BimboDiscovery(db_name)
            resultado = service.run()
        except Exception as exc:
            logger.error("Error agencia '%s': %s", db_name, exc)
            resultado = {"estado": "ERROR", "name": db_name, "motivo": str(exc)}
        resultados.append(resultado)
        provs = resultado.get("proveedores_bimbo", [])
        logger.info("  %-25s -> %s (proveedores: %s)", db_name, resultado.get("estado"), provs)

    return resultados
