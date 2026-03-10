"""
Provider Discovery: Valida proveedores BIMBO en SIDIS (multi-proveedor).

Reglas:
  - idProveedorBimbo viene de proveedores_agencia_bimbo (NO LIKE)
  - idProveedorFVP viene de mempresa
  - Cada proveedor se valida individualmente contra mproveedores
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts.bimbo.repositories.bimbo_repository import (
    AgenciaBimbo,
    BimboRepository,
)

logger = logging.getLogger(__name__)


class ProviderDiscovery:
    """Valida y registra proveedores BIMBO en SIDIS (multi-proveedor)."""

    def __init__(self, repo: BimboRepository):
        self._repo = repo

    def validar_proveedores_bimbo(
        self,
        engine_sidis: Engine,
        db_sidis: str,
        agencia: AgenciaBimbo,
    ) -> List[Tuple[str, str, Optional[str]]]:
        """
        Valida TODOS los proveedores de la agencia contra mproveedores.

        Busca en proveedores_agencia_bimbo los IDs confirmados,
        luego verifica que cada uno exista en mproveedores.

        Args:
            engine_sidis: Engine conectado al servidor SIDIS.
            db_sidis: Nombre BD SIDIS (dbamovil_xxx).
            agencia: AgenciaBimbo.

        Returns:
            Lista de (idProveedor, nmProveedor, nbDocumento) validados.
        """
        # Obtener IDs de proveedores desde la junction table
        prov_ids = self._repo.get_proveedores_ids(agencia.id)

        # Fallback: si no hay en junction, usar campo legacy
        if not prov_ids and agencia.id_proveedor_bimbo:
            prov_ids = [agencia.id_proveedor_bimbo]

        if not prov_ids:
            logger.warning(
                "Agencia %s (%s): sin proveedores configurados -> SKIP",
                agencia.id, agencia.nombre,
            )
            return []

        validados = []
        for id_prov in prov_ids:
            resultado = self._validar_un_proveedor(engine_sidis, db_sidis, agencia, id_prov)
            if resultado:
                validados.append(resultado)

        if not validados:
            logger.warning(
                "Agencia %s (%s): ninguno de los %d proveedores fue validado en mproveedores",
                agencia.id, agencia.nombre, len(prov_ids),
            )

        return validados

    def validar_proveedor_bimbo(
        self,
        engine_sidis: Engine,
        db_sidis: str,
        agencia: AgenciaBimbo,
    ) -> Optional[Tuple[str, str, Optional[str]]]:
        """
        Backward-compatible: valida el primer proveedor disponible.
        Usa proveedores_agencia_bimbo, con fallback a id_proveedor_bimbo.
        """
        validados = self.validar_proveedores_bimbo(engine_sidis, db_sidis, agencia)
        return validados[0] if validados else None

    def _validar_un_proveedor(
        self,
        engine_sidis: Engine,
        db_sidis: str,
        agencia: AgenciaBimbo,
        id_prov: str,
    ) -> Optional[Tuple[str, str, Optional[str]]]:
        """Valida un idProveedor exacto en mproveedores (NO LIKE)."""
        sql = text(f"""
            SELECT idProveedor, nmProveedor, nbDocumento
            FROM {db_sidis}.mproveedores
            WHERE idProveedor = :id_prov
            LIMIT 1
        """)

        try:
            with engine_sidis.connect() as conn:
                row = conn.execute(sql, {"id_prov": id_prov}).mappings().first()
        except Exception as exc:
            logger.error(
                "Agencia %s: error consultando mproveedores en %s (prov=%s): %s",
                agencia.id, db_sidis, id_prov, exc,
            )
            return None

        if not row:
            logger.warning(
                "Agencia %s (%s): idProveedor=%s NO encontrado en %s.mproveedores",
                agencia.id, agencia.nombre, id_prov, db_sidis,
            )
            return None

        logger.info(
            "Agencia %s (%s): Proveedor validado -> id=%s nombre='%s' NIT=%s",
            agencia.id, agencia.nombre,
            row["idProveedor"], row["nmProveedor"], row.get("nbDocumento"),
        )
        return (str(row["idProveedor"]), row["nmProveedor"] or "", row.get("nbDocumento"))

    def registrar_proveedor(
        self,
        agencia: AgenciaBimbo,
        id_prov_sidis: str,
        nm_proveedor: str,
        nb_documento: Optional[str],
    ) -> int:
        """Registra proveedor CONFIRMADO en proveedores_agencia_bimbo."""
        return self._repo.insertar_proveedor_confirmado(
            id_agencia=agencia.id,
            id_prov_sidis=id_prov_sidis,
            nm_proveedor=nm_proveedor,
            nb_documento=nb_documento,
        )

    def obtener_id_agente(
        self,
        engine_sidis: Engine,
        db_sidis: str,
        agencia: AgenciaBimbo,
    ) -> Optional[str]:
        """
        Obtiene idProveedorFVP del agente desde mempresa.
        SELECT idProveedorFVP FROM mempresa LIMIT 1
        """
        sql = text(f"SELECT idProveedorFVP FROM {db_sidis}.mempresa LIMIT 1")

        try:
            with engine_sidis.connect() as conn:
                row = conn.execute(sql).first()
        except Exception as exc:
            logger.error(
                "Agencia %s: error consultando mempresa en %s: %s",
                agencia.id, db_sidis, exc,
            )
            return None

        if row and row[0]:
            fvp = str(row[0])
            logger.info("Agencia %s (%s): idProveedorFVP = %s", agencia.id, agencia.nombre, fvp)
            self._repo.actualizar_proveedor_fvp(agencia.id, fvp)
            return fvp
        else:
            logger.warning("Agencia %s: mempresa sin idProveedorFVP", agencia.id)
            return None
