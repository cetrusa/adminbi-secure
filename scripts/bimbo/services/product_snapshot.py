"""
Product Snapshot: Extrae productos de mproductos e inserta SCD2.

v2 Corregido:
🔴 REGLA 2: Solo mproductos, NUNCA cuboventas
🔴 REGLA 3: Clave = (id_agencia, nbProducto)
🔴 REGLA 7: Solo productos del proveedor BIMBO (idProveedor = id_proveedor_bimbo)
🔴 Descarte ANTES de insertar
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts.bimbo.repositories.bimbo_repository import (
    BimboRepository,
    ProductoSidis,
)

logger = logging.getLogger(__name__)


@dataclass
class SnapshotMetrics:
    """Métricas acumuladas de un snapshot por agencia."""
    total: int = 0
    nuevos: int = 0
    actualizados: int = 0
    sin_cambio: int = 0
    descartados: int = 0
    auto_asignados: int = 0
    errores: int = 0


class ProductSnapshot:
    """Extrae productos BIMBO de mproductos y gestiona SCD2."""

    def __init__(self, repo: BimboRepository):
        self._repo = repo

    def extraer_productos_sidis(
        self,
        engine_sidis: Engine,
        db_sidis: str,
        id_proveedor_bimbo: str,
    ) -> List[ProductoSidis]:
        """
        Extrae productos BIMBO desde {dbSidis}.mproductos.

        🔴 REGLA 7: WHERE idProveedor = :id_proveedor_bimbo (un solo proveedor).
        🔴 REGLA 2: Solo mproductos, NUNCA cuboventas.

        Args:
            engine_sidis: Engine al servidor SIDIS.
            db_sidis: Nombre BD SIDIS (dbamovil_xxx).
            id_proveedor_bimbo: ID del proveedor BIMBO en esta agencia.

        Returns:
            Lista de ProductoSidis.
        """
        sql = text(f"""
            SELECT DISTINCT
                mp.nbProducto,
                mp.nmProducto,
                mp.idhmlProdProv
            FROM {db_sidis}.mproductos mp
            WHERE mp.idProveedor = :id_prov
            ORDER BY mp.nbProducto
        """)

        try:
            with engine_sidis.connect() as conn:
                rows = conn.execute(sql, {"id_prov": id_proveedor_bimbo}).mappings().all()
        except Exception as exc:
            logger.error(
                "Error extrayendo productos de %s.mproductos (prov=%s): %s",
                db_sidis, id_proveedor_bimbo, exc,
            )
            return []

        productos = [
            ProductoSidis(
                nbProducto=str(r["nbProducto"]).strip(),
                nmProducto=r.get("nmProducto"),
                idhmlProdProv=r.get("idhmlProdProv"),
            )
            for r in rows
        ]

        logger.info(
            "%s: %d productos BIMBO extraídos (proveedor=%s)",
            db_sidis, len(productos), id_proveedor_bimbo,
        )
        return productos

    def procesar_snapshot(
        self,
        id_agencia: int,
        id_proveedor_agencia: Optional[int],
        productos: List[ProductoSidis],
        palabras_descarte: List[str],
        codigos_canonicos: Set[str],
    ) -> SnapshotMetrics:
        """
        Procesa productos extraídos: descarte → SCD2 → auto-match.

        🔴 Descarte ANTES de insertar.

        Args:
            id_agencia: ID agencia BIMBO.
            id_proveedor_agencia: ID en proveedores_agencia_bimbo.
            productos: Lista de ProductoSidis extraídos.
            palabras_descarte: Palabras para auto-descarte.
            codigos_canonicos: Set de codigo_bimbo disponibles.

        Returns:
            SnapshotMetrics con contadores.
        """
        metrics = SnapshotMetrics(total=len(productos))

        for prod in productos:
            try:
                # ----- PASO 1: DESCARTE -----
                motivo_descarte = self._verificar_descarte(prod, palabras_descarte)
                if motivo_descarte:
                    self._procesar_descartado(
                        id_agencia, id_proveedor_agencia, prod, motivo_descarte
                    )
                    metrics.descartados += 1
                    continue

                # ----- PASO 2: SCD2 -----
                existente = self._repo.get_equivalencia_vigente(id_agencia, prod.nbProducto)

                if existente:
                    if existente["idhml_original"] == prod.idhmlProdProv:
                        # Sin cambio → idempotente
                        metrics.sin_cambio += 1
                    else:
                        # idhml cambió → cerrar anterior + insertar nueva
                        self._repo.cerrar_vigencia(
                            existente["id"],
                            motivo=f"idhml cambió: {existente['idhml_original']} → {prod.idhmlProdProv}",
                        )
                        self._insertar_nueva(
                            id_agencia, id_proveedor_agencia, prod, codigos_canonicos, metrics
                        )
                        metrics.actualizados += 1
                else:
                    # Nuevo → insertar
                    self._insertar_nueva(
                        id_agencia, id_proveedor_agencia, prod, codigos_canonicos, metrics
                    )
                    metrics.nuevos += 1

            except Exception as exc:
                logger.error("Error procesando %s agencia %s: %s", prod.nbProducto, id_agencia, exc)
                metrics.errores += 1

        return metrics

    def _verificar_descarte(self, producto: ProductoSidis, palabras: List[str]) -> Optional[str]:
        """Retorna motivo de descarte o None."""
        nm_upper = (producto.nmProducto or "").upper()
        idhml_upper = (producto.idhmlProdProv or "").upper()

        if idhml_upper in ("NO ES PRODUCTO", "NO ES BIMBO", ""):
            return f"idhml indica no-producto: '{producto.idhmlProdProv}'"

        for palabra in palabras:
            if palabra.upper() in nm_upper:
                return f"Nombre contiene '{palabra}': '{producto.nmProducto}'"

        return None

    def _procesar_descartado(
        self, id_agencia: int, id_prov: Optional[int],
        producto: ProductoSidis, motivo: str,
    ) -> None:
        self._repo.insertar_descartado(id_agencia, id_prov, producto, motivo, "DESCARTE")
        existente = self._repo.get_equivalencia_vigente(id_agencia, producto.nbProducto)
        if not existente:
            self._repo.insertar_equivalencia(
                id_agencia, id_prov, producto,
                tipo_asignacion="DESCARTADO", es_producto_real=0, motivo=motivo,
            )

    def _insertar_nueva(
        self, id_agencia: int, id_prov: Optional[int],
        producto: ProductoSidis, codigos_canonicos: Set[str], metrics: SnapshotMetrics,
    ) -> None:
        idhml = (producto.idhmlProdProv or "").strip()
        if idhml and idhml in codigos_canonicos:
            self._repo.insertar_equivalencia(
                id_agencia, id_prov, producto,
                tipo_asignacion="AUTO_EXACTO", codigo_canonico=idhml,
                estado_sync="NO_REQUIERE", motivo="Auto-match: código exacto en canónico",
            )
            metrics.auto_asignados += 1
        else:
            self._repo.insertar_equivalencia(
                id_agencia, id_prov, producto,
                tipo_asignacion="PENDIENTE", motivo="Pendiente de match",
            )
