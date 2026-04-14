"""
ImportMproductosService: Carga maestras de productos desde Excel al SIDIS.

Flujo:
  1. Leer Bimbo.xlsx — hojas mproductos_{sufijo}
  2. Mapear sufijo → agencia (agencias_bimbo.db_powerbi)
  3. Dry-run: comparar idhmlProdProv Excel vs SIDIS actual
  4. Execute: UPDATE mproductos SET idhmlProdProv WHERE nbProducto + idProveedor

Reglas invariantes:
  - NaN en Excel → omitir fila (nunca escribir NULL al SIDIS)
  - WHERE incluye idProveedor (no tocar otros proveedores)
  - Backup registrado antes de cada UPDATE
  - Solo registros con idhmlProdProv diferente son actualizados
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text

from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con

logger = logging.getLogger(__name__)

MPRODUCTOS_SHEET_PREFIX = "mproductos_"
COLS_MINIMAS = {"nbProducto", "idProveedor", "idhmlProdProv"}


# ---------------------------------------------------------------------------
# Dataclasses de resultado
# ---------------------------------------------------------------------------

@dataclass
class RowResult:
    nbProducto: int
    nmProducto: str
    idProveedor: int
    idhml_excel: Optional[str]
    idhml_sidis: Optional[str]
    estado: str  # "actualizar" | "sin_cambio" | "no_encontrado" | "sin_idhml"


@dataclass
class AgenciaPreview:
    sheet_name: str
    database_name: str
    agencia_id: int
    agencia_nombre: str
    db_sidis: str
    total_excel: int
    actualizar: int
    sin_cambio: int
    no_encontrado: int
    sin_idhml: int
    filas: List[RowResult] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class AgenciaResult:
    database_name: str
    agencia_nombre: str
    actualizados: int
    sin_cambio: int
    no_encontrado: int
    sin_idhml: int
    errores: int
    backup: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Servicio principal
# ---------------------------------------------------------------------------

class ImportMproductosService:
    """
    Servicio para importar idhmlProdProv desde Excel al SIDIS.

    Uso:
        svc = ImportMproductosService(excel_path, engine_bimbo)
        preview = svc.run_preview()          # Dry-run: lista AgenciaPreview
        results = svc.run_execute(           # Escritura real
            agencias_seleccionadas=["cima_b", "caribe_b"],
            usuario="admin"
        )
    """

    def __init__(self, excel_path: str, engine_bimbo):
        self.excel_path = excel_path
        self.engine_bimbo = engine_bimbo
        self._agencias_map: Dict[str, Dict[str, Any]] = {}  # db_powerbi → info agencia

    # -- Paso 1: leer agencias registradas en powerbi_bimbo -------------------

    def _cargar_mapa_agencias(self) -> None:
        """Carga db_powerbi → {id, Nombre, database_name} desde agencias_bimbo."""
        sql = text(
            "SELECT id, Nombre, db_powerbi "
            "FROM powerbi_bimbo.agencias_bimbo "
            "WHERE estado = 'ACTIVO' AND db_powerbi IS NOT NULL "
            "ORDER BY Nombre"
        )
        with self.engine_bimbo.connect() as conn:
            rows = conn.execute(sql).mappings().all()
        self._agencias_map = {
            str(r["db_powerbi"]).strip(): {
                "id": r["id"],
                "nombre": r["Nombre"],
                "database_name": str(r["db_powerbi"]).strip(),
            }
            for r in rows
        }

    # -- Paso 2: leer Excel y mapear sheets -----------------------------------

    def _leer_hojas_mproductos(self) -> Dict[str, pd.DataFrame]:
        """Lee todas las hojas mproductos_* del Excel. Retorna {sheet_name: df}."""
        xl = pd.ExcelFile(self.excel_path)
        hojas = {
            s: pd.read_excel(self.excel_path, sheet_name=s)
            for s in xl.sheet_names
            if s.startswith(MPRODUCTOS_SHEET_PREFIX)
        }
        return hojas

    def _mapear_sheet_a_agencia(self, sheet_name: str) -> Optional[Dict[str, Any]]:
        """
        Intenta encontrar la agencia correspondiente al sheet.
        sheet: "mproductos_cima" → sufijo "cima"
        Estrategias:
          1. db_powerbi == sufijo exacto
          2. db_powerbi termina con _{sufijo} o sufijo_b
          3. sufijo está contenido en db_powerbi
        """
        sufijo = sheet_name[len(MPRODUCTOS_SHEET_PREFIX):]  # "cima"

        # Estrategia 1: match exacto
        if sufijo in self._agencias_map:
            return self._agencias_map[sufijo]

        # Estrategia 2: db_powerbi termina en sufijo o sufijo_b
        for db_name, info in self._agencias_map.items():
            if db_name.endswith(f"_{sufijo}") or db_name.endswith(sufijo):
                return info
            if db_name.endswith(sufijo + "_b") or db_name == sufijo + "_b":
                return info

        # Estrategia 3: sufijo contenido en db_powerbi
        for db_name, info in self._agencias_map.items():
            if sufijo in db_name:
                return info

        return None

    # -- Paso 3: obtener engine SIDIS para una agencia ------------------------

    def _get_engine_sidis(self, database_name: str):
        """Crea engine al SIDIS de la agencia usando ConfigBasic."""
        config = ConfigBasic(database_name).config
        return con.ConexionMariadb3(
            str(config.get("nmUsrOut")),
            str(config.get("txPassOut")),
            str(config.get("hostServerOut")),
            int(config.get("portServerOut")),
            str(config.get("dbSidis")),
        ), str(config.get("dbSidis", ""))

    # -- Normalizar idhmlProdProv desde float pandas --------------------------

    @staticmethod
    def _normalizar_idhml(val) -> Optional[str]:
        """
        Convierte valor pandas a string limpio para comparar/escribir.
        NaN → None (no tocar)
        418.0 → "418"
        "418" → "418"
        """
        if val is None:
            return None
        if isinstance(val, float):
            if val != val:  # NaN (IEEE 754)
                return None
            # float sin decimales → entero
            if val == int(val):
                return str(int(val))
            return str(val)
        if isinstance(val, int):
            return str(val)
        s = str(val).strip()
        return s if s else None

    # -- Dry-run para una agencia ---------------------------------------------

    def _dry_run_agencia(
        self, sheet_name: str, df: pd.DataFrame, agencia_info: Dict[str, Any]
    ) -> AgenciaPreview:
        """Compara Excel vs SIDIS actual sin escribir nada."""
        database_name = agencia_info["database_name"]

        # Validar columnas mínimas
        faltantes = COLS_MINIMAS - set(df.columns)
        if faltantes:
            return AgenciaPreview(
                sheet_name=sheet_name,
                database_name=database_name,
                agencia_id=agencia_info["id"],
                agencia_nombre=agencia_info["nombre"],
                db_sidis="",
                total_excel=len(df),
                actualizar=0, sin_cambio=0, no_encontrado=0, sin_idhml=0,
                error=f"Columnas faltantes en Excel: {faltantes}",
            )

        try:
            engine_sidis, db_sidis = self._get_engine_sidis(database_name)
        except Exception as exc:
            return AgenciaPreview(
                sheet_name=sheet_name,
                database_name=database_name,
                agencia_id=agencia_info["id"],
                agencia_nombre=agencia_info["nombre"],
                db_sidis="",
                total_excel=len(df),
                actualizar=0, sin_cambio=0, no_encontrado=0, sin_idhml=0,
                error=f"No se pudo conectar al SIDIS: {exc}",
            )

        filas: List[RowResult] = []
        actualizar = sin_cambio = no_encontrado = sin_idhml = 0

        try:
            with engine_sidis.connect() as conn:
                for _, row in df.iterrows():
                    nb = int(row["nbProducto"])
                    id_prov = int(row["idProveedor"])
                    nm = str(row.get("nmProducto", "")).strip()
                    idhml_nuevo = self._normalizar_idhml(row["idhmlProdProv"])

                    if idhml_nuevo is None:
                        sin_idhml += 1
                        filas.append(RowResult(
                            nbProducto=nb, nmProducto=nm, idProveedor=id_prov,
                            idhml_excel=None, idhml_sidis=None, estado="sin_idhml"
                        ))
                        continue

                    # Leer valor actual del SIDIS
                    try:
                        result = conn.execute(
                            text(
                                f"SELECT idhmlProdProv FROM {db_sidis}.mproductos "
                                "WHERE nbProducto = :nb AND idProveedor = :prov LIMIT 1"
                            ),
                            {"nb": nb, "prov": id_prov},
                        ).first()
                    except Exception as exc:
                        logger.warning("Error leyendo nbProducto=%s: %s", nb, exc)
                        no_encontrado += 1
                        filas.append(RowResult(
                            nbProducto=nb, nmProducto=nm, idProveedor=id_prov,
                            idhml_excel=idhml_nuevo, idhml_sidis=None, estado="no_encontrado"
                        ))
                        continue

                    if result is None:
                        no_encontrado += 1
                        filas.append(RowResult(
                            nbProducto=nb, nmProducto=nm, idProveedor=id_prov,
                            idhml_excel=idhml_nuevo, idhml_sidis=None, estado="no_encontrado"
                        ))
                        continue

                    idhml_sidis = self._normalizar_idhml(result[0])

                    if (idhml_sidis or "").strip() == idhml_nuevo.strip():
                        sin_cambio += 1
                        filas.append(RowResult(
                            nbProducto=nb, nmProducto=nm, idProveedor=id_prov,
                            idhml_excel=idhml_nuevo, idhml_sidis=idhml_sidis, estado="sin_cambio"
                        ))
                    else:
                        actualizar += 1
                        filas.append(RowResult(
                            nbProducto=nb, nmProducto=nm, idProveedor=id_prov,
                            idhml_excel=idhml_nuevo, idhml_sidis=idhml_sidis, estado="actualizar"
                        ))

        except Exception as exc:
            logger.error("Error en dry-run agencia %s: %s", database_name, exc)
            return AgenciaPreview(
                sheet_name=sheet_name,
                database_name=database_name,
                agencia_id=agencia_info["id"],
                agencia_nombre=agencia_info["nombre"],
                db_sidis=db_sidis,
                total_excel=len(df),
                actualizar=actualizar, sin_cambio=sin_cambio,
                no_encontrado=no_encontrado, sin_idhml=sin_idhml,
                filas=filas,
                error=str(exc),
            )

        return AgenciaPreview(
            sheet_name=sheet_name,
            database_name=database_name,
            agencia_id=agencia_info["id"],
            agencia_nombre=agencia_info["nombre"],
            db_sidis=db_sidis,
            total_excel=len(df),
            actualizar=actualizar,
            sin_cambio=sin_cambio,
            no_encontrado=no_encontrado,
            sin_idhml=sin_idhml,
            filas=filas,
        )

    # -- Execute para una agencia ---------------------------------------------

    def _ejecutar_agencia(
        self,
        sheet_name: str,
        df: pd.DataFrame,
        agencia_info: Dict[str, Any],
        usuario: str = "IMPORT_EXCEL",
        progress_callback=None,
    ) -> AgenciaResult:
        """Aplica los UPDATEs al SIDIS para una agencia."""
        database_name = agencia_info["database_name"]
        nombre = agencia_info["nombre"]

        try:
            engine_sidis, db_sidis = self._get_engine_sidis(database_name)
        except Exception as exc:
            return AgenciaResult(
                database_name=database_name, agencia_nombre=nombre,
                actualizados=0, sin_cambio=0, no_encontrado=0, sin_idhml=0, errores=0,
                error=f"Conexión SIDIS fallida: {exc}",
            )

        actualizados = sin_cambio = no_encontrado = sin_idhml = errores = 0
        backup: List[Dict[str, Any]] = []
        total = len(df)

        try:
            with engine_sidis.connect() as conn:
                for idx, (_, row) in enumerate(df.iterrows()):
                    nb = int(row["nbProducto"])
                    id_prov = int(row["idProveedor"])
                    idhml_nuevo = self._normalizar_idhml(row["idhmlProdProv"])

                    if idhml_nuevo is None:
                        sin_idhml += 1
                        continue

                    # Leer valor actual (para backup y comparación)
                    try:
                        result = conn.execute(
                            text(
                                f"SELECT idhmlProdProv FROM {db_sidis}.mproductos "
                                "WHERE nbProducto = :nb AND idProveedor = :prov LIMIT 1"
                            ),
                            {"nb": nb, "prov": id_prov},
                        ).first()
                    except Exception as exc:
                        logger.warning("Error SELECT nbProducto=%s: %s", nb, exc)
                        no_encontrado += 1
                        continue

                    if result is None:
                        no_encontrado += 1
                        continue

                    idhml_actual = self._normalizar_idhml(result[0])

                    if (idhml_actual or "").strip() == idhml_nuevo.strip():
                        sin_cambio += 1
                        continue

                    # Registrar backup antes de actualizar
                    backup.append({
                        "nbProducto": nb,
                        "idProveedor": id_prov,
                        "idhml_antes": idhml_actual,
                        "idhml_despues": idhml_nuevo,
                    })

                    try:
                        conn.execute(
                            text(
                                f"UPDATE {db_sidis}.mproductos "
                                "SET idhmlProdProv = :val "
                                "WHERE nbProducto = :nb AND idProveedor = :prov"
                            ),
                            {"val": idhml_nuevo, "nb": nb, "prov": id_prov},
                        )
                        conn.commit()
                        actualizados += 1
                    except Exception as exc:
                        logger.error("Error UPDATE nbProducto=%s: %s", nb, exc)
                        errores += 1

                    if progress_callback and total > 0:
                        pct = int((idx + 1) / total * 80)
                        progress_callback("Actualizando SIDIS...", pct)

        except Exception as exc:
            logger.error("Error general en execute agencia %s: %s", database_name, exc)
            return AgenciaResult(
                database_name=database_name, agencia_nombre=nombre,
                actualizados=actualizados, sin_cambio=sin_cambio,
                no_encontrado=no_encontrado, sin_idhml=sin_idhml,
                errores=errores, backup=backup,
                error=str(exc),
            )

        # Marcar equivalencias afectadas como REQUIERE_REVISION
        if actualizados > 0:
            self._marcar_equivalencias_revision(agencia_info["id"])

        return AgenciaResult(
            database_name=database_name, agencia_nombre=nombre,
            actualizados=actualizados, sin_cambio=sin_cambio,
            no_encontrado=no_encontrado, sin_idhml=sin_idhml,
            errores=errores, backup=backup,
        )

    def _marcar_equivalencias_revision(self, id_agencia: int) -> None:
        """
        Marca bi_equivalencias como REQUIERE_REVISION para las vigentes de la agencia.
        El próximo snapshot re-evaluará los idhml_original contra los nuevos valores.
        """
        try:
            sql = text("""
                UPDATE powerbi_bimbo.bi_equivalencias
                SET estado_sync = 'REQUIERE_REVISION',
                    motivo_cambio = CONCAT(COALESCE(motivo_cambio,''), ' | import_excel')
                WHERE id_agencia = :id_ag
                  AND dt_fin IS NULL
                  AND tipo_asignacion IN ('AUTO_EXACTO', 'MANUAL', 'PENDIENTE')
                  AND estado_sync != 'REQUIERE_REVISION'
            """)
            with self.engine_bimbo.connect() as conn:
                conn.execute(sql, {"id_ag": id_agencia})
                conn.commit()
        except Exception as exc:
            logger.warning("No se pudo marcar equivalencias REQUIERE_REVISION agencia=%s: %s", id_agencia, exc)

    # -- API pública: preview --------------------------------------------------

    def run_preview(self) -> Dict[str, Any]:
        """
        Dry-run completo. Retorna dict con:
          - agencias_mapeadas: List[AgenciaPreview]
          - sin_mapeo: List[str] (sheet names sin agencia)
        """
        t0 = time.time()
        self._cargar_mapa_agencias()
        hojas = self._leer_hojas_mproductos()

        agencias_mapeadas: List[AgenciaPreview] = []
        sin_mapeo: List[str] = []

        for sheet_name, df in hojas.items():
            agencia_info = self._mapear_sheet_a_agencia(sheet_name)
            if agencia_info is None:
                sin_mapeo.append(sheet_name)
                logger.warning("Sin mapeo de agencia para sheet: %s", sheet_name)
                continue
            logger.info("Dry-run: %s → %s", sheet_name, agencia_info["database_name"])
            preview = self._dry_run_agencia(sheet_name, df, agencia_info)
            agencias_mapeadas.append(preview)

        return {
            "agencias_mapeadas": agencias_mapeadas,
            "sin_mapeo": sin_mapeo,
            "duracion_seg": round(time.time() - t0, 2),
        }

    # -- API pública: execute --------------------------------------------------

    def run_execute(
        self,
        agencias_seleccionadas: Optional[List[str]] = None,
        usuario: str = "IMPORT_EXCEL",
        progress_callback=None,
    ) -> Dict[str, Any]:
        """
        Ejecuta la escritura al SIDIS.

        Args:
            agencias_seleccionadas: Lista de database_name a procesar.
                                    None = todas las hojas mapeadas.
            usuario: Nombre del usuario que ejecuta (para logs).
            progress_callback: func(stage, pct) para progreso RQ.
        """
        t0 = time.time()
        self._cargar_mapa_agencias()
        hojas = self._leer_hojas_mproductos()

        resultados: List[AgenciaResult] = []
        sin_mapeo: List[str] = []
        total_actualizados = 0
        total_errores = 0

        hojas_items = list(hojas.items())
        n = len(hojas_items)

        for i, (sheet_name, df) in enumerate(hojas_items):
            agencia_info = self._mapear_sheet_a_agencia(sheet_name)
            if agencia_info is None:
                sin_mapeo.append(sheet_name)
                continue

            db_name = agencia_info["database_name"]
            if agencias_seleccionadas and db_name not in agencias_seleccionadas:
                continue

            if progress_callback:
                pct_base = int(i / n * 90)
                progress_callback(f"Procesando {agencia_info['nombre']}...", pct_base)

            logger.info("Execute: %s → %s", sheet_name, db_name)
            result = self._ejecutar_agencia(sheet_name, df, agencia_info, usuario, progress_callback)
            resultados.append(result)
            total_actualizados += result.actualizados
            total_errores += result.errores

        if progress_callback:
            progress_callback("Completado", 100)

        return {
            "resultados": resultados,
            "sin_mapeo": sin_mapeo,
            "total_actualizados": total_actualizados,
            "total_errores": total_errores,
            "duracion_seg": round(time.time() - t0, 2),
            "success": total_errores == 0,
        }
