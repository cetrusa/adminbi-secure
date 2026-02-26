"""Repositorio de configuración para entornos DataZenith."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Sequence, Union

from sqlalchemy import text
from sqlalchemy.engine import Engine, RowMapping
from sqlalchemy.sql.elements import TextClause


@dataclass(frozen=True)
class Credential:
    """Credenciales básicas de acceso a un recurso."""

    username: Optional[str]
    password: Optional[str]


@dataclass(frozen=True)
class ServerConfig:
    """Configuración de un servidor asociado a la empresa."""

    identifier: Optional[int]
    name: Optional[str]
    type_code: Optional[str]
    host: Optional[str]
    port: Optional[int]
    credential: Optional[Credential]


@dataclass(frozen=True)
class DateWindow:
    """Ventana de fechas utilizada por los reportes."""

    report_start: str
    report_end: str


@dataclass(frozen=True)
class EmpresaConfig:
    """Datos principales de la empresa."""

    id: Optional[int]
    nm_empresa: Optional[str]
    name: str
    dir_actual: Optional[str]
    nm_dt: Optional[str]
    nb_server_sidis: Optional[str]
    db_sidis: Optional[str]
    nb_server_bi: Optional[str]
    db_bi: Optional[str]
    tx_procedure_extrae: Optional[str]
    tx_procedure_cargue: Optional[str]
    nm_procedure_excel: Optional[str]
    tx_procedure_excel: Optional[str]
    nm_procedure_interface: Optional[str]
    tx_procedure_interface: Optional[str]
    nm_procedure_excel2: Optional[str]
    tx_procedure_excel2: Optional[str]
    nm_procedure_csv: Optional[str]
    tx_procedure_csv: Optional[str]
    nm_procedure_csv2: Optional[str]
    tx_procedure_csv2: Optional[str]
    nm_procedure_sql: Optional[str]
    tx_procedure_sql: Optional[str]
    group_id_powerbi: Optional[str]
    report_id_powerbi: Optional[str]
    dataset_id_powerbi: Optional[str]
    url_powerbi: Optional[str]
    id_tsol: Optional[str]
    raw: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        data = dict(self.raw)
        data.setdefault("name", self.name)
        return data


class ConfigRepository:
    """Encapsula el acceso a tablas de configuración."""

    _EMPRESA_COLUMNS = (
        "id",
        "nmEmpresa",
        "name",
        "nbServerSidis",
        "dbSidis",
        "nbServerBi",
        "dbBi",
        "txProcedureExtrae",
        "txProcedureCargue",
        "nmProcedureExcel",
        "txProcedureExcel",
        "nmProcedureInterface",
        "txProcedureInterface",
        "nmProcedureExcel2",
        "txProcedureExcel2",
        "nmProcedureCsv",
        "txProcedureCsv",
        "nmProcedureCsv2",
        "txProcedureCsv2",
        "nmProcedureSql",
        "txProcedureSql",
        "group_id_powerbi",
        "report_id_powerbi",
        "dataset_id_powerbi",
        "url_powerbi",
        "id_tsol",
    )

    def __init__(self, admin_engine_factory: Callable[[], Engine]) -> None:
        self._engine_factory = admin_engine_factory

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def get_empresa_config(self, database_name: str) -> EmpresaConfig:
        stmt = text(
            """
            SELECT {cols}
            FROM powerbi_adm.conf_empresas
            WHERE name = :name
            LIMIT 1
            """.format(cols=", ".join(self._EMPRESA_COLUMNS))
        )
        row = self._fetch_one(stmt, {"name": database_name})
        if not row:
            raise ValueError(
                f"No se encontró configuración para la empresa {database_name}"
            )

        return EmpresaConfig(
            id=row.get("id"),
            nm_empresa=row.get("nmEmpresa"),
            name=row.get("name", database_name),
            dir_actual=row.get("name", database_name),  # dir_actual es el name de la empresa
            nm_dt=None,  # nmDt no existe en conf_empresas, debe obtenerse de conf_dt
            nb_server_sidis=row.get("nbServerSidis"),
            db_sidis=row.get("dbSidis"),
            nb_server_bi=row.get("nbServerBi"),
            db_bi=row.get("dbBi"),
            tx_procedure_extrae=row.get("txProcedureExtrae"),
            tx_procedure_cargue=row.get("txProcedureCargue"),
            nm_procedure_excel=row.get("nmProcedureExcel"),
            tx_procedure_excel=row.get("txProcedureExcel"),
            nm_procedure_interface=row.get("nmProcedureInterface"),
            tx_procedure_interface=row.get("txProcedureInterface"),
            nm_procedure_excel2=row.get("nmProcedureExcel2"),
            tx_procedure_excel2=row.get("txProcedureExcel2"),
            nm_procedure_csv=row.get("nmProcedureCsv"),
            tx_procedure_csv=row.get("txProcedureCsv"),
            nm_procedure_csv2=row.get("nmProcedureCsv2"),
            tx_procedure_csv2=row.get("txProcedureCsv2"),
            nm_procedure_sql=row.get("nmProcedureSql"),
            tx_procedure_sql=row.get("txProcedureSql"),
            group_id_powerbi=row.get("group_id_powerbi"),
            report_id_powerbi=row.get("report_id_powerbi"),
            dataset_id_powerbi=row.get("dataset_id_powerbi"),
            url_powerbi=row.get("url_powerbi"),
            id_tsol=row.get("id_tsol"),
            raw=dict(row),
        )

    def get_date_window(self, nm_dt: str) -> Optional[DateWindow]:
        stmt = text(
            """
            SELECT txDtIni, txDtFin
            FROM powerbi_adm.conf_dt
            WHERE nmDt = :nm_dt
            LIMIT 1
            """
        )
        row = self._fetch_one(stmt, {"nm_dt": nm_dt})
        if not row:
            return None

        tx_dt_ini = row.get("txDtIni")
        tx_dt_fin = row.get("txDtFin")
        if not tx_dt_ini or not tx_dt_fin:
            return None

        reporte_ini = self._fetch_one(self._coerce_to_text(tx_dt_ini))
        reporte_fin = self._fetch_one(self._coerce_to_text(tx_dt_fin))
        if not reporte_ini or not reporte_fin:
            return None

        return DateWindow(
            report_start=str(reporte_ini.get("IdtReporteIni")),
            report_end=str(reporte_fin.get("IdtReporteFin")),
        )

    def get_server_config(self, identifier: Optional[object]) -> Optional[ServerConfig]:
        if identifier in (None, ""):
            return None

        stmt_server = text(
            """
            SELECT nbServer, nmServer, nbTipo, hostServer, portServer
            FROM powerbi_adm.conf_server
            WHERE nbServer = :identifier
            LIMIT 1
            """
        )
        row = self._fetch_one(stmt_server, {"identifier": str(identifier)})
        if not row:
            return None

        nb_tipo = row.get("nbTipo")
        credential = self.get_credentials(str(nb_tipo)) if nb_tipo is not None else None

        return ServerConfig(
            identifier=row.get("nbServer"),
            name=row.get("nmServer"),
            type_code=row.get("nbTipo"),
            host=row.get("hostServer"),
            port=row.get("portServer"),
            credential=credential,
        )

    def get_credentials(self, nb_tipo: str) -> Optional[Credential]:
        stmt = text(
            """
            SELECT nmUsr, txPass
            FROM powerbi_adm.conf_tipo
            WHERE nbTipo = :nb_tipo
            LIMIT 1
            """
        )
        row = self._fetch_one(stmt, {"nb_tipo": nb_tipo})
        if not row:
            return None
        return Credential(username=row.get("nmUsr"), password=row.get("txPass"))

    # ------------------------------------------------------------------
    # Utilidades internas
    # ------------------------------------------------------------------
    def _coerce_to_text(self, statement: Any) -> TextClause:
        return statement if isinstance(statement, TextClause) else text(str(statement))

    def _fetch_one(
        self, statement: TextClause, params: Optional[Dict[str, Any]] = None
    ) -> Optional[RowMapping]:
        rows = self._run_query(statement, params)
        return rows[0] if rows else None

    def _run_query(
        self, statement: TextClause, params: Optional[Dict[str, Any]] = None
    ) -> Sequence[RowMapping]:
        engine = self._engine_factory()
        with engine.connect() as connection:
            result = connection.execute(statement, params or {})
            return result.mappings().all()

    def run_query(
        self, statement: Union[str, TextClause], params: Optional[Dict[str, Any]] = None
    ) -> Sequence[Dict[str, Any]]:
        stmt = self._coerce_to_text(statement)
        rows = self._run_query(stmt, params)
        return [dict(row) for row in rows]
