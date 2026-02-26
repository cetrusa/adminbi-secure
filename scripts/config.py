"""Servicios de configuración para DataZenith basados en repositorios y servicios."""

from __future__ import annotations

import ast
import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

import pandas as pd
from sqlalchemy import text
from sqlalchemy.sql.elements import TextClause

from scripts.conexion import Conexion as con
from scripts.repositories.config_repository import ConfigRepository
from scripts.services.config_service import ConfigData, ConfigService

logger = logging.getLogger(__name__)

PermissionsLoader = Callable[[str, Optional[int]], Dict[str, Any]]

_DEFAULT_SERVICE: Optional[ConfigService] = None


@lru_cache(maxsize=1)
def _load_secrets(secrets_file: str = "secret.json") -> Dict[str, Any]:
    path = Path(secrets_file)
    if not path.exists():
        raise ValueError(f"No se encontró el archivo de configuración {secrets_file}.")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - corrupción de archivo
        raise ValueError(f"El archivo {secrets_file} no contiene JSON válido.") from exc


def get_secret(secret_name: str, secrets_file: str = "secret.json") -> str:
    secrets = _load_secrets(secrets_file)
    try:
        return secrets[secret_name]
    except KeyError as exc:
        raise ValueError(f"La variable {secret_name} no existe.") from exc


def default_admin_engine_factory() -> Any:
    """Crea el engine hacia la base administrativa usando la capa de conexión."""

    return con.ConexionMariadb3(
        get_secret("DB_USERNAME"),
        get_secret("DB_PASS"),
        get_secret("DB_HOST"),
        int(get_secret("DB_PORT")),
        get_secret("DB_NAME"),
    )


def default_repository_factory() -> ConfigRepository:
    return ConfigRepository(default_admin_engine_factory)


def default_permissions_loader(
    database_name: str, user_id: Optional[int]
) -> Dict[str, Any]:
    if user_id is None:
        return {"proveedores": [], "macrozonas": []}

    try:  # pragma: no cover - requiere entorno Django
        # Proteger contra Django no inicializado
        import os
        if not os.environ.get('DJANGO_SETTINGS_MODULE'):
            # Django no está configurado, retornar permisos vacíos sin error
            logger.debug(f"Django no inicializado para {database_name}/{user_id}, usando permisos por defecto")
            return {"proveedores": [], "macrozonas": []}
        
        from django.contrib.auth import get_user_model  # type: ignore
        from apps.users.models import UserPermission  # type: ignore

        get_user_model()
        permissions = (
            UserPermission.objects.filter(
                user_id=user_id, empresa__name=database_name
            )
            .only("proveedores", "macrozonas")
            .first()
        )
        if not permissions:
            return {"proveedores": [], "macrozonas": []}

        proveedores = (
            ast.literal_eval(permissions.proveedores)
            if permissions.proveedores
            else []
        )
        macrozonas = (
            ast.literal_eval(permissions.macrozonas)
            if permissions.macrozonas
            else []
        )
        return {"proveedores": proveedores, "macrozonas": macrozonas}
    except Exception as exc:  # pragma: no cover - logging defensivo
        logger.debug(
            "No se pudieron obtener permisos para %s/%s (Django no disponible): %s", database_name, user_id, exc
        )
        return {"proveedores": [], "macrozonas": []}


def get_default_service(cache_ttl: int = 600) -> ConfigService:
    global _DEFAULT_SERVICE
    if _DEFAULT_SERVICE is None:
        _DEFAULT_SERVICE = ConfigService(
            repository_factory=default_repository_factory,
            permissions_loader=default_permissions_loader,
            cache_ttl=cache_ttl,
        )
    return _DEFAULT_SERVICE


class ConfigBasic:
    """Contenedor ligero que delega la configuración al ``ConfigService``."""

    def __init__(
        self,
        database_name: str,
        user_id: Optional[int] = None,
        *,
        service: Optional[ConfigService] = None,
        repository_factory: Optional[Callable[[], ConfigRepository]] = None,
        permissions_loader: Optional[PermissionsLoader] = None,
        cache_ttl: int = 600,
    ) -> None:
        self.database_name = database_name
        self.user_id = user_id

        self._repository_factory = repository_factory or default_repository_factory
        if service is not None:
            self._service = service
        elif repository_factory or permissions_loader:
            self._service = ConfigService(
                repository_factory or default_repository_factory,
                permissions_loader or default_permissions_loader,
                cache_ttl=cache_ttl,
            )
        else:
            self._service = get_default_service(cache_ttl)
            self._repository_factory = default_repository_factory

        self._config_data: ConfigData = self._service.get_config(database_name, user_id)
        self.config: Dict[str, Any] = self._normalise_config(self._config_data)

    def _normalise_config(self, config_data: ConfigData) -> Dict[str, Any]:
        payload = config_data.as_dict()
        payload.setdefault("name", config_data.empresa.name)
        payload.setdefault(
            "dir_actual", config_data.empresa.dir_actual or "puente1dia"
        )
        payload.setdefault(
            "nmDt", config_data.empresa.nm_dt or payload["dir_actual"]
        )
        payload.setdefault("user_id", self.user_id)
        payload.setdefault(
            "proveedores", config_data.permisos.get("proveedores", [])
        )
        payload.setdefault(
            "macrozonas", config_data.permisos.get("macrozonas", [])
        )
        return payload

    def execute_sql_query(
        self, sql_query: Union[str, TextClause], params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        repository = self._repository_factory()
        rows = repository.run_query(sql_query, params)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    @classmethod
    def clear_cache(
        cls, database_name: Optional[str] = None, user_id: Optional[int] = None
    ) -> None:
        service = get_default_service()
        service.clear_cache(database_name, user_id)

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.config)
