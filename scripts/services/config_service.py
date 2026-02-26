"""Servicio de configuración que orquesta repositorio y permisos."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from cachetools import TTLCache  # type: ignore[import]

from scripts.repositories.config_repository import (
    ConfigRepository,
    Credential,
    DateWindow,
    EmpresaConfig,
    ServerConfig,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfigData:
    """Carga completa que expone el servicio de configuración."""

    empresa: EmpresaConfig
    date_window: Optional[DateWindow]
    server_out: Optional[ServerConfig]
    server_in: Optional[ServerConfig]
    powerbi_credentials: Optional[Credential]
    correo_credentials: Optional[Credential]
    permisos: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        payload.update(self.empresa.as_dict())
        if self.date_window:
            payload.update(
                {
                    "IdtReporteIni": self.date_window.report_start,
                    "IdtReporteFin": self.date_window.report_end,
                }
            )
        if self.server_out:
            payload.update(
                {
                    "hostServerOut": self.server_out.host,
                    "portServerOut": self.server_out.port,
                    "nmUsrOut": getattr(self.server_out.credential, "username", None),
                    "txPassOut": getattr(self.server_out.credential, "password", None),
                }
            )
        if self.server_in:
            payload.update(
                {
                    "hostServerIn": self.server_in.host,
                    "portServerIn": self.server_in.port,
                    "nmUsrIn": getattr(self.server_in.credential, "username", None),
                    "txPassIn": getattr(self.server_in.credential, "password", None),
                }
            )
        if self.powerbi_credentials:
            payload.update(
                {
                    "nmUsrPowerbi": self.powerbi_credentials.username,
                    "txPassPowerbi": self.powerbi_credentials.password,
                }
            )
        if self.correo_credentials:
            payload.update(
                {
                    "nmUsrCorreo": self.correo_credentials.username,
                    "txPassCorreo": self.correo_credentials.password,
                }
            )
        payload.update(self.permisos)
        return payload


class ConfigService:
    """Orquesta la obtención de configuración de empresas y usuarios."""

    def __init__(
        self,
        repository_factory: Callable[[], ConfigRepository],
        permissions_loader: Callable[[str, Optional[int]], Dict[str, Any]],
        cache_ttl: int = 600,
    ) -> None:
        self._repository_factory = repository_factory
        self._permissions_loader = permissions_loader
        self._cache = TTLCache(maxsize=256, ttl=cache_ttl)
        self._cache_index: Dict[str, Tuple[str, Optional[int]]] = {}

    def get_config(self, database_name: str, user_id: Optional[int]) -> ConfigData:
        cache_key = self._build_cache_key(database_name, user_id)
        cached = self._cache.get(cache_key)
        if cached is not None:
            logger.debug("ConfigService hit cache for %s", cache_key)
            return cached

        start_time = time.time()
        repository = self._repository_factory()
        empresa = repository.get_empresa_config(database_name)
        # Si nmDt no está configurado, usar dir_actual (rango del mes por defecto)
        nm_dt = empresa.nm_dt or empresa.dir_actual or "puente1dia"
        date_window = repository.get_date_window(nm_dt)
        server_out = repository.get_server_config(empresa.nb_server_sidis)
        server_in = repository.get_server_config(empresa.nb_server_bi)
        powerbi_credentials = repository.get_credentials("3")
        correo_credentials = repository.get_credentials("11")
        try:
            permisos = self._permissions_loader(database_name, user_id)
        except Exception:  # pragma: no cover - depende del entorno Django
            logger.exception(
                "Fallo obteniendo permisos para %s/%s", database_name, user_id
            )
            permisos = {}

        config_data = ConfigData(
            empresa=empresa,
            date_window=date_window,
            server_out=server_out,
            server_in=server_in,
            powerbi_credentials=powerbi_credentials,
            correo_credentials=correo_credentials,
            permisos=permisos,
        )

        self._cache[cache_key] = config_data
        self._cache_index[cache_key] = (database_name, user_id)
        logger.debug(
            "ConfigService caching result for %s en %.3fs",
            cache_key,
            time.time() - start_time,
        )
        return config_data

    def clear_cache(
        self, database_name: Optional[str] = None, user_id: Optional[int] = None
    ) -> None:
        if database_name is None and user_id is None:
            self._cache.clear()
            self._cache_index.clear()
            return

        keys_to_remove = []
        for key, value in list(self._cache_index.items()):
            db_name, cached_user = value
            if database_name is not None and db_name != database_name:
                continue
            if user_id is not None and cached_user != user_id:
                continue
            keys_to_remove.append(key)

        for key in keys_to_remove:
            self._cache.pop(key, None)
            self._cache_index.pop(key, None)

    @staticmethod
    def _build_cache_key(database_name: str, user_id: Optional[int]) -> str:
        raw = f"{database_name}:{user_id if user_id is not None else 'anon'}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()
