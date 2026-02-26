import hashlib
import logging
import os
import time
from contextlib import suppress
from threading import Lock
from typing import Any, Dict, Optional

import pymysql
import sqlalchemy
from cachetools import TTLCache  # type: ignore[import]
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import QueuePool


class Conexion:
    """
    Clase para gestionar conexiones a bases de datos con optimizaciones de rendimiento.
    Implementa un pool de conexiones y caché para mejorar el tiempo de respuesta.
    """

    # Caché de conexiones para reutilizar engines entre llamadas
    _cache_lock = Lock()
    _cache_ttl_seconds = 300
    _connection_cache: TTLCache[str, sqlalchemy.engine.Engine] = TTLCache(
        maxsize=256, ttl=_cache_ttl_seconds
    )
    _connection_labels: Dict[str, str] = {}
    _connection_timestamps: Dict[str, float] = {}

    @classmethod
    def _build_cache_key(cls, label: str) -> str:
        return hashlib.sha1(label.encode("utf-8")).hexdigest()

    @classmethod
    def _store_engine(
        cls,
        cache_key: str,
        engine: sqlalchemy.engine.Engine,
        label: str,
    ) -> None:
        cls._connection_cache[cache_key] = engine
        cls._connection_labels[cache_key] = label
        cls._connection_timestamps[cache_key] = time.time()

    @classmethod
    def _get_cached_engine(
        cls, cache_key: str
    ) -> Optional[sqlalchemy.engine.Engine]:
        try:
            engine = cls._connection_cache[cache_key]
        except KeyError:
            cls._connection_labels.pop(cache_key, None)
            cls._connection_timestamps.pop(cache_key, None)
            return None
        return engine

    @classmethod
    def _evict_cached_engine(cls, cache_key: str) -> None:
        engine = cls._connection_cache.pop(cache_key, None)
        if engine is not None:
            with suppress(Exception):
                engine.dispose()
        cls._connection_labels.pop(cache_key, None)
        cls._connection_timestamps.pop(cache_key, None)

    @staticmethod
    def ConexionMariadb3(user, password, host, port, database):
        """
        Crea una conexión optimizada a MariaDB/MySQL con pool de conexiones.

        Args:
            user (str): Nombre de usuario para la conexión.
            password (str): Contraseña del usuario.
            host (str): Host donde se encuentra la base de datos.
            port (int): Puerto para la conexión.
            database (str): Nombre de la base de datos.

        Returns:
            Engine: Objeto Engine de SQLAlchemy para ejecutar consultas.
        """
        def _timeout_from_env(
            env_name: str, default: int, minimum: int, maximum: int
        ) -> int:
            raw_value = os.getenv(env_name)
            if raw_value is None:
                return default
            try:
                value = int(raw_value)
            except ValueError:
                logging.warning(
                    "Valor de timeout inválido para %s: %s. Usando valor por defecto %s.",
                    env_name,
                    raw_value,
                    default,
                )
                return default
            return max(minimum, min(value, maximum))

        pool_size = int(os.getenv("DB_POOL_SIZE", 20))
        max_overflow = int(os.getenv("DB_MAX_OVERFLOW", 25))
        pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", 120))
        pool_recycle = int(os.getenv("DB_POOL_RECYCLE", 28000))

        connect_timeout = _timeout_from_env("DB_CONNECT_TIMEOUT", 20, 5, 60)
        read_timeout = _timeout_from_env("DB_READ_TIMEOUT", 600, 30, 3600)  # 10 min default, max 1 hora
        write_timeout = _timeout_from_env("DB_WRITE_TIMEOUT", 600, 30, 3600)  # 10 min default, max 1 hora

        connection_label = f"{user}@{host}:{port}/{database}"
        cache_key = Conexion._build_cache_key(connection_label)

        with Conexion._cache_lock:
            engine = Conexion._get_cached_engine(cache_key)
            if engine is not None:
                try:
                    with engine.connect():
                        pass
                    Conexion._connection_timestamps[cache_key] = time.time()
                    logging.debug(
                        "Reutilizando conexión existente para %s (cache_key=%s)",
                        connection_label,
                        cache_key,
                    )
                    return engine
                except Exception as exc:
                    logging.warning(
                        "Conexión en caché inválida para %s: %s. Regenerando...",
                        connection_label,
                        exc,
                    )
                    Conexion._evict_cached_engine(cache_key)

            logging.debug(
                "Creando nueva conexión para %s (cache_key=%s)",
                connection_label,
                cache_key,
            )

            try:
                connect_args = {
                    "charset": "utf8mb4",
                    "autocommit": True,
                    "client_flag": pymysql.constants.CLIENT.MULTI_STATEMENTS,
                    "connect_timeout": connect_timeout,
                    "read_timeout": read_timeout,
                    "write_timeout": write_timeout,
                }

                if os.getenv("DB_SSL_MODE", "false").lower() == "true":
                    ssl_args: Dict[str, str] = {}
                    ssl_ca = os.getenv("DB_SSL_CA")
                    ssl_cert = os.getenv("DB_SSL_CERT")
                    ssl_key = os.getenv("DB_SSL_KEY")
                    if ssl_ca:
                        ssl_args["ca"] = ssl_ca
                    if ssl_cert:
                        ssl_args["cert"] = ssl_cert
                    if ssl_key:
                        ssl_args["key"] = ssl_key
                    connect_args["ssl"] = ssl_args or {}

                engine = sqlalchemy.create_engine(
                    sqlalchemy.engine.url.URL.create(
                        drivername="mysql+pymysql",
                        username=user,
                        password=password,
                        host=host,
                        port=port,
                        database=database,
                    ),
                    connect_args=connect_args,
                    poolclass=QueuePool,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_timeout=pool_timeout,
                    pool_recycle=pool_recycle,
                    pool_pre_ping=True,
                    echo=False,
                    echo_pool=False,
                    future=True,
                    pool_reset_on_return="commit",
                )

                try:
                    engine = engine.execution_options(stream_results=True)
                except Exception as exc:
                    logging.debug(
                        "No se pudieron aplicar execution_options al engine de %s: %s",
                        connection_label,
                        exc,
                    )

                Conexion._store_engine(cache_key, engine, connection_label)
                return engine
            except Exception as exc:
                pool_snapshot = {"total_cached": len(Conexion._connection_cache)}
                logging.error(
                    "Error al conectar con la base de datos %s en %s: %s. Estado de caché/pool: %s",
                    database,
                    host,
                    exc,
                    pool_snapshot,
                )
                raise

    @staticmethod
    def configurar_timeouts_extendidos(connection):
        """
        Configura timeouts extendidos para consultas de larga duración.
        
        Args:
            connection: Conexión SQLAlchemy donde aplicar los timeouts
        """
        timeout_commands = [
            # Timeouts de sesión MySQL (en segundos)
            "SET SESSION wait_timeout = 7200",                    # 2 horas
            "SET SESSION interactive_timeout = 7200",             # 2 horas
            "SET SESSION net_read_timeout = 3600",                # 1 hora
            "SET SESSION net_write_timeout = 3600",               # 1 hora
            "SET SESSION max_execution_time = 7200000",           # 2 horas en milisegundos
            # Configuraciones adicionales para consultas largas
            "SET SESSION long_query_time = 3600",                 # Log consultas > 1 hora
            "SET SESSION tmp_table_size = 1073741824",            # 1GB para tablas temporales
            "SET SESSION max_heap_table_size = 1073741824",       # 1GB para tablas en memoria
            "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"  # Nivel de aislamiento
        ]
        
        for command in timeout_commands:
            try:
                connection.execute(sqlalchemy.text(command))
                logging.debug(f"Configurado: {command}")
            except Exception as e:
                # Solo advertir, no fallar por un comando específico
                logging.warning(f"No se pudo configurar: {command} - Error: {e}")
        
        logging.info("Configuración de timeouts extendidos aplicada correctamente")

    @staticmethod
    def ConexionMariadbExtendida(user, password, host, port, database):
        """
        Crea una conexión optimizada para consultas de larga duración.
        
        Args:
            user (str): Nombre de usuario para la conexión.
            password (str): Contraseña del usuario.
            host (str): Host donde se encuentra la base de datos.
            port (int): Puerto para la conexión.
            database (str): Nombre de la base de datos.

        Returns:
            Engine: Objeto Engine de SQLAlchemy configurado para consultas largas.
        """
        # Configurar timeouts extendidos para conexiones largas
        def _timeout_from_env(
            env_name: str, default: int, minimum: int, maximum: int
        ) -> int:
            raw_value = os.getenv(env_name)
            if raw_value is None:
                return default
            try:
                value = int(raw_value)
            except ValueError:
                logging.warning(
                    "Valor de timeout inválido para %s: %s. Usando valor por defecto %s.",
                    env_name,
                    raw_value,
                    default,
                )
                return default
            return max(minimum, min(value, maximum))

        pool_size = int(os.getenv("DB_POOL_SIZE", 20))
        max_overflow = int(os.getenv("DB_MAX_OVERFLOW", 25))
        pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", 300))  # Aumentamos a 5 minutos
        pool_recycle = int(os.getenv("DB_POOL_RECYCLE", 28000))

        # Timeouts extendidos específicos para consultas largas
        connect_timeout = _timeout_from_env("DB_CONNECT_TIMEOUT_EXTENDED", 60, 10, 120)
        read_timeout = _timeout_from_env("DB_READ_TIMEOUT_EXTENDED", 7200, 300, 7200)  # 2 horas
        write_timeout = _timeout_from_env("DB_WRITE_TIMEOUT_EXTENDED", 7200, 300, 7200)  # 2 horas

        connection_label = f"{user}@{host}:{port}/{database}_extended"
        cache_key = Conexion._build_cache_key(connection_label)

        with Conexion._cache_lock:
            engine = Conexion._get_cached_engine(cache_key)
            if engine is not None:
                try:
                    with engine.connect():
                        pass
                    Conexion._connection_timestamps[cache_key] = time.time()
                    logging.debug(
                        "Reutilizando conexión extendida para %s (cache_key=%s)",
                        connection_label,
                        cache_key,
                    )
                    return engine
                except Exception as exc:
                    logging.warning(
                        "Conexión extendida en caché inválida para %s: %s. Regenerando...",
                        connection_label,
                        exc,
                    )
                    Conexion._evict_cached_engine(cache_key)

            logging.debug(
                "Creando nueva conexión extendida para %s (cache_key=%s)",
                connection_label,
                cache_key,
            )

            try:
                connect_args = {
                    "charset": "utf8mb4",
                    "autocommit": True,
                    "client_flag": pymysql.constants.CLIENT.MULTI_STATEMENTS,
                    "connect_timeout": connect_timeout,
                    "read_timeout": read_timeout,
                    "write_timeout": write_timeout,
                }

                if os.getenv("DB_SSL_MODE", "false").lower() == "true":
                    ssl_args: Dict[str, str] = {}
                    ssl_ca = os.getenv("DB_SSL_CA")
                    ssl_cert = os.getenv("DB_SSL_CERT")
                    ssl_key = os.getenv("DB_SSL_KEY")
                    if ssl_ca:
                        ssl_args["ca"] = ssl_ca
                    if ssl_cert:
                        ssl_args["cert"] = ssl_cert
                    if ssl_key:
                        ssl_args["key"] = ssl_key
                    connect_args["ssl"] = ssl_args or {}

                engine = sqlalchemy.create_engine(
                    sqlalchemy.engine.url.URL.create(
                        drivername="mysql+pymysql",
                        username=user,
                        password=password,
                        host=host,
                        port=port,
                        database=database,
                    ),
                    connect_args=connect_args,
                    poolclass=QueuePool,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_timeout=pool_timeout,
                    pool_recycle=pool_recycle,
                    pool_pre_ping=True,
                    echo=False,
                    echo_pool=False,
                    future=True,
                    pool_reset_on_return="commit",
                )

                # Configurar engine específicamente para consultas largas
                engine = engine.execution_options(
                    autocommit=True
                )

                Conexion._store_engine(cache_key, engine, connection_label)
                return engine
            except Exception as exc:
                pool_snapshot = {"total_cached": len(Conexion._connection_cache)}
                logging.error(
                    "Error al conectar con la base de datos extendida %s en %s: %s. Estado de caché/pool: %s",
                    database,
                    host,
                    exc,
                    pool_snapshot,
                )
                raise

    @staticmethod
    def export_pool_metrics():
        """
        Devuelve un resumen de los pools activos para monitoreo externo (por ejemplo, Prometheus).
        """
        with Conexion._cache_lock:
            metrics = []
            snapshot = list(Conexion._connection_cache.items())

        for cache_key, engine in snapshot:
            pool = getattr(engine, "pool", None)
            if not pool:
                continue

            labels = {
                "connection": Conexion._connection_labels.get(cache_key, cache_key)
            }

            try:
                metrics.extend(
                    [
                        {
                            "metric": "db_pool_size",
                            "labels": labels.copy(),
                            "value": pool.size(),
                        },
                        {
                            "metric": "db_pool_checked_in",
                            "labels": labels.copy(),
                            "value": pool.checkedin(),
                        },
                        {
                            "metric": "db_pool_checked_out",
                            "labels": labels.copy(),
                            "value": pool.checkedout(),
                        },
                        {
                            "metric": "db_pool_overflow",
                            "labels": labels.copy(),
                            "value": pool.overflow(),
                        },
                    ]
                )
            except Exception as exc:
                logging.debug(
                    "No se pudieron obtener métricas del pool para %s: %s",
                    labels.get("connection"),
                    exc,
                )

        return metrics

    @staticmethod
    def ConexionSqlite(db_path: str = "mydata.db"):
        """
        Crea una conexión optimizada a SQLite con pool de conexiones y caché.

        Args:
            db_path (str): Ruta al archivo de la base de datos SQLite.

        Returns:
            Engine: Objeto Engine de SQLAlchemy para ejecutar consultas.
        """
        connection_label = f"sqlite:///{db_path}"
        cache_key = Conexion._build_cache_key(connection_label)

        with Conexion._cache_lock:
            engine = Conexion._get_cached_engine(cache_key)
            if engine is not None:
                try:
                    with engine.connect():
                        pass
                    Conexion._connection_timestamps[cache_key] = time.time()
                    logging.debug(
                        "Reutilizando conexión SQLite para %s (cache_key=%s)",
                        connection_label,
                        cache_key,
                    )
                    return engine
                except Exception as exc:
                    logging.warning(
                        "Conexión SQLite en caché inválida (%s): %s",
                        connection_label,
                        exc,
                    )
                    Conexion._evict_cached_engine(cache_key)

            try:
                engine = sqlalchemy.create_engine(
                    f"sqlite:///{db_path}",
                    connect_args={
                        "timeout": 60,
                        "check_same_thread": False,
                        "isolation_level": None,
                    },
                    poolclass=QueuePool,
                    pool_size=5,
                    max_overflow=10,
                    pool_timeout=60,
                    pool_recycle=1800,
                    pool_pre_ping=True,
                    echo=False,
                    future=True,
                )
                Conexion._store_engine(cache_key, engine, connection_label)
                return engine
            except Exception as exc:
                logging.error("Error al conectar con SQLite %s: %s", db_path, exc)
                raise

    @staticmethod
    def create_connection_with_retry(
        user, password, host, port, database, max_retries=3
    ):
        """
        Intenta crear una conexión con reintentos en caso de fallos temporales.

        Args:
            user (str): Nombre de usuario para la conexión.
            password (str): Contraseña del usuario.
            host (str): Host donde se encuentra la base de datos.
            port (int): Puerto para la conexión.
            database (str): Nombre de la base de datos.
            max_retries (int): Número máximo de reintentos.

        Returns:
            Engine: Objeto Engine de SQLAlchemy para ejecutar consultas.

        Raises:
            Exception: Si agotan los reintentos y no se puede establecer la conexión.
        """
        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            try:
                engine = Conexion.ConexionMariadb3(user, password, host, port, database)
                # Probar la conexión con una consulta simple
                with engine.connect():
                    pass
                return engine
            except Exception as e:
                last_error = e
                retry_count += 1
                wait_time = 2**retry_count  # Backoff exponencial
                logging.warning(
                    f"Intento {retry_count}/{max_retries} falló. Reintentando en {wait_time}s. Error: {e}"
                )
                time.sleep(wait_time)

        # Si llegamos aquí, todos los reintentos fallaron
        logging.error(f"Fallaron todos los reintentos de conexión: {last_error}")
        raise last_error  # Re-lanzar el último error

    @staticmethod
    def execute_with_retry(engine, statement, params=None, retries=3, base_backoff=1):
        """
        Ejecuta una consulta con reintentos ante fallos operacionales transitorios.

        Args:
            engine (Engine): Engine de SQLAlchemy donde ejecutar.
            statement (str | sqlalchemy.sql.elements.TextClause): Consulta SQL o sqlalchemy.text(...).
            params (dict | None): Parámetros para la consulta.
            retries (int): Número de reintentos.
            base_backoff (int | float): Tiempo base para backoff exponencial (segundos).

        Returns:
            Result: Resultado de la ejecución.
        """
        if isinstance(statement, str):
            statement = sqlalchemy.text(statement)

        attempt = 0
        last_error = None
        while attempt <= retries:
            try:
                with engine.connect() as conn:
                    return conn.execute(statement, params or {})
            except OperationalError as e:
                last_error = e
                if attempt < retries:
                    wait = (2 ** attempt) * base_backoff
                    logging.warning(f"Retry {attempt + 1}/{retries} tras OperationalError: {e}. Esperando {wait}s…")
                    time.sleep(wait)
                    attempt += 1
                else:
                    break
            except Exception as e:
                # No reintentar para otros errores no operacionales por defecto
                raise e

        # Si agota reintentos
        raise last_error

    @staticmethod
    def clear_connection_cache(connection_key=None):
        """
        Limpia la caché de conexiones.

        Args:
            connection_key (str, optional): Clave específica de conexión a limpiar.
                Si no se proporciona, se limpian todas las conexiones.
        """
        with Conexion._cache_lock:
            if connection_key:
                cache_key = (
                    connection_key
                    if connection_key in Conexion._connection_cache
                    else Conexion._build_cache_key(connection_key)
                )
                if cache_key in Conexion._connection_cache:
                    Conexion._evict_cached_engine(cache_key)
                    logging.info(
                        "Conexión %s (cache_key=%s) eliminada de la caché",
                        Conexion._connection_labels.get(cache_key, connection_key),
                        cache_key,
                    )
            else:
                for cache_key in list(Conexion._connection_cache.keys()):
                    Conexion._evict_cached_engine(cache_key)
                Conexion._connection_cache.clear()
                Conexion._connection_labels.clear()
                Conexion._connection_timestamps.clear()
                logging.info("Caché de conexiones limpiada completamente")

    @staticmethod
    def get_connection_status():
        """
        Devuelve información sobre el estado actual de las conexiones en caché.

        Returns:
            dict: Información sobre conexiones actuales y su tiempo de vida.
        """
        with Conexion._cache_lock:
            current_time = time.time()
            status = {
                "total_connections": len(Conexion._connection_cache),
                "connections": {},
            }

            for cache_key, engine in list(Conexion._connection_cache.items()):
                label = Conexion._connection_labels.get(cache_key, cache_key)
                timestamp = Conexion._connection_timestamps.get(cache_key)
                age = current_time - timestamp if timestamp else None

                pool_status: Dict[str, Any] = {}
                try:
                    pool = engine.pool
                    if hasattr(pool, "size") and hasattr(pool, "checkedin"):
                        pool_status = {
                            "size": pool.size(),
                            "checked_in": pool.checkedin(),
                            "overflow": pool.overflow(),
                            "checkedout": pool.checkedout(),
                        }
                except Exception as exc:
                    logging.debug(
                        "No se pudieron obtener métricas de pool para %s: %s",
                        label,
                        exc,
                    )

                status["connections"][label] = {
                    "age_seconds": round(age, 2) if age is not None else None,
                    "expires_in": (
                        round(Conexion._cache_ttl_seconds - age, 2)
                        if age is not None and age < Conexion._cache_ttl_seconds
                        else "expired"
                        if age is not None
                        else None
                    ),
                    "pool": pool_status,
                }

            return status

    @staticmethod
    def check_pool_health():
        """
        Verifica la salud de los pools de conexiones y cierra aquellos que podrían tener problemas.
        """
        with Conexion._cache_lock:
            for key, engine in list(Conexion._connection_cache.items()):
                try:
                    pool = engine.pool
                    # Detectar posibles condiciones problemáticas
                    if (
                        pool.overflow() > 10  # Muchas conexiones en overflow
                        or pool.checkedout()
                        > pool.size() * 0.9  # Más del 90% del pool en uso
                    ):
                        logging.warning(
                            f"Pool posiblemente en estado de saturación para {key}: "
                            f"overflow={pool.overflow()}, checkedout={pool.checkedout()}. Reiniciando."
                        )
                        # Eliminar y recrear esta conexión
                        Conexion.clear_connection_cache(key)
                except Exception as e:
                    logging.warning(
                        f"Error al verificar estado del pool para {key}: {e}"
                    )
