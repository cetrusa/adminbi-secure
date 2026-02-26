from functools import wraps
import logging
import json
from ipaddress import IPv4Address, IPv6Address, AddressValueError
from typing import Dict, Any, Callable

from django.http import HttpRequest, HttpResponse
from django.utils import timezone
from django.conf import settings
from django.utils.translation import gettext_lazy as _

import geocoder

# Remove the GeocoderError import since it doesn't exist in the current version
# and use a generic Exception instead

from apps.users.models import RegistroAuditoria
from scripts.StaticPage import StaticPage

# Configurar logger
logger = logging.getLogger(__name__)


def obtener_ip_cliente(request: HttpRequest) -> str:
    """
    Obtiene la dirección IP del cliente de forma segura.

    Prioriza X-Forwarded-For en caso de estar detrás de un proxy,
    con fallback a REMOTE_ADDR.

    Args:
        request: La solicitud HTTP

    Returns:
        str: La dirección IP del cliente
    """
    try:
        # Obtener IP considerando proxies
        x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
        if x_forwarded_for:
            ip = x_forwarded_for.split(",")[0].strip()
        else:
            ip = request.META.get("REMOTE_ADDR", "")

        # Validar que sea una dirección IP válida
        if ip:
            try:
                IPv4Address(ip) or IPv6Address(ip)
                return ip
            except AddressValueError:
                logger.warning(f"Dirección IP no válida: {ip}")
                return "0.0.0.0"  # IP de fallback
        else:
            return "0.0.0.0"  # IP de fallback

    except Exception as e:
        logger.error(f"Error al obtener IP: {str(e)}")
        return "0.0.0.0"  # IP de fallback


def obtener_ubicacion(ip: str) -> Dict[str, Any]:
    """
    Obtiene información de ubicación a partir de una IP.

    Args:
        ip: Dirección IP del cliente

    Returns:
        Dict: Información de ubicación (ciudad)
    """
    try:
        if ip in ("127.0.0.1", "localhost", "0.0.0.0", "::1"):
            return {"city": "Local"}

        # Establecer un timeout para evitar bloquear la aplicación
        gc = geocoder.ip(ip, timeout=3)

        if gc.ok:
            return {"city": gc.city}
        else:
            logger.warning(f"No se pudo geocodificar la IP: {ip}, Error: {gc.error}")
            return {"city": None}

    except Exception as e:
        logger.error(f"Error de geocodificación para IP {ip}: {str(e)}")
        return {"city": None}


def sanitizar_datos_sensibles(datos: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitiza datos sensibles que no deben guardarse en auditoría.

    Args:
        datos: Diccionario con datos de la solicitud

    Returns:
        Dict: Datos sanitizados
    """
    # Copia para no modificar el original
    datos_filtrados = datos.copy()

    # Lista de campos sensibles a sanitizar
    campos_sensibles = [
        "password",
        "password1",
        "password2",
        "token",
        "key",
        "secret",
        "credit_card",
        "authorization",
    ]

    # Sanitizar campos sensibles
    for campo in campos_sensibles:
        if campo in datos_filtrados:
            datos_filtrados[campo] = "********"

    return datos_filtrados


def grabar_auditoria(request: HttpRequest, detalle: Dict[str, Any]) -> None:
    """
    Guarda un registro de auditoría en la base de datos.

    Args:
        request: La solicitud HTTP
        detalle: Información detallada para auditar
    """
    try:
        # Asegurar que el usuario está autenticado
        if not request.user.is_authenticated:
            logger.warning("Intento de grabar auditoría de usuario no autenticado")
            return

        # Obtener IP del cliente
        ip = obtener_ip_cliente(request)

        # Obtener información de ubicación
        ubicacion = obtener_ubicacion(ip)

        # Sanitizar datos sensibles
        if "datos" in detalle and isinstance(detalle["datos"], dict):
            detalle["datos"] = sanitizar_datos_sensibles(detalle["datos"])

        # Crear y guardar el registro
        registro = RegistroAuditoria(
            usuario=request.user,
            ip=ip,
            transaccion=request.path_info,
            detalle=detalle,
            database_name=getattr(StaticPage, "name", None),
            city=ubicacion.get("city"),
        )
        registro.save()

    except Exception as e:
        # Loguea el error pero nunca interrumpe el flujo principal
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error al grabar auditoría: {str(e)}")
        # No relanzar la excepción


def registrar_auditoria(view_func: Callable) -> Callable:
    """
    Decorador que registra información de auditoría después de ejecutar una vista.

    Args:
        view_func: La función vista a decorar

    Returns:
        Callable: Función wrapper que ejecuta la vista y registra la auditoría
    """

    @wraps(view_func)
    def wrapper(request: HttpRequest, *args, **kwargs) -> HttpResponse:
        # Timestamp inicio
        tiempo_inicio = timezone.now()

        # Ejecutamos la vista y obtenemos la respuesta
        try:
            response = view_func(request, *args, **kwargs)
        except Exception as e:
            # Si hay una excepción, registramos la auditoría con información del error
            detalle = {
                "metodo": request.method,
                "datos": sanitizar_datos_sensibles(
                    request.POST.dict()
                    if request.method == "POST"
                    else request.GET.dict()
                ),
                "error": str(e),
                "duracion_ms": (timezone.now() - tiempo_inicio).total_seconds() * 1000,
            }
            try:
                grabar_auditoria(request, detalle)
            except Exception as err:
                # Nunca interrumpir el flujo por error de auditoría
                import logging

                logger = logging.getLogger(__name__)
                logger.error(f"Error en auditoría (except): {err}")
            # Re-lanzamos la excepción para que sea manejada por el middleware de Django
            raise

        # Grabamos los datos de auditoría en la base de datos
        detalle = {
            "metodo": request.method,
            "datos": sanitizar_datos_sensibles(
                request.POST.dict() if request.method == "POST" else request.GET.dict()
            ),
            "status_code": getattr(response, "status_code", None),
            "duracion_ms": (timezone.now() - tiempo_inicio).total_seconds() * 1000,
        }
        try:
            grabar_auditoria(request, detalle)
        except Exception as err:
            # Nunca interrumpir el flujo por error de auditoría
            import logging

            logger = logging.getLogger(__name__)
            logger.error(f"Error en auditoría (normal): {err}")

        return response

    return wrapper
