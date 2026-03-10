"""
Helpers de permisos para el módulo BIMBO.

Uso en vistas:
    from apps.bimbo.permissions import get_agencias_permitidas, puede_ejecutar

    agencias = get_agencias_permitidas(request.user)
    if puede_ejecutar(request.user, agencia_id):
        ...
"""
import logging

from sqlalchemy import text

from .models import PermisoBimboAgente

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Motor SQLAlchemy apuntando a powerbi_bimbo  (BI server)
# Se usa una empresa BIMBO cualquiera solo para obtener las
# credenciales del servidor BI, luego abrimos 'powerbi_bimbo'.
# ─────────────────────────────────────────────────────────────

def _get_bimbo_engine():
    """
    Retorna un engine SQLAlchemy conectado a powerbi_bimbo.
    Usa ConfigBasic de la primera empresa con es_bimbo=1 para
    obtener host/user/pass del servidor BI.
    """
    try:
        from scripts.config import ConfigBasic
        from scripts.conexion import Conexion
        from apps.permisos.models import ConfEmpresas

        primera = ConfEmpresas.objects.filter(es_bimbo=1).values(
            "name"
        ).first()
        if not primera:
            raise ValueError("No hay empresas bimbo configuradas en conf_empresas")

        config = ConfigBasic(primera["name"]).config
        return Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            "powerbi_bimbo",
        )
    except Exception as exc:
        logger.error("Error creando engine powerbi_bimbo en permissions: %s", exc)
        raise



def get_agencias_permitidas(user):
    """
    Retorna lista de dicts con las agencias BIMBO permitidas al usuario.

    Enfoque dual:
      - powerbi_adm.conf_empresas  → control de acceso (qué empresas ve el user)
      - powerbi_bimbo.agencias_bimbo → datos reales de agencia (CEVE, nombre, estado)

    Si la consulta a agencias_bimbo falla, intenta fallback con ConfEmpresas.
    """
    from apps.permisos.models import ConfEmpresas

    # 1) Empresas BIMBO a las que el usuario tiene acceso
    if user.is_superuser:
        empresas = ConfEmpresas.objects.filter(es_bimbo=True, estado=1)
    else:
        try:
            empresas = user.conf_empresas.filter(es_bimbo=True, estado=1)
        except Exception:
            return []

    db_names = [e.name for e in empresas if e.name]
    if not db_names:
        return []

    # 2) Cruzar con agencias_bimbo para obtener CEVE, nombre, estado reales
    try:
        engine = _get_bimbo_engine()
        placeholders = ", ".join(f":db{i}" for i in range(len(db_names)))
        params = {f"db{i}": name for i, name in enumerate(db_names)}

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, CEVE, Nombre, nmOficinaV, db_powerbi, estado "
                    f"FROM agencias_bimbo "
                    f"WHERE db_powerbi IN ({placeholders}) "
                    "ORDER BY CEVE"
                ),
                params,
            ).mappings().all()

        return [
            {
                "id": r["id"],
                "CEVE": r["CEVE"],
                "Nombre": r["Nombre"],
                "nmOficinaV": r["nmOficinaV"] or r["Nombre"],
                "db_powerbi": r["db_powerbi"],
                "estado": r["estado"],
            }
            for r in rows
        ]
    except Exception as exc:
        logger.error("Error consultando agencias_bimbo: %s", exc)
        # Fallback: usar datos de ConfEmpresas si tiene ceve poblado
        agencias = []
        for e in empresas:
            if e.ceve:
                agencias.append({
                    "id": e.id,
                    "CEVE": e.ceve,
                    "Nombre": e.nmEmpresa or e.name,
                    "nmOficinaV": e.nmEmpresa or e.name,
                    "db_powerbi": e.name,
                    "estado": "ACTIVO" if e.estado == 1 else "INACTIVO",
                })
        return agencias


def get_agencias_ids(user):
    """Retorna lista de IDs de agencias permitidas."""
    return [a["id"] for a in get_agencias_permitidas(user)]


def get_ceves_permitidos(user):
    """Retorna lista de CEVEs permitidos para el usuario."""
    return [a["CEVE"] for a in get_agencias_permitidas(user)]


def puede_ejecutar(user, agencia_id):
    """Verifica si el usuario puede ejecutar Discovery/Snapshot/Homologación."""
    if user.is_superuser:
        return True
    return PermisoBimboAgente.objects.filter(
        user=user,
        agencia_id=agencia_id,
        puede_ejecutar=True,
    ).exists()


def puede_editar(user, agencia_id):
    """Verifica si el usuario puede hacer match manual en equivalencias."""
    if user.is_superuser:
        return True
    return PermisoBimboAgente.objects.filter(
        user=user,
        agencia_id=agencia_id,
        puede_editar=True,
    ).exists()
