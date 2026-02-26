"""
Configuración de empresas/fuentes para cargue InfoProducto

Este archivo define las empresas que pueden cargar archivos InfoProducto.
Cada empresa tiene su propia configuración de fuente_id.
"""

EMPRESAS_INFOPRODUCTO = {
    "distrijass": {
        "fuente_id": "DISTRIJASS",
        "fuente_nombre": "Distrijass",
        "descripcion": "Distrijass",
        "slug": "distrijass",
        "url_path": "distrijass",
        "color": "#007bff",  # Azul
    },
    "eje": {
        "fuente_id": "EJE",
        "fuente_nombre": "Eje",
        "descripcion": "Eje",
        "slug": "eje",
        "url_path": "eje",
        "color": "#28a745",  # Verde
    },
    "nestle_cali": {
        "fuente_id": "NESTLE_CALI",
        "fuente_nombre": "Nestlé - Cali",
        "descripcion": "Nestlé Cali",
        "slug": "nestle-cali",
        "url_path": "nestle-cali",
        "color": "#dc3545",  # Rojo Nestlé
    },
    "nestle_popayan": {
        "fuente_id": "NESTLE_POPAYAN",
        "fuente_nombre": "Nestlé - Popayán",
        "descripcion": "Nestlé Popayán",
        "slug": "nestle-popayan",
        "url_path": "nestle-popayan",
        "color": "#dc3545",  # Rojo Nestlé
    },
}


def get_empresa_by_slug(slug: str):
    """Obtener configuración de empresa por slug"""
    return EMPRESAS_INFOPRODUCTO.get(slug)


def get_empresa_by_fuente_id(fuente_id: str):
    """Obtener configuración de empresa por fuente_id"""
    for empresa in EMPRESAS_INFOPRODUCTO.values():
        if empresa["fuente_id"] == fuente_id:
            return empresa
    return None


def get_todas_empresas():
    """Obtener lista de todas las empresas"""
    return list(EMPRESAS_INFOPRODUCTO.values())


def get_empresas_para_menu():
    """Obtener empresas ordenadas para el menú"""
    empresas = list(EMPRESAS_INFOPRODUCTO.values())
    # Ordenar: primero por nombre
    return sorted(empresas, key=lambda x: x["fuente_nombre"])
