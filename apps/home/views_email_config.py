"""
Views para configuracion de correos de reportes programados.
CRUD de proveedores_bi y supervisores sobre la base BI remota.
"""
import logging
import os

from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, PermissionRequiredMixin
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.views import View
from sqlalchemy import text

import django_rq

from scripts.config import ConfigBasic
from scripts.conexion import Conexion

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_bi_engine(database_name):
    """Crea engine SQLAlchemy hacia la base BI de la empresa."""
    config = ConfigBasic(database_name)
    c = config.config
    db_bi = c.get("dbBi")
    if not db_bi:
        return None, None
    engine = Conexion.ConexionMariadb3(
        str(c.get("nmUsrIn")),
        str(c.get("txPassIn")),
        str(c.get("hostServerIn")),
        int(c.get("portServerIn")),
        db_bi,
    )
    return engine, db_bi


def _require_database(request):
    """Valida que haya empresa seleccionada en sesion. Retorna database_name o None."""
    return request.session.get("database_name")


class EmailConfigBaseView(LoginRequiredMixin, PermissionRequiredMixin, View):
    """Base para todas las vistas de configuracion de email."""
    login_url = reverse_lazy("users_app:user-login")
    permission_required = "permisos.config_email_reportes"

    def dispatch(self, request, *args, **kwargs):
        # Interceptar cambio de base de datos desde el selector (mismo patrón que BaseView)
        if request.method == "POST" and "database_select" in request.POST:
            db = request.POST.get("database_select")
            if db and request.user.conf_empresas.filter(name=db).exists():
                request.session["database_name"] = db
                request.session.modified = True
                request.session.save()
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return JsonResponse({"status": "success", "database_name": db})
            return redirect(request.path)
        return super().dispatch(request, *args, **kwargs)


# ---------------------------------------------------------------------------
# Inicializar tablas
# ---------------------------------------------------------------------------

class InitEmailTablesView(EmailConfigBaseView):
    """Crea las tablas de email en la base BI seleccionada."""

    def post(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            messages.error(request, "No se pudo conectar a la base BI.")
            return redirect("home_app:panel_cubo")

        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "scripts", "sql", "create_tables_email_reports.sql",
        )
        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Eliminar lineas de comentario antes de dividir por ;
            lines = [
                line for line in sql_content.splitlines()
                if not line.strip().startswith("--")
            ]
            clean_sql = "\n".join(lines)

            with engine.connect() as conn:
                for statement in clean_sql.split(";"):
                    stmt = statement.strip()
                    if stmt:
                        conn.execute(text(stmt))
                conn.commit()

            messages.success(request, f"Tablas de correo creadas en {db_bi}.")
        except Exception as exc:
            logger.error("Error creando tablas email en %s: %s", db_bi, exc)
            messages.error(request, f"Error creando tablas: {exc}")

        return redirect("home_app:proveedores_bi_list")


# ---------------------------------------------------------------------------
# Proveedores CRUD
# ---------------------------------------------------------------------------

class ProveedoresBiListView(EmailConfigBaseView):
    """Lista proveedores_bi con sus correos."""

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            messages.error(request, "No se pudo conectar a la base BI.")
            return redirect("home_app:panel_cubo")

        proveedores = []
        tablas_existen = True
        try:
            with engine.connect() as conn:
                # Verificar si la tabla existe
                check = conn.execute(
                    text(
                        "SELECT COUNT(*) AS cnt FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'proveedores_bi'"
                    ),
                    {"db": db_bi},
                ).scalar()
                if not check:
                    tablas_existen = False
                else:
                    rows = conn.execute(
                        text(
                            "SELECT p.id, p.nombre, p.nit, p.proveedor_ids, p.activo, p.notas, "
                            "GROUP_CONCAT(CASE WHEN pc.activo = 1 THEN pc.correo END SEPARATOR ', ') AS correos "
                            "FROM proveedores_bi p "
                            "LEFT JOIN proveedores_correo pc ON pc.proveedor_id = p.id "
                            "GROUP BY p.id ORDER BY p.nombre"
                        )
                    ).mappings().all()
                    proveedores = [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Error listando proveedores en %s: %s", db_bi, exc)
            messages.error(request, f"Error consultando proveedores: {exc}")

        return render(request, "home/email_config/proveedores_list.html", {
            "proveedores": proveedores,
            "tablas_existen": tablas_existen,
            "database_name": database_name,
            "form_url": "home_app:proveedores_bi_list",
        })


class ProveedoresBiCreateView(EmailConfigBaseView):
    """Crear proveedor con correos."""

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")
        return render(request, "home/email_config/proveedores_form.html", {
            "modo": "crear",
            "database_name": database_name,
            "form_url": "home_app:proveedores_bi_list",
        })

    def post(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            messages.error(request, "No se pudo conectar a la base BI.")
            return redirect("home_app:proveedores_bi_list")

        nombre = request.POST.get("nombre", "").strip()
        nit = request.POST.get("nit", "").strip() or None
        proveedor_ids = request.POST.get("proveedor_ids", "").strip() or None
        notas = request.POST.get("notas", "").strip() or None
        activo = 1 if request.POST.get("activo") else 0
        correos = [c.strip() for c in request.POST.getlist("correos") if c.strip()]

        if not nombre:
            messages.error(request, "El nombre es obligatorio.")
            return render(request, "home/email_config/proveedores_form.html", {
                "modo": "crear",
                "database_name": database_name,
                "form_data": request.POST,
                "form_url": "home_app:proveedores_bi_list",
            })

        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO proveedores_bi (nombre, nit, proveedor_ids, activo, notas) "
                        "VALUES (:nombre, :nit, :proveedor_ids, :activo, :notas)"
                    ),
                    {"nombre": nombre, "nit": nit, "proveedor_ids": proveedor_ids,
                     "activo": activo, "notas": notas},
                )
                prov_id = result.lastrowid
                for correo in correos:
                    conn.execute(
                        text(
                            "INSERT INTO proveedores_correo (proveedor_id, correo) "
                            "VALUES (:prov_id, :correo)"
                        ),
                        {"prov_id": prov_id, "correo": correo},
                    )
                conn.commit()
            messages.success(request, f"Proveedor '{nombre}' creado correctamente.")
        except Exception as exc:
            logger.error("Error creando proveedor en %s: %s", db_bi, exc)
            messages.error(request, f"Error: {exc}")

        return redirect("home_app:proveedores_bi_list")


class ProveedoresBiEditView(EmailConfigBaseView):
    """Editar proveedor existente."""

    def get(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:proveedores_bi_list")

        try:
            with engine.connect() as conn:
                prov = conn.execute(
                    text("SELECT * FROM proveedores_bi WHERE id = :pk"),
                    {"pk": pk},
                ).mappings().first()
                if not prov:
                    messages.error(request, "Proveedor no encontrado.")
                    return redirect("home_app:proveedores_bi_list")

                correos = conn.execute(
                    text(
                        "SELECT id, correo, activo FROM proveedores_correo "
                        "WHERE proveedor_id = :pk ORDER BY id"
                    ),
                    {"pk": pk},
                ).mappings().all()

            return render(request, "home/email_config/proveedores_form.html", {
                "modo": "editar",
                "proveedor": dict(prov),
                "correos": [dict(c) for c in correos],
                "database_name": database_name,
                "form_url": "home_app:proveedores_bi_list",
            })
        except Exception as exc:
            logger.error("Error cargando proveedor %s en %s: %s", pk, db_bi, exc)
            messages.error(request, f"Error: {exc}")
            return redirect("home_app:proveedores_bi_list")

    def post(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:proveedores_bi_list")

        nombre = request.POST.get("nombre", "").strip()
        nit = request.POST.get("nit", "").strip() or None
        proveedor_ids = request.POST.get("proveedor_ids", "").strip() or None
        notas = request.POST.get("notas", "").strip() or None
        activo = 1 if request.POST.get("activo") else 0
        correos = [c.strip() for c in request.POST.getlist("correos") if c.strip()]

        if not nombre:
            messages.error(request, "El nombre es obligatorio.")
            return redirect("home_app:proveedores_bi_edit", pk=pk)

        try:
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "UPDATE proveedores_bi SET nombre=:nombre, nit=:nit, "
                        "proveedor_ids=:proveedor_ids, activo=:activo, notas=:notas "
                        "WHERE id=:pk"
                    ),
                    {"nombre": nombre, "nit": nit, "proveedor_ids": proveedor_ids,
                     "activo": activo, "notas": notas, "pk": pk},
                )
                # Reemplazar correos: borrar y reinsertar
                conn.execute(
                    text("DELETE FROM proveedores_correo WHERE proveedor_id = :pk"),
                    {"pk": pk},
                )
                for correo in correos:
                    conn.execute(
                        text(
                            "INSERT INTO proveedores_correo (proveedor_id, correo) "
                            "VALUES (:pk, :correo)"
                        ),
                        {"pk": pk, "correo": correo},
                    )
                conn.commit()
            messages.success(request, f"Proveedor '{nombre}' actualizado.")
        except Exception as exc:
            logger.error("Error actualizando proveedor %s en %s: %s", pk, db_bi, exc)
            messages.error(request, f"Error: {exc}")

        return redirect("home_app:proveedores_bi_list")


class ProveedoresBiDeleteView(EmailConfigBaseView):
    """Eliminar proveedor (CASCADE borra correos)."""

    def post(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:proveedores_bi_list")

        try:
            with engine.connect() as conn:
                conn.execute(
                    text("DELETE FROM proveedores_bi WHERE id = :pk"),
                    {"pk": pk},
                )
                conn.commit()
            messages.success(request, "Proveedor eliminado.")
        except Exception as exc:
            logger.error("Error eliminando proveedor %s en %s: %s", pk, db_bi, exc)
            messages.error(request, f"Error: {exc}")

        return redirect("home_app:proveedores_bi_list")


# ---------------------------------------------------------------------------
# Carga masiva desde Excel
# ---------------------------------------------------------------------------

class CargaMasivaProveedoresView(EmailConfigBaseView):
    """Carga masiva de proveedores + correos desde archivo Excel (.xlsx).

    Formato esperado del Excel:
      nombre | nit | proveedor_ids | correo1 | correo2 | ... | correoN

    - Columnas nombre es obligatoria.
    - Columnas correoX son opcionales (tantas como se necesiten).
    - Si el proveedor (por nombre) ya existe, se omite.
    """

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")
        return render(request, "home/email_config/carga_masiva.html", {
            "database_name": database_name,
            "form_url": "home_app:proveedores_bi_list",
            "tipo": "proveedores",
        })

    def post(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            messages.error(request, "No se pudo conectar a la base BI.")
            return redirect("home_app:proveedores_bi_list")

        archivo = request.FILES.get("archivo")
        if not archivo:
            messages.error(request, "Debe seleccionar un archivo Excel.")
            return redirect("home_app:carga_masiva_proveedores")

        try:
            import pandas as pd
            df = pd.read_excel(archivo, engine="openpyxl")
            df.columns = [c.strip().lower() for c in df.columns]

            if "nombre" not in df.columns:
                messages.error(request, "El Excel debe tener al menos la columna 'nombre'.")
                return redirect("home_app:carga_masiva_proveedores")

            # Detectar columnas de correo (correo, correo1, correo2, email, email1...)
            correo_cols = [c for c in df.columns if c.startswith("correo") or c.startswith("email")]

            creados = 0
            omitidos = 0
            with engine.connect() as conn:
                for _, row in df.iterrows():
                    nombre = str(row.get("nombre", "")).strip()
                    if not nombre:
                        continue

                    # Verificar duplicado por nombre
                    existe = conn.execute(
                        text("SELECT id FROM proveedores_bi WHERE nombre = :n"),
                        {"n": nombre},
                    ).scalar()
                    if existe:
                        omitidos += 1
                        continue

                    nit = str(row.get("nit", "")).strip() if pd.notna(row.get("nit")) else None
                    prov_ids = str(row.get("proveedor_ids", "")).strip() if pd.notna(row.get("proveedor_ids")) else None

                    result = conn.execute(
                        text(
                            "INSERT INTO proveedores_bi (nombre, nit, proveedor_ids, activo) "
                            "VALUES (:nombre, :nit, :prov_ids, 1)"
                        ),
                        {"nombre": nombre, "nit": nit, "prov_ids": prov_ids},
                    )
                    prov_id = result.lastrowid

                    # Insertar correos de todas las columnas de correo
                    for col in correo_cols:
                        correo = str(row.get(col, "")).strip() if pd.notna(row.get(col)) else ""
                        if correo and "@" in correo:
                            conn.execute(
                                text(
                                    "INSERT INTO proveedores_correo (proveedor_id, correo) "
                                    "VALUES (:pid, :correo)"
                                ),
                                {"pid": prov_id, "correo": correo},
                            )

                    creados += 1
                conn.commit()

            msg = f"Carga completada: {creados} proveedores creados"
            if omitidos:
                msg += f", {omitidos} omitidos (ya existían)"
            messages.success(request, msg)

        except Exception as exc:
            logger.error("Error en carga masiva proveedores en %s: %s", db_bi, exc)
            messages.error(request, f"Error procesando archivo: {exc}")

        return redirect("home_app:proveedores_bi_list")


class CargaMasivaSupervisoresView(EmailConfigBaseView):
    """Carga masiva de supervisores + correos desde archivo Excel (.xlsx).

    Formato esperado del Excel:
      nombre | correo1 | correo2 | ... | correoN

    - Columna nombre es obligatoria.
    - Macrozonas se asignan despues manualmente desde el formulario de edicion.
    """

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")
        return render(request, "home/email_config/carga_masiva.html", {
            "database_name": database_name,
            "form_url": "home_app:supervisores_list",
            "tipo": "supervisores",
        })

    def post(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            messages.error(request, "No se pudo conectar a la base BI.")
            return redirect("home_app:supervisores_list")

        archivo = request.FILES.get("archivo")
        if not archivo:
            messages.error(request, "Debe seleccionar un archivo Excel.")
            return redirect("home_app:carga_masiva_supervisores")

        try:
            import pandas as pd
            df = pd.read_excel(archivo, engine="openpyxl")
            df.columns = [c.strip().lower() for c in df.columns]

            if "nombre" not in df.columns:
                messages.error(request, "El Excel debe tener al menos la columna 'nombre'.")
                return redirect("home_app:carga_masiva_supervisores")

            correo_cols = [c for c in df.columns if c.startswith("correo") or c.startswith("email")]

            creados = 0
            omitidos = 0
            with engine.connect() as conn:
                for _, row in df.iterrows():
                    nombre = str(row.get("nombre", "")).strip()
                    if not nombre:
                        continue

                    existe = conn.execute(
                        text("SELECT id FROM supervisores WHERE nombre = :n"),
                        {"n": nombre},
                    ).scalar()
                    if existe:
                        omitidos += 1
                        continue

                    result = conn.execute(
                        text(
                            "INSERT INTO supervisores (nombre, activo) "
                            "VALUES (:nombre, 1)"
                        ),
                        {"nombre": nombre},
                    )
                    sup_id = result.lastrowid

                    for col in correo_cols:
                        correo = str(row.get(col, "")).strip() if pd.notna(row.get(col)) else ""
                        if correo and "@" in correo:
                            conn.execute(
                                text(
                                    "INSERT INTO supervisores_correo (supervisor_id, correo) "
                                    "VALUES (:sid, :correo)"
                                ),
                                {"sid": sup_id, "correo": correo},
                            )

                    creados += 1
                conn.commit()

            msg = f"Carga completada: {creados} supervisores creados"
            if omitidos:
                msg += f", {omitidos} omitidos (ya existían)"
            messages.success(request, msg)

        except Exception as exc:
            logger.error("Error en carga masiva supervisores en %s: %s", db_bi, exc)
            messages.error(request, f"Error procesando archivo: {exc}")

        return redirect("home_app:supervisores_list")


# ---------------------------------------------------------------------------
# Supervisores CRUD
# ---------------------------------------------------------------------------

class SupervisoresListView(EmailConfigBaseView):
    """Lista supervisores con macrozonas y correos."""

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            messages.error(request, "No se pudo conectar a la base BI.")
            return redirect("home_app:panel_cubo")

        supervisores = []
        tablas_existen = True
        try:
            with engine.connect() as conn:
                check = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'supervisores'"
                    ),
                    {"db": db_bi},
                ).scalar()
                if not check:
                    tablas_existen = False
                else:
                    rows = conn.execute(
                        text(
                            "SELECT s.id, s.nombre, s.activo, s.notas, "
                            "GROUP_CONCAT(DISTINCT CASE WHEN sc.activo = 1 THEN sc.correo END SEPARATOR ', ') AS correos, "
                            "GROUP_CONCAT(DISTINCT sm.macrozona_id SEPARATOR ', ') AS macrozonas "
                            "FROM supervisores s "
                            "LEFT JOIN supervisores_correo sc ON sc.supervisor_id = s.id "
                            "LEFT JOIN supervisores_macrozona sm ON sm.supervisor_id = s.id "
                            "GROUP BY s.id ORDER BY s.nombre"
                        )
                    ).mappings().all()
                    supervisores = [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Error listando supervisores en %s: %s", db_bi, exc)
            messages.error(request, f"Error consultando supervisores: {exc}")

        return render(request, "home/email_config/supervisores_list.html", {
            "supervisores": supervisores,
            "tablas_existen": tablas_existen,
            "database_name": database_name,
            "form_url": "home_app:supervisores_list",
        })


class SupervisoresCreateView(EmailConfigBaseView):
    """Crear supervisor con correos y macrozonas."""

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")
        return render(request, "home/email_config/supervisores_form.html", {
            "modo": "crear",
            "database_name": database_name,
            "form_url": "home_app:supervisores_list",
        })

    def post(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:supervisores_list")

        nombre = request.POST.get("nombre", "").strip()
        notas = request.POST.get("notas", "").strip() or None
        activo = 1 if request.POST.get("activo") else 0
        correos = [c.strip() for c in request.POST.getlist("correos") if c.strip()]
        macrozonas = request.POST.getlist("macrozonas")

        if not nombre:
            messages.error(request, "El nombre es obligatorio.")
            return render(request, "home/email_config/supervisores_form.html", {
                "modo": "crear",
                "database_name": database_name,
                "form_data": request.POST,
                "form_url": "home_app:supervisores_list",
            })

        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "INSERT INTO supervisores (nombre, activo, notas) "
                        "VALUES (:nombre, :activo, :notas)"
                    ),
                    {"nombre": nombre, "activo": activo, "notas": notas},
                )
                sup_id = result.lastrowid
                for correo in correos:
                    conn.execute(
                        text(
                            "INSERT INTO supervisores_correo (supervisor_id, correo) "
                            "VALUES (:sup_id, :correo)"
                        ),
                        {"sup_id": sup_id, "correo": correo},
                    )
                for mz_id in macrozonas:
                    conn.execute(
                        text(
                            "INSERT INTO supervisores_macrozona (supervisor_id, macrozona_id) "
                            "VALUES (:sup_id, :mz_id)"
                        ),
                        {"sup_id": sup_id, "mz_id": int(mz_id)},
                    )
                conn.commit()
            messages.success(request, f"Supervisor '{nombre}' creado correctamente.")
        except Exception as exc:
            logger.error("Error creando supervisor en %s: %s", db_bi, exc)
            messages.error(request, f"Error: {exc}")

        return redirect("home_app:supervisores_list")


class SupervisoresEditView(EmailConfigBaseView):
    """Editar supervisor existente."""

    def get(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:supervisores_list")

        try:
            with engine.connect() as conn:
                sup = conn.execute(
                    text("SELECT * FROM supervisores WHERE id = :pk"),
                    {"pk": pk},
                ).mappings().first()
                if not sup:
                    messages.error(request, "Supervisor no encontrado.")
                    return redirect("home_app:supervisores_list")

                correos = conn.execute(
                    text(
                        "SELECT id, correo, activo FROM supervisores_correo "
                        "WHERE supervisor_id = :pk ORDER BY id"
                    ),
                    {"pk": pk},
                ).mappings().all()

                macrozonas_asignadas = conn.execute(
                    text(
                        "SELECT macrozona_id FROM supervisores_macrozona "
                        "WHERE supervisor_id = :pk"
                    ),
                    {"pk": pk},
                ).scalars().all()

            return render(request, "home/email_config/supervisores_form.html", {
                "modo": "editar",
                "supervisor": dict(sup),
                "correos": [dict(c) for c in correos],
                "macrozonas_asignadas": list(macrozonas_asignadas),
                "database_name": database_name,
                "form_url": "home_app:supervisores_list",
            })
        except Exception as exc:
            logger.error("Error cargando supervisor %s en %s: %s", pk, db_bi, exc)
            messages.error(request, f"Error: {exc}")
            return redirect("home_app:supervisores_list")

    def post(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:supervisores_list")

        nombre = request.POST.get("nombre", "").strip()
        notas = request.POST.get("notas", "").strip() or None
        activo = 1 if request.POST.get("activo") else 0
        correos = [c.strip() for c in request.POST.getlist("correos") if c.strip()]
        macrozonas = request.POST.getlist("macrozonas")

        if not nombre:
            messages.error(request, "El nombre es obligatorio.")
            return redirect("home_app:supervisores_edit", pk=pk)

        try:
            with engine.connect() as conn:
                conn.execute(
                    text(
                        "UPDATE supervisores SET nombre=:nombre, activo=:activo, notas=:notas "
                        "WHERE id=:pk"
                    ),
                    {"nombre": nombre, "activo": activo, "notas": notas, "pk": pk},
                )
                # Reemplazar correos
                conn.execute(
                    text("DELETE FROM supervisores_correo WHERE supervisor_id = :pk"),
                    {"pk": pk},
                )
                for correo in correos:
                    conn.execute(
                        text(
                            "INSERT INTO supervisores_correo (supervisor_id, correo) "
                            "VALUES (:pk, :correo)"
                        ),
                        {"pk": pk, "correo": correo},
                    )
                # Reemplazar macrozonas
                conn.execute(
                    text("DELETE FROM supervisores_macrozona WHERE supervisor_id = :pk"),
                    {"pk": pk},
                )
                for mz_id in macrozonas:
                    conn.execute(
                        text(
                            "INSERT INTO supervisores_macrozona (supervisor_id, macrozona_id) "
                            "VALUES (:pk, :mz_id)"
                        ),
                        {"pk": pk, "mz_id": int(mz_id)},
                    )
                conn.commit()
            messages.success(request, f"Supervisor '{nombre}' actualizado.")
        except Exception as exc:
            logger.error("Error actualizando supervisor %s en %s: %s", pk, db_bi, exc)
            messages.error(request, f"Error: {exc}")

        return redirect("home_app:supervisores_list")


class SupervisoresDeleteView(EmailConfigBaseView):
    """Eliminar supervisor (CASCADE borra correos y macrozonas)."""

    def post(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return redirect("home_app:panel_cubo")

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return redirect("home_app:supervisores_list")

        try:
            with engine.connect() as conn:
                conn.execute(
                    text("DELETE FROM supervisores WHERE id = :pk"),
                    {"pk": pk},
                )
                conn.commit()
            messages.success(request, "Supervisor eliminado.")
        except Exception as exc:
            logger.error("Error eliminando supervisor %s en %s: %s", pk, db_bi, exc)
            messages.error(request, f"Error: {exc}")

        return redirect("home_app:supervisores_list")


# ---------------------------------------------------------------------------
# AJAX: Macrozonas disponibles desde tabla zona
# ---------------------------------------------------------------------------

class MacrozonasJsonView(EmailConfigBaseView):
    """Retorna macrozonas unicas de la tabla zona para checkboxes."""

    def get(self, request, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            return JsonResponse({"ok": False, "error": "No hay empresa seleccionada."})

        engine, db_bi = _get_bi_engine(database_name)
        if not engine:
            return JsonResponse({"ok": False, "error": "No se pudo conectar."})

        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT macrozona_id, macro "
                        "FROM zona WHERE macrozona_id IS NOT NULL "
                        "ORDER BY macro"
                    )
                ).mappings().all()
                macrozonas = [{"id": r["macrozona_id"], "nombre": r["macro"]} for r in rows]
            return JsonResponse({"ok": True, "macrozonas": macrozonas})
        except Exception as exc:
            logger.error("Error obteniendo macrozonas de %s: %s", db_bi, exc)
            return JsonResponse({"ok": False, "error": str(exc)})


# ---------------------------------------------------------------------------
# Envio individual de reporte a proveedor
# ---------------------------------------------------------------------------

class EnviarReporteProveedorView(EmailConfigBaseView):
    """Encola el envio de reporte para un proveedor especifico."""

    def post(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        try:
            from apps.home.tasks import enviar_reporte_email_proveedor_task
            queue = django_rq.get_queue("default")
            queue.enqueue(
                enviar_reporte_email_proveedor_task,
                database_name,
                pk,
            )
            messages.success(
                request,
                f"Envio de reporte encolado para proveedor #{pk}. "
                "Revise el log de envios en unos minutos.",
            )
        except Exception as exc:
            logger.error("Error encolando envio proveedor %s: %s", pk, exc)
            messages.error(request, f"Error al encolar envio: {exc}")

        return redirect("home_app:proveedores_bi_list")


class EnviarReporteSupervisorView(EmailConfigBaseView):
    """Encola el envio de reporte para un supervisor especifico."""

    def post(self, request, pk, *args, **kwargs):
        database_name = _require_database(request)
        if not database_name:
            messages.error(request, "No hay empresa seleccionada.")
            return redirect("home_app:panel_cubo")

        try:
            from apps.home.tasks import enviar_reporte_email_supervisor_task
            queue = django_rq.get_queue("default")
            queue.enqueue(
                enviar_reporte_email_supervisor_task,
                database_name,
                pk,
            )
            messages.success(
                request,
                f"Envio de reporte encolado para supervisor #{pk}. "
                "Revise el log de envios en unos minutos.",
            )
        except Exception as exc:
            logger.error("Error encolando envio supervisor %s: %s", pk, exc)
            messages.error(request, f"Error al encolar envio: {exc}")

        return redirect("home_app:supervisores_list")


# ---------------------------------------------------------------------------
# Programación de Tareas
# ---------------------------------------------------------------------------

def reprogramar_tareas(logger_inst=None):
    """Cancela y reprograma todas las tareas según la BD ProgramacionTarea."""
    import importlib
    from datetime import datetime, timedelta
    from apps.permisos.models import ProgramacionTarea

    log = logger_inst or logger

    try:
        from django_rq import get_scheduler
        scheduler = get_scheduler("default")
    except Exception as exc:
        log.error("No se pudo obtener el scheduler: %s", exc)
        return

    try:
        tareas = list(ProgramacionTarea.objects.select_related("empresa").all())
    except Exception:
        # Migracion 0029 no aplicada aun — campo empresa no existe
        log.warning("Campo 'empresa' no disponible. Cargando tareas sin select_related.")
        tareas = list(ProgramacionTarea.objects.all())

    func_paths = {t.func_path for t in tareas}

    # Cancelar jobs existentes que coincidan
    for job in scheduler.get_jobs():
        if job.func_name in func_paths:
            scheduler.cancel(job)

    # Reprogramar las activas
    for tarea in tareas:
        if not tarea.activo:
            log.info("Tarea '%s' desactivada, omitiendo.", tarea.nombre)
            continue

        try:
            module_path, func_name = tarea.func_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)
        except (ImportError, AttributeError) as exc:
            log.error("No se pudo importar %s: %s", tarea.func_path, exc)
            continue

        now_utc = datetime.utcnow()
        hora_utc = tarea.hora_utc
        next_run = now_utc.replace(
            hour=hora_utc.hour, minute=hora_utc.minute, second=0, microsecond=0,
        )
        if next_run <= now_utc:
            next_run += timedelta(days=1)

        # Determinar args según tipo de tarea
        kwargs = {}
        empresa = getattr(tarea, "empresa", None)
        if empresa:
            if "email" in tarea.func_path:
                kwargs["args"] = [empresa.name]   # email usa database_name
            else:
                kwargs["args"] = [empresa.id]     # CDT/TSOL/Cosmos usan empresa_id
        elif "clean_old_media" in tarea.func_path:
            kwargs["args"] = [4]

        scheduler.schedule(
            scheduled_time=next_run,
            func=func,
            interval=tarea.intervalo_segundos,
            repeat=None,
            **kwargs,
        )
        log.info(
            "Tarea '%s' [%s] programada: %s UTC (cada %ds)",
            tarea.nombre,
            getattr(empresa, "name", "Global") if empresa else "Global",
            next_run,
            tarea.intervalo_segundos,
        )


class ProgramacionListView(EmailConfigBaseView):
    """Lista y edicion de horarios de tareas programadas (per-empresa)."""

    def get(self, request, *args, **kwargs):
        from apps.permisos.models import ProgramacionTarea, ConfEmpresas

        database_name = request.session.get("database_name")
        empresa = (
            ConfEmpresas.objects.filter(name=database_name).first()
            if database_name else None
        )

        try:
            if empresa:
                # Auto-crear tareas si la empresa no tiene ninguna
                if not ProgramacionTarea.objects.filter(empresa=empresa).exists():
                    self._crear_tareas_empresa(empresa)
                tareas_empresa = ProgramacionTarea.objects.filter(empresa=empresa)
            else:
                tareas_empresa = ProgramacionTarea.objects.none()

            # Tareas globales (empresa=NULL) — solo lectura
            tareas_globales = ProgramacionTarea.objects.filter(empresa__isnull=True)
        except Exception:
            # Migracion 0029 no aplicada aun — fallback a todas las tareas sin filtro
            logger.warning("Campo 'empresa' no disponible en ProgramacionTarea. Ejecute: manage.py migrate permisos")
            tareas_empresa = ProgramacionTarea.objects.all()
            tareas_globales = ProgramacionTarea.objects.none()

        return render(request, "home/email_config/programacion_list.html", {
            "tareas": tareas_empresa,
            "tareas_globales": tareas_globales,
            "empresa_actual": empresa,
            "database_name": request.session.get("database_name", ""),
            "form_url": "home_app:programacion_list",
        })

    def post(self, request, *args, **kwargs):
        from apps.permisos.models import ProgramacionTarea, ConfEmpresas
        import datetime

        database_name = request.session.get("database_name")
        empresa = (
            ConfEmpresas.objects.filter(name=database_name).first()
            if database_name else None
        )

        if not empresa:
            messages.error(request, "Seleccione una empresa primero.")
            return redirect("home_app:programacion_list")

        # Solo editar tareas de esta empresa (fallback: todas si migracion no aplicada)
        try:
            tareas = ProgramacionTarea.objects.filter(empresa=empresa)
        except Exception:
            tareas = ProgramacionTarea.objects.all()
        actualizadas = 0

        for tarea in tareas:
            hora_str = request.POST.get(f"hora_{tarea.id}", "")
            activo = request.POST.get(f"activo_{tarea.id}") == "on"

            cambio = False
            if hora_str:
                try:
                    h, m = hora_str.split(":")
                    nueva_hora = datetime.time(int(h), int(m))
                    if tarea.hora_local != nueva_hora:
                        tarea.hora_local = nueva_hora
                        cambio = True
                except (ValueError, TypeError):
                    pass

            if tarea.activo != activo:
                tarea.activo = activo
                cambio = True

            if cambio:
                tarea.save()
                actualizadas += 1

        # Reprogramar en el scheduler
        if actualizadas > 0:
            try:
                reprogramar_tareas(logger)
                messages.success(
                    request,
                    f"{actualizadas} tarea(s) actualizada(s) y reprogramada(s) correctamente.",
                )
            except Exception as exc:
                logger.error("Error reprogramando tareas: %s", exc)
                messages.warning(
                    request,
                    f"{actualizadas} tarea(s) guardada(s) en BD, pero error al reprogramar: {exc}. "
                    "Los cambios se aplicarán al reiniciar el servidor.",
                )
        else:
            messages.info(request, "No hubo cambios.")

        return redirect("home_app:programacion_list")

    @staticmethod
    def _crear_tareas_empresa(empresa):
        """Crea tareas por defecto según integraciones activas de la empresa."""
        from apps.permisos.models import ProgramacionTarea
        import datetime

        tareas = []
        if empresa.envio_email_activo:
            tareas.append({
                "nombre": "Reportes por Correo",
                "descripcion": "Envio de reportes email a proveedores y supervisores.",
                "func_path": "apps.home.tasks.enviar_reportes_email_task",
                "hora_local": datetime.time(23, 30),
                "icono": "fas fa-envelope",
            })
        if getattr(empresa, "envio_cdt_activo", False) and empresa.cdt_codigo_proveedor:
            tareas.append({
                "nombre": "Planos CDT",
                "descripcion": "Generacion y envio de planos CDT por SFTP.",
                "func_path": "apps.home.tasks.cdt_empresa_scheduled",
                "hora_local": datetime.time(23, 0),
                "icono": "fas fa-file-export",
            })
        if getattr(empresa, "envio_tsol_activo", False) and empresa.tsol_codigo:
            tareas.append({
                "nombre": "Planos TSOL",
                "descripcion": "Generacion y envio de planos TSOL por FTP.",
                "func_path": "apps.home.tasks.tsol_empresa_scheduled",
                "hora_local": datetime.time(23, 15),
                "icono": "fas fa-file-code",
            })
        if getattr(empresa, "envio_cosmos_activo", False) and empresa.cosmos_empresa_id:
            tareas.append({
                "nombre": "Planos Cosmos",
                "descripcion": "Generacion y envio de planos Cosmos por FTPS.",
                "func_path": "apps.home.tasks.cosmos_empresa_scheduled",
                "hora_local": datetime.time(23, 45),
                "icono": "fas fa-satellite",
            })

        for t in tareas:
            ProgramacionTarea.objects.get_or_create(
                empresa=empresa, nombre=t["nombre"],
                defaults={**t, "activo": True, "intervalo_segundos": 86400},
            )
