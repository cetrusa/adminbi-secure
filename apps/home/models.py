from django.db import models
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
from django.core.exceptions import ValidationError
import re


class Reporte(models.Model):
    """
    Modelo para definir reportes personalizados con consultas SQL.
    Permite almacenar y gestionar reportes utilizados en el sistema.
    """

    nombre = models.CharField(
        max_length=100,
        verbose_name=_("Nombre del Reporte"),
        db_index=True,  # Índice para mejorar búsquedas por nombre
        help_text=_("Nombre descriptivo para identificar el reporte"),
    )
    descripcion = models.TextField(
        blank=True,
        verbose_name=_("Descripción"),
        help_text=_("Descripción detallada del propósito y contenido del reporte"),
    )
    sql_text = models.TextField(
        blank=True,
        verbose_name=_("Texto de SQLAlchemy"),
        help_text=_("Consulta SQL utilizando la sintaxis de SQLAlchemy"),
    )
    activo = models.BooleanField(
        default=True,
        verbose_name=_("Activo"),
        help_text=_("Indica si el reporte está disponible para su uso"),
    )
    fecha_creacion = models.DateTimeField(
        default=timezone.now,  # Use default instead of auto_now_add
        verbose_name=_("Fecha de creación"),
        help_text=_("Fecha y hora en que se creó el reporte"),
    )
    fecha_actualizacion = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Fecha de actualización"),
        help_text=_("Fecha y hora de la última actualización del reporte"),
    )

    def __str__(self):
        return self.nombre

    def clean(self):
        """Validación personalizada para el modelo."""
        # Validar que el nombre no contenga caracteres especiales
        if self.nombre and not re.match(r"^[a-zA-Z0-9_\s]+$", self.nombre):
            raise ValidationError(
                {
                    "nombre": _(
                        "El nombre del reporte solo puede contener letras, números, espacios y guiones bajos."
                    )
                }
            )

        # Verificar que el SQL no contiene sentencias peligrosas (básico)
        dangerous_keywords = ["DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT"]
        if self.sql_text:
            for keyword in dangerous_keywords:
                if re.search(r"\b" + keyword + r"\b", self.sql_text.upper()):
                    raise ValidationError(
                        {
                            "sql_text": _(
                                "La consulta SQL contiene operaciones potencialmente peligrosas."
                            )
                        }
                    )

    def get_elapsed_time(self):
        """Retorna el tiempo transcurrido desde la última actualización."""
        if self.fecha_actualizacion:
            return timezone.now() - self.fecha_actualizacion
        return None

    class Meta:
        verbose_name = _("Reporte")
        verbose_name_plural = _("Reportes")
        ordering = ["-fecha_actualizacion", "nombre"]  # Ordenamiento por defecto
        indexes = [
            models.Index(fields=["nombre"]),
            models.Index(fields=["fecha_creacion"]),
            models.Index(fields=["activo"]),
        ]
