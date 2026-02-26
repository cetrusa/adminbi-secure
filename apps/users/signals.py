from django.db.models.signals import m2m_changed, post_save, post_delete
from django.dispatch import receiver
from django.core.cache import cache
from .models import User
from apps.permisos.models import ConfEmpresas


# Invalida el caché de empresas del usuario cuando cambian sus empresas asignadas
def invalidate_user_databases_cache(user_id):
    cache_key = f"user_databases_{user_id}"
    cache.delete(cache_key)


@receiver(m2m_changed, sender=User.conf_empresas.through)
def user_empresas_changed(sender, instance, action, **kwargs):
    if action in ["post_add", "post_remove", "post_clear"]:
        invalidate_user_databases_cache(instance.id)


# Si tienes un modelo de permisos adicionales, puedes agregar señales similares para UserPermission
from .models import UserPermission


@receiver(post_save, sender=UserPermission)
def user_permission_saved(sender, instance, **kwargs):
    invalidate_user_databases_cache(instance.user.id)


@receiver(post_delete, sender=UserPermission)
def user_permission_deleted(sender, instance, **kwargs):
    invalidate_user_databases_cache(instance.user.id)
