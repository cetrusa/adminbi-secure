from django.core.cache import cache
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

from apps.bimbo.models import AgenciaBimbo


@receiver(post_save, sender=AgenciaBimbo)
@receiver(post_delete, sender=AgenciaBimbo)
def invalidate_agencias_cache(sender, **kwargs):
    """Invalida el cache de agencias cuando cambia AgenciaBimbo."""
    cache.delete("admin_bimbo_agencias_nombre_map")
