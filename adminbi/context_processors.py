from django.conf import settings


def session_settings(request):
    """Expone SESSION_EXPIRE_SECONDS al contexto de templates."""
    return {
        "SESSION_EXPIRE_SECONDS": getattr(settings, "SESSION_EXPIRE_SECONDS", 7200),
    }
