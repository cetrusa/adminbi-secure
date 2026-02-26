from .base import *
import sys


ALLOWED_HOSTS = [
    "interface.amovil.com.co",
    "interfacep.amovil.com.co",
    "127.0.0.1",
    "localhost",
    "localhost:4085",
    "104.21.34.249",
    "181.49.241.226:4084",
    "interface.amovil.co",
    "datazenith.online",
    "www.datazenith.online",
    "datazenith.online",
    "www.datazenith.online",
    "10.0.61.101",
]

RENDER_EXTERNAL_HOSTNAME = get_secret("RENDER_EXTERNAL_HOSTNAME")
if RENDER_EXTERNAL_HOSTNAME:
    ALLOWED_HOSTS.append(RENDER_EXTERNAL_HOSTNAME)
# SECURITY WARNING: don't run with debug turned on in production!

# esto es solo para no usar ssl mientras se soluciona
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context


DEBUG = True
SESSION_COOKIE_SECURE = False

CSRF_COOKIE_SECURE = True

CSRF_TRUSTED_ORIGINS = [
    "https://interface.amovil.com.co",
    "https://interfacep.amovil.com.co",
    "http://127.0.0.1",
    "http://localhost",
    "http://0.0.0.0",
    "http://181.49.241.226",
    "http://181.49.241.226:4084",
    "http://interface.amovil.co:4084",
    "http://interface.amovil.co",
    "https://datazenith.online:4084",
    "http://datazenith.online:4084",
    "https://datazenith.online:9191",
    "http://datazenith.online:9191",
    "http://10.0.61.101:4084",
    "http://10.0.61.101:9191",
]


# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

# Database
# https://docs.djangoproject.com/en/4.1/ref/settings/#databases

DB_ENGINE = get_secret("DB_ENGINE")
DB_USERNAME = get_secret("DB_USERNAME")
DB_PASS = get_secret("DB_PASS")
DB_HOST = get_secret("DB_HOST")
DB_PORT = get_secret("DB_PORT")
DB_NAME = get_secret("DB_NAME")

if DB_ENGINE and DB_NAME and DB_USERNAME:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends." + DB_ENGINE,
            "NAME": DB_NAME,
            "USER": DB_USERNAME,
            "PASSWORD": DB_PASS,
            "HOST": DB_HOST,
            "PORT": DB_PORT,
        },
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": "db.sqlite3",
        }
    }
# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.2/howto/static-files/

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR.child("static")]
STATIC_ROOT = BASE_DIR.child("staticfiles")
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"
# STATICFILES_STORAGE = "django.contrib.staticfiles.storage.ManifestStaticFilesStorage"

MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR.child("media")

# EMAIL SETTINGS
ADMINS = [
    ("Augusto", "cetrusa@hotmail.com"),
]
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
EMAIL_USE_TLS = True
EMAIL_HOST = "smtp.gmail.com"
EMAIL_HOST_USER = get_secret("EMAIL")
EMAIL_HOST_PASSWORD = get_secret("PASS_EMAIL")
EMAIL_PORT = 587
DEFAULT_FROM_EMAIL = EMAIL_HOST_USER

sys.path.append(BASE_DIR.child("scripts"))
sys.path.append(BASE_DIR.child("scripts", "extrae_bi"))


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "mail_admins": {
            "level": "ERROR",
            "class": "django.utils.log.AdminEmailHandler",
            "include_html": True,
        }
    },
    "root": {"handlers": ["mail_admins"], "level": "ERROR"},
}


# CELERY_BROKER_URL = "redis://redis:6379/0"
# CELERY_RESULT_BACKEND = "redis://redis:6379/0"
# CELERY_TASK_SOFT_TIME_LIMIT = 7200
# CELERY_TASK_TIME_LIMIT = 7200
# CELERY_TASK_EAGER_PROPAGATES = True

import os
RQ_DEFAULT_TIMEOUT = int(os.getenv("RQ_DEFAULT_TIMEOUT", 28800))  # 8h por defecto
RQ_RESULT_TTL = int(os.getenv("RQ_RESULT_TTL", 86400))  # 24h
RQ_FAILURE_TTL = int(os.getenv("RQ_FAILURE_TTL", 86400))  # 24h

RQ_QUEUES = {
    "default": {
        "HOST": "redis",
        "PORT": 6379,
        "DB": 0,
        # Nota: Las tareas ya especifican timeout explícito (p.ej. 7200s). Este DEFAULT_TIMEOUT
        # es un límite superior por defecto y debe ser >= al timeout especificado en los @job.
        "DEFAULT_TIMEOUT": RQ_DEFAULT_TIMEOUT,
        "RESULT_TTL": RQ_RESULT_TTL,
        "FAILURE_TTL": RQ_FAILURE_TTL,
        # "JOB_TIMEOUT": RQ_DEFAULT_TIMEOUT,  # Opcional: mantener igual al default
        "CONNECTION_TIMEOUT": 30,
    },
}


DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
FILE_UPLOAD_MAX_MEMORY_SIZE = 200 * 1024 * 1024  # 200 MB
DATA_UPLOAD_MAX_MEMORY_SIZE = 200 * 1024 * 1024  # 200 MB
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# Desactivar X-Accel-Redirect por defecto para no requerir configuración adicional en Nginx
USE_X_ACCEL_REDIRECT = False
