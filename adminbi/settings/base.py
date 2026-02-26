from django.core.exceptions import ImproperlyConfigured
import json
import pymysql
import os, random, string
from unipath import Path
import datetime

import pytz

BASE_DIR = Path(__file__).ancestor(3)

pymysql.install_as_MySQLdb()

# SECURITY WARNING: keep the secret key used in production secret!
with open("secret.json") as f:
    secret = json.loads(f.read())


def get_secret(secret_name, secrets=secret):
    try:
        return secrets[secret_name]
    except:
        msg = "la variable %s no existe" % secret_name
        raise ImproperlyConfigured(msg)


SECRET_KEY = get_secret("SECRET_KEY")
if not SECRET_KEY:
    SECRET_KEY = "".join(random.choice(string.ascii_lowercase) for i in range(32))


# Application definition

DJANGO_APPS = (
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
)

LOCAL_APPS = (
    "apps.users",
    "apps.home",
    "scripts",
    "scripts.extrae_bi",
    "apps.bi",
    "apps.permisos",
    "apps.cargues",
    "apps.monitor",
)

THIRD_PARTY_APPS = (
    "captcha",
    "django_rq",
    "import_export",  # Para exportar desde el admin
)

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",  # Primero SessionMiddleware
    "django_session_timeout.middleware.SessionTimeoutMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

INSTALLED_APPS = DJANGO_APPS + LOCAL_APPS + THIRD_PARTY_APPS
ROOT_URLCONF = "adminbi.urls"

# Tiempo de inactividad antes de cerrar la sesión automáticamente (en segundos)
SESSION_ENGINE = "django.contrib.sessions.backends.cached_db"
SESSION_COOKIE_AGE = 1209600  # 2 semanas
SESSION_EXPIRE_SECONDS = 7200  # Expire despues de 2 horas
SESSION_EXPIRE_AFTER_LAST_ACTIVITY = (
    True  # Invalide la sesión después de la actividad más reciente/última
)
SESSION_TIMEOUT_REDIRECT = "users_app:user-login"  # Redirect to the named URL
SESSION_EXPIRE_AT_BROWSER_CLOSE = (
    False  # Mantener sesión activa incluso al cerrar el navegador
)
SESSION_SAVE_EVERY_REQUEST = (
    True  # Guardar la sesión en cada solicitud para mantener su frescura
)

# Configuración de caché recomendada para producción (multiusuario seguro)
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://redis:6379/1",  # Asegúrate que el host y puerto coincidan con tu docker-compose
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
        },
    }
}

RECAPTCHA_PUBLIC_KEY = "6LeffTwlAAAAAKYsF2RHBuWmMxSMYLo7DvWb_szY"
RECAPTCHA_PRIVATE_KEY = "6LeffTwlAAAAAMwYZgijw9H4HQLMssatf7xayp8k"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR.child("templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "adminbi.wsgi.application"

# Password validation
# https://docs.djangoproject.com/en/4.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

AUTH_USER_MODEL = "users.User"

# Internationalization
# https://docs.djangoproject.com/en/4.1/topics/i18n/

LANGUAGE_CODE = "es-co"

USE_TZ = True

TIME_ZONE = "America/Bogota"

USE_I18N = True
