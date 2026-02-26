import random
import string
import re
import logging
import base64
import io
from django.utils.crypto import get_random_string
from django.utils.text import slugify
from django.utils import timezone
from datetime import timedelta

# Setup logger
logger = logging.getLogger(__name__)

# Try to import optional packages
try:
    import pyotp

    PYOTP_AVAILABLE = True
except ImportError:
    logger.warning("pyotp package not available. 2FA features will be disabled.")
    PYOTP_AVAILABLE = False

try:
    import qrcode

    QRCODE_AVAILABLE = True
except ImportError:
    logger.warning("qrcode package not available. QR code generation will be disabled.")
    QRCODE_AVAILABLE = False


def code_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """
    Genera un código aleatorio alfanumérico de la longitud especificada.

    Args:
        size (int): Longitud del código a generar. Por defecto es 6.
        chars (str): Caracteres a utilizar para generar el código.
                     Por defecto usa letras mayúsculas y dígitos.

    Returns:
        str: Código aleatorio generado.
    """
    return "".join(random.choice(chars) for _ in range(size))


def generate_secure_token(length=32):
    """
    Genera un token seguro para uso en operaciones sensibles como
    restablecimiento de contraseñas o validación en dos pasos.

    Args:
        length (int): Longitud del token. Por defecto es 32.

    Returns:
        str: Token seguro generado.
    """
    return get_random_string(length=length)


def generate_username(nombre, apellido):
    """
    Genera un nombre de usuario único basado en el nombre y apellido.

    Args:
        nombre (str): Nombre del usuario.
        apellido (str): Apellido del usuario.

    Returns:
        str: Nombre de usuario generado.
    """
    # Convertir a minúsculas y eliminar acentos
    nombre = slugify(nombre).replace("-", "")
    apellido = slugify(apellido).replace("-", "")

    # Crear nombre de usuario base
    if nombre and apellido:
        username = f"{nombre[0]}{apellido}"
    elif nombre:
        username = nombre
    elif apellido:
        username = apellido
    else:
        # Generar uno aleatorio si no hay nombre/apellido
        return code_generator(8, string.ascii_lowercase + string.digits)

    # Añadir caracteres aleatorios si es necesario para hacerlo único
    if len(username) < 4:
        username += code_generator(4 - len(username), string.ascii_lowercase)

    return username[:15]  # Limitar a 15 caracteres


def validate_password_strength(password):
    """
    Valida que una contraseña cumpla con requisitos mínimos de seguridad.

    Args:
        password (str): Contraseña a validar.

    Returns:
        tuple: (bool, str) Indicador de validez y mensaje descriptivo.
    """
    # Comprobar longitud mínima
    if len(password) < 8:
        return False, "La contraseña debe tener al menos 8 caracteres."

    # Comprobar presencia de al menos un dígito
    if not re.search(r"\d", password):
        return False, "La contraseña debe incluir al menos un número."

    # Comprobar presencia de al menos una letra mayúscula
    if not re.search(r"[A-Z]", password):
        return False, "La contraseña debe incluir al menos una letra mayúscula."

    # Comprobar presencia de al menos una letra minúscula
    if not re.search(r"[a-z]", password):
        return False, "La contraseña debe incluir al menos una letra minúscula."

    # Comprobar presencia de al menos un carácter especial
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
        return False, "La contraseña debe incluir al menos un carácter especial."

    return True, "Contraseña válida."


def is_token_valid(timestamp, expiry_hours=24):
    """
    Verifica si un timestamp está dentro del período de validez.

    Args:
        timestamp (datetime): Fecha y hora a verificar.
        expiry_hours (int): Horas de validez desde el timestamp.
                         Por defecto es 24 horas.

    Returns:
        bool: True si el token aún es válido, False en caso contrario.
    """
    if not timestamp:
        return False

    # Calcular tiempo de expiración
    expiry_time = timestamp + timedelta(hours=expiry_hours)

    # Verificar si ha expirado
    return timezone.now() <= expiry_time


def anonymize_email(email):
    """
    Anonimiza una dirección de correo electrónico para mostrarla parcialmente.
    Ejemplo: john.doe@example.com -> j******e@e****e.com

    Args:
        email (str): Email a anonimizar.

    Returns:
        str: Email anonimizado.
    """
    if not email or "@" not in email:
        return email

    local_part, domain = email.split("@")
    domain_parts = domain.split(".")

    # Anonimizar parte local
    if len(local_part) <= 2:
        anonymized_local = local_part
    else:
        anonymized_local = local_part[0] + "*" * (len(local_part) - 2) + local_part[-1]

    # Anonimizar dominio (excepto TLD)
    anonymized_domain = ""
    for i, part in enumerate(domain_parts):
        if i == len(domain_parts) - 1 or len(part) <= 2:  # TLD o parte corta
            anonymized_domain += part + ("." if i < len(domain_parts) - 1 else "")
        else:
            anonymized_domain += part[0] + "*" * (len(part) - 2) + part[-1] + "."

    return f"{anonymized_local}@{anonymized_domain}"


def generate_totp_secret():
    """
    Genera una clave secreta para TOTP (Time-based One-Time Password).
    """
    if not PYOTP_AVAILABLE:
        logger.warning("TOTP secret generation failed: pyotp module not available")
        return code_generator(16)  # Fallback to simple code generator

    return pyotp.random_base32()


def generate_totp_uri(secret, username, issuer="DataZenith BI"):
    """
    Genera un URI para configurar TOTP en aplicaciones de autenticación.

    Args:
        secret (str): La clave secreta TOTP
        username (str): Nombre de usuario
        issuer (str): Nombre del emisor (servicio)

    Returns:
        str: URI para configurar TOTP
    """
    if not PYOTP_AVAILABLE:
        logger.warning("TOTP URI generation failed: pyotp module not available")
        return f"otpauth://totp/{issuer}:{username}?secret={secret}&issuer={issuer}"

    totp = pyotp.TOTP(secret)
    return totp.provisioning_uri(username, issuer_name=issuer)


def generate_qr_code(data):
    """
    Genera un código QR a partir de los datos proporcionados.

    Args:
        data (str): Los datos a codificar en el QR

    Returns:
        str: Imagen del código QR en formato base64 o None si no está disponible
    """
    if not QRCODE_AVAILABLE:
        logger.warning("QR code generation failed: qrcode module not available")
        return None

    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")

    # Convertir a base64
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode("utf-8")


def verify_totp(secret, code):
    """
    Verifica un código TOTP.

    Args:
        secret (str): La clave secreta TOTP
        code (str): El código proporcionado por el usuario

    Returns:
        bool: True si el código es válido, False en caso contrario
    """
    if not PYOTP_AVAILABLE:
        logger.warning("TOTP verification failed: pyotp module not available")
        return code == code_generator(
            6
        )  # Very basic fallback, not secure for production

    totp = pyotp.TOTP(secret)
    return totp.verify(code)
