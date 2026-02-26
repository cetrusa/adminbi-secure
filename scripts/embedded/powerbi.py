import msal
import requests
import logging
from django.core.exceptions import ImproperlyConfigured
from scripts.config import ConfigBasic
import json

logger = logging.getLogger(__name__)


# Cargar secretos, por ejemplo TENANT_ID, CLIENT_ID
# Ajusta este método según tu forma de manejar secret.json
def get_secret(secret_name, secrets_file="secret.json"):
    try:
        with open(secrets_file) as f:
            secrets = json.load(f)
        return secrets[secret_name]
    except (KeyError, FileNotFoundError):
        msg = f"La variable {secret_name} no existe o 'secret.json' no se encontró."
        logger.error(msg)
        raise ImproperlyConfigured(msg)


class PbiEmbedServiceUserPwd:
    """Servicio para incrustar un reporte de Power BI usando usuario y contraseña (User Owns Data)."""

    def __init__(self, database_name):
        # 1. Cargamos la configuración específica de la empresa (database_name).
        self.config = ConfigBasic(database_name).config

        # 2. Credenciales de Azure AD (tenant/client) desde secrets
        self.tenant_id = get_secret("TENANT_ID")
        self.client_id = get_secret("CLIENT_ID")

        # 3. Credenciales de Power BI (usuario y contraseña) desde tu config
        # Intenta obtener desde config, sino desde secret.json
        self.username = self.config.get("nmUsrPowerbi")
        if not self.username:
            logger.warning(
                "nmUsrPowerbi no encontrado en config, usando POWER_BI_USER de secret.json"
            )
            self.username = get_secret("POWER_BI_USER")

        self.password = self.config.get("txPassPowerbi")
        if not self.password:
            logger.warning(
                "txPassPowerbi no encontrado en config, usando POWER_BI_PASS de secret.json"
            )
            self.password = get_secret("POWER_BI_PASS")

        # 4. workspaceId (group_id) y reportId
        self.workspace_id = self.config.get("group_id_powerbi")
        if not self.workspace_id:
            logger.warning(
                "group_id_powerbi no encontrado en config, usando GROUP_ID de secret.json"
            )
            self.workspace_id = get_secret("GROUP_ID")

        self.report_id = self.config.get("report_id_powerbi")

        # Asegurarnos de que existan
        if not all(
            [
                self.username,
                self.password,
                self.tenant_id,
                self.client_id,
                self.workspace_id,
                self.report_id,
            ]
        ):
            missing_fields = []
            if not self.username:
                missing_fields.append("username")
            if not self.password:
                missing_fields.append("password")
            if not self.tenant_id:
                missing_fields.append("tenant_id")
            if not self.client_id:
                missing_fields.append("client_id")
            if not self.workspace_id:
                missing_fields.append("workspace_id")
            if not self.report_id:
                missing_fields.append("report_id")

            error_msg = f"Faltan credenciales o IDs requeridos para Power BI: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise ImproperlyConfigured(error_msg)

        # 5. Generamos el token de Azure AD (este es el token 'user_access_token')
        self.access_token = self.acquire_user_token()

        # 6. Cabeceras para llamar a la Power BI API
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

    def acquire_user_token(self):
        """Obtiene un token de acceso de Azure AD usando user/password (User Owns Data)."""
        authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"
        scopes = ["https://analysis.windows.net/powerbi/api/.default"]

        try:
            public_app = msal.PublicClientApplication(
                self.client_id, authority=authority_url
            )
            token_response = public_app.acquire_token_by_username_password(
                username=self.username, password=self.password, scopes=scopes
            )

            if "access_token" not in token_response:
                error_desc = token_response.get(
                    "error_description",
                    "No se pudo obtener el token con user/password.",
                )
                raise Exception(f"Error en acquire_user_token: {error_desc}")

            return token_response["access_token"]
        except Exception as ex:
            logger.exception("Error inesperado al obtener token de usuario.")
            raise Exception(f"Error retrieving user token: {str(ex)}") from ex

    def get_embed_params(self):
        """Obtiene embedUrl y embedToken para el reporte, retornando un diccionario con la info."""
        # 1. Llamar a GET /reports para obtener embedUrl, datasetId
        try:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/reports/{self.report_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            embed_url = data.get("embedUrl")
            dataset_id = data.get("datasetId")

            if not embed_url or not dataset_id:
                raise Exception(
                    "No se pudo obtener 'embedUrl' o 'datasetId' del reporte."
                )

            # 2. Generar el token de incrustación (embed token)
            embed_token = self.generate_embed_token(self.report_id, dataset_id)

            return {
                "report_id": self.report_id,
                "embed_url": embed_url,
                "embed_token": embed_token,
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener info del reporte: {str(e)}")
            raise Exception(f"Error al obtener info del reporte: {str(e)}")

    def generate_embed_token(self, report_id, dataset_id):
        """
        Genera el token de incrustación para un reporte específico
        usando la autenticación de usuario (acces_token ya obtenido).
        """
        endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/reports/{report_id}/GenerateToken"

        # En el payload podemos incluir datasets, reports, etc. de forma más completa.
        payload = {
            "datasets": [{"id": dataset_id}],
            "reports": [{"id": report_id}],
            "accessLevel": "View",
        }

        try:
            response = requests.post(endpoint, headers=self.headers, json=payload)
            response.raise_for_status()

            result = response.json()
            embed_token = result.get("token")
            if not embed_token:
                raise Exception("La respuesta no trajo 'token' para la incrustación.")

            return embed_token

        except requests.exceptions.RequestException as e:
            logger.error(f"Error al generar embed token: {str(e)}")
            raise Exception(f"Error al generar embed token: {str(e)}")
