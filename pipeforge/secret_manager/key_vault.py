import hvac
import requests
from hvac import exceptions
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import MaxRetryError
from requests.exceptions import ConnectionError

from pipeforge.secret_manager.secret_manager import SecretManager


class KeyVault(SecretManager):
    """
    A class that extends SecretManager.
    It interacts with a Key-Value (KV) secrets engine, possibly in HashiCorp’s Vault.
    """

    def __init__(self, vault_server: str, vault_token: str, username: str, password: str):
        # Create a requests.Session with Basic Authentication.
        # This will add an Authorization header to all requests made using this session,
        # with your username and password encoded in base64.
        self.mount_points = None
        self.error = None

        try:
            session = requests.Session()
            session.auth = HTTPBasicAuth(username, password)
            self.client = hvac.Client(url=vault_server, token=vault_token, session=session)
        except (exceptions.VaultError, ConnectionError, MaxRetryError) as e:
            self.error = e
            return

        try:
            self.mount_points, self.error = self.get_secrets_engine_list()
        except Exception as e:
            self.error = e

    @property
    def get_connection(self):
        if self.client.is_authenticated():
            return self.client
        else:
            raise Exception("Error authenticating against Vault")

    def get_kv_secret(self, path: str, mount_point='kv') -> (dict, Exception):
        if f'{mount_point}/' in self.mount_points:
            try:
                connection = self.get_connection
                secret = connection.secrets.kv.read_secret_version(
                    mount_point=mount_point, path=path,
                    raise_on_deleted_version=True
                )
                return secret["data"]['data'], None
            except exceptions.InvalidPath as err:
                return None, err
            except Exception as err:
                return None, err
        else:
            return None, ValueError("Invalid mount point")

    def get_kv_secrets_list(self, path, mount_point='kv'):
        mount_point = mount_point if mount_point else None
        secret = self.client.secrets.kv.list_secrets(
            path=path, mount_point=mount_point
        )

        return secret["data"]["keys"]

    def get_secrets_engine_list(self) -> (list, Exception):
        try:
            secrets_engines = self.client.sys.list_mounted_secrets_engines()
            mount_points = secrets_engines.keys()
            return mount_points, None
        except (exceptions.VaultError, ConnectionError, MaxRetryError) as e:
            return None, e

    def get_auth_methods(self):
        auth_methods = self.client.sys.list_auth_methods()['data']

        return auth_methods

    def get_credentials(self):
        pass  # Implement this method if needed.
