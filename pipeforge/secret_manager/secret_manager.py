from abc import ABC, abstractmethod


class SecretManager(ABC):
    @abstractmethod
    def get_credentials(self):
        pass
