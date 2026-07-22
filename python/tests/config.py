from tests.base_config import LocalTestConfig as BaseLocalTestConfig

CONFIG: type[BaseLocalTestConfig]
try:
    from tests.local_config import LocalTestConfig as CustomLocalTestConfig
except ImportError:
    CONFIG = BaseLocalTestConfig
else:
    CONFIG = CustomLocalTestConfig
