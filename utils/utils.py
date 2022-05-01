from typing import Any

import yaml


def load_config_yaml() -> Any:
    with open("../conf/config.yaml", "r") as stream:
        try:
            config = (yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)
    return config
