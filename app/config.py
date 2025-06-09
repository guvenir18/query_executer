import glob
import os

from omegaconf import OmegaConf


def get_env_yaml_paths():
    """
    Get paths of yaml files that will be used from /config
    :return:
    """
    directory = "/config"
    if not os.path.exists(directory):
        return []
    return glob.glob(os.path.join(directory, "*.yaml"))


def load_config():
    """
    Load configs and merge into single config. The latest one has the highest priority
    :return:
    """
    base_config = OmegaConf.create()

    if os.path.exists("config/main/settings.yaml"):
        base_config = OmegaConf.merge(base_config, OmegaConf.load("config/main/settings.yaml"))
    if os.path.exists("config/local/settings.yaml"):
        base_config = OmegaConf.merge(base_config, OmegaConf.load("config/local/settings.yaml"))

    config_paths = get_env_yaml_paths()
    for path in config_paths:
        base_config = OmegaConf.merge(base_config, OmegaConf.load(path))

    return base_config
