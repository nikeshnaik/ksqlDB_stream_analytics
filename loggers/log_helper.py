import logging
import os
from logging.config import dictConfig

import yaml


log_config = yaml.load(open("./loggers/log_config.yaml", "r"), Loader=yaml.FullLoader)
dictConfig(log_config)

system_logger = logging.getLogger("local")