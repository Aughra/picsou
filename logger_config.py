import logging
import sys

# Config du logger unique pour le dépôt
logger = logging.getLogger("app_logger")
logger.setLevel(logging.INFO)

# Handler console (stdout)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Format : timestamp, niveau, fichier, fonction, message
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] %(message)s"
)
console_handler.setFormatter(formatter)

# Ajouter handler si pas déjà présent
if not logger.hasHandlers():
    logger.addHandler(console_handler)
