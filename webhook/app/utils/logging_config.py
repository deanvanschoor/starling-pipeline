import logging
from app.constants import APP_DIR

def setup_logging(log_to_file: bool = True, log_level: int = logging.INFO) -> None:
    handlers = [logging.StreamHandler()]

    if log_to_file:
        app_dir = APP_DIR
        log_dir = app_dir / "utils"
        log_dir.mkdir(exist_ok=True,parents=True)
        handlers.append(logging.FileHandler(log_dir / "app.log", encoding="utf-8"))

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )
