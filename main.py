from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys


APP_DIR = Path(__file__).resolve().parent / "phonepe-parser"
APP_FILE = APP_DIR / "main.py"

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

spec = spec_from_file_location("phonepe_parser_main", APP_FILE)
if spec is None or spec.loader is None:
    raise ImportError(f"Unable to load application module from {APP_FILE}")

module = module_from_spec(spec)
spec.loader.exec_module(module)

app = module.app
