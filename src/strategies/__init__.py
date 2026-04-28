import importlib
import importlib.util
from pathlib import Path
from .base import Strategy


def load_strategy(name, user_dir="strategies"):
    try:
        module = importlib.import_module(f"src.strategies.examples.{name}")
        return _find_strategy(module)
    except ImportError:
        pass

    filepath = Path(user_dir) / f"{name}.py"
    if filepath.exists():
        spec = importlib.util.spec_from_file_location(name, filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return _find_strategy(module)

    raise ValueError(f"策略 '{name}' 未找到")


def _find_strategy(module):
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, type) and issubclass(attr, Strategy) and attr is not Strategy:
            return attr()
    raise ValueError(f"模块中未找到 Strategy 子类")
