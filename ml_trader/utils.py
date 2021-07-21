import json
import os
from typing import Dict, Any, List, Union


BASE_DIR = os.path.dirname(os.getcwd())


def load_config() -> Dict[str, Any]:
    try:
        with open(BASE_DIR + '/.config.json', "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def check_create_folder(file_path: str) -> None:
    if '/' in file_path:
        folder_path = '/'.join(file_path.split('/')[:-1])
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)


def save_json(file_path: str, data: Union[Dict[str, Any], List[Any]]) -> None:
    check_create_folder(file_path)
    with open(file_path, "w") as write_file:
        json.dump(data, write_file, ensure_ascii=False)


def chunks(lst: List[Any], n: int) -> List[Any]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
