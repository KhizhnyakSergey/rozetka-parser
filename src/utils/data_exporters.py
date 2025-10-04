import json
import csv
from pathlib import Path
from typing import List, Dict, Any


class JSONWriter:
    """
    Класс для записи данных в JSON-файл в папку data.
    """
    def __init__(self, filename: str):
        # Создаем путь к папке data
        data_dir = Path("src/data")
        data_dir.mkdir(parents=True, exist_ok=True)  # Создаем папку если не существует
        self.filename = data_dir / filename

    def write(self, data: List[Dict[str, Any]], ensure_ascii: bool = False, indent: int = 4) -> None:
        """
        Записывает список словарей в JSON-файл.

        :param data: Данные в формате списка словарей
        :param ensure_ascii: Сохранять ли ASCII (по умолчанию False, чтобы сохранять кириллицу)
        :param indent: Отступы для форматирования JSON
        """
        with self.filename.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent)


class CSVWriter:
    """
    Класс для записи данных в CSV-файл в папку data.
    """
    def __init__(self, filename: str):
        # Создаем путь к папке data
        data_dir = Path("src/data")
        data_dir.mkdir(parents=True, exist_ok=True)  # Создаем папку если не существует
        self.filename = data_dir / filename

    def write(self, data: List[Dict[str, Any]]) -> None:
        """
        Записывает список словарей в CSV-файл.

        :param data: Данные в формате списка словарей
        """
        if not data:
            print("Нет данных для записи.")
            return

        fieldnames = list(data[0].keys())

        with self.filename.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        


def read_txt(file_path: str) -> List[str]:
    with open(file_path, 'r', encoding='utf-8') as file:
        return [line for raw in file.readlines() if (line := raw.strip())]