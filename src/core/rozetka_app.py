import re
import time
import concurrent.futures
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Optional, 
    Dict, 
    Tuple, 
    List, 
    Set, 
    Any
)

from fake_useragent import UserAgent

from src.api.rozetka import RozetkaAPI
from src.utils.logger import Logger
from src.core.settings import load_settings, Settings
from src.utils.data_exporters import (
    CSVWriter, 
    JSONWriter, 
    read_txt
)


class ApplicationRozetka:

    def __init__(
            self, 
            settings: Optional[Settings] = None, 
            logger: Optional[Logger] = None
    ) -> None:
        self.logger = logger or Logger()
        self.settings = settings or load_settings()
        self.data = [] 


    def create_api_workers(
            self, 
            num_workers: int = 10
        ) -> List[RozetkaAPI]:
        """
        Создает несколько воркеров RozetkaAPI с разными настройками
        """
        workers = []
        ua = UserAgent()  # Создаем один экземпляр
        
        for i in range(num_workers):
            # Генерируем уникальный User-Agent для каждого воркера
            user_agent = ua.random
            
            worker = RozetkaAPI(
                proxy=None,  # Можно добавить ротацию прокси
                user_agent=user_agent,  # Передаем уникальный User-Agent
                logger=self.logger
            )
            workers.append(worker)
        
        self.logger.info(f"Создано {len(workers)} воркеров с разными User-Agent")
        return workers
    
    def category_name_and_id(
            self, 
            url: str
        ) -> Tuple[Optional[str], Optional[int]]:
        """
        Извлекает название и ID категории из URL
        
        Returns:
            Tuple[Optional[str], Optional[int]]: (название_категории, id_категории) или (None, None) при ошибке
        """
        try:
            # Регулярка для извлечения названия категории и ID
            pattern = r'/ua/([^/]+)/c(\d+)/'
            match = re.search(pattern, url)
            
            if not match:
                self.logger.error(f"Не удалось извлечь данные из URL: {url}")
                return None, None
                
            category_name = match.group(1)  # monitors
            category_id = int(match.group(2))  # 80089
            
            # Можно преобразовать название в читаемый вид
            category_name = category_name.replace('_', ' ').title()
            
            self.logger.info(f"Извлечено: {category_name} (ID: {category_id})")
            return category_name, category_id
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке URL {url}: {e}")
            return None, None
        
    def collect_product_ids_with_workers(
            self, 
            category_id: int = 80089, 
            max_workers: int = 10,
            filters: Optional[List[str]] = None,
            sort_list: Optional[List[str]] = None,
            default_parse: bool = False
        ) -> Set[str]:
        """
        Собирает ID товаров для категории используя многопоточность.
        """
        # Сначала получаем все бренды для категории
        self.logger.info("Получение списка брендов...")
        api = RozetkaAPI()
        brands_data = api.search_brands_for_category(category_id)
        
        if not brands_data:
            self.logger.error("Не удалось получить бренды для категории")
            return set()
        
        self.logger.info(f"Найдено {len(brands_data)} брендов. Начинаем сбор ID товаров...")
        
        all_product_ids = set()
        
        # Создаем воркеры для обработки брендов
        workers = self.create_api_workers(max_workers)
        
        # Используем ThreadPoolExecutor для параллельной обработки брендов
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Распределяем бренды между воркерами
            future_to_brand = {
                executor.submit(workers[i % len(workers)].get_product_ids_for_brand, 
                            brand_data, category_id, filters, sort_list, default_parse): brand_name 
                for i, (brand_name, brand_data) in enumerate(brands_data.items())
            }
            
            # Обрабатываем завершенные задачи - используем concurrent.futures.as_completed
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_brand):  # Исправлено здесь
                brand_name = future_to_brand[future]
                completed_count += 1
                
                try:
                    brand_product_ids = future.result()
                    all_product_ids.update(brand_product_ids)
                    brand_stats = f'[{completed_count}/{len(brands_data)}]'
                    self.logger.info(f"{brand_stats} Бренд {brand_name}: добавлено {len(brand_product_ids)} ID, всего: {len(all_product_ids)}")
                    
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке бренда {brand_name}: {e}")
        
        self.logger.info(f"Всего собрано уникальных ID товаров: {len(all_product_ids)}")
        return all_product_ids

    def process_batches_with_workers(
            self, 
            product_ids: Set[str], 
            batch_size: int = 60, 
            num_workers: int = 10
        ) -> List[Dict[str, Any]]:
        """
        Обрабатывает батчи продуктов используя несколько воркеров
        
        Args:
            product_ids (Set[str]): Множество ID товаров
            batch_size (int): Размер батча
            num_workers (int): Количество воркеров
            
        Returns:
            List[Dict[str, Any]]: Список распарсенных данных
        """
        # Конвертируем set в list
        product_ids_list = list(product_ids)
        total_products = len(product_ids_list)
        
        self.logger.info(f"Начинаем обработку {total_products} продуктов с {num_workers} воркерами")
        
        # Создаем батчи
        batches = [product_ids_list[i:i + batch_size] for i in range(0, total_products, batch_size)]
        self.logger.info(f"Создано {len(batches)} батчей по {batch_size} продуктов")
        
        # Создаем воркеры
        workers = self.create_api_workers(num_workers)
        
        all_parsed_data = []
        
        # ПАРАЛЛЕЛЬНЫЙ ПАРСИНГ БАТЧЕЙ
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Распределяем батчи между воркерами
            future_to_batch = {
                executor.submit(workers[i % len(workers)].process_single_batch, batch): batch 
                for i, batch in enumerate(batches)
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    # Получаем распарсенные данные для батча
                    parsed_batch = future.result()
                    
                    if parsed_batch:
                        all_parsed_data.extend(parsed_batch)
                        completed += 1
                        self.logger.info(f"✅ Батч: {len(parsed_batch)} продуктов | Всего: {len(all_parsed_data)}/{total_products}")
                    else:
                        self.logger.warning(f"⚠️ Батч не содержит данных для парсинга")
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка обработки батча: {e}")
        
        return all_parsed_data

    def collect_product_ids(
            self, 
            category_id: int
        ) -> Set[str]:
        """
        Собирает ID товаров для парсинга
        """
        self.logger.info("🎯 Сбор ID товаров...")
        
        product_ids = self.collect_product_ids_with_workers(
            category_id=category_id,
            max_workers=self.settings.rozetka.max_workers,
            filters=self.settings.rozetka.filters,
            sort_list=self.settings.rozetka.sort_list,
            default_parse=self.settings.rozetka.default_parse
        )
        
        self.logger.info(f"📦 Собрано {len(product_ids)} уникальных ID товаров")
        return product_ids

    def save_results(
            self, 
            data: List[Dict[str, Any]], 
            name_category: str
        ) -> None:
        """
        Сохраняет результаты в файлы в зависимости от настроек
        """
        self.logger.info("💾 Сохранение результатов...")
        
        save_format = self.settings.rozetka.save_data.lower()
        
        if save_format in ['csv', 'both']:
            csv_writer = CSVWriter(f"{name_category}_data.csv")
            csv_writer.write(data)
            self.logger.info(f"✅ Данные сохранены в {name_category}_data.csv")
        
        if save_format in ['json', 'both']:
            json_writer = JSONWriter(f"{name_category}_data.json")
            json_writer.write(data)
            self.logger.info(f"✅ Данные сохранены в {name_category}_data.json")
        
        if save_format not in ['csv', 'json', 'both']:
            self.logger.warning(f"⚠️ Неизвестный формат сохранения: {save_format}")

    def print_statistics(
            self, 
            start_time: float, 
            stages_timing: Dict[str, float], 
            total_ids: int, 
            parsed_products: int
        ) -> None:
        """
        Выводит статистику выполнения
        """
        total_duration = time.time() - start_time
        
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 ДЕТАЛЬНАЯ СТАТИСТИКА ВРЕМЕНИ ВЫПОЛНЕНИЯ")
        self.logger.info("="*60)
        
        for stage_name, stage_duration in stages_timing.items():
            self.logger.info(f"⏰ {stage_name}: {stage_duration:.3f} сек")
        
        self.logger.info(f"🕐 Общее время выполнения: {total_duration:.3f} сек")
        
        self.logger.info("\n📈 ПРОИЗВОДИТЕЛЬНОСТЬ:")
        self.logger.info(f"   Всего ID: {total_ids}")
        self.logger.info(f"   Собрано продуктов: {parsed_products}")
        
        if total_duration > 0:
            total_speed = parsed_products / total_duration
            self.logger.info(f"   Общая скорость: {total_speed:.3f} продуктов/сек")
        
        self.logger.info(f"\n🚀 Скрипт запущен в: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"✅ Скрипт завершен в: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("="*60)

    def start(self):

        self.logger.info(f"🚀 Скрипт запущен в: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        categories = read_txt('category_to_parse.txt')
        if not categories: 
            self.logger.error(f"❌ Нету категорий...")
            return

        for category_url in categories:
            # Общее время начала
            total_start_time = time.time()
            stages_timing = {}
            name, category_id = self.category_name_and_id(category_url)
            
            if not category_id:
                self.logger.error(f"❌ Пропускаем категорию: {category_url}")
                continue
                
            self.logger.info(f"🎯 Обработка категории: {name} (ID: {category_id})")
            try:
                # 1. Сбор ID товаров
                stage_start = time.time()
                product_ids = self.collect_product_ids(category_id)
                stages_timing['Сбор ID товаров'] = time.time() - stage_start

                if not product_ids:
                    self.logger.error("❌ Не найдено ID для обработки")
                    return

                # 2. Обработка батчей с воркерами
                stage_start = time.time()
                self.logger.info("🚀 Обработка батчей с воркерами...")
                parsed_data = self.process_batches_with_workers(
                    product_ids=product_ids,
                    batch_size=60,
                    num_workers=10
                )
                stages_timing['Обработка батчей'] = time.time() - stage_start

                # 3. Сохранение результатов
                stage_start = time.time()
                self.save_results(parsed_data, name)
                stages_timing['Сохранение результатов'] = time.time() - stage_start

                # 4. Вывод статистики
                self.print_statistics(total_start_time, stages_timing, len(product_ids), len(parsed_data))

            except Exception as e:
                self.logger.error(f"❌ Критическая ошибка в методе start: {e}")