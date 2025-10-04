
import cloudscraper
from typing import Union, Dict, Any, Optional, List, Set
import time
import random

from requests.exceptions import Timeout, ConnectionError, RequestException

from src.core.settings import load_settings
from src.utils.logger import Logger


class RozetkaAPI:

    API: str = 'https://rozetka.com.ua/'

    def __init__(
            self, 
            proxy: Optional[str] = None,
            user_agent: Optional[str] = None,
            logger: Optional[Logger] = None
    ) -> None:
        self._session = cloudscraper.create_scraper()
        self.settings = load_settings()
        self.logger = logger or Logger()
        self._headers = {
            # 'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self._cookies = {}
        self._proxy = {'http': proxy, 'https': proxy}

    def __aenter__(self) -> "RozetkaAPI":
        return self
    
    def __aexit__(self, *args) -> None:
        self._session.close()

    def _make_request(
            self, 
            url: str, 
            page: int = None
        ) -> Optional[Union[str, Dict[str, Any]]]:
        """
        Универсальный метод для выполнения HTTP запросов с повторными попытками и задержками
        """
        full_url = f"{url}?page={page}/" if page else url
        max_retries = self.settings.rozetka.max_retries
        min_delay = self.settings.rozetka.min_delay
        max_delay = self.settings.rozetka.max_delay
        
        for attempt in range(max_retries):
            try:
                # Случайная задержка между запросами
                delay = random.uniform(min_delay, max_delay)
                time.sleep(delay)

                # self.logger.debug(f"Запрос {attempt + 1}/{max_retries} к: {full_url}")
                
                response = self._session.get(
                    url=full_url,
                    headers=self._session.headers.update(self._headers),
                    proxies=self._proxy,
                    timeout=self.settings.rozetka.request_timeout
                )

                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    
                    if 'application/json' in content_type:
                        try:
                            return response.json()
                        except ValueError as e:
                            self.logger.error(f"Ошибка парсинга JSON: {e}")
                            if attempt == max_retries - 1:
                                return None
                            continue
                    elif 'text/html' in content_type:
                        return response.text
                    else:
                        return response.text
                
                # Обработка HTTP ошибок
                elif response.status_code == 403:
                    self.logger.warning(f"403 Forbidden (попытка {attempt + 1})")
                    if attempt == max_retries - 1:
                        self.logger.error("Превышено количество попыток для 403 ошибки")
                        return None
                        
                elif response.status_code == 429:
                    self.logger.warning(f"429 Too Many Requests (попытка {attempt + 1})")
                    time.sleep(5 * (attempt + 1))
                    if attempt == max_retries - 1:
                        self.logger.error("Превышено количество попыток для 429 ошибки")
                        return None
                        
                elif response.status_code == 404:
                    self.logger.warning(f"404 Not Found: {full_url}")
                    return None
                    
                elif 500 <= response.status_code < 600:
                    self.logger.warning(
                        f"Серверная ошибка {response.status_code} (попытка {attempt + 1})"
                    )
                    if attempt == max_retries - 1:
                        self.logger.error(
                            f"Сервер постоянно возвращает ошибку {response.status_code}"
                        )
                        return None
                else:
                    self.logger.warning(
                        f"HTTP Error {response.status_code} для {full_url} (попытка {attempt + 1})"
                    )
                    if attempt == max_retries - 1:
                        return None
            
            except Timeout:
                self.logger.warning(f"Таймаут запроса (попытка {attempt + 1})")
                if attempt == max_retries - 1:
                    self.logger.error("Превышено количество попыток из-за таймаутов")
                    return None
                    
            except ConnectionError:
                self.logger.warning(f"Ошибка соединения (попытка {attempt + 1})")
                if attempt == max_retries - 1:
                    self.logger.error("Не удалось установить соединение после всех попыток")
                    return None
                    
            except RequestException as e:
                self.logger.warning(f"Ошибка запроса: {e} (попытка {attempt + 1})")
                if attempt == max_retries - 1:
                    self.logger.error(f"Критическая ошибка запроса: {e}")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Неожиданная ошибка: {e} (попытка {attempt + 1})")
                if attempt == max_retries - 1:
                    return None
            
            # Экспоненциальная задержка перед следующей попыткой
            if attempt < max_retries - 1:
                backoff_delay = (2 ** attempt) + random.uniform(0, 1)
                self.logger.debug(f"Задержка {backoff_delay:.2f} сек перед следующей попыткой")
                time.sleep(backoff_delay)
        
        self.logger.error(f"Все {max_retries} попыток завершились ошибкой для {full_url}")
        return None

    def parse_batch_data(
            self, 
            batch_data: Optional[List[Dict[str, Any]]]
        ) -> List[Dict[str, Any]]:
        """
        Парсит данные нескольких продуктов из батча
        Возвращает список словарей
        """
        parsed_results = []
        
        if not batch_data or not isinstance(batch_data, list):
            self.logger.warning("Нет данных для парсинга или неверный формат")
            return parsed_results
        
        for product_info in batch_data:
            if not isinstance(product_info, dict):
                continue
                
            result = {
                'title': product_info.get('title', ''),
                'price': product_info.get('price', ''),
                'url': product_info.get('href', ''),
                'sku': product_info.get('id', ''),
                'availability': product_info.get('sell_status', ''),
            }
            parsed_results.append(result)
        
        return parsed_results

    def search_brands_for_category(
            self, 
            category_id: int = 80089
        ) -> Dict[str, Dict[str, Any]]:
        """
        Получает все бренды для указанной категории, отправляя запросы для всех букв алфавита.
        """
        base_url = "https://catalog-api.rozetka.com.ua/v0.1/api/category/search-brands"
        
        english_alphabet = [chr(i) for i in range(65, 91)]  # A-Z
        russian_alphabet = [chr(i) for i in range(1040, 1072)]  # А-Я
        all_letters = english_alphabet + russian_alphabet
        
        brands_data = {}
        
        for letter in all_letters:
            response = self._make_request(
                f'{base_url}?country=UA&lang=ua&id={category_id}&idType=catalog&query={letter}'
            )
            
            if response and 'data' in response and 'options' in response['data']:
                for option in response['data']['options']:
                    brand_name = option.get('option_value_name')
                    if brand_name and brand_name not in brands_data:
                        brands_data[brand_name] = {
                            'option_value_name': brand_name,
                            'option_value_title': option.get('option_value_title', ''),
                            'products_quantity': option.get('products_quantity', 0),
                            'option_value_id': option.get('option_value_id'),
                            'is_chosen': option.get('is_chosen', False),
                            'order': option.get('order', 0),
                            'is_value_show': option.get('is_value_show', False),
                            'option_value_image': option.get('option_value_image')
                        }
        
        return brands_data

    def get_product_ids_for_brand(
            self, 
            brand_data: Dict[str, Any], 
            category_id: int = 80089, 
            filters: Optional[List[str]] = None,
            sort_list: Optional[List[str]] = None,
            default_parse: bool = False
        ) -> Set[int]:
        """
        Получает все ID товаров для конкретного бренда с учетом пагинации и фильтрации.
        """
        brand_name = brand_data['option_value_name']
        products_quantity = brand_data['products_quantity']
        
        self.logger.info(f"Обработка бренда {brand_name} (товаров: {products_quantity})")
        
        all_product_ids = set()
        found_any_filter = False
        
        if filters:
            for filter_name in filters:
                filter_ids = self._process_with_filters_and_sort(brand_name, category_id, products_quantity, filter_name, sort_list)
                
                if filter_ids:
                    found_any_filter = True
                    all_product_ids.update(filter_ids)
                    self.logger.info(f"Бренд {brand_name} с фильтром '{filter_name}': собрано {len(filter_ids)} ID")
        
        if default_parse:
            self.logger.info(f"Бренд {brand_name}: выполняется дополнительный парсинг без фильтров (default_parse=True)")
            default_ids = self._process_default_parsing(brand_name, category_id, products_quantity, sort_list)
            all_product_ids.update(default_ids)
            self.logger.info(f"Бренд {brand_name} дополнительный парсинг: собрано {len(default_ids)} ID")
        
        elif filters and not found_any_filter and not default_parse:
            self.logger.info(f"Бренд {brand_name}: ни один фильтр не найден, выполняется fallback парсинг")
            fallback_ids = self._process_default_parsing(brand_name, category_id, products_quantity, sort_list)
            all_product_ids.update(fallback_ids)
            self.logger.info(f"Бренд {brand_name} fallback парсинг: собрано {len(fallback_ids)} ID")
        
        elif not filters and not sort_list and not default_parse:
            all_product_ids = self._process_simple_pagination(brand_name, category_id, products_quantity)
        
        self.logger.info(f"Бренд {brand_name}: итого собрано {len(all_product_ids)} ID товаров")
        return all_product_ids

    def _process_with_sort(
            self, 
            brand_name: str, 
            category_id: int, 
            products_quantity: int, 
            sort_value: str
        ) -> Set[int]:
        """Обрабатывает бренды с использованием сортировки."""
        all_product_ids = set()
        
        first_page_url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                        f"country=UA&lang=ua&filters=page:1;producer:{brand_name};sort:{sort_value}&id={category_id}")
        
        first_page_response = self._make_request(first_page_url)
        
        if not first_page_response or 'data' not in first_page_response or 'goods' not in first_page_response['data']:
            self.logger.warning(
                f"Не удалось получить данные для бренда {brand_name} с сортировкой {sort_value}"
            )
            return all_product_ids
        
        goods_data = first_page_response['data']['goods']
        
        if 'ids' in goods_data:
            all_product_ids.update(goods_data['ids'])
        
        total_pages = goods_data.get('total_pages', 1)
        
        self.logger.info(f"  {brand_name}| Сортировка {sort_value}: всего страниц {total_pages}")
        
        for page in range(2, total_pages + 1):
            url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                f"country=UA&lang=ua&filters=page:{page};producer:{brand_name};sort:{sort_value}&id={category_id}")
            
            response = self._make_request(url)
            
            if response and 'data' in response and 'goods' in response['data']:
                goods_data = response['data']['goods']
                if 'ids' in goods_data:
                    all_product_ids.update(goods_data['ids'])
            
            time.sleep(0.1)
        
        return all_product_ids

    def _process_simple_pagination(
            self, 
            brand_name: str, 
            category_id: int, 
            products_quantity: int
        ) -> Set[int]:
        """Обрабатывает бренды с небольшим количеством товаров через простую пагинацию."""
        all_product_ids = set()
        
        first_page_url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                        f"country=UA&lang=ua&filters=page:1;producer:{brand_name}&id={category_id}")
        
        first_page_response = self._make_request(first_page_url)
        
        if not first_page_response or 'data' not in first_page_response or 'goods' not in first_page_response['data']:
            self.logger.warning(f"Не удалось получить данные для бренда {brand_name}")
            return all_product_ids
        
        goods_data = first_page_response['data']['goods']
        
        if 'ids' in goods_data:
            all_product_ids.update(goods_data['ids'])
        
        total_pages = goods_data.get('total_pages', 1)
        
        self.logger.info(f"  Всего страниц для бренда {brand_name}: {total_pages}")
        
        for page in range(2, total_pages + 1):
            url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                f"country=UA&lang=ua&filters=page:{page};producer:{brand_name}&id={category_id}")
            
            response = self._make_request(url)
            
            if response and 'data' in response and 'goods' in response['data']:
                goods_data = response['data']['goods']
                if 'ids' in goods_data:
                    all_product_ids.update(goods_data['ids'])
            
            time.sleep(0.1)
        
        return all_product_ids

    def _get_filter_id_by_name(
            self, 
            brand_name: str, 
            category_id: int, 
            filter_name: str
        ) -> Optional[str]:
        """Находит ID фильтра по названию."""
        url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
            f"country=UA&lang=ua&filters=page:1;producer:{brand_name}&id={category_id}")
        
        response = self._make_request(url)
        
        if response and 'data' in response and 'filters' in response['data']:
            filters = response['data']['filters']
            if 'options' in filters:
                for filter_id, filter_data in filters['options'].items():
                    if filter_data.get('option_title') == filter_name:
                        return filter_id
        
        return None

    def _get_filter_values(
            self, 
            brand_name: str, 
            category_id: int, 
            filter_id: str
        ) -> List[Dict[str, Any]]:
        """Получает доступные значения для фильтра."""
        url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
            f"country=UA&lang=ua&filters=page:1;producer:{brand_name}&id={category_id}")
        
        response = self._make_request(url)
        
        if response and 'data' in response and 'filters' in response['data']:
            filters = response['data']['filters']
            if 'options' in filters and filter_id in filters['options']:
                filter_data = filters['options'][filter_id]
                return filter_data.get('option_values', [])
        
        return []

    def _process_with_filters_and_sort(
            self, 
            brand_name: str, 
            category_id: int, 
            products_quantity: int, 
            filter_name: str, 
            sort_list: Optional[List[str]] = None
        ) -> Set[int]:
        """Обрабатывает бренды с фильтрами и сортировками."""
        all_product_ids = set()
        
        filter_id = self._get_filter_id_by_name(brand_name, category_id, filter_name)
        
        if not filter_id:
            self.logger.info(f"Фильтр '{filter_name}' не найден для бренда {brand_name}, пропускаем")
            return all_product_ids
        
        filter_values = self._get_filter_values(brand_name, category_id, filter_id)
        
        if not filter_values:
            self.logger.info(f"Нет фильтра '{filter_name}' для бренда {brand_name}, пропускаем")
            return all_product_ids
        
        self.logger.info(f"  {brand_name}| Найден фильтр '{filter_name}' с {len(filter_values)} значениями")
        
        if sort_list:
            for filter_value in filter_values:
                value_name = filter_value['option_value_name']
                value_quantity = filter_value['products_quantity']
                value_title = filter_value['option_value_title']
                
                for sort_value in sort_list:
                    self.logger.info(f"  {brand_name}| Обработка: {filter_name}={value_title} + sort:{sort_value} (товаров: {value_quantity})")
                    
                    ids_for_combo = self._process_filter_sort_combo(brand_name, category_id, filter_id, value_name, sort_value)
                    all_product_ids.update(ids_for_combo)
                    
                    self.logger.info(f"  {brand_name}| Комбинация {filter_name}={value_title} + sort:{sort_value}: собрано {len(ids_for_combo)} ID")
        else:
            for filter_value in filter_values:
                value_name = filter_value['option_value_name']
                value_quantity = filter_value['products_quantity']
                value_title = filter_value['option_value_title']
                
                self.logger.info(f"  {brand_name}| Обработка фильтра {value_name} ({value_title}) (товаров: {value_quantity})")
                
                ids_for_filter = self._process_single_filter(brand_name, category_id, filter_id, value_name)
                all_product_ids.update(ids_for_filter)
                
                self.logger.info(f"  {brand_name}| Фильтр {value_name} ({value_title}): собрано {len(ids_for_filter)} ID")
        
        return all_product_ids

    def _process_filter_sort_combo(
            self, 
            brand_name: str, 
            category_id: int, 
            filter_id: str, 
            filter_value: str, 
            sort_value: str
        ) -> Set[int]:
        """Обрабатывает комбинацию фильтра и сортировки."""
        all_product_ids = set()
        
        first_page_url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                        f"country=UA&lang=ua&filters=page:1;producer:{brand_name};{filter_id}:{filter_value};sort:{sort_value}&id={category_id}")
        
        first_page_response = self._make_request(first_page_url)
        
        if not first_page_response or 'data' not in first_page_response or 'goods' not in first_page_response['data']:
            self.logger.warning(f"    Не удалось получить данные для комбинации {filter_id}:{filter_value} + sort:{sort_value}")
            return all_product_ids
        
        goods_data = first_page_response['data']['goods']
        
        if 'ids' in goods_data:
            all_product_ids.update(goods_data['ids'])
        
        total_pages = goods_data.get('total_pages', 1)
        
        for page in range(2, total_pages + 1):
            url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                f"country=UA&lang=ua&filters=page:{page};producer:{brand_name};{filter_id}:{filter_value};sort:{sort_value}&id={category_id}")
            
            response = self._make_request(url)
            
            if response and 'data' in response and 'goods' in response['data']:
                goods_data = response['data']['goods']
                if 'ids' in goods_data:
                    all_product_ids.update(goods_data['ids'])
            
            time.sleep(0.1)
        
        return all_product_ids

    def _process_single_filter(
            self, 
            brand_name: str, 
            category_id: int, 
            filter_id: str, 
            filter_value: str
        ) -> Set[int]:
        """Обрабатывает один фильтр без сортировки."""
        all_product_ids = set()
        
        first_page_url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                        f"country=UA&lang=ua&filters=page:1;producer:{brand_name};{filter_id}:{filter_value}&id={category_id}")
        
        first_page_response = self._make_request(first_page_url)
        
        if not first_page_response or 'data' not in first_page_response or 'goods' not in first_page_response['data']:
            self.logger.warning(f"    Не удалось получить данные для фильтра {filter_id}:{filter_value}")
            return all_product_ids
        
        goods_data = first_page_response['data']['goods']
        
        if 'ids' in goods_data:
            all_product_ids.update(goods_data['ids'])
        
        total_pages = goods_data.get('total_pages', 1)
        
        self.logger.info(f"    Всего страниц для фильтра: {total_pages}")
        
        for page in range(2, total_pages + 1):
            url = (f"https://catalog-api.rozetka.com.ua/v0.1/api/category/catalog?"
                f"country=UA&lang=ua&filters=page:{page};producer:{brand_name};{filter_id}:{filter_value}&id={category_id}")
            
            response = self._make_request(url)
            
            if response and 'data' in response and 'goods' in response['data']:
                goods_data = response['data']['goods']
                if 'ids' in goods_data:
                    all_product_ids.update(goods_data['ids'])
            
            time.sleep(0.1)
        
        return all_product_ids

    def _process_default_parsing(
            self, 
            brand_name: str, 
            category_id: int, 
            products_quantity: int, 
            sort_list: Optional[List[str]] = None
        ) -> Set[int]:
        """Обрабатывает бренд без фильтров (только с сортировками или простую пагинацию)."""
        all_product_ids = set()
        
        if sort_list:
            for sort_value in sort_list:
                sort_ids = self._process_with_sort(brand_name, category_id, products_quantity, sort_value)
                all_product_ids.update(sort_ids)
        else:
            all_product_ids = self._process_simple_pagination(brand_name, category_id, products_quantity)
        
        return all_product_ids

    def process_single_batch(
            self, 
            product_ids_batch: List[str]
        ) -> List[Dict[str, Any]]:
        """
        Обрабатывает один батч продуктов (для использования воркерами)
        """
        batch_raw_data = self.get_batch_data(product_ids_batch)
        
        if not batch_raw_data:
            return []
        
        parsed_batch = self.parse_batch_data(batch_raw_data)
        return parsed_batch if parsed_batch else []

    def get_all_product_ids_for_category(
            self, 
            category_id: int = 80089,
            filters: Optional[List[str]] = None,
            sort_list: Optional[List[str]] = None,
            default_parse: bool = False
        ) -> Set[str]:
        """
        Получает все ID товаров для категории (синхронная версия без многопоточности)
        """
        self.logger.info("Получение списка брендов...")
        brands_data = self.search_brands_for_category(category_id)
        
        if not brands_data:
            self.logger.error("Не удалось получить бренды для категории")
            return set()
        
        self.logger.info(f"Найдено {len(brands_data)} брендов. Начинаем сбор ID товаров...")
        
        all_product_ids = set()
        completed_count = 0
        
        for brand_name, brand_data in brands_data.items():
            brand_product_ids = self.get_product_ids_for_brand(
                brand_data, category_id, filters, sort_list, default_parse
            )
            all_product_ids.update(brand_product_ids)
            completed_count += 1
            self.logger.info(f"[{completed_count}/{len(brands_data)}] Бренд {brand_name}: добавлено {len(brand_product_ids)} ID, всего: {len(all_product_ids)}")
        
        self.logger.info(f"Сбор завершен! Всего собрано уникальных ID товаров: {len(all_product_ids)}")
        return all_product_ids

    def get_batch_data(
            self, 
            product_ids: List[str]
        ) -> List[Dict[str, Any]]:
        """
        Получает данные для нескольких продуктов одним запросом по их ID
        """
        if not product_ids:
            return []
        
        batch_ids = [str(pid) for pid in product_ids]
        ids_param = ','.join(batch_ids)

        api_url = f'https://common-api.rozetka.com.ua/v1/api/product/details?country=UA&lang=ua&ids={ids_param}'

        response_data = self._make_request(api_url)
        
        if isinstance(response_data, dict) and 'data' in response_data:
            return response_data['data']
        else:
            self.logger.error(f"Ожидался JSON с 'data', получен: {type(response_data)}")
            return []