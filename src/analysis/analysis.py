import pandas as pd
import asyncio
import logging
import os
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None
    
    async def load_data(self):
        """Асинхронная загрузка всех данных"""
        logger.info("=== ЗАГРУЗКА ДАННЫХ ===")
        try:
            loop = asyncio.get_event_loop()
            self.df = await loop.run_in_executor(
                None, 
                lambda: pd.read_csv(self.file_path, delimiter=';', low_memory=False)
            )
            logger.info(f"Успешно загружено: {self.df.shape[0]:,} записей, {self.df.shape[1]} колонок")
            return True
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            return False

async def main():
    """Главная функция - запуск всех анализов"""
    file_path = './data/clean/cleaned_sources.csv'
    
    if not os.path.exists(file_path):
        logger.error(f"Файл не найден: {file_path}")
        return
    
    # Загружаем данные
    loader = DataLoader(file_path)
    if not await loader.load_data():
        return
    
    # Импортируем анализаторы
    from multiple_names_analyzer import MultipleNamesAnalyzer
    from routes_analyzer import RoutesAnalyzer
    from flights_analyzer import FlightsAnalyzer
    from bonus_programs_analyzer import BonusProgramsAnalyzer
    
    # Создаем анализаторы
    analyzers = [
        MultipleNamesAnalyzer(loader.df),
        RoutesAnalyzer(loader.df),
        FlightsAnalyzer(loader.df),
        BonusProgramsAnalyzer(loader.df)
    ]
    
    # Запускаем все анализы параллельно
    logger.info("Запуск всех анализов...")
    tasks = [analyzer.analyze() for analyzer in analyzers]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Генерируем отчеты
    report_tasks = [analyzer.generate_report() for analyzer in analyzers]
    await asyncio.gather(*report_tasks, return_exceptions=True)
    
    print("\n" + "="*60)
    print("ВСЕ АНАЛИЗЫ ЗАВЕРШЕНЫ!")
    print("="*60)
    for analyzer in analyzers:
        print(f"✅ {analyzer.__class__.__name__}: reports/{analyzer.__class__.__name__.lower()}/")

if __name__ == "__main__":
    asyncio.run(main())