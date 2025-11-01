import pandas as pd
import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class RoutesAnalyzer:
    def __init__(self, df):
        self.df = df
        self.results = {}
        self.report_dir = 'reports/routes_analyzer'
    
    async def analyze(self):
        """Анализ маршрутов и направлений"""
        logger.info("Запуск анализа маршрутов...")
        
        try:
            # Топ-10 городов вылета
            self.results['top_departure_cities'] = self.df['DepartCity'].value_counts().head(10).to_dict()
            
            # Топ-10 городов прилета
            self.results['top_arrival_cities'] = self.df['ArrivalCity'].value_counts().head(10).to_dict()
            
            # Самые популярные направления (пары городов)
            self.df['route'] = self.df['DepartCity'] + ' → ' + self.df['ArrivalCity']
            self.results['top_routes'] = self.df['route'].value_counts().head(10).to_dict()
            
            # Баланс вылетов/прилетов по городам
            departures = self.df['DepartCity'].value_counts()
            arrivals = self.df['ArrivalCity'].value_counts()
            
            city_balance = (departures - arrivals).sort_values(ascending=False)
            self.results['city_balance'] = {
                'most_departures': city_balance.head(5).to_dict(),
                'most_arrivals': city_balance.tail(5).to_dict()
            }
            
            logger.info("Анализ маршрутов завершен")
            return self.results
            
        except Exception as e:
            logger.error(f"Ошибка в анализе маршрутов: {e}")
            return {}
    
    async def generate_report(self):
        """Генерация отчета с диаграммами"""
        try:
            os.makedirs(self.report_dir, exist_ok=True)
            
            # Создаем визуализации
            await self._create_diagrams()
            
            # Текстовый отчет
            await self._create_text_report()
            
            logger.info(f"Отчет маршрутов сохранен в {self.report_dir}")
            
        except Exception as e:
            logger.error(f"Ошибка генерации отчета маршрутов: {e}")
    
    async def _create_diagrams(self):
        """Создание красивых диаграмм"""
        fig, axes = plt.subplots(2, 2, figsize=(20, 12))
        fig.suptitle('АНАЛИЗ МАРШРУТОВ И НАПРАВЛЕНИЙ', fontsize=16, fontweight='bold')
        
        # 1. Топ городов вылета
        departures = pd.Series(self.results['top_departure_cities'])
        axes[0, 0].barh(range(len(departures)), departures.values, color='skyblue')
        axes[0, 0].set_yticks(range(len(departures)))
        axes[0, 0].set_yticklabels(departures.index, fontsize=9)
        axes[0, 0].set_title('ТОП-10 ГОРОДОВ ВЫЛЕТА', fontweight='bold')
        axes[0, 0].set_xlabel('Количество вылетов')
        
        # 2. Топ городов прилета
        arrivals = pd.Series(self.results['top_arrival_cities'])
        axes[0, 1].barh(range(len(arrivals)), arrivals.values, color='lightcoral')
        axes[0, 1].set_yticks(range(len(arrivals)))
        axes[0, 1].set_yticklabels(arrivals.index, fontsize=9)
        axes[0, 1].set_title('ТОП-10 ГОРОДОВ ПРИЛЕТА', fontweight='bold')
        axes[0, 1].set_xlabel('Количество прилетов')
        
        # 3. Топ маршрутов
        routes = pd.Series(self.results['top_routes'])
        axes[1, 0].bar(range(len(routes)), routes.values, color='lightgreen', alpha=0.7)
        axes[1, 0].set_xticks(range(len(routes)))
        axes[1, 0].set_xticklabels(routes.index, rotation=45, ha='right', fontsize=8)
        axes[1, 0].set_title('ТОП-10 ПОПУЛЯРНЫХ МАРШРУТОВ', fontweight='bold')
        axes[1, 0].set_ylabel('Количество рейсов')
        
        # 4. Баланс городов
        balance_data = {
            **self.results['city_balance']['most_departures'],
            **self.results['city_balance']['most_arrivals']
        }
        balance_series = pd.Series(balance_data)
        colors = ['red' if x > 0 else 'blue' for x in balance_series.values]
        
        axes[1, 1].bar(range(len(balance_series)), balance_series.values, color=colors, alpha=0.7)
        axes[1, 1].set_xticks(range(len(balance_series)))
        axes[1, 1].set_xticklabels(balance_series.index, rotation=45, ha='right', fontsize=8)
        axes[1, 1].set_title('БАЛАНС ВЫЛЕТОВ/ПРИЛЕТОВ ПО ГОРОДАМ', fontweight='bold')
        axes[1, 1].set_ylabel('Разница (вылеты - прилеты)')
        axes[1, 1].axhline(y=0, color='black', linestyle='-', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{self.report_dir}/routes_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    async def _create_text_report(self):
        """Создание текстового отчета"""
        report = [
            "="*60,
            "АНАЛИЗ МАРШРУТОВ И НАПРАВЛЕНИЙ",
            "="*60,
            f"Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "ТОП-10 ГОРОДОВ ВЫЛЕТА:",
            "-" * 40
        ]
        
        for city, count in self.results['top_departure_cities'].items():
            report.append(f"{city}: {count:,} вылетов")
        
        report.extend([
            "",
            "ТОП-10 ГОРОДОВ ПРИЛЕТА:",
            "-" * 40
        ])
        
        for city, count in self.results['top_arrival_cities'].items():
            report.append(f"{city}: {count:,} прилетов")
        
        report.extend([
            "",
            "ГОРОДА С НАИБОЛЬШИМ ПРЕОБЛАДАНИЕМ ВЫЛЕТОВ:",
            "-" * 40
        ])
        
        for city, balance in self.results['city_balance']['most_departures'].items():
            report.append(f"{city}: +{balance:,} (больше вылетов)")
        
        report.extend([
            "",
            "ГОРОДА С НАИБОЛЬШИМ ПРЕОБЛАДАНИЕМ ПРИЛЕТОВ:",
            "-" * 40
        ])
        
        for city, balance in self.results['city_balance']['most_arrivals'].items():
            report.append(f"{city}: {balance:,} (больше прилетов)")
        
        with open(f'{self.report_dir}/report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))