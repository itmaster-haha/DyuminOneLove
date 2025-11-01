import pandas as pd
import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class FlightsAnalyzer:
    def __init__(self, df):
        self.df = df
        self.results = {}
        self.report_dir = 'reports/flights_analyzer'
    
    async def analyze(self):
        """Анализ рейсов и перелетов"""
        logger.info("Запуск анализа рейсов...")
        
        try:
            # Топ рейсов по номерам
            self.results['top_flight_numbers'] = self.df['FlightNumber'].value_counts().head(15).to_dict()
            
            # Распределение по классам обслуживания
            self.results['travel_classes'] = self.df['TrvCls'].value_counts().to_dict()
            
            # Анализ кодов шеринга
            codeshare_stats = self.df['CodeShare'].value_counts()
            self.results['codeshare_analysis'] = {
                'total_codeshare': codeshare_stats.get('LANDED', 0),
                'total_direct': len(self.df) - codeshare_stats.get('LANDED', 0),
                'codeshare_percentage': (codeshare_stats.get('LANDED', 0) / len(self.df)) * 100
            }
            
            # Анализ времени вылетов
            self.df['DepartHour'] = pd.to_datetime(self.df['DepartTime'], format='%H:%M').dt.hour
            self.results['departure_hours'] = self.df['DepartHour'].value_counts().sort_index().to_dict()
            
            # Самые загруженные дни
            self.df['DepartDate'] = pd.to_datetime(self.df['DepartDate'], format='%d.%m.%Y')
            self.df['DepartDayOfWeek'] = self.df['DepartDate'].dt.day_name()
            self.results['busiest_days'] = self.df['DepartDayOfWeek'].value_counts().to_dict()
            
            # Анализ по месяцам
            self.df['DepartMonth'] = self.df['DepartDate'].dt.month_name()
            self.results['monthly_traffic'] = self.df['DepartMonth'].value_counts().to_dict()
            
            logger.info("Анализ рейсов завершен")
            return self.results
            
        except Exception as e:
            logger.error(f"Ошибка в анализе рейсов: {e}")
            return {}
    
    async def generate_report(self):
        """Генерация отчета"""
        try:
            os.makedirs(self.report_dir, exist_ok=True)
            await self._create_diagrams()
            await self._create_text_report()
            logger.info(f"Отчет рейсов сохранен в {self.report_dir}")
        except Exception as e:
            logger.error(f"Ошибка генерации отчета рейсов: {e}")
    
    async def _create_diagrams(self):
        """Создание диаграмм для рейсов"""
        fig, axes = plt.subplots(2, 2, figsize=(20, 12))
        fig.suptitle('АНАЛИЗ РЕЙСОВ И ПЕРЕЛЕТОВ', fontsize=16, fontweight='bold')
        
        # 1. Топ номеров рейсов
        flights = pd.Series(self.results['top_flight_numbers']).head(10)
        axes[0, 0].bar(range(len(flights)), flights.values, color='#3498db', alpha=0.8)
        axes[0, 0].set_xticks(range(len(flights)))
        axes[0, 0].set_xticklabels(flights.index, rotation=45, ha='right', fontsize=9)
        axes[0, 0].set_title('ТОП-10 НОМЕРОВ РЕЙСОВ', fontweight='bold')
        axes[0, 0].set_ylabel('Количество перелетов')
        
        # Добавляем значения на столбцы
        for i, count in enumerate(flights.values):
            axes[0, 0].text(i, count + max(flights.values)*0.01, f'{count}', 
                           ha='center', va='bottom', fontweight='bold', fontsize=8)
        
        # 2. Распределение по времени суток
        hours = pd.Series(self.results['departure_hours']).sort_index()
        axes[0, 1].plot(hours.index, hours.values, marker='o', linewidth=2, 
                       color='#e74c3c', markersize=6)
        axes[0, 1].fill_between(hours.index, hours.values, alpha=0.3, color='#e74c3c')
        axes[0, 1].set_title('РАСПРЕДЕЛЕНИЕ ВЫЛЕТОВ ПО ЧАСАМ', fontweight='bold')
        axes[0, 1].set_xlabel('Час дня')
        axes[0, 1].set_ylabel('Количество вылетов')
        axes[0, 1].grid(True, alpha=0.3)
        
        # 3. Загруженность дней недели
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        days_ru = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        busy_days = pd.Series(self.results['busiest_days']).reindex(days_order)
        
        colors_days = plt.cm.Set3(range(len(busy_days)))
        axes[1, 0].bar(range(len(busy_days)), busy_days.values, color=colors_days, alpha=0.8)
        axes[1, 0].set_xticks(range(len(busy_days)))
        axes[1, 0].set_xticklabels(days_ru, rotation=45, ha='right', fontsize=9)
        axes[1, 0].set_title('ЗАГРУЖЕННОСТЬ ДНЕЙ НЕДЕЛИ', fontweight='bold')
        axes[1, 0].set_ylabel('Количество вылетов')
        
        # 4. Классы обслуживания и Codeshare
        # Две круговые диаграммы в одном subplot
        from mpl_toolkits.axes_grid1 import make_axes_locatable
        
        # Классы обслуживания
        classes = pd.Series(self.results['travel_classes'])
        wedges1, texts1, autotexts1 = axes[1, 1].pie(
            classes.values, labels=classes.index, autopct='%1.1f%%', 
            startangle=90, colors=['#2ecc71', '#3498db', '#f39c12'],
            textprops={'fontsize': 9}
        )
        axes[1, 1].set_title('КЛАССЫ ОБСЛУЖИВАНИЯ И CODESHARE', fontweight='bold', pad=20)
        
        # Добавляем легенду для codeshare
        codeshare_text = f"Codeshare: {self.results['codeshare_analysis']['codeshare_percentage']:.1f}%"
        axes[1, 1].text(0.5, -0.2, codeshare_text, ha='center', va='center', 
                       fontsize=10, fontweight='bold', transform=axes[1, 1].transAxes,
                       bbox=dict(boxstyle="round,pad=0.3", facecolor='lightgray', alpha=0.7))
        
        plt.tight_layout()
        plt.savefig(f'{self.report_dir}/flights_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    async def _create_text_report(self):
        """Текстовый отчет"""
        report = [
            "="*60,
            "АНАЛИЗ РЕЙСОВ И ПЕРЕЛЕТОВ",
            "="*60,
            f"Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "ТОП НОМЕРОВ РЕЙСОВ:",
            "-" * 40
        ]
        
        for flight, count in list(self.results['top_flight_numbers'].items())[:10]:
            report.append(f"Рейс {flight}: {count:,} перелетов")
        
        report.extend([
            "",
            "РАСПРЕДЕЛЕНИЕ ПО ВРЕМЕНИ СУТОК:",
            "-" * 40
        ])
        
        # Самые популярные часы
        popular_hours = sorted(self.results['departure_hours'].items(), 
                             key=lambda x: x[1], reverse=True)[:3]
        for hour, count in popular_hours:
            report.append(f"{hour:02d}:00 - {hour+1:02d}:00: {count:,} вылетов")
        
        report.extend([
            "",
            "ЗАГРУЖЕННОСТЬ ДНЕЙ НЕДЕЛИ:",
            "-" * 40
        ])
        
        days_ru = {
            'Monday': 'Понедельник', 'Tuesday': 'Вторник', 'Wednesday': 'Среда',
            'Thursday': 'Четверг', 'Friday': 'Пятница', 'Saturday': 'Суббота', 
            'Sunday': 'Воскресенье'
        }
        
        for day, count in self.results['busiest_days'].items():
            report.append(f"{days_ru.get(day, day)}: {count:,} вылетов")
        
        report.extend([
            "",
            "КЛАССЫ ОБСЛУЖИВАНИЯ:",
            "-" * 40
        ])
        
        for cls, count in self.results['travel_classes'].items():
            report.append(f"Класс {cls}: {count:,} пассажиров")
        
        report.extend([
            "",
            "АНАЛИЗ CODESHARE:",
            "-" * 40,
            f"Прямые рейсы: {self.results['codeshare_analysis']['total_direct']:,}",
            f"Codeshare рейсы: {self.results['codeshare_analysis']['total_codeshare']:,}",
            f"Доля codeshare: {self.results['codeshare_analysis']['codeshare_percentage']:.1f}%",
            "",
            "СТАТИСТИКА ПО МЕСЯЦАМ:",
            "-" * 40
        ])
        
        months_ru = {
            'January': 'Январь', 'February': 'Февраль', 'March': 'Март',
            'April': 'Апрель', 'May': 'Май', 'June': 'Июнь',
            'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь',
            'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
        }
        
        for month, count in self.results['monthly_traffic'].items():
            report.append(f"{months_ru.get(month, month)}: {count:,} перелетов")
        
        with open(f'{self.report_dir}/report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))