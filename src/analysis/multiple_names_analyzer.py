import pandas as pd
import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class MultipleNamesAnalyzer:
    def __init__(self, df):
        self.df = df
        self.results = {}
        self.report_dir = 'reports/names_analyzer'
    
    async def analyze(self):
        """Анализ нескольких имен на один паспорт"""
        logger.info("Запуск анализа нескольких имен...")
        
        try:
            # Группируем по номеру паспорта
            passport_names = self.df.groupby('TravelDoc')['FirstName'].agg([
                ('unique_names', 'nunique'),
                ('names_list', lambda x: list(x.unique())),
                ('ticket_count', 'count')
            ]).reset_index()
            
            # Находим паспорта с несколькими именами
            multiple_names = passport_names[passport_names['unique_names'] > 1]
            
            # Получаем полные данные по проблемным паспортам
            problematic_passports = multiple_names['TravelDoc'].tolist()
            full_problematic_data = self.df[self.df['TravelDoc'].isin(problematic_passports)]
            
            self.results = {
                'count': len(multiple_names),
                'details': multiple_names.to_dict('records'),
                'total_passports': len(passport_names),
                'total_records': len(self.df),
                'problematic_flights': full_problematic_data.to_dict('records')
            }
            
            logger.info(f"Найдено паспортов с несколькими именами: {len(multiple_names)}")
            return self.results
            
        except Exception as e:
            logger.error(f"Ошибка в анализе имен: {e}")
            return {}
    
    async def generate_report(self):
        """Генерация отчета"""
        try:
            os.makedirs(self.report_dir, exist_ok=True)
            await self._create_diagrams()
            await self._create_text_report()
            await self._save_csv_reports()
            logger.info(f"Отчет имен сохранен в {self.report_dir}")
        except Exception as e:
            logger.error(f"Ошибка генерации отчета имен: {e}")
    
    async def _create_diagrams(self):
        """Создание красивых диаграмм с именами"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
        fig.suptitle('АНАЛИЗ ПАСПОРТОВ С НЕСКОЛЬКИМИ ИМЕНАМИ', fontsize=16, fontweight='bold', y=0.95)
        
        if self.results['count'] > 0:
            details_df = pd.DataFrame(self.results['details'])
            flights_df = pd.DataFrame(self.results['problematic_flights'])
            
            # 1. ЛЕВАЯ ДИАГРАММА: Билеты по паспортам с именами
            details_df = details_df.sort_values('ticket_count', ascending=False)
            
            colors1 = plt.cm.Reds(np.linspace(0.6, 0.9, len(details_df)))
            bars1 = ax1.bar(range(len(details_df)), details_df['ticket_count'], 
                           color=colors1, alpha=0.8, edgecolor='white', linewidth=2)
            
            ax1.set_title('БИЛЕТЫ ПО ПАСПОРТАМ С НЕСКОЛЬКИМИ ИМЕНАМИ', fontweight='bold', pad=15)
            ax1.set_xlabel('Паспорта и имена')
            ax1.set_ylabel('Количество билетов')
            
            # Создаем подписи с именами
            x_labels = []
            for _, row in details_df.iterrows():
                names_str = '/'.join([str(name) for name in row['names_list']])
                passport_short = str(row['TravelDoc'])[-4:]  # последние 4 цифры
                x_labels.append(f"{passport_short}\n{names_str}")
            
            ax1.set_xticks(range(len(details_df)))
            ax1.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=9)
            
            # Добавляем значения на столбцы
            for bar, count in zip(bars1, details_df['ticket_count']):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1, 
                        f'{int(count)}', ha='center', va='bottom', fontweight='bold', fontsize=10)
            
            # 2. ПРАВАЯ ДИАГРАММА: Активность по месяцам с детализацией
            if len(flights_df) > 0:
                # Конвертируем даты
                flights_df['DepartDate'] = pd.to_datetime(flights_df['DepartDate'], format='%d.%m.%Y', errors='coerce')
                monthly_activity = flights_df['DepartDate'].dt.to_period('M').value_counts().sort_index()
                
                # Преобразуем периоды в русские названия месяцев
                month_names_ru = {
                    'January': 'Янв', 'February': 'Фев', 'March': 'Мар',
                    'April': 'Апр', 'May': 'Май', 'June': 'Июн',
                    'July': 'Июл', 'August': 'Авг', 'September': 'Сен',
                    'October': 'Окт', 'November': 'Ноя', 'December': 'Дек'
                }
                
                month_labels = []
                for period in monthly_activity.index:
                    month_name = period.strftime('%B')
                    month_labels.append(f"{month_names_ru.get(month_name, month_name)}\n{period.year}")
                
                colors2 = plt.cm.Blues(np.linspace(0.6, 0.9, len(monthly_activity)))
                bars2 = ax2.bar(range(len(monthly_activity)), monthly_activity.values, 
                               color=colors2, alpha=0.8, edgecolor='white', linewidth=2)
                
                ax2.set_title('АКТИВНОСТЬ ПО МЕСЯЦАМ', fontweight='bold', pad=15)
                ax2.set_xlabel('Месяц и год')
                ax2.set_ylabel('Количество рейсов')
                ax2.set_xticks(range(len(monthly_activity)))
                ax2.set_xticklabels(month_labels, rotation=45, ha='right', fontsize=9)
                
                # Добавляем значения на столбцы
                for bar, count in zip(bars2, monthly_activity.values):
                    height = bar.get_height()
                    ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1, 
                            f'{int(count)}', ha='center', va='bottom', fontweight='bold', fontsize=9)
                
                # Добавляем общее количество рейсов в заголовок
                total_flights = len(flights_df)
                ax2.text(0.02, 0.98, f'Всего рейсов: {total_flights}', 
                        transform=ax2.transAxes, fontsize=10, fontweight='bold',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='yellow', alpha=0.7))
            else:
                ax2.text(0.5, 0.5, 'НЕТ ДАННЫХ\nО РЕЙСАХ', 
                        ha='center', va='center', transform=ax2.transAxes,
                        fontsize=12, fontweight='bold', color='gray')
                ax2.set_title('АКТИВНОСТЬ ПО МЕСЯЦАМ', fontweight='bold', pad=15)
            
            # Настройка внешнего вида
            for ax in [ax1, ax2]:
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.grid(True, axis='y', alpha=0.3, linestyle='--')
                ax.set_axisbelow(True)
                
        else:
            # Если нет проблемных паспортов
            for ax in [ax1, ax2]:
                ax.text(0.5, 0.5, 'ПРОБЛЕМНЫХ ПАСПОРТОВ\nНЕ ОБНАРУЖЕНО', 
                       ha='center', va='center', transform=ax.transAxes,
                       fontsize=14, fontweight='bold', color='green')
                ax.set_facecolor('#f0f8f0')
            
            ax1.set_title('БИЛЕТЫ ПО ПАСПОРТАМ', fontweight='bold', pad=15)
            ax2.set_title('АКТИВНОСТЬ ПО МЕСЯЦАМ', fontweight='bold', pad=15)
        
        plt.tight_layout()
        plt.savefig(f'{self.report_dir}/names_analysis.png', dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
    
    async def _save_csv_reports(self):
        """Сохранение CSV отчетов"""
        if self.results['count'] > 0:
            # 1. Детали по проблемным паспортам
            details_df = pd.DataFrame(self.results['details'])
            details_df.to_csv(f'{self.report_dir}/problematic_passports.csv', index=False, sep=';')
            
            # 2. Полные данные по рейсам проблемных паспортов
            flights_df = pd.DataFrame(self.results['problematic_flights'])
            flights_df.to_csv(f'{self.report_dir}/problematic_flights_full.csv', index=False, sep=';')
    
    async def _create_text_report(self):
        """Текстовый отчет"""
        report = [
            "="*60,
            "АНАЛИЗ НЕСКОЛЬКИХ ИМЕН НА ПАСПОРТ",
            "="*60,
            f"Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "ОБЩАЯ СТАТИСТИКА:",
            "-" * 40,
            f"Всего записей: {self.results['total_records']:,}",
            f"Уникальных паспортов: {self.results['total_passports']:,}",
            f"Паспортов с несколькими именами: {self.results['count']:,}",
            ""
        ]
        
        if self.results['count'] > 0:
            # Детали по паспортам
            report.extend([
                "ДЕТАЛИ ПО ПАСПОРТАМ:",
                "-" * 40
            ])
            for detail in self.results['details']:
                names_str = ' / '.join([str(name) for name in detail['names_list']])
                report.append(f"Паспорт: {detail['TravelDoc']}")
                report.append(f"  Имена: {names_str}")
                report.append(f"  Билетов: {int(detail['ticket_count'])}")
                report.append("")
            
            # Статистика по рейсам
            flights_df = pd.DataFrame(self.results['problematic_flights'])
            total_flights = len(flights_df)
            
            if len(flights_df) > 0:
                unique_routes = flights_df.groupby(['DepartCity', 'ArrivalCity']).ngroups
                unique_airlines = flights_df['Airline'].nunique()
                
                # Анализ по месяцам
                flights_df['DepartDate'] = pd.to_datetime(flights_df['DepartDate'], format='%d.%m.%Y', errors='coerce')
                monthly_stats = flights_df['DepartDate'].dt.to_period('M').value_counts().sort_index()
                
                report.extend([
                    "СТАТИСТИКА ПО РЕЙСАМ:",
                    "-" * 40,
                    f"Всего рейсов: {total_flights}",
                    f"Уникальных маршрутов: {unique_routes}",
                    f"Авиакомпаний: {unique_airlines}",
                    ""
                ])
                
                # Самые активные месяцы
                if len(monthly_stats) > 0:
                    report.extend([
                        "АКТИВНОСТЬ ПО МЕСЯЦАМ:",
                        "-" * 40
                    ])
                    for period, count in monthly_stats.items():
                        month_name = period.strftime('%B')
                        month_names_ru = {
                            'January': 'Январь', 'February': 'Февраль', 'March': 'Март',
                            'April': 'Апрель', 'May': 'Май', 'June': 'Июнь',
                            'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь',
                            'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
                        }
                        report.append(f"  {month_names_ru.get(month_name, month_name)} {period.year}: {int(count)} рейсов")
                    report.append("")
            
            report.extend([
                "ВЫВОДЫ:",
                "-" * 40,
                f"Обнаружено {self.results['count']} подозрительных паспортов.",
                f"Совершено {total_flights} рейсов с использованием этих паспортов.",
                "Рекомендуется дополнительная проверка."
            ])
        else:
            report.extend([
                "ВЫВОДЫ:",
                "-" * 40,
                "Аномалий не обнаружено.",
                "Все паспорта корректны."
            ])
        
        report.extend([
            "",
            "СОХРАНЕННЫЕ ФАЙЛЫ:",
            "-" * 40,
            "problematic_passports.csv - детали по проблемным паспортам",
            "problematic_flights_full.csv - полные данные рейсов",
            "names_analysis.png - визуализация анализа"
        ])
        
        with open(f'{self.report_dir}/report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))