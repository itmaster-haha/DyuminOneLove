import pandas as pd
import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
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
            
            self.results = {
                'count': len(multiple_names),
                'details': multiple_names.to_dict('records'),
                'total_passports': len(passport_names),
                'total_records': len(self.df)
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
            logger.info(f"Отчет имен сохранен в {self.report_dir}")
        except Exception as e:
            logger.error(f"Ошибка генерации отчета имен: {e}")
    
    async def _create_diagrams(self):
        """Создание диаграмм"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        fig.suptitle('АНАЛИЗ НЕСКОЛЬКИХ ИМЕН НА ПАСПОРТ', fontsize=16, fontweight='bold')
        
        # 1. Общая статистика
        labels = ['Обычные паспорта', 'Паспорта с несколькими именами']
        sizes = [
            self.results['total_passports'] - self.results['count'],
            self.results['count']
        ]
        colors = ['#66b3ff', '#ff6666']
        
        ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax1.set_title('РАСПРЕДЕЛЕНИЕ ПАСПОРТОВ', fontweight='bold')
        
        # 2. Детали по проблемным паспортам
        if self.results['count'] > 0:
            details_df = pd.DataFrame(self.results['details'])
            ax2.bar(range(len(details_df)), details_df['ticket_count'], color='#e74c3c', alpha=0.7)
            ax2.set_title('КОЛИЧЕСТВО БИЛЕТОВ НА ПРОБЛЕМНЫЕ ПАСПОРТА', fontweight='bold')
            ax2.set_xlabel('Паспорта')
            ax2.set_ylabel('Количество билетов')
            
            # Форматируем labels
            passport_labels = []
            for doc in details_df['TravelDoc']:
                doc_str = str(doc)
                if len(doc_str) > 8:
                    passport_labels.append(f"{doc_str[:4]}...{doc_str[-4:]}")
                else:
                    passport_labels.append(doc_str)
            
            ax2.set_xticks(range(len(passport_labels)))
            ax2.set_xticklabels(passport_labels, rotation=45, ha='right')
            
            # Добавляем значения
            for i, count in enumerate(details_df['ticket_count']):
                ax2.text(i, count + 0.1, str(count), ha='center', va='bottom', fontweight='bold')
        else:
            ax2.text(0.5, 0.5, 'ПРОБЛЕМНЫХ ПАСПОРТОВ\nНЕ ОБНАРУЖЕНО', 
                    ha='center', va='center', transform=ax2.transAxes,
                    fontsize=12, fontweight='bold', color='green')
            ax2.set_title('КОЛИЧЕСТВО БИЛЕТОВ', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f'{self.report_dir}/names_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
    
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
            report.extend([
                "ДЕТАЛИ ПО ПАСПОРТАМ:",
                "-" * 40
            ])
            for detail in self.results['details']:
                names_str = ', '.join(detail['names_list'])
                report.append(f"Паспорт: {detail['TravelDoc']}")
                report.append(f"  Имена: {names_str}")
                report.append(f"  Билетов: {detail['ticket_count']}")
                report.append("")
        
        report.extend([
            "ВЫВОДЫ:",
            "-" * 40
        ])
        
        if self.results['count'] > 0:
            report.append(f"Обнаружено {self.results['count']} подозрительных паспортов.")
            report.append("Рекомендуется дополнительная проверка.")
        else:
            report.append("Аномалий не обнаружено.")
            report.append("Все паспорта корректны.")
        
        with open(f'{self.report_dir}/report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))