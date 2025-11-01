import pandas as pd
import asyncio
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class BonusProgramsAnalyzer:
    def __init__(self, df):
        self.df = df
        self.results = {}
        self.report_dir = 'reports/bonus_analyzer'
    
    async def analyze(self):
        """Анализ бонусных программ"""
        logger.info("Запуск анализа бонусных программ...")
        
        try:
            # Статистика использования бонусных программ
            bonus_usage = self.df['BonusProgramm'].notna().value_counts()
            self.results['bonus_usage'] = {
                'with_bonus': bonus_usage.get(True, 0),
                'without_bonus': bonus_usage.get(False, 0),
                'usage_percentage': (bonus_usage.get(True, 0) / len(self.df)) * 100
            }
            
            # Топ бонусных программ
            if self.results['bonus_usage']['with_bonus'] > 0:
                bonus_programs = self.df[self.df['BonusProgramm'].notna()]['BonusProgramm']
                # Берем первые 2 символа как код программы
                program_codes = bonus_programs.str[:2]
                self.results['top_programs'] = program_codes.value_counts().head(10).to_dict()
            else:
                self.results['top_programs'] = {}
            
            # Связь бонусных программ с авиакомпаниями
            if self.results['bonus_usage']['with_bonus'] > 0:
                bonus_data = self.df[self.df['BonusProgramm'].notna()]
                airline_bonus = bonus_data.groupby('Airline').size().sort_values(ascending=False).head(10)
                self.results['airline_bonus'] = airline_bonus.to_dict()
            else:
                self.results['airline_bonus'] = {}
            
            logger.info("Анализ бонусных программ завершен")
            return self.results
            
        except Exception as e:
            logger.error(f"Ошибка в анализе бонусных программ: {e}")
            return {}
    
    async def generate_report(self):
        """Генерация отчета"""
        try:
            os.makedirs(self.report_dir, exist_ok=True)
            await self._create_diagrams()
            await self._create_text_report()
            logger.info(f"Отчет бонусных программ сохранен в {self.report_dir}")
        except Exception as e:
            logger.error(f"Ошибка генерации отчета бонусных программ: {e}")
    
    async def _create_diagrams(self):
        """Создание диаграмм для бонусных программ"""
        fig, axes = plt.subplots(2, 2, figsize=(20, 12))
        fig.suptitle('АНАЛИЗ БОНУСНЫХ ПРОГРАММ', fontsize=16, fontweight='bold')
        
        # 1. Использование бонусных программ
        usage_data = [
            self.results['bonus_usage']['with_bonus'],
            self.results['bonus_usage']['without_bonus']
        ]
        usage_labels = ['С бонусной программой', 'Без бонусной программы']
        colors_usage = ['#ff9999', '#66b3ff']
        axes[0, 0].pie(usage_data, labels=usage_labels, autopct='%1.1f%%', colors=colors_usage, startangle=90)
        axes[0, 0].set_title('ИСПОЛЬЗОВАНИЕ БОНУСНЫХ ПРОГРАММ', fontweight='bold')
        
        # 2. Топ бонусных программ
        if self.results['top_programs']:
            programs = pd.Series(self.results['top_programs']).head(8)
            axes[0, 1].bar(range(len(programs)), programs.values, color='green', alpha=0.7)
            axes[0, 1].set_xticks(range(len(programs)))
            axes[0, 1].set_xticklabels(programs.index, fontsize=10)
            axes[0, 1].set_title('ТОП БОНУСНЫХ ПРОГРАММ', fontweight='bold')
            axes[0, 1].set_ylabel('Количество участников')
        else:
            axes[0, 1].text(0.5, 0.5, 'НЕТ ДАННЫХ\nО БОНУСНЫХ ПРОГРАММАХ', 
                           ha='center', va='center', transform=axes[0, 1].transAxes,
                           fontsize=12, fontweight='bold')
            axes[0, 1].set_title('ТОП БОНУСНЫХ ПРОГРАММ', fontweight='bold')
        
        # 3. Связь с авиакомпаниями
        if self.results['airline_bonus']:
            airline_data = pd.Series(self.results['airline_bonus']).head(8)
            axes[1, 0].barh(range(len(airline_data)), airline_data.values, color='orange', alpha=0.7)
            axes[1, 0].set_yticks(range(len(airline_data)))
            axes[1, 0].set_yticklabels(airline_data.index, fontsize=9)
            axes[1, 0].set_title('АВИАКОМПАНИИ С БОНУСНЫМИ ПРОГРАММАМИ', fontweight='bold')
            axes[1, 0].set_xlabel('Количество участников')
        else:
            axes[1, 0].text(0.5, 0.5, 'НЕТ ДАННЫХ', 
                           ha='center', va='center', transform=axes[1, 0].transAxes,
                           fontsize=12, fontweight='bold')
            axes[1, 0].set_title('АВИАКОМПАНИИ С БОНУСНЫМИ ПРОГРАММАМИ', fontweight='bold')
        
        # 4. Общая статистика
        axes[1, 1].axis('off')
        stats_text = (
            f"ОБЩАЯ СТАТИСТИКА:\n\n"
            f"Всего пассажиров: {len(self.df):,}\n"
            f"С бонусной программой: {self.results['bonus_usage']['with_bonus']:,}\n"
            f"Без бонусной программы: {self.results['bonus_usage']['without_bonus']:,}\n"
            f"Процент использования: {self.results['bonus_usage']['usage_percentage']:.1f}%"
        )
        axes[1, 1].text(0.1, 0.9, stats_text, fontsize=12, va='top', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f'{self.report_dir}/bonus_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    async def _create_text_report(self):
        """Текстовый отчет"""
        report = [
            "="*60,
            "АНАЛИЗ БОНУСНЫХ ПРОГРАММ",
            "="*60,
            f"Дата генерации: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            "",
            "СТАТИСТИКА ИСПОЛЬЗОВАНИЯ:",
            "-" * 40,
            f"Всего пассажиров: {len(self.df):,}",
            f"С бонусной программой: {self.results['bonus_usage']['with_bonus']:,}",
            f"Без бонусной программы: {self.results['bonus_usage']['without_bonus']:,}",
            f"Процент использования: {self.results['bonus_usage']['usage_percentage']:.1f}%",
            ""
        ]
        
        if self.results['top_programs']:
            report.extend([
                "ТОП БОНУСНЫХ ПРОГРАММ:",
                "-" * 40
            ])
            for program, count in self.results['top_programs'].items():
                report.append(f"Программа {program}: {count:,} участников")
            report.append("")
        
        if self.results['airline_bonus']:
            report.extend([
                "АВИАКОМПАНИИ С БОНУСНЫМИ ПРОГРАММАМИ:",
                "-" * 40
            ])
            for airline, count in self.results['airline_bonus'].items():
                report.append(f"{airline}: {count:,} участников")
        
        with open(f'{self.report_dir}/report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))