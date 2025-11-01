import pandas as pd
import re
from typing import List, Dict, Any

try:
    import pypdf
except ImportError:
    import PyPDF2 as pypdf

def simple_flight_parser(pdf_path: str, start_page: int = 5, end_page: int = 10) -> pd.DataFrame:
    flights = []
    
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = pypdf.PdfReader(file)
            print(f" Файл: {pdf_path}")
            print(f" Всего страниц: {len(pdf_reader.pages)}")
            print(f" Анализируем страницы: {start_page}-{end_page}")
            
            current_from = "Unknown"
            current_to = "Unknown"
            
            for page_num in range(start_page - 1, min(end_page, len(pdf_reader.pages))):
                print(f"\n Страница {page_num + 1}:")
                page = pdf_reader.pages[page_num]
                text = page.extract_text()
                
                if text:
                    lines = text.split('\n')
                    for line in lines:
                        line = line.strip()
                        
                        # Определяем FROM/TO
                        if line.startswith("FROM:"):
                            current_from = line.replace("FROM:", "").strip()
                            continue
                        if line.startswith("TO:"):
                            current_to = line.replace("TO:", "").strip()
                            continue
                        
                        # Пропускаем заголовки
                        if any(k in line for k in ["Validity", "Days", "Dep", "Arr", "Flight", "Aircraft", "Travel"]):
                            continue
                        
                        # Простейшая проверка на рейс: два времени + номер рейса
                        if re.search(r'\d{2}:\d{2}\s+\d{2}:\d{2}\s+[A-Z0-9]+', line):
                            flight_data = parse_flight_line(line, current_from, current_to, page_num + 1)
                            if flight_data:
                                flights.append(flight_data)
    
    except Exception as e:
        print(f" Ошибка: {e}")
    
    return pd.DataFrame(flights)

def parse_flight_line(line: str, from_loc: str, to_loc: str, page_num: int) -> Dict[str, Any]:
    """Парсинг строки рейса, стараемся разделить все поля"""
    try:
        line_clean = re.sub(r'\s+', ' ', line.strip())
        parts = line_clean.split()
        
        # Пытаемся угадать позиции полей
        if len(parts) >= 6:
            # Ищем времена
            dep_time_idx = next((i for i, p in enumerate(parts) if re.match(r'\d{2}:\d{2}', p)), None)
            arr_time_idx = dep_time_idx + 1 if dep_time_idx is not None else None
            
            if dep_time_idx is not None and arr_time_idx is not None:
                dep_time = parts[dep_time_idx]
                arr_time = parts[arr_time_idx]
                flight_num = parts[arr_time_idx + 1] if len(parts) > arr_time_idx + 1 else "Unknown"
                aircraft = parts[arr_time_idx + 2] if len(parts) > arr_time_idx + 2 else "Unknown"
                travel_time = parts[arr_time_idx + 3] if len(parts) > arr_time_idx + 3 else "Unknown"
                
                # Validity и Days - всё, что до dep_time_idx
                validity_days = " ".join(parts[:dep_time_idx])
                if len(validity_days.split()) >= 2:
                    validity = " ".join(validity_days.split()[:4])
                    days = " ".join(validity_days.split()[4:])
                else:
                    validity = validity_days
                    days = ""
                
                return {
                    'From': from_loc,
                    'To': to_loc,
                    'Validity': validity,
                    'Days': days,
                    'Departure': dep_time,
                    'Arrival': arr_time,
                    'Flight': flight_num,
                    'Aircraft': aircraft,
                    'TravelTime': travel_time,
                    'Page': page_num,
                    'RawLine': line_clean
                }
    
    except Exception as e:
        print(f" Ошибка парсинга строки: {line} - {e}")
    
    return None

# Пример запуска
if __name__ == "__main__":
    pdf_path = "./data/raw/Skyteam_Timetable.pdf"
    df = simple_flight_parser(pdf_path, start_page=5, end_page=27514)
    print(df.head())
    df.to_csv("./data/processed/trial_flights.csv", index=False, sep=';', encoding='utf-8-sig')
