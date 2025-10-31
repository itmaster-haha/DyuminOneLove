import json
import pandas as pd
import logging


def read_json_file(file_path: str) -> pd.DataFrame:
    """
    Парсит Forum Profiles JSON в таблицу, совместимую с merge_all_data().
    Заполняет базовые колонки: TravelDoc, TicketNumber, FlightNumber, DepartDate, ArrivalCity и т.д.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"❌ Ошибка чтения JSON {file_path}: {e}")
        return pd.DataFrame()

    rows = []

    for profile in data.get("Forum Profiles", []):
        for flight in profile.get("Registered Flights", []):
            # Собираем одну запись рейса
            rows.append({
                "TravelDoc": "",  # В JSON этого нет
                "TicketNumber": "",  # тоже нет
                "FlightNumber": flight.get("Flight", ""),
                "DepartDate": flight.get("Date", ""),
                "CodeShare": flight.get("Codeshare", ""),
                "DepartCity": flight.get("Departure", {}).get("City", ""),
                "DepartCode": flight.get("Departure", {}).get("Airport", ""),
                "ArrivalCity": flight.get("Arrival", {}).get("City", ""),
                "ArrivalCode": flight.get("Arrival", {}).get("Airport", ""),
                "DepartCountry": flight.get("Departure", {}).get("Country", ""),
                "ArrivalCountry": flight.get("Arrival", {}).get("Country", ""),
            })

    if not rows:
        logging.warning(" В JSON нет данных о рейсах.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Приводим к формату, который ждут merge-функции
    expected_cols = ["TravelDoc", "TicketNumber", "FlightNumber"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    logging.info(f" JSON файл прочитан: {len(df)} записей, колонки: {list(df.columns)}")
    return df
