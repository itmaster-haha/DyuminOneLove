import yaml
import pandas as pd
import logging
from datetime import date, datetime


def _to_datestr(v) -> str:
    """Преобразует дату (date, datetime, str) в строку формата YYYY-MM-DD."""
    if isinstance(v, (date, datetime)):
        return pd.to_datetime(v).strftime("%Y-%m-%d")
    return str(v).strip()


def read_yaml_file(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logging.error(f" Ошибка чтения YAML {file_path}: {e}")
        return pd.DataFrame()

    rows = []

    # уровень 1: даты
    for flight_date_key, flights in (data or {}).items():
        flight_date = _to_datestr(flight_date_key)
        flights = flights or {}

        # уровень 2: рейсы
        for flight_number, flight_info in flights.items():
            flight_number = str(flight_number).strip()
            flight_info = flight_info or {}

            from_code = str(flight_info.get("FROM", "")).strip()
            to_code = str(flight_info.get("TO", "")).strip()
            status = str(flight_info.get("STATUS", "")).strip()
            ff_block = flight_info.get("FF", {}) or {}

            # уровень 3: бонусные программы (FF)
            for bonus_prog, pax_data in ff_block.items():
                bonus_programm = str(bonus_prog).strip()  
                pax_data = pax_data or {}

                cls = str(pax_data.get("CLASS", "")).strip()
                fare = str(pax_data.get("FARE", "")).strip()

                rows.append({
                    "DepartDate": flight_date,
                    "FlightNumber": flight_number,
                    "TravelDoc": "",                 
                    "BonusProgramm": bonus_programm, 
                    "TrvCls": cls,
                    "Fare": fare,
                    "DepartCode": from_code,
                    "ArrivalCode": to_code,
                    "CodeShare": status,
                })

    df = pd.DataFrame(rows)

    if df.empty:
        logging.warning(f" YAML файл {file_path} не содержит записей.")
    else:
        logging.info(
            f" Прочитано из YAML: {len(df)} строк; "
            f"уникальных рейсов: {df['FlightNumber'].nunique()}; "
            f"уникальных программ: {df['BonusProgramm'].nunique()}"
        )

    return df


if __name__ == "__main__":
    yaml_file = "./data/raw/SkyTeam-Exchange.yaml"
    df = read_yaml_file(yaml_file)
    print(df.head(10))
