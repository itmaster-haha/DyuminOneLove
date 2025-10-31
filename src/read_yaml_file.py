import yaml
import pandas as pd
import logging


def read_yaml_file(file_path: str) -> pd.DataFrame:
    """
    Парсит YAML файл в формате авиарейсов Ямала.
    Структура: <дата> → <рейс> → {FROM, TO, STATUS, FF → <код: {CLASS, FARE}>}
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as e:
        logging.error(f" Ошибка чтения YAML {file_path}: {e}")
        return pd.DataFrame()

    rows = []

    # уровень 1: даты
    for flight_date, flights in data.items():
        # уровень 2: номера рейсов
        for flight_number, flight_info in flights.items():
            from_code = flight_info.get("FROM", "")
            to_code = flight_info.get("TO", "")
            status = flight_info.get("STATUS", "")
            ff_block = flight_info.get("FF", {})

            # уровень 3: пассажиры в FF
            for pax_id, pax_data in ff_block.items():
                cls = pax_data.get("CLASS", "")
                fare = pax_data.get("FARE", "")

                rows.append({
                    "DepartDate": flight_date,
                    "FlightNumber": flight_number,
                    "TravelDoc": pax_id,
                    "Class": cls,
                    "Fare": fare,
                    "Status": status,
                    "DepartCode": from_code,
                    "ArrivalCode": to_code
                })

    df = pd.DataFrame(rows)
    logging.info(f" Прочитано рейсов из YAML: {len(df)} записей")
    return df
