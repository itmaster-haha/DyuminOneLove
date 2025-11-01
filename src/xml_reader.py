import pandas as pd
import xml.etree.ElementTree as ET
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def format_date(date_str: str) -> str:
    """Преобразовать дату в формат ДД.ММ.ГГГГ (если возможно)."""
    if not date_str or pd.isna(date_str):
        return ""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return date_str.strip()


def read_xml_file(file_path: str) -> pd.DataFrame:
    """
    Читает XML PointzAggregator-AirlinesData.xml и возвращает DataFrame.
    В колонке BonusProgramm сохраняется только значение из атрибута 'number'
    (например, 'FB 171388778').
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        records = []

        user_count = 0
        flight_count = 0

        for user in root.findall("user"):
            user_count += 1
            uid = user.get("uid", "").strip()
            name = user.find("name")
            first_name = name.get("first", "").strip() if name is not None else ""
            last_name = name.get("last", "").strip() if name is not None else ""

            for card in user.findall(".//card"):
                bonus_programm = card.get("number", "").strip()  

                for activity in card.findall(".//activity"):
                    flight_count += 1
                    records.append({
                        "TravelDoc": uid,
                        "FirstName": first_name,
                        "LastName": last_name,
                        "TicketNumber": "",                 
                        "BonusProgramm": bonus_programm,    
                        "FlightNumber": activity.findtext("Code", "").strip(),
                        "DepartDate": format_date(activity.findtext("Date", "")),
                        "DepartCode": activity.findtext("Departure", "").strip(),
                        "ArrivalCode": activity.findtext("Arrival", "").strip(),
                        "Fare": activity.findtext("Fare", "").strip()
                    })

        df = pd.DataFrame(records)

        if df.empty:
            logging.warning(f" XML файл {file_path} не содержит данных.")
        else:
            logging.info(
                f"    XML прочитан: {file_path}\n"
                f"    Пользователей: {user_count}\n"
                f"    Рейсов: {flight_count}\n"
                f"    Строк в DataFrame: {df.shape[0]}"
            )

        return df

    except Exception as e:
        logging.error(f" Ошибка при чтении XML {file_path}: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    xml_file = "./data/raw/PointzAggregator-AirlinesData.xml"
    df = read_xml_file(xml_file)
    print(df.head(10))
