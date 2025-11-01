import os
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def safe_get(sheet_df, r, c):
    """Безопасное извлечение ячейки с проверкой на nan/None."""
    try:
        val = str(sheet_df.iat[r, c]).strip()
        if val.lower() in ("nan", "none", ""):
            return ""
        return val
    except Exception:
        return ""

def get_gender(value):
    """Преобразование MR/MRS → Male/Female."""
    val = str(value).strip().upper()
    if val == "MR":
        return "Male"
    elif val == "MRS":
        return "Female"
    return ""

def extract_flight_info_resilient(sheet_df):
    """
    Извлекает информацию из boarding-pass Excel листа:
    MRS LIDIYA ZHDANOVA ... SU5436 ... VVO -> REA ...
    """
    return {
        "FlightNumber": safe_get(sheet_df, 4, 0),
        "FullName": safe_get(sheet_df, 2, 1),
        "PassengerSex": get_gender(safe_get(sheet_df, 2, 0)),
        "TrvCls": safe_get(sheet_df, 2, 7),
        "DepartCity": safe_get(sheet_df, 4, 3),
        "ArrivalCity": safe_get(sheet_df, 4, 7),
        "Gate": safe_get(sheet_df, 6, 1),
        "DepartCode": safe_get(sheet_df, 6, 3),
        "ArrivalCode": safe_get(sheet_df, 6, 7),
        "DepartDate": safe_get(sheet_df, 8, 0),
        "DepartTime": safe_get(sheet_df, 8, 2),
        "Airline": safe_get(sheet_df, 8, 4),
        "BookingCode": safe_get(sheet_df, 12, 1),
        "TicketNumber": safe_get(sheet_df, 12, 4),
        "Seat": safe_get(sheet_df, 10, 7),
        "BonusProgramm": safe_get(sheet_df, 2, 5),
        "Fare": "",
    }


def process_single_excel(file_path):
    """Обработка одного Excel файла (все листы)."""
    try:
        logging.info(f" Обработка {os.path.basename(file_path)}")
        excel_data = pd.ExcelFile(file_path)
        all_records = []

        for sheet_name in excel_data.sheet_names:
            sheet_df = excel_data.parse(sheet_name, header=None)
            record = extract_flight_info_resilient(sheet_df)
            all_records.append(record)

        df = pd.DataFrame(all_records)

        #  Убираем полностью пустые строки
        df = df[(df["FlightNumber"].astype(str).str.strip() != "") &
                (df["FullName"].astype(str).str.strip() != "")]

        #  Приводим все значения к строковому типу
        for col in df.columns:
            df[col] = df[col].astype(str).fillna("")

        #  Убеждаемся, что DataFrame корректный
        if not isinstance(df, pd.DataFrame):
            logging.error(f" process_single_excel: вернул не DataFrame ({type(df)}) для {file_path}")
            return pd.DataFrame()

        logging.info(f" {os.path.basename(file_path)}: {len(df)} записей извлечено.")
        return df

    except Exception as e:
        logging.error(f" Ошибка при обработке {file_path}: {e}")
        return pd.DataFrame()


def read_excel_dir(directory: str) -> pd.DataFrame:
    """
    Читает все Excel-файлы в указанной папке, 
    параллельно обрабатывает каждый файл и объединяет в один DataFrame.
    """
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.lower().endswith(".xlsx")]
    if not files:
        logging.warning(" В папке нет Excel файлов.")
        return pd.DataFrame()

    logging.info(f" Найдено {len(files)} Excel файлов для обработки.")
    all_data = []

    with ProcessPoolExecutor(max_workers=max(os.cpu_count() - 1, 1)) as executor:
        futures = {executor.submit(process_single_excel, f): f for f in files}
        for fut in as_completed(futures):
            df = fut.result()
            if isinstance(df, pd.DataFrame) and not df.empty:
                all_data.append(df)

    if not all_data:
        logging.warning(" Нет данных после обработки Excel файлов.")
        return pd.DataFrame()

    merged_df = pd.concat(all_data, ignore_index=True)

    #  Приводим все колонки и значения к строкам (финальная защита)
    merged_df.columns = merged_df.columns.map(str)
    for c in merged_df.columns:
        merged_df[c] = merged_df[c].astype(str).fillna("")

    logging.info(f" Все Excel файлы объединены. Итоговая форма: {merged_df.shape}")
    logging.info(f" Пример данных Excel:\n{merged_df.head(3).to_string(index=False)}")
    return merged_df


