import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


#        Безопасная обработка дат и времени (пустые -> "")

def format_date(date_val):
    """Приведение значения к формату ДД.ММ.ГГГГ без предупреждений и 'nan'."""
    if pd.isna(date_val):
        return ""
    val = str(date_val).strip()
    if val == "" or val.upper() == "N/A" or val.lower() == "nan":
        return ""
    try:
        # Проверяем несколько распространённых форматов
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                dt = pd.to_datetime(val, format=fmt, errors="raise")
                return dt.strftime("%d.%m.%Y")
            except Exception:
                continue
        # Последняя попытка — автодетект pandas
        dt = pd.to_datetime(val, errors="coerce", dayfirst=True)
        return dt.strftime("%d.%m.%Y") if not pd.isna(dt) else ""
    except Exception:
        return ""

def format_time(time_val):
    """Обрезаем время до HH:MM, убираем секунды и мусор."""
    if pd.isna(time_val):
        return ""
    s = str(time_val).strip()
    if s == "" or s.lower() == "nan":
        return ""
    # Если значение похоже на время — обрезаем до HH:MM
    if ":" in s and len(s) >= 5:
        return s[:5]
    return ""


#                  Основная функция чтения


def read_sirena_excel(file_path):
    """
    Чтение Excel Sirena файла с безопасной обработкой.
    Пропущенные значения остаются пустыми строками.
    Возвращает DataFrame без сохранения в файл.
    """
    logging.info(f" Чтение Excel Sirena файла: {file_path}")
    try:
        df = pd.read_excel(file_path, dtype=str)  # сразу читаем как строки
        logging.info(f" Исходная форма Excel: {df.shape}")

        # Заменяем NaN на пустые строки
        df = df.fillna("")

        # Обрабатываем даты и время
        for col in ["PassengerBirthDate", "DepartDate", "ArrivalDate"]:
            if col in df.columns:
                df[col] = df[col].apply(format_date)
        for col in ["DepartTime", "ArrivalTime"]:
            if col in df.columns:
                df[col] = df[col].apply(format_time)

        logging.info(f" Обработанный DataFrame: {df.shape}")
        return df

    except Exception as e:
        logging.error(f" Ошибка при чтении Excel Sirena файла: {e}")
        return pd.DataFrame()



if __name__ == "__main__":
    sirena_excel_file = "./data/raw/Sirena-export-fixed.xlsx"
    df = read_sirena_excel(sirena_excel_file)
    logging.info(f" Предпросмотр данных Sirena:\n{df.head(5)}")
