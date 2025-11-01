import pandas as pd
import logging
from pathlib import Path
import re
from datetime import datetime
import time

# --- Парсеры ---
from boarding_reader import read_boarding_csv
from sirena_reader import read_sirena_excel
from excel_reader import read_excel_dir
from xml_reader import read_xml_file
from json_reader import read_json_file
from read_yaml_file import read_yaml_file
from normalize_flight_table import normalize_flight_table
from name_utils import split_full_name, merge_duplicate_passengers

#                  НАСТРОЙКА ЛОГИРОВАНИЯ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


#              НОРМАЛИЗАЦИЯ / ОБЩИЕ УТИЛИТЫ

SCHEMA = [
    "FirstName","SecondName","LastName","PassengerSex","PassengerBirthDate",
    "TravelDoc","BookingCode","TicketNumber","Baggage",
    "DepartDate","DepartTime","FlightNumber","CodeShare",
    "DepartCity","ArrivalCity","DepartCode","ArrivalCode",
    "Airline","Fare","Seat","TrvCls","Meal","BonusProgramm","AgentInfo"
]


def normalize(df, mapping: dict | None = None) -> pd.DataFrame:
    """Безопасное выравнивание структуры DataFrame под SCHEMA с автоисправлением 'битых' колонок."""
    if not isinstance(df, pd.DataFrame):
        logging.error(f" normalize получил {type(df)}, ожидался DataFrame. df={repr(df)[:200]}")
        return pd.DataFrame(columns=SCHEMA)

    if mapping is not None and not isinstance(mapping, dict):
        logging.error(f" mapping имеет неверный тип: {type(mapping)} → {repr(mapping)[:100]}")
        mapping = None

    # --- Переименование колонок ---
    try:
        if mapping:
            df = df.rename(columns=mapping)
    except Exception as e:
        logging.warning(f" Ошибка при переименовании колонок: {e}")

    # --- Очистка названий колонок (Excel часто добавляет пробелы или tuple) ---
    try:
        df.columns = [str(c).strip() for c in df.columns]
    except Exception as e:
        logging.warning(f" Ошибка при очистке имён колонок: {e}")

    # --- Проверка на «битые» колонки (где df[c] — строка) ---
    bad_cols = []
    for c in df.columns:
        if not hasattr(df[c], "fillna") or isinstance(df[c], str):
            bad_cols.append(c)
    if bad_cols:
        logging.warning(f" Найдены битые колонки: {bad_cols}. Преобразую их в Series.")
        for c in bad_cols:
            val = df[c]
            df[c] = pd.Series([val] * len(df))

    # --- fillna + astype ---
    try:
        df = df.fillna("").astype(str)
    except Exception as e:
        logging.error(f" Ошибка при fillna/astype(str): {e}")
        for c in df.columns:
            logging.error(f"  • {c}: {type(df[c])}")
        return pd.DataFrame(columns=SCHEMA)

    # --- Добавляем отсутствующие колонки ---
    for col in SCHEMA:
        if col not in df.columns:
            df[col] = ""

    # --- Гарантируем порядок 
    try:
        df = df[SCHEMA]
    except Exception as e:
        logging.error(f" normalize: ошибка при фильтрации колонок: {e}")
        df = pd.DataFrame({col: df[col] if col in df.columns else "" for col in SCHEMA})

    return df


def clean_key(x):
    if pd.isna(x):
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(x).strip().upper())


def normalize_date(val: str) -> str:
    """Привести дату к ISO-формату YYYY-MM-DD для стабильного объединения."""
    if not val or pd.isna(val):
        return ""
    val = str(val).strip()
    if re.fullmatch(r"\d{8}", val):  
        return f"{val[:4]}-{val[4:6]}-{val[6:]}"
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val



#                  ВОССТАНОВЛЕНИЕ ИМЁН
POSSIBLE_FULLNAME_COLS = ["FullName", "PassengerName", "PAX", "Name"]


def recover_names_from_fullname(df: pd.DataFrame) -> pd.DataFrame:
    fn_col = next((c for c in POSSIBLE_FULLNAME_COLS if c in df.columns), None)
    if not fn_col:
        return df
    try:
        need_split = (
            df.get("FirstName", pd.Series([""] * len(df))).fillna("") == ""
        ) & (df[fn_col].fillna("").str.strip() != "")
        if need_split.any():
            parsed = split_full_name(
                df.loc[need_split, [fn_col]].rename(columns={fn_col: "FullName"}), "FullName", ""
            )
            for c in ["FirstName", "SecondName", "LastName"]:
                if c not in df.columns:
                    df[c] = ""
                df.loc[need_split & (df[c].fillna("") == ""), c] = parsed[c]
    except Exception as e:
        logging.error(f" Ошибка при разбиении FullName: {e}")
    return df

def merge_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.DataFrame()
    base = sorted(set([re.sub(r'_[xy]$', '', c) for c in df.columns]))
    for col in base:
        col_x, col_y = f"{col}_x", f"{col}_y"
        if col_x in df.columns and col_y in df.columns:
            merged[col] = df[col_x].fillna("").astype(str)
            yvals = df[col_y].fillna("").astype(str)
            merged[col] = merged[col].where(merged[col] != "", yvals)
        elif col_x in df.columns:
            merged[col] = df[col_x]
        elif col_y in df.columns:
            merged[col] = df[col_y]
        elif col in df.columns:
            merged[col] = df[col]
        else:
            merged[col] = ""
    return merged




def build_passenger_block(paths: dict, output: Path):
    start = time.time()
    logging.info(" Этап 1: объединение всех пассажирских источников...")

    dfs = []
    mappings = {
        "boarding": {
            "PassengerFirstName": "FirstName",
            "PassengerSecondName": "SecondName",
            "PassengerLastName": "LastName",
            "PassengerDocument": "TravelDoc",
            "PassengerSex": "PassengerSex",
            "PassengerBirthDate": "PassengerBirthDate",
            "FlightDate": "DepartDate",
            "FlightTime": "DepartTime",
            "Destination": "ArrivalCity",
        },
        "yaml": {"Class": "TrvCls", "Status": "CodeShare"},
    }

    readers = {
        "boarding": read_boarding_csv,
        "sirena": read_sirena_excel,
        "excel": read_excel_dir,
        "xml": read_xml_file,
    }

    for name, reader in readers.items():
        try:
            df = reader(paths[name])
            logging.info(f" Источник: {name:8s} | тип данных = {type(df)}, размер = {getattr(df, 'shape', 'n/a')}")

            if not isinstance(df, pd.DataFrame):
                logging.warning(f" {name} вернул {type(df)}, ожидается DataFrame. Источник пропущен.")
                continue
            if df.empty:
                logging.warning(f" {name} пуст.")
                continue

            # ---  Автоматическое восстановление имён ---
            has_fullname = "FullName" in df.columns
            has_names = any(c in df.columns for c in ["FirstName", "LastName"])
            if has_fullname and not has_names:
                logging.info(f" {name}: обнаружен FullName без отдельных имён — выполняю разбиение...")
                df = recover_names_from_fullname(df)
                try:
                    sample = df.loc[:, ["FullName", "FirstName", "LastName"]].head(3).to_dict(orient="records")
                    logging.info(f" Пример разбиения: {sample}")
                except Exception:
                    pass

            df = normalize(df, mappings.get(name))

            if "FirstName" in df.columns:
                df = df[df["FirstName"].astype(str).str.strip() != ""]

            dfs.append(df)
            logging.info(f" {name}: {len(df)} строк после фильтрации.")
        except Exception as e:
            logging.error(f" Ошибка при обработке {name}: {e}")

    if not dfs:
        logging.error(" Нет ни одного корректного DataFrame для объединения.")
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    logging.info(f" До объединения по документам: {len(merged)} строк")

    suspicious_log = Path("./data/processed/suspicious_docs.log")
    merged = merge_duplicate_passengers(merged, suspicious_log_path=suspicious_log)

    merged.to_csv(output, sep=";", index=False, encoding="utf-8")
    logging.info(f" Пассажирский блок сохранён: {output} ({len(merged)} строк, время: {time.time()-start:.1f}s)")
    return output




def attach_flights(passengers: Path, flights: Path, json_path: Path, yaml_path: Path, output: Path):
    start = time.time()
    logging.info(" Этап 2: присоединение рейсов (включая JSON и YAML)...")

    # --- Чтение пассажиров ---
    p = pd.read_csv(passengers, sep=";", dtype=str).fillna("")
    logging.info(f" Загружено пассажиров: {len(p)}")

    # --- Основной flights.csv ---
    f = normalize_flight_table(flights)
    f["DepartDate"] = f["DepartDate"].apply(normalize_date)
    logging.info(f" Основной flights.csv: {len(f)} строк")

    # --- JSON и YAML ---
    json_df = read_json_file(json_path)
    yaml_df = read_yaml_file(yaml_path)
    json_df = normalize(json_df)
    yaml_df = normalize(yaml_df)

    for d in (json_df, yaml_df):
        if "DepartDate" in d.columns:
            d["DepartDate"] = d["DepartDate"].apply(normalize_date)

    f_all = pd.concat([f, json_df, yaml_df], ignore_index=True)
    logging.info(f" Объединённый блок рейсов: {len(f_all)} строк (CSV+JSON+YAML)")

    # --- Очистка ключей (TravelDoc НЕ трогаем!) ---
    for df in (p, f_all):
        for c in ["TicketNumber", "FlightNumber", "DepartDate"]:
            if c in df.columns:
                df[c] = df[c].fillna("").apply(clean_key)

    # --- Определение ключей для объединения ---
    if "TravelDoc" in p.columns and p["TravelDoc"].str.strip().ne("").any():
        key_cols = ["TravelDoc", "FlightNumber", "DepartDate"]
        logging.info(" Использую TravelDoc для точного объединения.")
    elif "TicketNumber" in p.columns and p["TicketNumber"].str.strip().ne("").any():
        key_cols = ["TicketNumber", "FlightNumber", "DepartDate"]
        logging.info(" Использую TicketNumber для объединения.")
    elif "BonusProgramm" in p.columns and p["BonusProgramm"].str.strip().ne("").any():
        key_cols = ["BonusProgramm", "FlightNumber", "DepartDate"]
        logging.info(" Использую BonusProgramm как ключ (если нет документов и билетов).")
    else:
        key_cols = ["FlightNumber", "DepartDate"]
        logging.info(" Нет документов, билетов и бонусных программ — объединение только по рейсу.")

    logging.info(f" Ключи объединения: {key_cols}")

    # --- Объединение ---
    merged = p.merge(f_all, on=key_cols, how="left")
    merged = merge_duplicate_columns(merged)

    # --- Исправляем путаницу паспорт ↔ билет ---
    def fix_passport_ticket_conflict(row):
        doc = str(row.get("TravelDoc", "")).strip()
        ticket = str(row.get("TicketNumber", "")).strip()
        if re.fullmatch(r"\d{10,}", doc) and not ticket:
            row["TicketNumber"], row["TravelDoc"] = doc, ""
        return row

    merged = merged.apply(fix_passport_ticket_conflict, axis=1)

    # --- Подсчёт совпадений ---
    matched = merged["Airline"].notna().sum()
    logging.info(f" Совпадений по рейсам: {matched}/{len(merged)} ({matched/len(merged):.1%})")

    # --- Финальная очистка и сохранение ---
    merged.to_csv(output, sep=";", index=False, encoding="utf-8")
    logging.info(f" Пассажиры + рейсы сохранены: {output} (время: {time.time()-start:.1f}s)")

    return output




#                     ЭТАП 3 — ФИНАЛ
def clean_columns(input_path: Path, output: Path):
    start = time.time()
    logging.info(" Этап 3: очистка и выравнивание колонок...")
    df = pd.read_csv(input_path, sep=";", dtype=str)
    df.columns = [c.replace("flights_", "").replace("sirena_", "").replace("boarding_", "")
                    .replace("excel_", "").replace("xml_", "").replace("json_", "")
                    .replace("yaml_", "") for c in df.columns]
    df = merge_duplicate_columns(df)
    df.to_csv(output, sep=";", index=False, encoding="utf-8")
    logging.info(f" Финальный CSV готов: {output} ({len(df)} строк, {len(df.columns)} колонок, {time.time()-start:.1f}s)")
    return output

#                     PIPELINE

if __name__ == "__main__":
    base = Path("./data/processed")
    base.mkdir(parents=True, exist_ok=True)

    paths = {
        "boarding": "./data/raw/BoardingData.csv",
        "sirena": "./data/raw/Sirena-export-fixed.xlsx",
        "excel": "./data/raw/YourBoardingPassDotAero",
        "xml": "./data/raw/PointzAggregator-AirlinesData.xml",
        "json": "./data/raw/FrequentFlyerForum-Profiles.json",
        "yaml": "./data/raw/SkyTeam-Exchange.yaml",
        "flights": "./data/processed/trial_flights.csv",
    }

    p1 = build_passenger_block(paths, base / "merged_passengers.csv")
    p2 = attach_flights(p1, paths["flights"], paths["json"], paths["yaml"], base / "merged_passengers_flights.csv")
    p3 = clean_columns(p2, base / "merged_all_sources.csv")

    # --- финальный отчёт ---
    df = pd.read_csv(p3, sep=";", dtype=str)
    logging.info(" --- ФИНАЛЬНЫЙ ОТЧЁТ ---")
    logging.info(f" Всего строк: {len(df):,}")
    missing_airline = (df["Airline"].fillna("") == "").sum()
    logging.info(f" Без данных о рейсе: {missing_airline} ({missing_airline/len(df):.1%})")
    missing_name = (df["FirstName"].fillna("") == "").sum()
    logging.info(f" Без имени: {missing_name} ({missing_name/len(df):.1%})")
    logging.info(" Полный конвейер завершён!")
