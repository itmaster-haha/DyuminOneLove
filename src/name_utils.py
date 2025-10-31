import re
import pandas as pd
import logging
from pathlib import Path

#    Транслитерация и сравнение имён

RUS_TO_LAT = {
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Е": "E", "Ё": "E",
    "Ж": "Zh", "З": "Z", "И": "I", "Й": "Y", "К": "K", "Л": "L", "М": "M",
    "Н": "N", "О": "O", "П": "P", "Р": "R", "С": "S", "Т": "T", "У": "U",
    "Ф": "F", "Х": "Kh", "Ц": "Ts", "Ч": "Ch", "Ш": "Sh", "Щ": "Shch",
    "Ы": "Y", "Э": "E", "Ю": "Yu", "Я": "Ya", "Ь": "", "Ъ": "",
}

def rus_to_lat(text: str) -> str:
    """Транслитерация кириллицы в латиницу."""
    result = []
    for ch in str(text):
        up = ch.upper()
        if up in RUS_TO_LAT:
            tr = RUS_TO_LAT[up]
            if ch.islower():
                tr = tr.lower()
            result.append(tr)
        else:
            result.append(ch)
    return "".join(result)

def normalize_name(name: str) -> str:
    """Приведение имени к унифицированной форме (латиница, без пробелов и точек)."""
    if not isinstance(name, str):
        return ""
    name = rus_to_lat(name)
    name = re.sub(r"[^a-zA-Z]", "", name)
    return name.lower()

def names_are_equivalent(a: str, b: str) -> bool:
    """Проверяет эквивалентность имён (учитывает транслитерацию и инициалы)."""
    a_norm, b_norm = normalize_name(a), normalize_name(b)
    if not a_norm or not b_norm:
        return False
    # совпадение по началу или по полному совпадению
    return a_norm.startswith(b_norm) or b_norm.startswith(a_norm)

def name_completeness(name: str) -> int:
    """Оценка полноты имени (больше букв — выше балл, инициалы меньше)."""
    name = str(name or "")
    score = len(re.findall(r"[A-Za-zА-Яа-я]", name))
    score -= name.count(".")
    return max(score, 0)



#    Объединение пассажиров по TravelDoc


def merge_duplicate_passengers(df: pd.DataFrame, suspicious_log_path: Path = None) -> pd.DataFrame:
    """
    Объединяет пассажиров с одинаковым TravelDoc:
      - эквивалентные имена (включая рус↔лат, инициалы) унифицирует;
      - разные имена и рейсы логируются;
      - лог теперь содержит все рейсы, даты и города по каждому документу.
    """
    if "TravelDoc" not in df.columns:
        logging.warning(" merge_duplicate_passengers: нет TravelDoc, объединение пропущено.")
        return df

    df = df.copy()
    merged_rows = []
    suspicious_records = []
    merged_docs = 0
    replaced_equivalents = 0

    for doc, group in df.groupby("TravelDoc", dropna=False):
        doc_str = str(doc).strip()

        # Пропускаем пустые документы
        if not doc_str or len(group) == 1:
            merged_rows.extend(group.to_dict("records"))
            continue

        merged_docs += 1

        group = group.assign(
            completeness=group["FirstName"].apply(name_completeness)
            + group["LastName"].apply(name_completeness)
            + group["SecondName"].apply(name_completeness)
        )

        # Находим наиболее полную запись
        best_row = group.loc[group["completeness"].idxmax()]

        # Проверяем различия
        unique_firsts = set(group["FirstName"].astype(str).str.strip().unique())
        unique_lasts = set(group["LastName"].astype(str).str.strip().unique())

        def diff_names(names, ref):
            return [n for n in names if not names_are_equivalent(n, ref)]

        distinct_firsts = diff_names(unique_firsts, best_row["FirstName"])
        distinct_lasts = diff_names(unique_lasts, best_row["LastName"])

        #  Собираем все рейсы по этому документу
        flights = ", ".join(sorted(set(group["FlightNumber"].astype(str).str.strip().unique())))
        dates = ", ".join(sorted(set(group["DepartDate"].astype(str).str.strip().unique())))
        cities = ", ".join(sorted(set(group["DepartCity"].astype(str).str.strip().unique())))
        airlines = ", ".join(sorted(set(group["Airline"].astype(str).str.strip().unique())))

        # Если есть различия — фиксируем в лог
        if distinct_firsts or distinct_lasts:
            suspicious_records.append({
                "TravelDoc": doc_str,
                "FirstNames": ", ".join(sorted(unique_firsts)),
                "LastNames": ", ".join(sorted(unique_lasts)),
                "Count": len(group),
                "Flights": flights,
                "Dates": dates,
                "Cities": cities,
                "Airlines": airlines,
            })

        # Унификация (инициалы и кириллица → как у best_row)
        for i, r in group.iterrows():
            if (
                names_are_equivalent(r["FirstName"], best_row["FirstName"])
                and names_are_equivalent(r["LastName"], best_row["LastName"])
            ):
                group.at[i, "FirstName"] = best_row["FirstName"]
                group.at[i, "LastName"] = best_row["LastName"]
                group.at[i, "SecondName"] = best_row["SecondName"]
                replaced_equivalents += 1

        merged_rows.extend(group.to_dict("records"))

    result = pd.DataFrame(merged_rows).drop(columns=["completeness"], errors="ignore")

    # Запись подозрительных документов
    if suspicious_log_path and suspicious_records:
        susp_df = pd.DataFrame(suspicious_records)
        susp_df.to_csv(suspicious_log_path, sep=";", index=False, encoding="utf-8")
        logging.warning(
            f" Обнаружено {len(suspicious_records)} документов с разными ФИО "
            f"(сохранено в {suspicious_log_path})"
        )
        #  Показать первые 3 примера прямо в консоли
        logging.info(f" Примеры расхождений по документам:\n{susp_df.head(3).to_string(index=False)}")

    logging.info(
        f" Объединено документов: {merged_docs}, "
        f"заменено эквивалентных имён: {replaced_equivalents}, "
        f"итоговых строк: {len(result)}"
    )

    return result


#    Разделение полного ФИО с транслитерацией


def split_full_name(df_or_str, col: str = None, lang: str = "auto") -> pd.DataFrame:
    """
    Универсальный парсер ФИО.
    • Определяет язык (рус/лат)
    • Делает транслитерацию русских имён в латиницу
    • Разбивает строку на FirstName / SecondName / LastName
    """
    if isinstance(df_or_str, str):
        df = pd.DataFrame({"FullName": [df_or_str]})
        col = "FullName"
    else:
        df = df_or_str.copy()

    result = {"FirstName": [], "SecondName": [], "LastName": []}

    for val in df[col].fillna(""):
        val = str(val).strip()
        parts = re.split(r"[\s,.;]+", val)
        parts = [p for p in parts if p]

        if not parts:
            result["FirstName"].append("")
            result["SecondName"].append("")
            result["LastName"].append("")
            continue

        # Детект языка
        has_cyrillic = any("А" <= ch <= "я" for ch in val)
        if has_cyrillic:
            val = rus_to_lat(val)
            parts = re.split(r"[\s,.;]+", val)
            parts = [p for p in parts if p]

        # Определяем порядок (Last First Second)
        if len(parts) == 1:
            last, first, second = parts[0], "", ""
        elif len(parts) == 2:
            if re.search(r"(ov|ev|in|sky|ski|ova|eva|vna|ich)$", parts[0].lower()):
                last, first = parts
            else:
                first, last = parts
            second = ""
        else:
            last, first, second = parts[:3]

        result["FirstName"].append(first)
        result["SecondName"].append(second)
        result["LastName"].append(last)

    return pd.DataFrame(result)
