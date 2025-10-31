import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Словарь префиксов авиакомпаний

AIRLINE_PREFIXES = {
    "SU": "Aeroflot",
    "AR": "Aerolneas Argentinas",
    "AM": "Aeromexico",
    "UX": "Air Europa",
    "AF": "Air France",
    "AZ": "Alitalia",
    "CI": "China Airlines",
    "MU": "China Eastern",
    "CZ": "China Southern",
    "OK": "Czech Airlines",
    "DL": "Delta Air Lines",
    "GA": "Garuda Indonesia",
    "KQ": "Kenya Airways",
    "KL": "KLM",
    "KE": "Korean Air",
    "ME": "Middle East Airlines",
    "SV": "Saudia",
    "RO": "TAROM",
    "VN": "Vietnam Airlines",
    "MF": "Xiamen Airlines",
}


# Вспомогательные функции


def parse_validity(validity_str: str):
    if not isinstance(validity_str, str):
        return "", ""
    match = re.match(r"(\d{2}\s+\w{3})\s*-\s*(\d{2}\s+\w{3})", validity_str)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def split_city_airport(text: str):
    if not isinstance(text, str):
        return "", ""
    text = text.strip()
    match = re.match(r"(.+?)\s+([A-Z]{3})$", text)
    if match:
        return match.group(1).strip().replace(" ,", ","), match.group(2)
    match = re.match(r"([A-Z]{3})\s*-\s*(.+)", text)
    if match:
        return match.group(2).strip(), match.group(1)
    match = re.match(r"(.+)\s*\(([A-Z]{3})\)", text)
    if match:
        return match.group(1).strip(), match.group(2)
    return text, ""


def get_airline_and_codeshare(flight_number: str):
    if not isinstance(flight_number, str) or len(flight_number) < 2:
        return "", ""
    prefix = flight_number[:2].upper()
    airline = AIRLINE_PREFIXES.get(prefix, "")
    codeshare = "Y" if airline != "" else ""
    return airline, codeshare


def normalize_flight_table(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", dtype=str).fillna("")
    logging.info(f" Загружено строк: {len(df)}")

    df[["DepartCity", "DepartCode"]] = df["From"].apply(lambda x: pd.Series(split_city_airport(x)))
    df[["ArrivalCity", "ArrivalCode"]] = df["To"].apply(lambda x: pd.Series(split_city_airport(x)))
    df[["ValidityStart", "ValidityEnd"]] = df["Validity"].apply(lambda x: pd.Series(parse_validity(x)))

    df["Days"] = df["Days"].str.replace(r"[^0-9]", "", regex=True)
    df["Departure"] = df["Departure"].str.strip().str.replace(" ", "")
    df["Arrival"] = df["Arrival"].str.strip().str.replace(" ", "")
    df["TravelTime"] = df["TravelTime"].str.replace(" ", "").str.upper()

    df[["Airline", "CodeShare"]] = df["Flight"].apply(lambda x: pd.Series(get_airline_and_codeshare(x)))

    normalized = pd.DataFrame({
        "FirstName": "",
        "SecondName": "",
        "LastName": "",
        "PassengerSex": "",
        "PassengerBirthDate": "",
        "TravelDoc": "",
        "BookingCode": "",
        "TicketNumber": "",
        "Baggage": "",
        "DepartDate": "",
        "DepartTime": df["Departure"],
        "FlightNumber": df["Flight"],
        "CodeShare": df["CodeShare"],
        "ArrivalCity": df["ArrivalCity"],
        "FullName": "",
        "DepartTime_r": "",
        "ArrivalDate": "",
        "ArrivalTime": df["Arrival"],
        "CodeShare_r": "",
        "DepartCode": df["DepartCode"],
        "ArrivalCode": df["ArrivalCode"],
        "Seat": "",
        "Meal": "",
        "TrvCls": "",
        "Fare": "",
        "Baggage_r": "",
        "PaxAdditionalInfo": "",
        "BonusProgramm": "",
        "AgentInfo": "",
        "DepartCity": df["DepartCity"],
        "ValidityStart": df["ValidityStart"],
        "ValidityEnd": df["ValidityEnd"],
        "Days": df["Days"],
        "TravelTime": df["TravelTime"],
        "Gate": "",
        "Airline": df["Airline"],
    })

    normalized = normalized.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return normalized


if __name__ == "__main__":
    input_path = "./data/processed/trial_flights.csv"
    df_norm = normalize_flight_table(input_path)
    print(df_norm.head(3))
