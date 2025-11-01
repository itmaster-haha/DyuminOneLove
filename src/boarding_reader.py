import pandas as pd
import logging

def read_boarding_csv(file_path):
    logging.info(f" Чтение Boarding CSV: {file_path}")
    df = pd.read_csv(file_path, sep=";")
    df.rename(columns={
        "PassengerFirstName": "FirstName",
        "PassengerSecondName": "SecondName",
        "PassengerLastName": "LastName",
        "PassengerSex": "PassengerSex",
        "PassengerBirthDate": "PassengerBirthDate",
        "PassengerDocument": "TravelDoc",
        "BookingCode": "BookingCode",
        "TicketNumber": "TicketNumber",
        "Baggage": "Baggage",
        "FlightDate": "DepartDate",
        "FlightTime": "DepartTime",
        "FlightNumber": "FlightNumber",
        "CodeShare": "CodeShare",
        "Destination": "ArrivalCity"
    }, inplace=True)
    logging.info(f" Boarding CSV dataframe shape: {df.shape}")
    return df
