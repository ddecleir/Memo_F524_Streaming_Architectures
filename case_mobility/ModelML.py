import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestClassifier
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import pickle
import matplotlib.pyplot as pl
from pandas.plotting import scatter_matrix
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import time

def retrieve_data_from_cassandra():
    # connect to cassandra
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # retrieve the data from cassandra
    rows = session.execute("SELECT * FROM brusselmobility.temp_data")

    data = []
    for row in rows:
        data.append(
            [row.id, row.requestdate, row.t1_m1_count, row.t1_m1_speed, row.t1_m1_occupancy, row.t1_m1_start_time,
             row.t1_m1_end_time, row.t1_5m_count, row.t1_5m_speed, row.t1_5m_occupancy, row.t1_5m_start_time,
             row.t1_5m_end_time, row.t1_15m_count, row.t1_15m_speed, row.t1_15m_occupancy, row.t1_15m_start_time,
             row.t1_15m_end_time, row.t1_60m_count, row.t1_60m_speed, row.t1_60m_occupancy, row.t1_60m_start_time,
             row.t1_60m_end_time])
    data = np.array(data)
    return pd.DataFrame(data)


def date_treatement(data):
    data["REQUEST_DATE"] = pd.to_datetime(data["REQUEST_DATE"])
    data["REQUEST_DATE"] = data["REQUEST_DATE"].map(pd.Timestamp.to_julian_date)
    data["T1_M1_START_TIME"] = pd.to_datetime(data["T1_M1_START_TIME"])
    data["T1_M1_START_TIME"] = data["T1_M1_START_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_M1_END_TIME"] = pd.to_datetime(data["T1_M1_END_TIME"])
    data["T1_M1_END_TIME"] = data["T1_M1_END_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_5M_START_TIME"] = pd.to_datetime(data["T1_5M_START_TIME"])
    data["T1_5M_START_TIME"] = data["T1_5M_START_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_5M_END_TIME"] = pd.to_datetime(data["T1_5M_END_TIME"])
    data["T1_5M_END_TIME"] = data["T1_5M_END_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_15M_START_TIME"] = pd.to_datetime(data["T1_15M_START_TIME"])
    data["T1_15M_START_TIME"] = data["T1_15M_START_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_15M_END_TIME"] = pd.to_datetime(data["T1_15M_END_TIME"])
    data["T1_15M_END_TIME"] = data["T1_15M_END_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_60M_START_TIME"] = pd.to_datetime(data["T1_60M_START_TIME"])
    data["T1_60M_START_TIME"] = data["T1_60M_START_TIME"].map(pd.Timestamp.to_julian_date)
    data["T1_60M_END_TIME"] = pd.to_datetime(data["T1_60M_END_TIME"])
    data["T1_60M_END_TIME"] = data["T1_60M_END_TIME"].map(pd.Timestamp.to_julian_date)
    return data


def missing_data_treatment(data):
    data = data.replace('-', np.nan)
    data = data.dropna()
    return data


def generate_a_dataframe_ready_for_the_model():
    data = retrieve_data_from_cassandra()
    # rename the columns
    data.columns = ['ID', 'REQUEST_DATE', 'T1_M1_COUNT', 'T1_M1_SPEED', 'T1_M1_OCCUPANCY', 'T1_M1_START_TIME',
                    'T1_M1_END_TIME', 'T1_5M_COUNT', 'T1_5M_SPEED', 'T1_5M_OCCUPANCY', 'T1_5M_START_TIME',
                    'T1_5M_END_TIME', 'T1_15M_COUNT', 'T1_15M_SPEED', 'T1_15M_OCCUPANCY', 'T1_15M_START_TIME',
                    'T1_15M_END_TIME', 'T1_60M_COUNT', 'T1_60M_SPEED', 'T1_60M_OCCUPANCY', 'T1_60M_START_TIME',
                    'T1_60M_END_TIME']

    # remplace tous les '-' par des NaN et supprimer les lignes avec des NaN
    data = missing_data_treatment(data)
    # convertir les dates en nombre
    data = date_treatement(data)

    # I need to scale the data to have a better accuracy
    # I will use the StandardScaler function from sklearn to scale the data except the ID column
    scaler = StandardScaler()
    data.iloc[:, 1:] = scaler.fit_transform(data.iloc[:, 1:])

    # I need to convert the column ID to a numerical value
    # I will use the LabelEncoder function from sklearn to convert the ID column
    # Create a data structure that keeps in memory the mapping between the numerical value and the ID
    le = LabelEncoder()
    data.iloc[:, 0] = le.fit_transform(data.iloc[:, 0])
    id_mapping = dict(zip(le.classes_, le.transform(le.classes_)))
    print(data)
    print(id_mapping)
    return data, id_mapping


def create_prediction_model(data, id_mapping):
    # Séparation des données en ensembles d'entrainement et de test
    train, test = train_test_split(data, test_size=0.2, random_state=42)

    # ID pour lequel on souhaite faire la prédiction
    id_pred = 0

    # Séparation des données en ensembles d'entrainement et de test pour l'ID choisi
    X_train = train[train['ID'] == id_pred]
    X_test = test[test['ID'] == id_pred]

    # Colonne target pour l'ID spécifié
    target_col = 'T1_M1_COUNT'
    y_train = X_train[target_col]
    y_test = X_test[target_col]

    # Suppression de la colonne target
    X_train = X_train.drop(columns=[target_col])
    X_test = X_test.drop(columns=[target_col])

    # Entrainement du modèle
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Prédiction sur les données de test
    # Pour faire des prédictions sur tranche de données précises, je ne sélectionne qu'un sous ensemble des valeurs de X associée à mon ID
    y_pred = model.predict(X_test)

    # Calcul de l'erreur
    print("Random Forest Regression Model Evaluation Metrics for ID: ", id_pred)
    print('Mean Absolute Error:', mean_absolute_error(y_test, y_pred))
    print('Mean Squared Error:', mean_squared_error(y_test, y_pred))
    print('Root Mean Squared Error:', np.sqrt(mean_squared_error(y_test, y_pred)))

    return


if __name__ == '__main__':
    data, id_mapping = generate_a_dataframe_ready_for_the_model()
    create_prediction_model(data, id_mapping)
