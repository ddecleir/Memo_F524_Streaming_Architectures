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
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import time

def evaluate_model(model, X_test, Y_test):
    # Evaluate the model with regression metrics
    y_pred = model.predict(X_test)

    mean_absolute_error_value = mean_absolute_error(Y_test, y_pred)
    mean_squared_error_value = mean_squared_error(Y_test, y_pred)
    root_mean_squared_error_value = np.sqrt(mean_squared_error(Y_test, y_pred))

    return mean_absolute_error_value, mean_squared_error_value, root_mean_squared_error_value



def print_evaluation_metrics(model_name, accuracy, precision, recall, f1, auc_roc):
    #Print the evaluation metrics
    print(f"Performance metrics for {model_name}:")
    print(f"Accuracy: {accuracy}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1-score: {f1}")
    print(f"AUC-ROC: {auc_roc}\n")

def retrieve_data_from_cassandra(tableName):
    # connect to cassandra
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # retrieve the data from cassandra
    rows = session.execute("SELECT * FROM brusselmobility."+tableName)

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


def generate_a_dataframe_ready_for_the_model(tableName):
    data = retrieve_data_from_cassandra(tableName)
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
    encoder = StandardScaler()
    data.iloc[:, 1:-1] = encoder.fit_transform(data.iloc[:, 1:-1])
    # remove the first column which only contains an ID for each transaction
    #data = data.iloc[:, 1:]
    #data['ID'] = range(1, len(data) + 1)

    # Save the scaler for future use
    with open("scaler.pkl", "wb") as scaler_file:
        pickle.dump(encoder, scaler_file)

    print(data)
    return data


def create_prediction_model(data):
    # ...
    # Séparation des données en ensembles d'entraînement et de test
    target_col = 'T1_60M_END_TIME'
    id_pred = "MON_TD1"
    # Gardez uniquement les données dans 'data' avec ID == id_pred
    data = data[data['ID'] == id_pred]
    # X doit contenir toutes les données sauf target_col
    X = data.drop(columns=[target_col])
    # Supprimez également la colonne 'ID'
    X = X.drop(columns=['ID'])
    # Y doit contenir uniquement target_col
    Y = data[target_col]
    print("X: ", X)
    print("Y: ", Y)
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=0)

    # Utilisez un modèle de régression
    model_random = RandomForestRegressor()
    model_random.fit(X_train, Y_train)
    # Prédiction
    predicted = model_random.predict(X_test)
    print("Predicted: ", predicted)
    # Évaluation du modèle
    print('Accuracy of the Random Forest Regressor model: ', model_random.score(X_test, Y_test))

    # Sauvegardez le modèle pour une utilisation future
    save_model(model_random)

    return


def save_model(model):
    """ Save the model """
    with open('./model', 'wb') as f:
        pickle.dump(model, f)

def import_model():
    """ Import the model """
    with open('./model', 'rb') as f:
        model = pickle.load(f)
    return model

def make_a_prediction(model, line):
    """ Make a prediction with the model and the line  """
    return model.predict(line)

if __name__ == '__main__':
    #data, id_mapping = generate_a_dataframe_ready_for_the_model("temp_data")
    #create_prediction_model(data, id_mapping)
    data = generate_a_dataframe_ready_for_the_model("temp_data")
    create_prediction_model(data)
