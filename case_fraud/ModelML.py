import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
import pickle

def retrieve_data_from_cassandra():
    # connect to cassandra
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # retrieve the data from cassandra
    rows = session.execute("SELECT * FROM detectionfraud.transactions")
    data = []
    for row in rows:
        data.append([row.transaction_id, row.tx_datetime, row.tx_amount, row.tx_time_seconds, row.tx_time_days, row.x_terminal_id, row.y_terminal_id, row.x_customer_id, row.y_customer_id, row.mean_account, row.std_account, row.mean_nb_tx_per_day, row.tx_fraud])
    data = np.array(data)
    return pd.DataFrame(data)

def read_data(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    # create an array that contains the data
    data = []
    for line in lines:
        temp = line[3:]
        line = temp[:-5]
        data.append(line)
    data = [x.strip().split(', ') for x in data]
    data = np.array(data)
    return pd.DataFrame(data)

def create_prediction_model(data):
    # the element at each line at index 1 is a datetime, I need to convert it to numeric
    data[1] = pd.to_datetime(data[1])
    data[1] = data[1].map(pd.Timestamp.to_julian_date)

    # add a name to each column
    data.columns = ['TRANSACTION_ID', 'TX_DATETIME', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS', 'X_TERMINAL_ID', 'Y_TERMINAL_ID', 'X_CUSTOMER_ID', 'Y_CUSTOMER_ID', 'MEAN_ACCOUNT', 'STD_ACCOUNT', 'MEAN_NB_TX_PER_DAY', 'TX_FRAUD']

    print(data)

    # I need to scale the data to have a better accuracy
    # I will use the StandardScaler function from sklearn to scale the data except the last column and the first column
    encoder = StandardScaler()
    data.iloc[:, 1:-1] = encoder.fit_transform(data.iloc[:, 1:-1])
    # remove the first column which only contains an ID for each transaction
    data = data.iloc[:, 1:]

    # Creation of a test set (20% of the data) and a training set
    X = data.iloc[:, :-1]
    Y = data.iloc[:, -1]
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=0)

    # Creation of the model
    model_logistic = LogisticRegression()
    model_logistic.fit(X_train, Y_train)
    # Prediction
    expected = Y_test
    predicted = model_logistic.predict(X_test)
    # Evaluation of the model
    print('Accuracy of the Logistic Regression model: ', model_logistic.score(X_test, Y_test))
    # Print the confusion matrix
    cm = confusion_matrix(expected, predicted)
    print(cm)

    # Try another model
    model_random = RandomForestClassifier()
    model_random.fit(X_train, Y_train)
    # Prediction
    expected = Y_test
    predicted = model_random.predict(X_test)
    # Evaluation of the model
    print('Accuracy of the Random Forest Classifier model: ', model_random.score(X_test, Y_test))
    # Print the confusion matrix
    cm = confusion_matrix(expected, predicted)
    print(cm)


def save_model(model):
    with open('model', 'wb') as f:
        pickle.dump(model, f)

def import_model():
    with open('model', 'rb') as f:
        model = pickle.load(f)
    return model

def make_a_prediction(model, line):
    encoder = StandardScaler()
    line = encoder.fit_transform(line)
    return model.predict(line)

if __name__ == '__main__':
    file = 'data/data1'
    data_file = read_data(file)
    data_cassandra = retrieve_data_from_cassandra()
    create_prediction_model(data_cassandra)