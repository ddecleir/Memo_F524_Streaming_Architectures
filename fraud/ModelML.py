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
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier


def evaluate_model(model, X_test, Y_test):
    #Evaluate the model with different metrics"""
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(Y_test, y_pred)
    precision = precision_score(Y_test, y_pred)
    recall = recall_score(Y_test, y_pred)
    f1 = f1_score(Y_test, y_pred)
    auc_roc = roc_auc_score(Y_test, y_pred)

    return accuracy, precision, recall, f1, auc_roc


def print_evaluation_metrics(model_name, accuracy, precision, recall, f1, auc_roc):
    #Print the evaluation metrics
    print(f"Performance metrics for {model_name}:")
    print(f"Accuracy: {accuracy}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1-score: {f1}")
    print(f"AUC-ROC: {auc_roc}\n")

def retrieve_data_from_cassandra(tableName):
    """ Retrieve the data from cassandra """
    # connect to cassandra
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # retrieve the data from cassandra
    rows = session.execute("SELECT * FROM detectionfraud." + tableName)
    data = []
    for row in rows:
        data.append([row.transaction_id, row.tx_datetime, row.tx_amount, row.tx_time_seconds, row.tx_time_days,
                     row.x_terminal_id, row.y_terminal_id, row.x_customer_id, row.y_customer_id, row.mean_account,
                     row.std_account, row.mean_nb_tx_per_day, row.tx_fraud])
    data = np.array(data)
    return pd.DataFrame(data)


def create_prediction_model(data):
    """ Creation of the model and save it in a pickle file """
    # the element at each line at index 1 is a datetime, I need to convert it to numeric
    data[1] = pd.to_datetime(data[1])
    data[1] = data[1].map(pd.Timestamp.to_julian_date)

    # add a name to each column
    data.columns = ['TRANSACTION_ID', 'TX_DATETIME', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS', 'X_TERMINAL_ID',
                    'Y_TERMINAL_ID', 'X_CUSTOMER_ID', 'Y_CUSTOMER_ID', 'MEAN_ACCOUNT', 'STD_ACCOUNT',
                    'MEAN_NB_TX_PER_DAY', 'TX_FRAUD']

    # Convert 'TX_FRAUD' column to integer
    data['TX_FRAUD'] = data['TX_FRAUD'].astype(int)

    print(data.head())

    # I need to scale the data to have a better accuracy
    # I will use the StandardScaler function from sklearn to scale the data except the last column and the first column
    encoder = StandardScaler()
    data.iloc[:, 1:-1] = encoder.fit_transform(data.iloc[:, 1:-1])
    # remove the first column which only contains an ID for each transaction
    data = data.iloc[:, 1:]

    # Save the scaler for future use
    with open("scaler.pkl", "wb") as scaler_file:
        pickle.dump(encoder, scaler_file)

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

    # Evaluation of the Logistic Regression model
    accuracy, precision, recall, f1, auc_roc = evaluate_model(model_logistic, X_test, Y_test)
    print_evaluation_metrics("Logistic Regression", accuracy, precision, recall, f1, auc_roc)

    # Evaluation of the Random Forest Classifier model
    accuracy, precision, recall, f1, auc_roc = evaluate_model(model_random, X_test, Y_test)
    print_evaluation_metrics("Random Forest Classifier", accuracy, precision, recall, f1, auc_roc)

    save_model(model_random)


def other_models_for_comparison(X_train, X_test, Y_train, Y_test):
    # Création d'un modèle SVM
    model_svm = SVC()
    model_svm.fit(X_train, Y_train)
    # Evaluation du modèle SVM
    accuracy, precision, recall, f1, auc_roc = evaluate_model(model_svm, X_test, Y_test)
    print_evaluation_metrics("Support Vector Machine", accuracy, precision, recall, f1, auc_roc)

    # Création d'un modèle k-NN
    model_knn = KNeighborsClassifier()
    model_knn.fit(X_train, Y_train)
    # Evaluation du modèle k-NN
    accuracy, precision, recall, f1, auc_roc = evaluate_model(model_knn, X_test, Y_test)
    print_evaluation_metrics("k-Nearest Neighbors", accuracy, precision, recall, f1, auc_roc)

    # Création d'un modèle Gradient Boosting Classifier
    model_gbc = GradientBoostingClassifier()
    model_gbc.fit(X_train, Y_train)
    # Evaluation du modèle Gradient Boosting Classifier
    accuracy, precision, recall, f1, auc_roc = evaluate_model(model_gbc, X_test, Y_test)
    print_evaluation_metrics("Gradient Boosting Classifier", accuracy, precision, recall, f1, auc_roc)


def test_saved_model_on_transactions(data, n_transactions=100):
    """ Test the saved model on n_transactions from the data """
    model = import_model()

    # Select n_transactions random samples from the data
    sample_data = data.sample(n_transactions)

    # Remove TRANSACTION_ID column
    sample_data = sample_data.iloc[:, 1:]

    X_sample = sample_data.iloc[:, :-1]
    Y_sample = sample_data.iloc[:, -1]

    # Make predictions on the sample data
    y_pred = model.predict(X_sample)

    # Print the results
    print("Results of the saved model on", n_transactions, "transactions:")
    for i in range(n_transactions):
        print(f"Transaction {i + 1}: Actual = {Y_sample.iloc[i]}, Predicted = {y_pred[i]}")

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


def main(tableName):
    data_cassandra = retrieve_data_from_cassandra(tableName)
    create_prediction_model(data_cassandra)


    # Test the saved model on 100 transactions
    test_saved_model_on_transactions(data_cassandra, n_transactions=100)


if __name__ == '__main__':
    main('transactions')
