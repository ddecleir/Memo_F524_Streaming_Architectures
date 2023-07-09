import threading
from flask import Flask, render_template
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import ConsistencyLevel
from RealTimeAnalysis import RealTimeAnalysis
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()
session.set_keyspace('brusselmobility')

rta = RealTimeAnalysis()
rt_thread = threading.Thread(target=rta.real_time_analysis)
rt_thread.start()

app = Flask(__name__)


def get_counter():
    return RealTimeAnalysis.get_counter()


def get_list_elements():
    query_transactions = "SELECT transaction_id, mean_Account, mean_nb_tx_per_day, std_account, tx_amount, " \
                         "tx_datetime, tx_fraud, tx_time_days," \
                         "tx_time_seconds, x_customer_id, x_terminal_id, y_customer_id, y_terminal_id " \
                         "FROM mobility_prediction;"
    statement_transactions = SimpleStatement(query_transactions)
    results_transactions = session.execute(statement_transactions)

    return results_transactions[0:100]


@app.route('/')
def index():
    return render_template('index.html', counter=get_counter(), list_elements=get_list_elements())


if __name__ == '__main__':
    app.run(debug=True)
