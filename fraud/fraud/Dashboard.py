import threading
from flask import Flask, render_template
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from RealTimeAnalysis import RealTimeAnalysis

cluster = Cluster(["cassandra"])
session = cluster.connect()
session.set_keyspace('detectionfraud')

rta = RealTimeAnalysis()
rt_thread = threading.Thread(target=rta.real_time_analysis)
rt_thread.start()

app = Flask(__name__)


def get_counter():
    return RealTimeAnalysis.get_counter()


def get_list_elements():
    query_predictions = """
    SELECT transaction_id, prediction
    FROM transaction_prediction
    LIMIT 100;
    """
    statement_predictions = SimpleStatement(query_predictions)
    results_predictions = session.execute(statement_predictions)

    return results_predictions


@app.route('/')
def index():
    return render_template('index.html', counter=get_counter(), predictions=get_list_elements())


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, threaded=True)
