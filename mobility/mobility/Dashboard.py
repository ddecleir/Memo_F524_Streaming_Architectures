import threading
from flask import Flask, render_template, request
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from RealTimeAnalysis import RealTimeAnalysis
import ModelML

cluster = Cluster(["cassandra"])
session = cluster.connect()
session.set_keyspace('brusselmobility')

app = Flask(__name__)

def get_available_ids_and_target_cols():
    ids = ['SUL62_BGin', 'HAL_292', 'PNA_203', 'TRO_TD2', 'VP_103', 'SB020_BDout', 'BET_TD3', 'SB0236_BHout',
           'LEO_207_5', 'LEO_138', 'SUL62_BHout', 'DEL_103', 'LOU_TD1', 'LEO_124', 'SUL62_BGout', 'MON_TD2',
           'LEO_112', 'LEO_230', 'LEO_135', 'RMO_TD1', 'BAI_TD2', 'LEO_110', 'SB0246_BAout', 'ARL_203', 'PNA_103',
           'SB121_BBin', 'TER_TD2', 'SUL62_BHin', 'SB020_BBin', 'TEST_ARLON', 'LEO_128', 'LEO_129', 'BOT_TD2', 'SB020_BCin',
           'SWB22_VL_XB2', 'MON_TD1', 'SB0246_BXout', 'LEO_207_12', 'ROG_TD2', 'LEO_117', 'BEL_TD5', 'TRO_TD1', 'LOI_103', 'MAD_203',
           'BET_TD2_12', 'SB125_BBout', 'NATO_204', 'SWB22_VL_XA2', 'TRO_203', 'NATO_104', 'LEO_219', 'BET_TD2_6', 'BEL_TD4', 'LOU_110',
           'RCE_TD1', 'LEO_204', 'STE_TD1', 'SGN02_BAout', 'TER_TD1', 'ARL_103', 'RME_TD2', 'SUL62_BDin', 'LEO_210', 'RMO_TD2', 'RME_TD1',
           'SUL62_BA1out', 'SB020_BAout', 'LEO_235', 'LEO_131', 'BAI_TD1', 'STE_TD2', 'SGN02_BBout', 'LOU_TD2', 'SB0236_BCout', 'VLE_203',
           'MAD_103', 'BE_TD1', 'SB1201_BAout', 'LEO_116', 'HAL_191', 'CIN_TD2', 'VP_203', 'STE_TD3', 'SUL62_BDout', 'CIN_TD1', 'LOI_109',
           'LEO_211', 'VLE_103', 'ROG_TD1']

    target_cols = ['T1_M1_COUNT', 'T1_M1_SPEED', 'T1_M1_OCCUPANCY', 'T1_M1_START_TIME',
                    'T1_M1_END_TIME', 'T1_5M_COUNT', 'T1_5M_SPEED', 'T1_5M_OCCUPANCY', 'T1_5M_START_TIME',
                    'T1_5M_END_TIME', 'T1_15M_COUNT', 'T1_15M_SPEED', 'T1_15M_OCCUPANCY', 'T1_15M_START_TIME',
                    'T1_15M_END_TIME', 'T1_60M_COUNT', 'T1_60M_SPEED', 'T1_60M_OCCUPANCY', 'T1_60M_START_TIME',
                    'T1_60M_END_TIME']  # Add other target columns here
    return ids, target_cols

def run_model(id_target, id_col):
    ModelML.create_prediction_model("data_input", id_col, id_target)
    rta = RealTimeAnalysis()
    rt_thread = threading.Thread(target=rta.run_real_time_analysis)
    rt_thread.start()



def get_list_elements():
    query_transactions = """
        SELECT transaction_id, prediction, prediction_time
        FROM mobility_prediction
        LIMIT 100;
    """
    results_transactions = session.execute(query_transactions)

    query_data_input = """
        SELECT id
        FROM data_input;
    """
    results_data_input = session.execute(query_data_input)

    unique_ids = set(row.id for row in results_data_input)

    return list(results_transactions), list(unique_ids)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        id_target = request.form.get('id_target')
        id_col = request.form.get('id_col')
        print(id_target, id_col)
        run_model(id_target, id_col)

    ids, target_cols = get_available_ids_and_target_cols()
    transactions, data_input = get_list_elements()
    return render_template('index.html', transactions=transactions, data_input=data_input, ids=ids, target_cols=target_cols)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, threaded=True)
