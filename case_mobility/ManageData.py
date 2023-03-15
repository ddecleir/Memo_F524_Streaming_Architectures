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

def gestion(session, tableName):
    start = time.time()
    alreadyIn = session.execute("SELECT * FROM brusselmobility.temp_data")
    rows1 = session.execute("SELECT * FROM brusselmobility."+tableName)
    dict = {}
    #Complete Dict
    for alr in alreadyIn:
        id = alr.id
        requestDate = alr.requestdate
        if id in dict:
            dict[id].append(requestDate)
        else:
            dict[id] = []
            dict[id].append(requestDate)
    # Check if already in temp_data
    for row in rows1:
        requestDate = row.requestdate
        id = row.id
        insert = True
        if id in dict:
            for element in dict[id]:
                if element == requestDate:
                    insert = False
        # Insert in temp_data if not already in
        if insert:
            if id in dict:
                dict[id].append(requestDate)
            else:
                dict[id] = []
                dict[id].append(requestDate)
            insert_transport_stmt = SimpleStatement("INSERT INTO brusselmobility.temp_data "
                                                    "(key_id, id, requestDate, t1_m1_count, t1_m1_speed, t1_m1_occupancy, "
                                                    "t1_m1_start_time, t1_m1_end_time, t1_5m_count, t1_5m_speed, t1_5m_occupancy, "
                                                    "t1_5m_start_time, t1_5m_end_time, t1_15m_count, "
                                                    "t1_15m_speed, t1_15m_occupancy, t1_15m_start_time, t1_15m_end_time, "
                                                    "t1_60m_count, t1_60m_speed, t1_60m_occupancy, t1_60m_start_time, "
                                                    "t1_60m_end_time) "
                                                    "VALUES (now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                                    "%s, %s, %s, %s, %s, %s)", consistency_level=ConsistencyLevel.ONE)
            session.execute(insert_transport_stmt, (row.id, row.requestdate, row.t1_m1_count, row.t1_m1_speed,
                                                    row.t1_m1_occupancy, row.t1_m1_start_time, row.t1_m1_end_time,
                                                    row.t1_5m_count, row.t1_5m_speed, row.t1_5m_occupancy,
                                                    row.t1_5m_start_time, row.t1_5m_end_time, row.t1_15m_count,
                                                    row.t1_15m_speed, row.t1_15m_occupancy, row.t1_15m_start_time,
                                                    row.t1_15m_end_time, row.t1_60m_count, row.t1_60m_speed,
                                                    row.t1_60m_occupancy, row.t1_60m_start_time, row.t1_60m_end_time))
            print("Insertion in temp_data: ", row.id, row.requestdate, " FROM ", tableName)
    end = time.time()
    print("Time to insert in temp_data: ", end - start)

def delete_duplicated(session):
    session.execute("CREATE TABLE IF NOT EXISTS brusselmobility.temp_data "
                    "(key_id timeuuid PRIMARY KEY, id text, requestDate text, t1_m1_count text, t1_m1_speed text, t1_m1_occupancy text, "
                    "t1_m1_start_time text, t1_m1_end_time text, t1_5m_count text, t1_5m_speed text, t1_5m_occupancy "
                    "text, t1_5m_start_time text, t1_5m_end_time text, t1_15m_count text, t1_15m_speed text, "
                    "t1_15m_occupancy text, t1_15m_start_time text, t1_15m_end_time text, t1_60m_count text, t1_60m_speed "
                    "text, t1_60m_occupancy text, t1_60m_start_time text, t1_60m_end_time text)")

    # Table = data_publictransport, data_transport, data_transports, transport, transports, public_transport
    gestion(session, "data_input")

def retrieve_data_from_cassandra():
    # connect to cassandra
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()

    # retrieve the data from cassandra
    delete_duplicated(session)
    print(session.execute("SELECT count(*) FROM brusselmobility.temp_data").one())

if __name__ == '__main__':
    retrieve_data_from_cassandra()