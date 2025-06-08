import pandas as pd
from influxdb_client import InfluxDBClient
from datetime import timedelta, datetime
import os
from dotenv import load_dotenv
load_dotenv()
import time

# from alert_ui import get_ui

DEV_MODE = True if os.environ['DEV_MODE'] == 'true' else False

if DEV_MODE:
    BUCKET = os.environ['INFLUX_BUCKET_DEV']
    TOKEN = os.environ['INFLUX_TOKEN_DEV']
else:
    BUCKET = os.environ['INFLUX_BUCKET_DEPLOY']
    TOKEN = os.environ['INFLUX_TOKEN_DEPLOY']

ORG = os.environ['INFLUX_ORG']
URL = os.environ['INFLUX_URL']

strf = lambda t : datetime.strftime(t,'%Y-%m-%dT%H:%M:%S.000Z') # time to str
strp = lambda st : datetime.strptime(st, '%Y-%m-%dT%H:%M:%S.000Z') # str to time

client = InfluxDBClient(url=URL, token=TOKEN)
query_api = client.query_api()

FREQUENCY=30 # 30seconds

def get_time_range():
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(seconds=FREQUENCY)
    start_time = start_time.isoformat() + "Z"
    end_time = end_time.isoformat() + "Z"
    return end_time, start_time


def query_cobble_time(end_time, start_time, region):
    measurement = f"{region}_anomaly_score_cobble"
    field = "predicted_cobble"

    query = f"""from(bucket: "{BUCKET}")
    |> range(start: {end_time}, stop: {start_time})
    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
    |> filter(fn: (r) => r["_field"] == "{field}")
    |> filter(fn: (r) => r["_value"] == 1)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")"""

    df =  query_api.query_data_frame(org=ORG, query=query)

    if df.empty:
        return None
    
    timestamp = df.loc[0,'_time']
    return timestamp

def query_top_signals(timestamp, region, n=4):

    measurement = f"{region}_loss_per_signal"

    # 
    query = f"""from(bucket: "{BUCKET}")
    |> range(start: {strf(timestamp)}, stop: {strf(timestamp + timedelta(seconds=1))})
    |> filter(fn: (r) => r["_measurement"] == "{measurement}")
    |> filter(fn: (r) => r["_time"] == {strf(timestamp)})
    |> group()
    |> sort(columns: ["_value"], desc: true)
    |> limit(n:{n})
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    return query_api.query_data_frame(org=ORG,query=query).drop(columns=['result','_time','table']).columns.tolist()



if __name__ == '__main__':

    regions = [
        'CVR_L1', 
        'CVR_L2', 
        'CVAH_L1', 
        'CVAH_L2', 
        'Pinch_Roll_L1', 
        'Pinch_Roll_L2', 
        'HMD', 
        'SH1', 
        'SH2', 
        # 'SH3', 
        'Stand_13-18', 
        'Furnace_Exit', 
        'Overall', 
        'Stand_1-6', 
        'Stand_7-12'
    ]

    while True:

        region_top_signals = {r:[] for r in regions}

        # end_time, start_time = get_time_range()
        end_time = "2024-02-15T02:20:00.000Z"
        start_time = "2024-02-15T02:30:00.000Z"

        for region in regions:
            timestamp = query_cobble_time(end_time, start_time, region)
            if timestamp is None:
                continue
            region_top_signals[region] = query_top_signals(timestamp, region)

        # PYGAME
        # get_ui(region_top_signals)
        for k,v in region_top_signals.items():
            print(k)
            for signal in v:
                print(f'\t{signal}')
            print()
        time.sleep(FREQUENCY)