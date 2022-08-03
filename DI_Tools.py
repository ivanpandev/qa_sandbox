import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from influxdb import DataFrameClient
from tqdm import tqdm
import boto3
import datetime as dt

def readme():
    print('''Welcome to the Arene Data Integrity ToolBox! \n
    \n
    Generate DataIntegrity objects using the csv_to_object(), log_to_object(), or query_TRP() functions. \n
    Analyze DataIntegrity objects using the compare(), full_DI_TRP(), or properties() attributes of the DataIntegrity objects. \n
    Visualize DataIntegrity objects using the plot_values() or plot_timesteps() methods. \n
    \n
    For full documentation, vist confluence/Arene/QA/DI. \n
    Happy bug hunting!''')

def csv_to_object(csv, time_col, val_col):
    df = pd.read_csv(csv, header=None)
    df = df.copy().sort_values(by = time_col)
    df['timestep'] = df[time_col].diff()
    df[time_col] = pd.to_datetime(df[time_col], unit = 'us')
    #df[val_col] = df[val_col]/1.60934
    df = df.rename(columns={time_col: "time"})
    df = df.rename(columns={val_col: "values"})
    return df[['time', 'values', 'timestep']]

def log_to_object(log, time_col, val_col):
    df = pd.read_table(log, sep=' ', header=None)
    df[time_col] = df[time_col].map(lambda x: x.strip('()'))
    df['timestep'] = df[time_col]
    df['timestep'] = df[time_col].astype(float).diff()*1000000
    df[time_col] = pd.to_datetime(df[time_col], unit = 's')
    #df[val_col] = df.apply(lambda x: ((int(x[val_col][12:14], 16)) + (int(x[val_col][14:16], 16))*256)*0.0062, axis=1)
    df[val_col] = df.apply(lambda x: 0.01*int(str(x[val_col][14:16]+x[val_col][12:14]), 16), axis=1)
    df = df.rename(columns={time_col: "time"})
    df = df.rename(columns={val_col: "values"})
    return df[['time', 'values', 'timestep']]

def query_TRP(database, start_time, end_time, vehicleID): #100% coverage of TRP databases
    if database == 'influx':
        client = DataFrameClient(host='influxdb', port=8086, database= vehicleID)
        query = """
        SELECT time, VehicleSpeed
        FROM ESC_VEHICLE_STAT
        WHERE time > '%s' AND time < '%s' 
        ORDER by time ASC
        """%(start_time, end_time)
        df = client.query(query)['ESC_VEHICLE_STAT']
        df = df.tz_localize(None)
        df = df.reset_index()
        df = df.rename(columns={"index": "time"})
        df = df.rename(columns={"VehicleSpeed": "values"})
        #df['values'] = df['values']/1.60934
        df['timeStep'] = df['time'].diff().dt.total_seconds()*1000000
        return df
    elif database == 'postgres':
        paramsQ = {'startTime':start_time, 'endTime':end_time}
        engine = create_engine(os.getenv("DATABASE_URL"))
        df = pd.read_sql("""select time, esc_vehicle_stat_vehiclespeed from drivingdata where vehicle_id = 'WAUSPBFF7HA137841' and time >= %(startTime)s and time <= %(endTime)s order by time ASC""", params=paramsQ, con=engine)
        df = df.rename(columns={"time": "time"})
        df = df.rename(columns={"esc_vehicle_stat_vehiclespeed": "values"})
        NANnum = df['values'].isnull().sum()
        #need some dropna() function here
        df['timestep'] = df['time'].diff().dt.total_seconds()*1000000
        #df['values'] = df['values']/1.60934
        return df
    else:
        return

def query_APC_s3_by_LastMod_time(start_time, end_time, bucket, download):
    s3 = boto3.client('s3')

    if download=='Y':
        object_list = []
        for key in s3.list_objects(Bucket=bucket)['Contents']:
            if key['LastModified'] >= start_time and key['LastModified'] <= end_time:
                target_name = str(key['Key'])
                file_name = str('s3_artifact_' + str(dt.datetime.now().time()) + '.avro')
                #print(file_name)
                #print('\n')
                s3.download_file('can-001-1-03-ap-northeast-1-staging', target_name, file_name)
                object_list.append(key)
        print('Above items have been downloaded locally.')
        return(object_list)
    else:
        object_list = []
        for key in s3.list_objects(Bucket=bucket)['Contents']:
            #print(key['LastModified'])
            if key['LastModified'] >= start_time and key['LastModified'] <= end_time:
                object_list.append(key)
        return(object_list)

def avro_to_object():
    return()
    
class DataIntegrity:
    
    def __init__(self, df, name, sampled):
        #columns
        self.time = df.iloc[:,0]
        self.values = df.iloc[:,1]
        self.timestep = df.iloc[:,2]
        self.table = df
        #properties
        self.name = name
        self.sampled = sampled
        self.length = len(df)
        self.start_time = df.head(1).iloc[0,0]
        self.end_time = df.tail(1).iloc[0,0]
        self.frequency = 1000000/round(np.median(np.array(self.timestep.dropna())), 0)
        self.time_tol_ms = round(np.median(np.array(self.timestep.dropna()/2)), 0)/1000
        if self.sampled == 'N':
            self.val_tol = (np.max(self.values)-np.min(self.values))/250
        elif self.sampled == 'Y':
            self.val_tol = (np.max(self.values)-np.min(self.values))/40
        #DI results
        self.nan_total = df.iloc[:,1].isnull().sum()
        if self.sampled == 'N':
            self.irregular_timesteps = len(self.timestep[self.timestep<19000] + self.timestep[self.timestep>21000])
        if self.sampled == 'Y':
            self.irregular_timesteps = str('n/a')
        self.interruptions = len(self.timestep[self.timestep>1000000])
        print('Basic properties determined.')
        #OUTLIER SECTION________________________________________________________________________________________
        print('Finding contextual outliers.')
        AR_old=0
        winSize=20
        winStep=2
        anomalies={}

        #MAJOR LOOP__________________________________________________________________iterate windows
        for i in tqdm(range(int(len(self.table.index)/winStep))):
            if i*winStep+winSize >= len(self.table):
                break
            window = self.table.iloc[i*winStep:i*winStep+winSize]
            std = np.std(window['values'])
            avg = np.average(window['values'])

            #MINOR LOOP______________________________________________________________iterate values within a window
            for j in range(int(winSize/2-5), int(winSize/2+5)):
                if abs(window['values'].iloc[j] - avg) > std*3:
                    AR_now = window.index[j]
                    try:
                        anomalies[AR_now] += 1
                    except:
                        anomalies[AR_now] = 1
        self.outliers = len(anomalies)
        self.outliers_loc = anomalies
        print('Done. Data Integrity object is complete.')
    
    def compare(self, df):

        timeDelta =  pd.Timedelta(np.timedelta64(int(self.time_tol_ms), "ms"))
        df_frequency = df.frequency
        df_table = df.table.set_index('time').copy()
        deltaName = str(df.name+'Delta')
        matchName = str(df.name+'Match')
        self.table
        self.table[deltaName]=0
        self.table[matchName]=0
        
        for i in tqdm(range(0, len(self.table))):
            mask = df_table.truncate(before =  self.time[i] - timeDelta, after = self.time[i] + timeDelta)

            for j in range (0, len(mask)):
                self.table.loc[i, deltaName] = min(abs(mask[self.values.name] - self.values[i]))

                if(abs(mask[self.values.name].iloc[j] - self.values[i]) <= self.val_tol):
                    self.table.loc[i, matchName] = float(len(mask))
                    break

        matchPercent = 100*(len(self.table[self.table[matchName]==1])/len(self.table))/(df_frequency/self.frequency)
        maxDelta = np.max(self.table[deltaName].dropna())
        top5Delta = np.percentile(self.table[deltaName].dropna(), 95)
        avgDelta = np.average(self.table[deltaName].dropna())

        return(matchPercent, maxDelta, top5Delta, avgDelta)
    
    def full_DI_TRP(self, *args, combine): #output file format is specific to TRP.
        
        if combine[0] == 'N':
            for other in args:
                self.compare(df=other)
                print(other.name + ' finished analyzing.')
                
        elif combine[0] == 'Y':
            d = {'gt_name': self.name,
                'gt_start_time': self.start_time,
                'gt_end_time': self.end_time,
                'length': self.length,
                'gt_nans': self.nan_total,
                'gt_irregular_timesteps': self.irregular_timesteps,
                'gt_interruptions': self.interruptions,
                'gt_outliers': self.outliers}
            for other in args:
                results = self.compare(df=other)
                print(other.name + ' finished analyzing.')
                
                d[str(other.name + '_match_percent')] = results[0]
                d[str(other.name + '_max_delta')] = results[1]
                d[str(other.name + '_95th_p_delta')] = results[2]
                d[str(other.name + '_avg_delta')] = results[3]
                d[str(other.name + '_nans')] = other.nan_total
                d[str(other.name + '_irregular_timesteps')] = other.irregular_timesteps
                d[str(other.name + '_interruptions')] = other.interruptions
                d[str(other.name + '_outliers')] = other.outliers
                
            df = pd.DataFrame(data=d, index=[0])
            try:
                df2 = pd.read_csv(str(combine[1]), index_col=0)
                df_final = pd.concat([df2, df]).reset_index(drop=True)
            except:
                print('File not found. Creating new file with given name: ' + combine[1])
                df_final = df
            df_final.to_csv(str(combine[1]))
            print('Outputs combined with file: ' + combine[1])
        return
    
    def properties(self):
        print('Name: ' + str(self.name))
        print('Sampled: ' + str(self.sampled))
        print('Length: ' + str(self.length))
        print('Start Time: ' + str(self.start_time))
        print('End Time: ' + str(self.end_time))
        print('Frequency (hz): ' + str(self.frequency))
        print('Time Tolerance (ms): ' + str(self.time_tol_ms))
        print('Value Tolerance: ' + str(self.val_tol))
        print('Total NANs: ' + str(self.nan_total))
        print('Irregular Timesteps: ' + str(self.irregular_timesteps))
        print('Interruptions: ' + str(self.interruptions))
        print('Outliers: ' + str(self.outliers))
        return

def plot_values(*args):
    plt.rcParams["figure.figsize"] = (15,6)
    for other in args:
        plt.plot(other.time, other.values, '.', label=str(other.name))
    plt.ylabel("value")
    plt.legend()
    plt.show()
    return
    
def plot_timesteps(*args):
    plt.rcParams["figure.figsize"] = (15,6)
    for other in args:
        plt.plot(other.time, other.timestep, '.', label=str(other.name))
    plt.ylabel("time step (microseconds)")
    plt.ylim([0, 40000])
    plt.legend()
    plt.show()
    return

def plot_outliers(df):
    for i in df.outliers_loc:
        if df.outliers_loc[i] > 1:
            vals = np.array(df.table.loc[i-35:i+35, 'values'])
            times = np.array(df.table.loc[i-35:i+35, 'time'])
            anom = df.table.loc[i, 'values']
            anom_time = df.table.loc[i, 'time']
            plt.plot(times, vals, 'x',  label='values')
            plt.plot(anom_time, anom, 'o', label='anomolous point')
            plt.axvspan(df.table.loc[i-15, 'time'], df.table.loc[i+15, 'time'], alpha=0.2, color='g', label='anomalous range')
            plt.ylabel('values')
            plt.xlabel('time')
            plt.title(str('Row ' + str(i) + ' is anomalous (std_dev > 3.0) in at least ' + str(df.outliers_loc[i]) + ' windows.'))
            plt.legend()
            plt.show()