#Big Data Lab - Assignment 2
#Arjun Balamurali | CE20B015

#Importing required modeules to run
    #The following lines contain all the modules used within the tasks
    
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from bs4 import BeautifulSoup
import os
import random
import urllib
import shutil
from datetime import datetime, timedelta
import apache_beam as beam
import pandas as pd
import geopandas as gpd
from geodatasets import get_path
import matplotlib.pyplot as plt
import numpy as np
import logging
from ast import literal_eval as make_tuple

#TASK 1  - Datafetch pipeline

#Defining Base url,base year and number of files for the task

base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
year = 2002     #Change the year to the required year (Task 1.1)
num_files = 2   #Change the number of files to the required number (Task 1.2)
archive_output_dir = '/tmp/archives'

data_file_output_dir = '/tmp/data/' + '{{params.year}}/' #Place to save csv data files
HTML_file_save_dir = '/tmp/html/' #Place to save html if required


#Creating a default conf dictionary for the task
conf = dict(
    base_url = base_url,
    year = Param(year, type="integer", minimum=1901,maximum=2024),  #Making sure base year is between defined values
    num_files = num_files,
    archive_output_dir = archive_output_dir,  
)



#Defining the DAG

dag_id_1 = "Fetch_NCEI_Data"
args = {
    'owner': 'admin',
    'start_date': datetime(2024,1,1),
    'retries': 1,
}


dag1 = DAG(
    dag_id= dag_id_1,
    default_args= args,
    params= conf,
    description= 'DataFetch Pipeline'
)

#Task 1.1 - Fetch the page containing the location wise data for that year
#Year defined at the start
#Defining params

fetch_page_params = dict(
    base_url = "{{ dag_run.conf.get('base_url', params.base_url) }}",
    file_save_dir = HTML_file_save_dir
)

fetch_page_task = BashOperator(
    task_id = f"Download_HTML_data",
    bash_command= "curl {{params.base_url}}{{params.year }}/ --create-dirs -o {{params.file_save_dir}}{{params.year}}.html",
    params = fetch_page_params,
    dag=dag1,
)


#Task 1.2 - Based on the required number of data files, select the data files randomly from the available list of files
#Number of files defined at the start

#Defining functions to select random data files

def search_page(page_data,base_url,year):
    """
    The function extracts the links from the HTML file for the given year
    
    Returns all links in the page
    """

    res = []
    page_url = f"{base_url}/{year}/"
    
    #Using BeatifulSoup to parse to get all hyperlinks
    soup = BeautifulSoup(page_data, 'html.parser')
    hyperlinks = soup.find_all('a')
    
    for link in hyperlinks:
        href = link.get('href')
        
        #Checking if it contains a CSV file
        if ".csv" in href:
            file_url = f'{page_url}{href}'
            res.append(file_url)
            
    return res

def select_random(num_files, base_url,year,file_save_dir,**kwargs):
    """
    To select random files from the list
    
    Returns random <given number of files> links
    """
    
    filename = f"{file_save_dir}{year}.html"
    with open(filename, "r") as f:
        pages_content = f.read()
    
    available_files = search_page(pages_content,base_url=base_url,year=year)
    selected_files_url = random.sample(available_files, int(num_files))
    return selected_files_url


#Defining params
    
select_file_params = dict(
    num_files = "{{ dag_run.conf.get('num_files', params.num_files) }}",
    year_param = "{{ dag_run.conf.get('year', params.year) }}",
    base_url = "{{ dag_run.conf.get('base_url', params.base_url) }}",
    file_save_dir = HTML_file_save_dir
)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random,
    op_kwargs=select_file_params,
    dag=dag1,
)

#Task 1.3 - Fetch the individual data files

#Defining functions to download the files
def download_files(file_url,csv_output_dir):
    os.makedirs(csv_output_dir,exist_ok=True)
    file_name = urllib.parse.unquote(os.path.basename(file_url))
    file_path = os.path.join(csv_output_dir, file_name)
    os.system(f"curl {file_url} -o {file_path}") #Download URL to Path
    return file_name

def fetch_files(csv_output_dir,**kwargs):
    ti=kwargs['ti']
    selected_files = ti.xcom_pull(task_ids='select_files')
    
    for file_url in selected_files:
        download_files(file_url,csv_output_dir)

#Defining Params
fetch_param = dict(csv_output_dir = data_file_output_dir)

fetch_files_task = PythonOperator(
    task_id = 'fetch_files',
    python_callable=fetch_files,
    op_kwargs=fetch_param,
    dag=dag1,
)


#Task 1.4 - Zip into an archive

def zip_files(output_dir, archive_path, **kwargs):
    shutil.make_archive(archive_path, 'zip', output_dir)

archive_path = data_file_output_dir[:-1] if data_file_output_dir[-1]=='/' else data_file_output_dir

#Defining params
zip_params = dict(output_dir = data_file_output_dir,
                  archive_path = archive_path)

zip_files_task = PythonOperator(
    task_id = 'zip_files',
    python_callable=zip_files,
    op_kwargs=zip_params,
    dag=dag1,
)


#Task 1.5 - Place the archive at a required location

def move_archive(archive_path, target_location, **kwargs):
    os.makedirs(target_location,exist_ok=True)
    shutil.move(archive_path+'.zip',os.path.join(target_location , str(kwargs['dag_run'].conf.get('year'))) + '.zip')
    
#Defining Params
move_params = dict(target_location = "{{ dag_run.conf.get('archive_output_dir', params.archive_output_dir) }}",
                        archive_path = archive_path)

move_archive_task = PythonOperator(
    task_id='move_archive',
    python_callable=move_archive,
    op_kwargs=move_params,
    dag=dag1,
)


#Defining Task Dependencies for DAG dag1

fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> move_archive_task


# -------------------- End of Task 1 -----------------------------

#TASK 2 - Analytics Pipeline

archive_path = "/tmp/archives/2002.zip"
required_fields = "WindSpeed, BulbTemperature"

#Creating a default conf and default args dictionary for the task 
conf = dict(
    archive_path = archive_path,
    required_fields = required_fields
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Initiate the DAG

dag2 = DAG(
    dag_id= 'Analytics_Pipeline',
    default_args= default_args,
    description= 'Analytics pipeline',
    params = conf,
    schedule_interval='*/1 * * * *',    #Given auto retrigger happens every minute
    catchup = False
)

#Task 2.1 - Wait for the archive to be available
#poke every 5 seconds

wait_task = FileSensor(
    task_id = 'wait_for_archive',
    mode="poke",
    poke_interval = 5,  # Check every 5 seconds
    timeout = 5,  # Timeout after 5 seconds
    filepath = "{{params.archive_path}}",
    dag=dag2,
    fs_conn_id = "my_file_system", # File path system must be defined
)

#Task 2.2 - Unzip Archive

unzip_task = BashOperator(
    task_id='unzip_archive',
    bash_command="unzip -o {{params.archive_path}} -d /tmp/data2",
    dag=dag2,
)

#Task 2.3 - Extract CSV Contents into a dataframe and filter based on required fields
#Fields defined at start of task 2

def parseCSV(data):
    df = data.split('","')
    df[0] = df[0].strip('"')
    df[-1] = df[-1].strip('"')
    return list(df)

class ExtractAndFilterFields(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)
        self.headers = {i:ind for ind,i in enumerate(headers_csv)} # defining headers

    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            yield ((lat, lon), data)
            
    #The process will yield the latitude longitude and windspeed data


#Process CSV using Apache Beam

def process_csv(required_fields,**kwargs):
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/tmp/results/result.txt')

required_f = dict(
    required_fields = "{{ params.required_fields }}",
)


process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv,
    op_kwargs = required_f,
    dag=dag2,
)

#Task 2.4 - Setup another PythonOperator over ApacheBeam to compute monthly averages

class ExtractFieldsWithMonth(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers_csv):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)

        self.headers = {i:ind for ind,i in enumerate(headers_csv)} # defining headers

    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            Measuretime = datetime.strptime(element[headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            Month_format = "%Y-%m"
            Month = Measuretime.strftime(Month_format)
            yield ((Month, lat, lon), data)
            #The process will yield the month latitude longitude and windspeed data

#Function to compute averages

def compute_avg(data):
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    res = np.ma.average(masked_data, axis=0)
    res = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),res)

def compute_monthly_avg(required_fields, **kwargs):
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    os.makedirs('/tmp/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/data2/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractFieldsWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_avg(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/tmp/results/averages.txt')
        

compute_monthly_avg_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_avg,
    op_kwargs = required_f,
    dag=dag2,
)

#Task 2.5 - Using 'geopandas' and 'geodatasets' create a visualisation

class Aggregated(beam.CombineFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(i.replace('Hourly',''))

    def create_accumulator(self):
        return []
    
    def add_input(self, accumulator, element):
        accumulator2 = {key:value for key,value in accumulator}
        data = element[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') # converting to float removing empty string and replace with nan
        val_data = np.reshape(val_data,val_data_shape)
        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
        res = np.ma.average(masked_data, axis=0)
        res = list(res.filled(np.nan))
        for ind,i in enumerate(self.required_fields):
            accumulator2[i] = accumulator2.get(i,[]) + [(element[0],element[1],res[ind])]

        return list(accumulator2.items())
    
    def merge_accumulators(self, accumulators):
        merged = {}
        for a in accumulators:
                a2 = {key:value for key,value in a}
                for i in self.required_fields:
                    merged[i] = merged.get(i,[]) + a2.get(i,[])

        return list(merged.items())
    
    def extract_output(self, accumulator):
        return accumulator
    
#defining function to plot geomaps

def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1],dtype='float')
    d1 = np.array(data,dtype='float')
    
    world = gpd.read_file(get_path('naturalearth.land'))

    data = gpd.GeoDataFrame({
        values[0]:d1[:,2]
    }, geometry=gpd.points_from_xy(*d1[:,(1,0)].T))
    
    
    # Plotting
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    world.plot(ax=ax, color='white', edgecolor='black')
    
    
    # Plot heatmap
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/tmp/results/plots', exist_ok=True)
    
    plt.savefig(f'/tmp/results/plots/{values[0]}_heatmap_plot.png')

#Creating heatmaps with geopandas
def create_heatmap_visualization(required_fields,**kwargs):
    
    required_fields = list(map(lambda a:a.strip(),required_fields.split(",")))
    with beam.Pipeline(runner='DirectRunner') as p:
        
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/tmp/results/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(required_fields = required_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(plot_geomaps)            
        )
        
create_heatmap_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    op_kwargs = required_f,
    dag=dag2,
)

#Skipping Option Task Task 2.6
    #I did not get enough time to try out the task. I will surely be trying it out after submission

#Task 2.7 - Upon completion, delete the CSV Files from the destination

def delete_csv(**kwargs):
    shutil.rmtree('/tmp/data2')

delete_csv_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv,
    dag=dag2,
)

#Creating task dependancies

wait_task >> unzip_task >> process_csv_files_task
process_csv_files_task >> delete_csv_task
unzip_task >> compute_monthly_avg_task
compute_monthly_avg_task >> create_heatmap_task
create_heatmap_task >> delete_csv_task


# -------------------- End of Task 2 -----------------------------

# -------------------- End of Assignment 2 -----------------------------
