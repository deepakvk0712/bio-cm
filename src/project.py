"""
Function Name: distance_to_station
Input:
Output:  
Description: This function takes the latitude and longitude value of a given point nad returns the closest weather station to the location. The function checks in increasing distance bubbles. First, a check is made to see is there is any weather station within 5 kilometers to the location, then 10 kilometers, finally 20 kilometers.
"""

from pandas_profiling import ProfileReport
import csv
import os
import shutil
import ssl
import zipfile
from datetime import date, datetime, timedelta
from io import BytesIO
from urllib.request import urlopen
import IPython
# Packages that need to be imported
import geopy.distance
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from meteostat import Point, Daily

user_coordinates = (34.298409, -83.832855)
gainesville_myplace = Point(user_coordinates[0], user_coordinates[1], 70)


path_to_FAWN_station_csv = "/Users/deepak/Desktop/biocm/Code/bio-cm/data/FAWN_stations.csv"
mypath = "/Users/deepak/Desktop/biocm/Code/bio-cm/Fawn_Generated_Zip"
URL = "https://fawn.ifas.ufl.edu/data/fawnpub/daily_summaries/BY_YEAR/"
required_fields = ['StationID', 'date', 'avg_rfd_2m_wm2', 'sum_rain_2m_inches', 'min_temp_air_60cm_C', 'max_temp_air_60cm_C', 'min_temp_air_2m_C', 'max_temp_air_2m_C', 'min_temp_air_10m_C', 'max_temp_air_10m_C']


def distance_to_station(inputStation, checkStation):
    # The two inputs are tuples that contain the station ID, station name, latitude and longitude
    return geopy.distance.geodesic(inputStation, checkStation).km


# inputStation, allStations  ----> arguments
def get_closest_station(my_coordinates, path_to_FAWN_station_csv):
    # The function takes two inputs. First tuple contains location longitude and latitude and the other is a list of stations - contains station ID, station name, latitude and longitude of the station.
    df = pd.read_csv(path_to_FAWN_station_csv)

    # Converting the W , N symbols in the latitudes and longitude values
    lat = []
    lng = []
    for i, val in df.loc[:,'Latitude (deg)'].items():
        if(val[0] == 'S'):
            temp = val[2:]
            lat.append(-float(temp))
        elif(val[0] == 'N'):
            temp = val[2:]
            lat.append(float(temp))

    for i, val in df.loc[:,'Longitude (deg)'].items():
        if(val[0] == 'W'):
            temp = val[2:]
            lng.append(-float(temp))
        elif(val[0] == 'E'):
            temp = val[2:]
            lng.append(float(temp))

    # Adding the latitude and longitude values to the dataframe
    df["latitude"] = lat
    df["longitude"] = lng


    # Finding the distance of each station from the given location
    # df["distance"] = distance_to_station(user_coordinates, (df.loc[:,'latitude'], df.loc[:,'longitude']))
    dist = []
    for i in range (len(df)):
        dist.append(distance_to_station(my_coordinates, (df["latitude"].values[i], df["longitude"].values[i])))
    

    df["distance"] = dist


    mini = min(dist)
    mini_index = dist.index(mini)

    selected_stations = df.loc[df['distance'] == mini]

    # Returning the selected stations pandas dataframe here
    # return selected_stations
    return int(selected_stations['Station ID'])

# Writing a function which goes which fetches the dataset from the csv files available per weather station for the given station
# Currently only fetching from FAWN station
def get_data_from_station(selected_station, source):
    # The list below contains all the years for which we are trying to get the data for the current station
    y = datetime.today().year
    years = list(range(y, y - 51, -1))
    # print(years)


    # dateList = pd.date_range(datetime.today(), periods=18250).to_pydatetime().tolist()
    base = datetime.today()
    dateList = [base - timedelta(days=x) for x in range(18250)]
    for i in range(len(dateList)):
        dateList[i] = dateList[i].strftime("%Y-%m-%d")


    data = []
    data_df = pd.DataFrame()
    data_of_selected_station = pd.DataFrame()
    
    # We are downloading all the possible data available from FAWN below (all years adn all stations per day)
    if source == "FAWN":

        # Reference links that show URL structure of the various FAWN files
        # https://fawn.ifas.ufl.edu/data/fawnpub/daily_summaries/BY_YEAR/1997_daily.csv.zip
        # https://fawn.ifas.ufl.edu/data/fawnpub/daily_summaries/BY_YEAR/2022_daily.csv


        # Going to Fawn website (FTP) and getting the files for each year
        i = 1997

        if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
                ssl._create_default_https_context = ssl._create_unverified_context

        while i <= 2020:
            # Creating the path for each file
            path_to_file = URL + str(i) + '_daily.csv.zip'
            path_to_file_1 = URL + str(i) + '_daily.zip'
            path_to_file_2 = URL + str(i) + '_daily.csv'

            # Creating a new directory to store the zip files
            # mypath = "/Users/deepak/Desktop/biocm/Code/bio-cm/Fawn_Generated_Zip"
            if not os.path.isdir(mypath):
                os.makedirs(mypath)

            resp = requests.get(path_to_file)
            if i<=2012 and resp.ok:
                # Below method is extracting the zip files correctly.
                zf = zipfile.ZipFile(BytesIO(resp.content))
                zf.extractall(mypath)
                i+=1
                continue

            resp = requests.get(path_to_file_1)
            if i>2012 and i<=2020 and resp.ok:
                # Below method is extracting the zip files correctly.
                zf = zipfile.ZipFile(BytesIO(resp.content))
                zf.extractall(mypath)

                # os.remove(mypath + '/' + str(i) + '_daily.zip')
                
                i+=1
                continue

            # resp = requests.get(path_to_file_2)
            # if i>2020 and resp.ok:
            #     df_temp = pd.read_csv(path_to_file_2)
            #     df_temp.to_csv(mypath + '/' + str(i) + '.csv')
            #     # data.append(df_temp)
            #     i+=1
            #     continue

        # resetting date so we can form the entire dataset
        i = 1997

        # Currently we are only considering till year 2020 as the headers and csv file headings have changed for 2021 and 2022.
        # date.today().year
        while i <= 2020:
            if i<=2007:
                df_temp = pd.read_csv(mypath + '/' + str(i) + '_daily.csv')
                data.append(df_temp)
                i+=1
                continue
            if i>2007 and i<=2020:
                df_temp = pd.read_csv(mypath + '/' + str(i) + '_daily' + '/' + str(i) + '-1.csv')
                data.append(df_temp)
                df_temp = pd.read_csv(mypath + '/' + str(i) + '_daily' + '/' + str(i) + '-2.csv')
                data.append(df_temp)
                i+=1  
                continue

            # Currently not reading the data from 2021 and 2022 as the csv format has been changed in these years. Need to figure out what changes to make.

            # if i > 2020:
            #     df_temp = pd.read_csv(URL + str(i) + '_daily.csv')
            #     if i == 2022:
            #         df_temp['Date Time'] = df_temp['Date Time'].str[:10]
            #     data.append(df_temp)
            #     i+=1
            #     continue
            
        
        data_df = pd.concat(data, ignore_index = True)
        data_df.to_csv(mypath + '/' + 'final.csv')

        # We are extracting all data for the weather station available (From earliest possible date)
        data_of_selected_station_all = data_df.loc[data_df['StationID'] == selected_station]

        #Keeping only columns that we need for our analysis, dropping non essential columns
        data_of_selected_station = drop_non_required_fields(data_of_selected_station_all)

        #Filling missing values using data from meteostat
        data_meteo_filled_missing_data = fill_missing_values(data_of_selected_station)

    # print(data_of_selected_station)
    return data_meteo_filled_missing_data


# This function basically drops unwanted fields and converts the values into required unit. The input to the function is a dataframe.
def drop_non_required_fields(data_of_selected_station):
    # This function drops the non essential fields
    df = pd.read_csv(path_to_FAWN_station_csv)
    modified_df = data_of_selected_station[required_fields]
    return modified_df


# modified_df, selected_station, path_to_FAWN_station_csv ---> Params
def fill_missing_values(data_of_selected_station):
    FAWN_station_path_to_csv = path_to_FAWN_station_csv
    FAWN_stations_info_df = pd.read_csv(FAWN_station_path_to_csv)
    # print(FAWN_stations_info_df)


    # Below code is for getting all rows which have a NAN value or an empty value
    df = data_of_selected_station.replace(' ', np.nan)                   # to get rid of empty values
    nan_values = df[df.isna().any(axis=1)]         # to get all rows with Na

    # df = data_of_selected_station.fillna(-999) 


    # print(nan_values)

    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    for index in nan_values.index:
        # if index == 7961:
        try:
            daily_date = data_of_selected_station.iloc[index]['date'].split("-")
        except:
            continue
        try:
            indiv_stationID = data_of_selected_station.iloc[index]['StationID']
        except:
            continue

        #Getting latitude and longitude from the station ID
        # station_latitude = FAWN_stations_info_df.loc[FAWN_stations_info_df['Station ID'] == indiv_stationID]['Latitude (deg)']
        # station_longitude = FAWN_stations_info_df.loc[FAWN_stations_info_df['Station ID'] == indiv_stationID]['Longitude (deg)']

        #Getting the data for the users location (latitude, longtiude)
        cur_date = datetime(int(daily_date[0]), int(daily_date[1]), int(daily_date[2]))
        daily_data = Daily(gainesville_myplace, cur_date, cur_date)

        daily_data = daily_data.fetch()


        if (np.isnan(data_of_selected_station.iloc[index]['min_temp_air_2m_C']) or np.isnan(data_of_selected_station.iloc[index]['min_temp_air_60cm_C']) or np.isnan(data_of_selected_station.iloc[index]['min_temp_air_10m_C'])):
            if daily_data['tmin'].isnull().values.any() == False:
                data_of_selected_station.iloc[index]['min_temp_air_60cm_C'] = daily_data['tmin']
                data_of_selected_station.iloc[index]['min_temp_air_2m_C'] = daily_data['tmin']
                data_of_selected_station.iloc[index]['min_temp_air_10m_C'] = daily_data['tmin']


        if (np.isnan(data_of_selected_station.iloc[index]['max_temp_air_60cm_C']) or np.isnan(data_of_selected_station.iloc[index]['max_temp_air_2m_C']) or np.isnan(data_of_selected_station.iloc[index]['max_temp_air_10m_C'])):
            if daily_data['tmax'].isnull().values.any() == False:
                data_of_selected_station.iloc[index]['max_temp_air_60cm_C'] = daily_data['tmax']
                data_of_selected_station.iloc[index]['max_temp_air_2m_C'] = daily_data['tmax']
                data_of_selected_station.iloc[index]['max_temp_air_10m_C'] = daily_data['tmax']


        if (np.isnan(data_of_selected_station.iloc[index]['sum_rain_2m_inches'])):
            if daily_data['prcp'].isnull().values.any() == False:
                data_of_selected_station.iloc[index]['sum_rain_2m_inches'] = daily_data['prcp']*(0.0394)


        # if (np.isnan(data_of_selected_station.iloc[index]['avg_temp_air_60cm_C']) or np.isnan(data_of_selected_station.iloc[index]['avg_temp_air_2m_C']) or np.isnan(data_of_selected_station.iloc[index]['avg_temp_air_10m_C'])):
        #     if daily_data['tavg'].isnull().values.any() == False:
        #         data_of_selected_station.iloc[index]['avg_temp_air_60cm_C'] = daily_data['tavg']
        #         data_of_selected_station.iloc[index]['avg_temp_air_2m_C'] = daily_data['tavg']
        #         data_of_selected_station.iloc[index]['avg_temp_air_10m_C'] = daily_data['tavg']


        # if (np.isnan(data_of_selected_station.iloc[index]['avg_wind_speed_10m_mph']) or np.isnan(data_of_selected_station.iloc[index]['wind_speed_max_10m_mph'])):
        #     if daily_data['wspd'].isnull().values.any() == False:
        #         data_of_selected_station.iloc[index]['avg_wind_speed_10m_mph'] = daily_data['wspd'] * (0.621)
        #         data_of_selected_station.iloc[index]['wind_speed_max_10m_mph'] = daily_data['wspd'] * (0.621)
        
        # if (np.isnan(data_of_selected_station.iloc[index]['wind_direction_10m_deg'])):
        #     if daily_data['wdir'].isnull().values.any() == False:  
        #         data_of_selected_station.iloc[index]['wind_direction_10m_deg'] = daily_data['wdir']

    # print(data_of_selected_station)
    return data_of_selected_station


def main():
    stationID = get_closest_station(user_coordinates, path_to_FAWN_station_csv)
    final_df = get_data_from_station(stationID, "FAWN")
    final_df.to_csv('output.csv')
    df1 = pd.read_csv("output.csv")
    profile = ProfileReport(df1, title="Pandas Profiling Report", explorative=True)
    profile.to_notebook_iframe()
    profile.to_file("your_report.html")
    return final_df


main()
