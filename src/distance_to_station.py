"""
Function Name: distance_to_station
Input:
Output:  
Description: This function takes the latitude and longitude value of a given point nad returns the closest weather station to the location. The function checks in increasing distance bubbles. First, a check is made to see is there is any weather station within 5 kilometers to the location, then 10 kilometers, finally 20 kilometers.
"""

# Packages that need to be imported
import geopy.distance
import pandas as pd
import requests
from datetime import datetime, timedelta, date
import os
import zipfile
from io import BytesIO
import csv, ssl
from urllib.request import urlopen
import shutil

gainesville_coord = (34.298409, -83.832855)
# coords_1 = (52.2296756, 21.0122287)
# coords_2 = (52.406374, 16.9251681)
path_to_csv = "/Users/deepak/Desktop/biocm/Code/bio-cm/data/FAWN_stations.csv"
URL = "https://fawn.ifas.ufl.edu/data/fawnpub/daily_summaries/BY_YEAR/"


def distance_to_station(inputStation, checkStation):
    # The two inputs are tuples that contain the station ID, station name, latitude and longitude
    return geopy.distance.geodesic(inputStation, checkStation).km


# inputStation, allStations  ----> arguments
def get_closest_station(inputStation, path_to_csv):
    # The function takes two inputs. First tuple contains location longitude and latitude and the other is a list of stations - contains station ID, station name, latitude and longitude of the station.
    df = pd.read_csv(path_to_csv)

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


    # print(df)
    # print(type(df.loc[:,'Latitude (deg)']))
    # print(row['Latitude (deg)'], row['Longitude (deg)'])
    # print(df["Latitude"])


    # Finding the distance of each station from the given location
    # df["distance"] = distance_to_station(gainesville_coord, (df.loc[:,'latitude'], df.loc[:,'longitude']))
    dist = []
    for i in range (len(df)):
        dist.append(distance_to_station(gainesville_coord, (df["latitude"].values[i], df["longitude"].values[i])))
    

    df["distance"] = dist


    mini = min(dist)
    mini_index = dist.index(mini)

    selected_stations = df.loc[df['distance'] == mini]
    print(selected_stations)
    # print(dist)
    print("This is very special value  "  + str(int(selected_stations['Station ID'])))

    # Returning the selected stations pandas dataframe here
    # return selected_stations
    return int(selected_stations['Station ID'])

# Writing a function which goes which fetches the dataset from the csv files available per weather station for the given station
# Currently only fetching from FAWN station
def get_data_from_station(selected_station, source):
    # The list below contains all the years for which we are trying to get the data for the current station
    y = datetime.today().year
    years = list(range(y, y - 51, -1))
    print(years)


    # dateList = pd.date_range(datetime.today(), periods=18250).to_pydatetime().tolist()
    base = datetime.today()
    dateList = [base - timedelta(days=x) for x in range(18250)]
    for i in range(len(dateList)):
        dateList[i] = dateList[i].strftime("%Y-%m-%d")
    # print(dateList)

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
            mypath = "/Users/deepak/Desktop/biocm/Code/bio-cm/Fawn_Generated_Zip"
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
        data_of_selected_station = data_df.loc[data_df['StationID'] == selected_station]

    print(data_of_selected_station)
    return data_of_selected_station



        
        
            







    # Fetching the daily update for all stations year wise

    

# print(distance_to_station(coords_1, coords_2))
get_data_from_station(get_closest_station(gainesville_coord, path_to_csv), "FAWN")