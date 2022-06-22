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
from datetime import datetime, timedelta

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
    # print(type(selected_stations))

    # Returning the selected stations pandas dataframe here
    return selected_stations

# Writing a function which goes which fetches the dataset from the csv files available per weather station for the given station
# Currently only fetching from FAWN station
def get_data_from_station(selected_stations, source):
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

    # We are downloading all the possible data available from FAWN below (all years adn all stations per day)
    if source == "FAWN":
        curYear = years[0]
        while curYear > selected_stations["Start Date"]:
            # extensionFlag = True
            # try:
            #     r = requests.get(URL + )
            
        # Going to Faw website (FTP) and getting the files for each year


    # Fetching the daily update for all stations year wise

    

# print(distance_to_station(coords_1, coords_2))
get_data_from_station(gainesville_coord, path_to_csv)
