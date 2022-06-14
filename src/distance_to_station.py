"""
Function Name: distance_to_station
Input:
Output:  
Description: This function takes the latitude and longitude value of a given point nad returns the closest weather station to the location. The function checks in increasing distance bubbles. First, a check is made to see is there is any weather station within 5 kilometers to the location, then 10 kilometers, finally 20 kilometers.
"""

# Packages that need to be imported
import geopy.distance

# coords_1 = (52.2296756, 21.0122287)
# coords_2 = (52.406374, 16.9251681)

def distance_to_station(inputStation, checkStation):
    # The two inputs are tuples that contain the station ID, station name, latitude and longitude
    return geopy.distance.geodesic(inputStation, checkStation).km

def get_closest_station(inputStation, allStations):
    # The function takes two inputs. First tuple contains location longitude and latitude and the other is a list of stations - contains station ID, station name, latitude and longitude of the station.



# print(distance_to_station(coords_1, coords_2))

