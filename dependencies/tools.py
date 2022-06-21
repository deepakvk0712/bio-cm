import pandas as pd

"""
Function Name: convert excel file to csv file
Input: path to excel file, path to store created csv file
Output: None (csv file created at the specified path)
Description: This function takes an excel file and converts it into a csv file
"""

path_to_excel = "/Users/deepak/Library/Containers/com.microsoft.Excel/Data/Desktop/biocm/Data/FAWN-stations.xlsx"
path_to_csv = "/Users/deepak/Desktop/biocm/Code/bio-cm/data/FAWN_stations.csv"

def convert_excel_to_csv(pathToExcelFile, pathToCSVFile):
    read_file = pd.read_excel(pathToExcelFile)
    read_file.to_csv(pathToCSVFile, index = None, header=True)

# Returns a pandas dataframe of csv file
def convert_csv_to_pdf(pathToCSVFile):
    return pd.csv_read(pathToCSVFile)

 



convert_excel_to_csv(path_to_excel, path_to_csv)