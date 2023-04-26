# Name: Minh Vu
# UT EID: mdv894
# Final Project
# ME 397

import geopandas as gpd 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import sys



# Import data
ridership = pd.read_csv('http://web.mta.info/developers/data/nyct/turnstile/turnstile_230415.txt')
ridership_ext = pd.read_csv('http://web.mta.info/developers/data/nyct/turnstile/turnstile_230422.txt')
stations = gpd.read_file('Subway Stations/geo_export_694046d0-35f2-486d-9a8b-dbec9ae6fa74.shp')
lines = gpd.read_file('Subway Lines/geo_export_19eeab5e-8949-43d7-b19d-6e2e1b7f1ffb.shp')
nyc_map = gpd.read_file('Borough Boundaries/geo_export_908a0820-2b7c-4276-b903-0d81f947c4aa.shp')

'''
    Process and analyze ridership data
'''
## Combine the original ridership to the extended dataset to get friday night (8pm - 12am) ridership data
ridership = pd.concat([ridership, ridership_ext], axis=0)
ridership.sort_values(['C/A', 'UNIT', 'SCP', 'STATION', 'LINENAME', 'DIVISION', 'DATE', 'TIME'], inplace=True)
ridership.columns = ridership.columns.str.rstrip()
ridership = ridership[~((ridership.DATE > '04/15/2023') | ((ridership.DATE == '04/15/2023') & (ridership.TIME >= '02:00:00')))]

## Standardize LINENAME using dictionary
line_name_dictionary = {}
for line_name in ridership['LINENAME'].unique():
    line_name_dictionary[line_name] = ''.join(sorted(line_name))
ridership=ridership.replace({"LINENAME": line_name_dictionary})
ridership.loc[ridership.STATION=='14TH STREET', ['STATION']]='14 ST'

## Calculate number of riders entered and exited each turnstille every 4 hours 
ridership['RIDERS_ENTERED'] = ridership['ENTRIES'].diff()
ridership['RIDERS_EXITED'] = ridership['EXITS'].diff()

## Remove initial readings of turnstiles at the beginning of the 7-day period
ridership = ridership.groupby(['UNIT', 'SCP']).apply(lambda group: group.iloc[1:])

## Process date time from string to timestamp and standardize them to 4-hour intervals
ridership['NEW_DATE'] = pd.to_datetime(ridership.DATE, format='%m/%d/%Y')
ridership["TIME"] = np.where(ridership['TIME'] < '02:00:00', '00:00:00', ridership["TIME"])
ridership["TIME"] = np.where(('02:00:00' <= ridership['TIME']) & (ridership['TIME'] < '06:00:00'), '04:00:00', ridership["TIME"])
ridership["TIME"] = np.where(('06:00:00' <= ridership['TIME']) & (ridership['TIME'] < '10:00:00'), '08:00:00', ridership["TIME"])
ridership["TIME"] = np.where(('10:00:00' <= ridership['TIME']) & (ridership['TIME'] < '14:00:00'), '12:00:00', ridership["TIME"])
ridership["TIME"] = np.where(('14:00:00' <= ridership['TIME']) & (ridership['TIME'] < '18:00:00'), '16:00:00', ridership["TIME"])
ridership["TIME"] = np.where(('18:00:00' <= ridership['TIME']) & (ridership['TIME'] < '22:00:00'), '20:00:00', ridership["TIME"])
ridership["NEW_DATE"] = np.where(ridership["TIME"] >= "22:00:00", ridership["NEW_DATE"] + pd.Timedelta(days=1), ridership["NEW_DATE"])
ridership["TIME"] = np.where(ridership["TIME"] >= "22:00:00", '00:00:00', ridership["TIME"])
ridership.loc[ridership.NEW_DATE==pd.Timestamp(2023, 4, 15), ['NEW_DATE']]=pd.Timestamp(2023, 4, 8)

ridership["DATE"] = ridership['NEW_DATE']
ridership.drop(['NEW_DATE'], axis=1, inplace=True)

## Translate the dates to day of the week
ridership['DAY'] = ridership['DATE'].map({
    pd.Timestamp(2023, 4, 8) : 'Saturday',
    pd.Timestamp(2023, 4, 9) : 'Sunday',
    pd.Timestamp(2023, 4, 10) : 'Monday',
    pd.Timestamp(2023, 4, 11) : 'Tuesday',
    pd.Timestamp(2023, 4, 12) : 'Wednesday',
    pd.Timestamp(2023, 4, 13) : 'Thursday',
    pd.Timestamp(2023, 4, 14) : 'Friday'
})


## Remove readings where numbers of riders entered or exited are negative
ridership = ridership[ridership['RIDERS_ENTERED'] >= 0] 
ridership = ridership[ridership['RIDERS_EXITED'] >= 0]

## Remove some outliers
ridership = ridership[ridership['RIDERS_ENTERED'] < 100000] 
ridership = ridership[ridership['RIDERS_EXITED'] < 100000] 


## Estimate traffic PER LINE at each stations
ridership.RIDERS_ENTERED = ridership.RIDERS_ENTERED / ridership.LINENAME.str.len()
ridership.RIDERS_EXITED = ridership.RIDERS_EXITED / ridership.LINENAME.str.len()
ridership.drop(['LINENAME'], axis=1, inplace=True)

## Aggregate to a station - time level
ridership = ridership.groupby(['STATION', 'DAY', 'TIME'], as_index=False).agg({"RIDERS_ENTERED": "sum", "RIDERS_EXITED":"sum"})
ridership['TOTAL_TRAFFIC'] = ridership['RIDERS_ENTERED'] + ridership['RIDERS_EXITED']



'''
    Process station and lines geopandas data
'''
## Rename the stations to match with ridership stations data
with open("stations_mapping.txt", "r") as fp:
    station_mapping = json.load(fp)
stations=stations.replace({"name": station_mapping})

## Split stations by the lines that run through them
stations.line = stations.line.str.split('-')
stations = stations.explode('line')
stations = stations[~((stations.line == '6 Express') | (stations.line == '7 Express'))]
stations = stations[~((stations.index.isin([458, 459, 464, 191, 195, 103, 82, 97])) & (stations.line == 'S'))] # Drop some stations that were deemed to be on the S line in the dataset but no longer are in reality
stations = stations[~stations.duplicated(['line', 'name'])] # Remove some duplicates station-line combinations
stations = stations.drop(['url', 'notes', 'objectid'], axis=1) # Clean up

lines.name = lines.name.str.split('-')
lines = lines.explode('name')

'''
    Analysis and visualization
'''
## Aggregate ridership and station datasets
agg_stations = ridership.merge(stations, how="left", left_on=["STATION"], right_on=['name'])
agg_stations = gpd.GeoDataFrame(agg_stations)
agg_stations = agg_stations.dropna()

## Add actual colors to the lines for better visualization
color_map = {
    'A' : '#0039a6',
    'C' : '#0039a6',
    'E' : '#0039a6',
    'B' : '#ff6319',
    'D' : '#ff6319',
    'F' : '#ff6319',
    'M' : '#ff6319',
    'G' : '#6cbe45',
    'L' : '#a7a9ac',
    'J' : '#996633',
    'Z' : '#996633',
    'N' : '#fccc0a',
    'Q' : '#fccc0a',
    'R' : '#fccc0a',
    'W' : '#fccc0a',
    '1' : '#ee352e',
    '2' : '#ee352e',
    '3' : '#ee352e',
    '4' : '#00933c',
    '5' : '#00933c',
    '6' : '#00933c',
    '7' : '#b933ad',
    'T' : '#00add0',
    'S' : '#808183',
}

def plot_subway(line, day):
    curr_line = lines[lines.name == line]
    

    title_map = [['From 12 a.m. to 4 a.m.', 'From 4 a.m. to 8 a.m.'], ['From 8 a.m. to 12 p.m.', 'From 12 p.m. to 4 p.m.'], ['From 4 p.m. to 8 p.m.', 'From 8 p.m. to 12 a.m.']]
    time_map = [['04:00:00', '08:00:00'], ['12:00:00', '16:00:00'], ['20:00:00', '00:00:00']]
    forward_day = {'Monday' : 'Tuesday', 'Tuesday' : 'Wednesday', 'Wednesday' : 'Thursday', 'Thursday' : 'Friday', 'Friday' : 'Saturday', 'Saturday' : 'Sunday', 'Sunday' : 'Monday'}
    fig, ax = plt.subplots(3, 2, figsize=(12, 30), dpi=100)
    
    for i in range(3):
        for j in range(2):
            if i == 2 and j == 1:
                continue
            curr_stations = agg_stations[(agg_stations.line == line) & (agg_stations.DAY==day) & (agg_stations.TIME==time_map[i][j])]
            nyc_map.plot(ax = ax[i][j], color='none')
            curr_line.plot(ax=ax[i][j], lw=6, color=color_map[line])
            curr_stations.plot(ax=ax[i][j], zorder=2, facecolors='orange', edgecolors='black', markersize=curr_stations.TOTAL_TRAFFIC/20)
            ax[i][j].set_ylim([40.55921006311441, 40.91950155233627])
            ax[i][j].set_xlim([-74.04464955081772, -73.74163145169605])
            ax[i][j].set_title(title_map[i][j])
    
    # Plot the last subplot since it is a new day (00:00:00 of the next day)
    curr_stations = agg_stations[(agg_stations.line == line) & (agg_stations.DAY==forward_day[day]) & (agg_stations.TIME==time_map[2][1])]
    nyc_map.plot(ax = ax[2][1], color='none')
    curr_line.plot(ax=ax[2][1], lw=6, color=color_map[line])
    curr_stations.plot(ax=ax[2][1], zorder=2, facecolors='orange', edgecolors='black', markersize=curr_stations.TOTAL_TRAFFIC/20)
    ax[2][1].set_ylim([40.55921006311441, 40.91950155233627])
    ax[2][1].set_xlim([-74.04464955081772, -73.74163145169605])
    ax[2][1].set_title(title_map[2][1])

    fig.supylabel(line + ' train traffic on ' + day + " in 4-hour intervals", fontsize=20)
    plt.show()
    
## Individual plot function
def ind_plot(line, day, time):
    fig, ax = plt.subplots(figsize=(6, 9), dpi=100)
    curr_line = lines[lines.name == line]
    curr_stations = agg_stations[(agg_stations.line == line) & (agg_stations.DAY==day) & (agg_stations.TIME==time)]
    nyc_map.plot(ax = ax, color='none')
    curr_line.plot(ax=ax, lw=6, color=color_map[line])
    curr_stations.plot(ax=ax, zorder=2, facecolors='orange', edgecolors='black', markersize=curr_stations.TOTAL_TRAFFIC/20)
    ax.set_ylim([40.55921006311441, 40.91950155233627])
    ax.set_xlim([-74.04464955081772, -73.74163145169605])
    ax.set_title(line + ' train traffic on ' + day + " in the 4-hour period ending at " + time)
    plt.show()

## Display results depending on parameters provided
if len(sys.argv) == 3:
    subway_line = sys.argv[1]
    day = sys.argv[2]
    plot_subway(str(subway_line), str(day))
elif len(sys.argv) == 4:
    subway_line = sys.argv[1]
    day = sys.argv[2]
    time = sys.argv[3]
    ind_plot(str(subway_line), str(day), str(time))

## Run 
## To obtain visualization of a line throughout one day: python LINE DAY_OF_WEEK 
## To obtain visualization of a line in a 4-hour period ending at a specific hour: python LINE DAY_OF_WEEK ENDING_HOUR 

## Possible lines:
# ['G', 'Q', 'M', 'S', 'A', 'B', 'D', 'F', 'R', 'N', 'E', '7', 'J', 'Z', 'L', 'C', '1', '2', '3', '4', '5', '6', 'W']

## Possible days:
# ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

## Possible times:
# ['00:00:00', '04:00:00','08:00:00','12:00:00','16:00:00','20:00:00']