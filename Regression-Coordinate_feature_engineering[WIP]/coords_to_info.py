#!/usr/bin/env python3

"""
coords_to_info.py transforms the latitude and longitude coordinates of the
DataFrame to location information such as countr, state, ...

Note that the script will stop before handling the last batch of lines if the
length of the input file is not a multiple of 100. In this case, the script
should simply be rerun by setting the pickup variable to the index number of
the last processed line.
"""

# Import packages
import pandas as pd
import numpy as np
import os
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from time import sleep
from random import randint
# Ignore warnings
import warnings
warnings.filterwarnings("ignore")

"""
Function definition
"""

def reset_temp_memory_df():
    """
    reset_temp_memory_df resets the temporary DataFrame used to store results
    100 at a time after the 100 lines have been written out to the permanent
    output file. The counter j is also reset.
    """
    global temp_memory_df, j
    temp_memory_df = pd.DataFrame(columns=all_data.columns,
                                  index=np.arange(0, 100))
    j=0

def reverse_geocode(geolocator, latlon, sleep_sec):
    """
    reverse_geocode returns geographical information from provided latitude and
    longitude coordinates.

    This function is robust to the geolocator timing out (in which case the
    script sleeps for a random duration between 1 and sleep_sec seconds before
    calling the geolocator again).

    Mandatory arguments:
    geolocator: a Nominatim geocoder instance
    latlon: coordinates formatted as 'latitude,longitude'
    sleep_sec: maximum duration for sleep function
    """
    try:
        return geolocator.reverse(latlon)
    except GeocoderTimedOut:
        logging.info('TIMED OUT: GeocoderTimedOut: Retrying...')
        sleep(randint(1*100,sleep_sec*100)/100)
        return reverse_geocode(geolocator, latlon, sleep_sec)
    except GeocoderServiceError as e:
        logging.info('CONNECTION REFUSED: GeocoderServiceError encountered.')
        logging.error(e)
        return None
    except Exception as e:
        logging.info('ERROR: Terminating due to exception {}'.format(e))
        return None

"""
Script
"""

# Set variables
wd = './'
infile = 'all_data.csv'
outfile = 'all_data_coord_features.csv'
# Debut variable prints out info if set to True
debug=False

# Set to different value than 0 to start from another line of the input file
pickup = 0

# Load data file
all_data = pd.read_csv(wd + infile)

# Set list of descriptors to extract from the geolocator
descriptors = ['county', 'neighbourhood', 'state_district', 'state',
               'postcode', 'country']

# Set NaN columns in the input DataFrame for the features to be created
for desc in descriptors:
    all_data[desc] = np.nan

# Initialize Nominatim geolocator with randomized user_agent
user_agent = 'user_me_{}'.format(randint(10000,99999))
geolocator = Nominatim(user_agent=user_agent)

# Initialize temporary memory DataFrame
reset_temp_memory_df()

# Iterate over each entry in the input DataFrame
for i in np.arange(pickup, all_data.shape[0]):
    # Get latitude and longitude from the input DF, create combined coordinate
    lat = all_data.loc[i, 'Latitude']
    long = all_data.loc[i, 'Longitude']
    coord = str(lat) + ',' + str(long)

    # Get location info from the geocoder
    location = reverse_geocode(geolocator, coord, 3)

    # Extract all requested descriptors from the location info if available
    try:
        for desc in descriptors:
            if desc in location.raw['address']:
                all_data.loc[i, desc] = location.raw['address'][desc]
    # Pass if the geolocator did not return location info from the coordinates
    except:
        pass

    # transfer entry with location info to temporary DataFrame
    temp_memory_df.loc[j] = all_data.loc[i]
    j+=1 # Increment temporary memory counter

    # Export temporary DataFrame to final output file each 100 entries
    if j==100:
        # Print advancement information if debug is True
        if debug:
            print(f'Currently executing line {i} out of {all_data.shape[0]}',
                  end='\r')

        # Write out temporary DataFrame and reset the temporary DataFrame
        temp_memory_df.to_csv(wd + outfile,
                              mode='a',
                              header=not os.path.exists(wd + outfile))
        reset_temp_memory_df()
