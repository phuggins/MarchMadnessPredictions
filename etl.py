#%%
# Data description
# Each file contains 54-55 columns
## shooting_play = TRUE/FALSE
## sequnce_number = For each game, basically a play # ID
## period_display_value = Denotes what half/ot the game is in. Oth Half/1st Half/2nd Half/OT/2OT/3OT/NA
## period_number = 1/2/3/4/5/NA
## home_score = home team score 
## scoring_play = TRUE/FALSE
## clock_display_value = time on the clock
## team_id = unique ID of the home team
## type_id = unique ID of the type of play
## type_text = what type of play
## away_score = away team score
## id = unique ID of each play
## text = description of the play
## score_value = how many points were scored on the play
## participants_0_athlete_id = player id
## season = season year
## season_type = ????
## away_team_id = away team unique ID
## away_team_name = away team name
## away_team_mascot = away team mascot
## away_team_abbrev = away school name abbreviation
## away_team_name_alt = alternate away team name
## home_team_id = home team unique ID
## home_team_name = home team name
## home_team_mascot = home team mascot
## home_team_abbrev = home school name abbreviation
## home_team_name_alt = alternate home team name
## home_team_spread = game point spread for home team
## game_spread = overall game spread
## home_favorite = TRUE/FALSE
## game_spread_available = TRUE/FALSE
## game_id = unique ID for each game
## qtr = qtr of the game
## time = time on the clock
## clock_minutes = time in minutes
## clock_seconds = time in seconds
## half = denotes which half of the game
## game_half = denotes which half of the game
## lag_qtr = ??
## lead_qtr = ??
## lag_game_half = ??
## lead_game_half = ??
## start_quarter_seconds_remaining = seconds remaining in quarter
## start_half_seconds_remaining = seconds remining in half
## start_game_seconds_remaining = seconds remaining in game
## game_play_number = play # of the game
## end_quarter_seconds_remaining = 
## end_half_seconds_remaining
## end_game_seconds_remaining
## period = period of the game
## participants_1_athlete_id = if 2 players are involved, the 2nd player id goes here
## coordinate_x = ??
## coordinate_y = ??

#%%
# ETL Modules
import pandas as pd
import numpy as np
import time
import os
import dask.dataframe as dd
from dask import delayed # pip install dask[complete]

# Variables to use later
csv_file_path = "C:/Users/Paulh/Documents/GitHub/MarchMadnessPredictions/rawdata"

#%%
# Read in all the csv files in the rawdata folder and add a column with file name
def read_and_label_csv(filename):
    # reads each csv file to a pandas.DataFrame
    df_csv = pd.read_csv(filename, encoding='latin-1')
    df_csv['partition'] = filename.split('\\')[-1]
    return df_csv

# build a file_list that contains the path of each file in the directory
file_list = []
for file in os.listdir(csv_file_path):
    if file.endswith(".csv"):
        file_list.append(os.path.join(csv_file_path, file))

#%%
# Read in the files and get total time
s_time_dask = time.time()
dfs = [delayed(read_and_label_csv)(fname) for fname in file_list]
ddf = dd.from_delayed(dfs)
e_time_dask = time.time()
print("Time to read in all files: ", (e_time_dask - s_time_dask), " seconds")
# %%

#%% 
# There's a chance that we have duplicate rows just because of how the r scripts to load the data were ran
s_time_dask = time.time()
print("# of rows prior to de-dupe: ", ddf.shape[0].compute())
ddf.drop_duplicates()  
print("# of rows after de-dupe: ", ddf.shape[0].compute())
e_time_dask = time.time()
print("Time to read in all files: ", (e_time_dask - s_time_dask), " seconds")

# %%
## Step 1: set datatypes
ddf.astype({'col1': 'int32'}).dtypes 