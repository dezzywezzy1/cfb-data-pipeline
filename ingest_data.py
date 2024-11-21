import os
import pandas as pd
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import dotenv
import time


path = dotenv.find_dotenv('.env')
load_dotenv(path)
api_key= os.environ.get("CFB_API_KEY")

#get current year of CFB. Consider the start of a new season June 1.
if datetime.now().month <= 6:
    current_football_year = (datetime.now().year) - 1
else:
    current_football_year = datetime.now().year
    
    
# function to get JSON data through API call
def get_cfb_data(endpoint: str, params: dict = None) -> dict:
    headers = {'Authorization': "Bearer " +  api_key, 'Accept': 'application/json'}
    if params:
        try:
            response = requests.get(f"https://api.collegefootballdata.com{endpoint}", params=params, headers=headers)
        except Exception as e:
            return str(e)
    else:
        try:
            response = requests.get(f"https://api.collegefootballdata.com{endpoint}", headers=headers)
        except Exception as e:
            return str(e)          
    return response.json()


# test that original data has the same amount of keys/columns as the flattened data
def test_cols(data: dict, df: pd.DataFrame) -> bool:
    count_nested_keys = 0
    for key in data.keys():
        if type(data[key]) == dict:
            count_nested_keys += len(data[key].keys())
            # don't double count original key
            count_nested_keys -= 1
    count_keys = len(data.keys())
    count_keys += count_nested_keys
    if count_keys == len(df.columns):
        return True
    else:
        return False

def test_rows(data: list, df: pd.DataFrame) -> bool:
    if len(data) == len(df):
        return True
    else:
        return False
    


# get only FBS conference names and abbreviations for API parameters
start_time = time.time()
conferences= get_cfb_data(endpoint="/conferences")
fbs_conference_names = [conf['name'] for conf in conferences if 'fbs' in conf['classification']]
fbs_conference_abbrev = [conf['abbreviation'] for conf in conferences if 'fbs' in conf['classification']]
end_time = time.time()
duration = end_time - start_time
print(f'Conferences: {duration:.2f}')

#get FBS team data from API
start_time = time.time()
fbs_teams = get_cfb_data('/teams/fbs', params={'year': current_football_year})
end_time = time.time()
duration = end_time - start_time
print(f'Team: {duration:.2f}')

#get current team records by conference and flatten them into a data frame
start_time = time.time()
records = []
for conference in fbs_conference_names:
    records += get_cfb_data('/records', params={'year': current_football_year, 'conference': conference})
df_records = pd.json_normalize(records, sep='_')
end_time = time.time()
duration = end_time - start_time
print(f'Records: {duration:.2f}')

#get date ranges for each week (year=current_year) from API
start_time = time.time()
week_dates = get_cfb_data('/calendar', params={'year': current_football_year})
df_week_dates = pd.DataFrame(week_dates)
end_time = time.time()
duration = end_time - start_time
print(f'Week Dates: {duration:.2f}')

#get current year game results for all fbs schools
#fix nested structure of 'line_score'
'''game_results = get_cfb_data('/games', params={'yeear': current_football_year, 'division': 'fbs'})
df_game_results = pd.DataFrame(game_results)'''

#get all current year game media information
start_time = time.time()
media_info = get_cfb_data('/games/media', params={'year': current_football_year, 'division': 'fbs'})
df_media_info = pd.DataFrame(media_info)
end_time = time.time()
duration = end_time - start_time
print(f'Media: {duration:.2f}')


#get all current year player season stats from FBS schools
start_time = time.time()
player_stats = []
for conference in fbs_conference_abbrev:
    player_stats += get_cfb_data('/stats/player/season', params={'year': current_football_year, 'seasonType': 'regular', 'conference': conference})
df_player_stats = pd.DataFrame(player_stats)
end_time = time.time()
duration = end_time - start_time
print(f'Player Stats: {duration:.2f}')

    
#get team rankings by week in current year and flatten
start_time = time.time()
team_rankings = get_cfb_data('/rankings', params={'year': current_football_year, 'seasonType': 'regular'})

df_team_rankings = pd.json_normalize(
    data=team_rankings, 
    record_path=["polls", "ranks"],
    meta=["season", "seasonType", "week", ["polls", "poll"]],
    meta_prefix=''
)
end_time = time.time()
duration = end_time - start_time
print(f'Team Rankings: {duration:.2f}')

print(f"\n\n\n{df_team_rankings.head(15)}\n\n\n")
df_team_rankings.info()


''' 
-Don't worry about looping through FBS conferences as this significantly slows down the script-
-Start saving the data that doesn't have any issues as files so we can stop calling the API-
'''

