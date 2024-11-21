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


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"API call {list(kwargs.items())[0]} executed in {end_time - start_time:.4f} seconds")
        return result
    return wrapper

#get current year of CFB. Consider the start of a new season June 1.
if datetime.now().month <= 6:
    current_football_year = (datetime.now().year) - 1
else:
    current_football_year = datetime.now().year
    
    
# function to get JSON data through API call
@timing_decorator
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
conferences= get_cfb_data(endpoint="/conferences")
fbs_conference_names = [conf['name'] for conf in conferences if 'fbs' in conf['classification']]
fbs_conference_abbrev = [conf['abbreviation'] for conf in conferences if 'fbs' in conf['classification']]

#get FBS team data from API
fbs_teams = get_cfb_data(endpoint='/teams/fbs', params={'year': current_football_year})

#get current team records by conference and flatten them into a data frame
records = []
for conference in fbs_conference_names:
    records += get_cfb_data(endpoint='/records', params={'year': current_football_year, 'conference': conference})
df_records = pd.json_normalize(records, sep='_')

#get date ranges for each week (year=current_year) from API
week_dates = get_cfb_data(endpoint='/calendar', params={'year': current_football_year})
df_week_dates = pd.DataFrame(week_dates)

#get current year game results for all fbs schools
#fix nested structure of 'line_score'
'''game_results = get_cfb_data('/games', params={'yeear': current_football_year, 'division': 'fbs'})
df_game_results = pd.DataFrame(game_results)'''

#get all current year game media information
media_info = get_cfb_data(endpoint='/games/media', params={'year': current_football_year, 'division': 'fbs'})
df_media_info = pd.DataFrame(media_info)


#get all current year player season stats from FBS schools
player_stats = []
for conference in fbs_conference_abbrev:
    player_stats += get_cfb_data(endpoint='/stats/player/season', params={'year': current_football_year, 'seasonType': 'regular', 'conference': conference})
df_player_stats = pd.DataFrame(player_stats)

    
#get team rankings by week in current year and flatten
team_rankings = get_cfb_data(endpoint='/rankings', params={'year': current_football_year, 'seasonType': 'regular'})

df_team_rankings = pd.json_normalize(
    data=team_rankings, 
    record_path=["polls", "ranks"],
    meta=["season", "seasonType", "week", ["polls", "poll"]],
    meta_prefix=''
)

print(f"\n\n\n{df_team_rankings.head(15)}\n\n\n")
df_team_rankings.info()


''' 
-Don't worry about looping through FBS conferences as this significantly slows down the script-
-Start saving the data that doesn't have any issues as files so we can stop calling the API-
'''

