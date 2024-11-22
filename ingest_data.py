import os
import pandas as pd
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import dotenv
import time
from pathlib import Path


path = dotenv.find_dotenv('.env')
load_dotenv(path)
api_key= os.environ.get("CFB_API_KEY")

#get current year of CFB. Consider the start of a new season June 1.
if datetime.now().month <= 6:
    current_football_year = (datetime.now().year) - 1
else:
    current_football_year = datetime.now().year


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"API call {list(kwargs.items())[0]} executed in {end_time - start_time:.4f} seconds")
        return result
    return wrapper  
    

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
records = get_cfb_data(endpoint='/records', params={'year': current_football_year})
df_records = pd.json_normalize(records, sep='_')

#get date ranges for each week (year=current_year) from API
week_dates = get_cfb_data(endpoint='/calendar', params={'year': current_football_year})
df_week_dates = pd.DataFrame(week_dates)

#get current year game results for all fbs schools
#fix nested structure of 'line_score'
'''game_results = get_cfb_data('/games', params={'year': current_football_year, 'division': 'fbs'})
df_game_results = pd.DataFrame(game_results)'''

#get all current year game media information
media_info = get_cfb_data(endpoint='/games/media', params={'year': current_football_year, 'division': 'fbs'})
df_media_info = pd.DataFrame(media_info)


#get all current year player season stats from FBS schools
player_stats = get_cfb_data(endpoint='/stats/player/season', params={'year': current_football_year, 'seasonType': 'regular'})
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

class Cfb_Data():
    
    @timing_decorator
    def request(self, endpoint: str, api_key: str, params: dict = None) -> any:
        if not endpoint:
            raise TypeError("request() requires 'endpoint' argument")
        if not isinstance(endpoint, str):
            raise TypeError("'endpoint' argument must be of type: str")
        if params:
            if not isinstance(params, dict):
                raise TypeError("'params' argument must be of type: dict")
        
        headers = {
            'Authorization': "Bearer " +  api_key,
            'Accept': 'application/json'
        }
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
    
    def load(self, file_name: str | Path) -> pd.DataFrame:
        try:
            file_name = Path(file_name)
        except Exception as e:
            return str(e)
        
        if file_name.exists():
            try:
                return pd.read_csv(file_name)
            except Exception as e:
                return str(e)
        else:
            raise FileNotFoundError("Could not find file at given path")
    
    def save(self, data: pd.DataFrame, file_path: str | Path):
        try:
            file_name = Path(file_name)
        except Exception as e:
            return str(e)
        
        if isinstance(data, pd.DataFrame) or (isinstance(data, list) and all(isinstance(item, dict) for item in data)):
            try:
                if isinstance(data, pd.DataFrame):
                    data.to_csv(file_path)
                else:
                    pd.DataFrame(data).to_csv(file_path)
            except Exception as e:
                return str(e)
            
        else:
            raise TypeError("data must be of type DataFrame or flattened JSON")
    
    def flatten_json():
        pass
        '''
        -create function to flatten json data    
        '''
    
    '''
    -input data integrity tests from top of page into request()
    '''
