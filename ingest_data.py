from cfb_data import Data
from datetime import datetime
import json
import csv
import sys


#get current year of CFB. Consider the start of a new season June 1.
if datetime.now().month <= 6:
    current_football_year = (datetime.now().year) - 1
else:
    current_football_year = datetime.now().year
         
    data_sources= {
        'fbs_teams': {
            'endpoint': 'teams/fbs',
            'params': {
                'year': current_football_year
            }
        },
        'records': {
            'endpoint': 'records',
            'params': {
                'year': current_football_year
            }
        },
        'game_results': {
            'endpoint': 'games',
            'params': {
                'year': current_football_year,
                'division': 'fbs'
            }
        },
        'week_dates': {
            'endpoint': 'calendar',
            'params': {
                'year': current_football_year
            }
        },
        'media_info': {
            'endpoint': 'games/media',
            'params': {
                'year': current_football_year,
                'division': 'fbs'
            } 
        },
        'player_stats': {
            'endpoint': 'stats/player/season',
            'params': {
                'year': current_football_year,
                'seasonType': 'regular'
            } 
        },
        'team_rankings': {
            'endpoint': 'rankings',
            'params': {
                'year': current_football_year,
                'seasonType': 'regular'
            }
        },
        'conferences': {
            'endpoint': 'conferences',
            'params': {}
        }      
    }


def cache_data(obj: Data, endpoint: str):
    if not isinstance(obj, Data):
        raise TypeError(f"Expected instance of Data, but got {type(obj)}")
    if obj.json_data is None:
        raise ValueError(f"Object has no JSON data.")
    if not endpoint:
        raise TypeError(f"Expected argument endpoint, but got None")
    if not isinstance(endpoint, str):
        raise TypeError (f"Endpoint must be of type str")
    file_name = endpoint.replace("/","_") + ".json"
    with open(file_name, "w") as outf:
        outf = json.dump(obj=obj.json_data, fp=outf)
        
def load_data(endpoint: str):
    file_name = endpoint.replace("/", "_") + ".json"
    try:
        with open(file_name, "r") as inf:
            return json.load(inf)
    except Exception as e:
        return str(e)
    

def get_data(obj: Data, endpoint=None, params=None):
    if not isinstance(obj, Data):
        raise TypeError(f"Expected instance of Data, but got {type(obj)}")
    try:
        obj.request(endpoint=endpoint, params=params)
        return f"Success for endpoint: {endpoint}"
    except Exception as e:
        return f" Failure for endpoint: {endpoint}\n{str(e)}"
    

def first_data_call():
    not_flat = []
    
    for source in data_sources.keys():
        try:
            obj = Data()
            obj.request(data_sources[source]['endpoint'], data_sources[source]['params'])
            cache_data(obj=obj, endpoint=data_sources[source]['endpoint'])
            if not obj.is_flat(obj.json_data):
                not_flat.append(data_sources[source]['endpoint'].replace("/", '_') + ".json")
            print(f"Successful API call for endpoint: {data_sources[source]['endpoint']} \n\n")
        except Exception as e:
            print(str(e))
    
    with open('not_flat.csv', "w") as outf:
            writer = csv.writer(outf)
            for item in not_flat:
                writer.writerow(item)

        
             

        
    

# test that original data has the same amount of keys/columns as the flattened data
''' DON'T NEED THIS QUITE YET'''
'''def test_cols(data: dict, df: pd.DataFrame) -> bool:
    count_nested_keys = 0
    for key in data.keys():
        if isinstance(data[key], (list, dict, tuple)): 
            count_nested_keys += len(data[key])
            # don't double count original key
            count_nested_keys -= 1
    count_keys = len(data.keys())
    count_keys += count_nested_keys
    if count_keys == len(df.columns):
        return True
    else:
        return False'''

'''
fix nested structure of 'line_score in /games'


df_team_rankings = pd.json_normalize(
    data=team_rankings, 
    record_path=["polls", "ranks"],
    meta=["season", "seasonType", "week", ["polls", "poll"]],
    meta_prefix=''
)
'''


if __name__ == "__main__":
    '''Make user type second arg for loading data or getting data from API'''
    if sys.argv[1] == 'new':
        first_data_call()
    elif sys.argv[1] == 'load':
        load_data()