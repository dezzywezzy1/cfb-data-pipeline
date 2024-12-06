from cfb_data import Data
from datetime import datetime


#get current year of CFB. Consider the start of a new season June 1.
if datetime.now().month <= 6:
    current_football_year = (datetime.now().year) - 1
else:
    current_football_year = datetime.now().year
         


def get_data(obj: Data, endpoint=None, params=None):
    if not isinstance(obj, Data):
        raise TypeError(f"get_data() expected argument of type Data, but got {type(obj)}")
    try:
        obj.request(endpoint=endpoint, params=params)
        return f"Success for endpoint: {endpoint}"
    except Exception as e:
        return f" Failure for endpoint: {endpoint}\n{str(e)}"
    

def main():
    not_flat = []
    fbs_teams = Data()
    records= Data()
    game_results= Data()
    media_info= Data()
    player_stats= Data()
    team_rankings= Data()
    week_dates= Data()
    conferences= Data()
    
    data_sources= [{
        'obj': fbs_teams, 
        'endpoint': 'teams/fbs',
        'params': {
            'year': current_football_year
        }
    },
    {
        'obj': records, 
        'endpoint': 'records',
        'params': {
            'year': current_football_year
        }
    },
    {
        'obj': game_results, 
        'endpoint': 'games',
        'params': {
            'year': current_football_year,
            'division': 'fbs'
        }
    },
    {
        'obj': week_dates, 
        'endpoint': 'calendar',
        'params': {
            'year': current_football_year
        }
    },
    {
        'obj': media_info, 
        'endpoint': 'games/media',
        'params': {
            'year': current_football_year,
            'division': 'fbs'
        }
    },
    {
        'obj': player_stats, 
        'endpoint': 'stats/player/season',
        'params': {
            'year': current_football_year,
            'seasonType': 'regular'
        }
    },
    {
        'obj': team_rankings, 
        'endpoint': 'rankings',
        'params': {
            'year': current_football_year,
            'seasonType': 'regular'
        }
    },
    {
        'obj': conferences, 
        'endpoint': 'conferences',
        'params': {}
    }]
    
    for source in data_sources:
        obj= source['obj']
        get_data(source['obj'], endpoint=source['endpoint'], params=source['params'])
        if not obj.is_flat(obj.json_data):
            not_flat.append(obj)
        print(f"{obj.json_data[0:5]}\n{'-'*30}")
        
             

        
    

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
    main()