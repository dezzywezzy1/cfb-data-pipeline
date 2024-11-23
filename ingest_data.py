from cfb_data import Data
from datetime import datetime


#get current year of CFB. Consider the start of a new season June 1.
if datetime.now().month <= 6:
    current_football_year = (datetime.now().year) - 1
else:
    current_football_year = datetime.now().year
         


def main():
    player_stats = Data()
    player_stats.request('stats/player/season', params={
        'year': current_football_year
    })
    player_stats.save(f'player_stats_{current_football_year}.csv')
    print(player_stats.data_frame.head(15))
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






''' SAVING BELOW FOR LATER SO I DON'T HAVE TO RE-TYPE THE ENDPOINTS'''

'''
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
game_results = get_cfb_data('/games', params={'year': current_football_year, 'division': 'fbs'})
df_game_results = pd.DataFrame(game_results)

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
'''


if __name__ == "__main__":
    main()