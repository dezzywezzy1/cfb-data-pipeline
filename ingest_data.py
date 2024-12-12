from cfb_data import Data
from datetime import datetime
import sys
from pathlib import Path


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

def load_data():
    for source in data_sources.keys():
        obj = Data()
        file_path= data_sources[source]['endpoint'].replace("/", "_")
        obj.name= file_path
        if Path(f'ingestion/{file_path}.csv').exists():
            obj.load(file_path=f"ingestion/{file_path}.csv")
        elif Path(f'ingestion/{file_path}.json').exists():
            obj.load(file_path=f"ingestion/{file_path}.json")
        else:
            raise FileNotFoundError(f"File: {file_path} not found")  
        data_sources[source]['object'] = obj

def first_data_call():
    for source in data_sources.keys():
        print(data_sources[source]['endpoint'])
        try:
            obj = Data()
            obj.request(endpoint=data_sources[source]['endpoint'], params=data_sources[source]['params'])      
            if not obj.is_flat(obj.json_data):
                obj.flatten()
            obj.save(f'ingestion/{data_sources[source]["endpoint"].replace("/", "_")}.csv')
            print(f"Successful API call for endpoint: {data_sources[source]['endpoint']} \n\n")
        except Exception as e:
            print(str(e)) 
        
    
def start_ingestion():
    if len(sys.argv) > 1:    
        if sys.argv[1] == 'new':
            first_data_call()
        elif sys.argv[1] == 'load':
            load_data()
    else:
        print("Please enter command line argument 'new' or 'load'.")
        x = True
        while x:
            inp = input("new or load: ")
            if inp.lower() == 'new' or inp.lower() == 'load':
                x = False
        if inp == 'new':
            first_data_call()
        elif inp == 'load':
            load_data()
            
            
       
        
if __name__ == "__main__":
    start_ingestion()
    