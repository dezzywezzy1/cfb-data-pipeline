import os
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
import time
from pathlib import Path
import json
from google.cloud import storage, secretmanager

BUCKET_NAME = os.getenv("BUCKET_NAME")
SECRET_NAME = os.getenv("SECRET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")


class Data:
    api_url = "https://api.collegefootballdata.com/"
    instances = []
    
    def __init__(self):
        self.data_frame = None
        self.json_data = None
        self.file_path = None
        self.name = None
        Data.instances.append(self)
        path = find_dotenv('.env')
        load_dotenv(path)
     
        
    @classmethod
    def get_instances(cls):
        return cls.instances
    
    
    @staticmethod
    def is_flat(data):
        if isinstance(data, list):
            if isinstance(data[0], dict):
                if all(not isinstance(value, (dict, list, tuple, set)) for value in data[0].values()):
                    return True
        elif isinstance(data, dict):
            if all(not isinstance(value, (dict, list, tuple, set)) for value in data.values()):
                    return True
        return False  
    
    @staticmethod
    def timing_decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"\nAPI call {kwargs.get('endpoint')} executed in {end_time - start_time:.4f} seconds\n")
            return result
        return wrapper  
    
            
    def _get_api_key(self):
        client= secretmanager.SecretManagerServiceClient()
        secret_path= f'projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/1'
        response= client.access_secret_version(request={"name": secret_path})
        return response.payload.data.decode("UTF-8")
    
    def _upload_data(self):
        client= storage.Client()
        bucket= client.bucket(BUCKET_NAME)
        blob= bucket.blob(f"raw_data/{self.name}.json")
        #json_string= json.dumps(self.json_data, indent=2)
        blob.upload_from_string(self.json_data, content_type='application/json')
        print(f"JSON data uploaded to gs://{BUCKET_NAME}/raw_data/{self.name}.json")
        
    @timing_decorator
    def request(self, endpoint: str, params: dict = None) -> any:
        api_key= self._get_api_key()
        headers = {
            'Authorization': "Bearer " +  api_key,
            'Accept': 'application/json'
        }
        
        if not endpoint:
            raise TypeError("API requires 'endpoint' argument")
        if not isinstance(endpoint, str):
            raise TypeError("'endpoint' argument must be of type: str")
        if params:
            if not isinstance(params, dict):
                raise TypeError("'params' argument must be of type: dict")
            else:
                try:
                    response = requests.get(f"{self.api_url}{endpoint}", params=params, headers=headers)
                except requests.RequestException as e:
                    raise RuntimeError(f"Error during API request: {e}")
        else:
            try:
                response = requests.get(f"{self.api_url}{endpoint}", headers=headers)
            except requests.RequestException as e:
                raise RuntimeError(f"Error during API request: {e}")   
            
        self.json_data = response.json()
        ndjson_data= '\n'.join(json.dumps(obj) for obj in self.json_data)
        self.json_data= ndjson_data
        self.name = endpoint.replace("/", "_")
        self._upload_data() 
        
        if self.is_flat(self.json_data):
            self.data_frame = pd.DataFrame(self.json_data)
            return self.data_frame
        elif isinstance(self.json_data, list):
            row = (self.json_data)[0]
            problem_data = {key: row[key] for key in row.keys() if isinstance(row[key], (dict, list, tuple, set))}
            print(f"\n\nAPI Request Success, but not able to convert into DataFrame. Data is not flat.\n\n {problem_data}\n\n")
        elif isinstance(self.json_data, dict):
            problem_data = {key: (self.json_data)[key] for key in self.json_data.keys() if isinstance((self.json_data)[key], (dict, list, tuple, set))}
            print(f"\n\nAPI Request Success, but not able to convert into DataFrame. Data is not flat.\n\n {problem_data}\n\n")
    
    
    def load(self, file_path: str | Path) -> pd.DataFrame:
        if self.file_path is None:
            if not file_path:
                raise TypeError("No file path to load file.")
            file_path = Path(file_path)
        else:
            file_path = Path(self.file_path)

        self.file_path= file_path
        if file_path.suffix != ".csv":
            try:
                with open(file_path, "r") as inf:
                    self.json_data= json.load(inf)
                return
            except Exception as e:
                return str(e)
        try:
            self.data_frame = pd.read_csv(file_path)
            return pd.read_csv(file_path)
        except Exception as e:
            raise NotImplementedError(f"Error reading CSV file {file_path}: {e}")

   
    def save(self, file_path: str | Path):
        if not file_path:
            if not self.file_path:
                raise AttributeError("No file path found. Enter file_path argument to save.")
            file_path= self.file_path
        if self.data_frame is None:
            if self.json_data is None:
                raise ValueError("There is no data to save.")
            else:
                file_path = Path(file_path.replace(".csv", ".json"))
                with open(file_path, "w") as outf:
                    outf = json.dump(self.json_data, fp= outf)
                    self.file_path= file_path
                print("\nCould not save as .csv, saved as .json instead")
        else:
            try:
                self.data_frame.to_csv(file_path)
                self.file_path = file_path
            except Exception as e:
                raise IOError(f"Error saving DataFrame to CSV or JSON: {e}")
    
    
    def flatten(self):
        if self.name == 'teams_fbs':
            fbs_teams = self.json_data
            meta_keys = [key for key in fbs_teams[0].keys() if not isinstance(fbs_teams[0][key], list)]
            meta_keys.pop(meta_keys.index('location'))
            location_keys= [["location", key] for key in fbs_teams[0]['location']]
            meta_keys += location_keys
            fbs_teams = pd.json_normalize(fbs_teams, record_path='logos', meta=meta_keys, sep='_', record_prefix='logo')
            fbs_teams= fbs_teams.rename(columns={"logo0":"logo"})
            self.data_frame= fbs_teams
               
        elif self.name == 'records':
            records = self.json_data
            records = pd.json_normalize(records, sep="_")
            self.data_frame= records
            
        elif self.name == 'games':
            games = self.json_data
            line_scores = {}
            for i in range(1, 13):
                if i > 4:
                    line_scores[f'OT_{i}']= None
                else:
                    line_scores[f'Qtr_{i}']= None
            for game in games:
                home_scores = game['home_line_scores']
                away_scores = game['away_line_scores']
                if home_scores:
                    for i in range(1, len(home_scores)+1):
                        if i > 4:
                            line_scores[f'OT_{i}'] = home_scores[i-1]
                        else:
                            line_scores[f'Qtr_{i}'] = home_scores[i-1]
                    game['home_line_scores'] = line_scores.copy()
                else:
                    game['home_line_scores'] = line_scores.copy()       
                for key in line_scores.keys():
                    line_scores[key] = None     
                if away_scores:
                    for i in range(1, len(away_scores)+1):
                        if i > 4:
                            line_scores[f'OT_{i}'] = away_scores[i-1]
                        else:
                            line_scores[f'Qtr_{i}'] = away_scores[i-1]
                    game['away_line_scores'] = line_scores.copy()
                else:
                    game['away_line_scores'] = line_scores.copy()
                for key in line_scores.keys():
                    line_scores[key] = None         
            games = pd.json_normalize(games, sep="_")
            self.data_frame = games   

        elif self.name == 'rankings':
            rankings= self.json_data
            rankings= pd.json_normalize(rankings, record_path=['polls', 'ranks'], meta=['season', 'seasonType', 'week', ['polls','poll']], sep="_")
            rankings= rankings.rename(columns={
                'polls_poll': 'poll'
            })
            self.data_frame = rankings
        
        else:
            raise ValueError("Cannot flatten this endpoint, please do it manually or modify the 'flatten' method of class: Data")
            