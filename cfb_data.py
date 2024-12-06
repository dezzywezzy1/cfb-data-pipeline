import os
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
import time
from pathlib import Path






class Data:
    api_url = "https://api.collegefootballdata.com/"
    instances = []
    
    def __init__(self):
        self.data_frame = None
        self.json_data = None
        self.csv_path = None
        Data.instances.append(self)
        path = find_dotenv('.env')
        load_dotenv(path)
        self.api_key= os.environ.get("CFB_API_KEY")
        self.headers = {
            'Authorization': "Bearer " +  self.api_key,
            'Accept': 'application/json'
        }
     
        
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
    
    
        
    def save(self, file_path: str | Path):
        if not file_path:
            file_path = self.csv_path
            if not self.csv_path:
                raise AttributeError("No .csv file path found. Enter file_path argument to save.")
        file_path = Path(file_path)
        
        if self.data_frame is None:
            raise ValueError("No data to save. Make sure self.data_frame is populated.")
        
        try:
            self.data_frame.to_csv(file_path)
            self.csv_path = file_path
        except Exception as e:
            raise IOError(f"Error saving DataFrame to CSV: {e}")
    
            
    
    @timing_decorator
    def request(self, endpoint: str, params: dict = None) -> any:
        if not endpoint:
            raise TypeError("API requires 'endpoint' argument")
        if not isinstance(endpoint, str):
            raise TypeError("'endpoint' argument must be of type: str")
        if params:
            if not isinstance(params, dict):
                raise TypeError("'params' argument must be of type: dict")
       
        if params:
            try:
                response = requests.get(f"{self.api_url}{endpoint}", params=params, headers=self.headers)
            except requests.RequestException as e:
                raise RuntimeError(f"Error during API request: {e}")
        else:
            try:
                response = requests.get(f"{self.api_url}{endpoint}", headers=self.headers)
            except requests.RequestException as e:
                raise RuntimeError(f"Error during API request: {e}")   
            
        self.json_data = response.json()  
         
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
        
        
        return self.json_data
    
    
    def load(self, file_path: str | Path) -> pd.DataFrame:
        file_path = Path(file_path)
        if file_path.suffix != ".csv":
            raise ValueError(f"{file_path} must reference a CSV file")
        if file_path.exists():
            try:
                self.data_frame = pd.read_csv(file_path)
                return pd.read_csv(file_path)
            except Exception as e:
                raise NotImplementedError(f"Error reading CSV file {file_path}: {e}")
        else:
            raise FileNotFoundError(f"{file_path} does not exist")
    
 