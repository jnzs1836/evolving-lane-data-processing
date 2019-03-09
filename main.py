# Data process for bicycle-lane-planning project.
import sys
from home_finder_process import process_homefinder_data
from msra_prosess import process_msra_data
from trajectory_process import process_trajectory_data
from fin_process import process_fin_data
from mock_data import process_mock_data
from config import data_path
if __name__ == '__main__':
    help_info = """Program Direction
    python main.py [config1,]
    help: show help document
    trajectory: process trajectory data 
    msra: process msra data
    home_finder: process home finder data
    fin: process fin_* data
    mock: process mock data
    you can also combine trajectory process with other data like python main.py trajectory msra
    """
    configs = list(map(lambda x:str(x),sys.argv))
    flag = 0
    if  'help' in configs:
        print(help_info)
        flag = 1
    if 'msra' in configs:
        process_msra_data(data_path)
        flag = 1
    elif 'home_finder' in configs:
        process_homefinder_data()
        flag = 1
    elif 'fin' in configs:
        process_fin_data()
        flag = 1
    elif 'mock' in configs:
        process_mock_data()
        flag = 1
    if flag == 0:
        print(help_info)