import sys
import json
from pprint import pprint
from mpi4py import MPI

#MPI Variables
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

#Global Variables
mapFilePath = "./data/melbGrid.json"
twitterFilePath = "./data/tinyTwitter.json"

#Class That Holds Map Data

class mapData:
    def __init__(self,dataPath):
        map_json = json.load(open(dataPath,'r'))
        #Return a list of 
        self.data = [j["properties"] for j in map_json["features"]]
    
    def printMap(self):
        print("From Processors:",rank)
        pprint(self.data)

def main():
    map = mapData(mapFilePath)
    map.printMap()

if __name__ == "__main__":
    main()


