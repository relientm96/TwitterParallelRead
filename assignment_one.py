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
        try: 
            open(dataPath,'r')
            map_json = json.load(open(dataPath,'r'))
            #Return a list of the properties of each grid
            self.data = [j["properties"] for j in map_json["features"]]
        except:
            print("Error in map file path!")    
            print("Map File path is",dataPath)

    def printMap(self):
        print("From Processors:",rank)
        pprint(self.data)

    #Returns the grid's id based on the given coordinates
    def idFromCoordinates(self,x,y):
        for grid in self.data:
            if( x >= grid["xmin"] ) and ( x < grid["xmax"] ) and ( y >= grid["ymin"] ) and ( y < grid["ymax"] ):
                return grid["id"]
            
def main():
    #Class containing boundaries for each grid and it's methods
    map = mapData(mapFilePath)
    

if __name__ == "__main__":
    main()


