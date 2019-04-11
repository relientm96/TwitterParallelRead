import sys
import json
from collections import Counter
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
        with open(dataPath,'r',encoding='utf8') as fp:  
            map_json = json.load(open(dataPath,'r'))
            #Return a list of the properties of each grid
            self.data = [j["properties"] for j in map_json["features"]]

    def printMap(self):
        print("From Processors:",rank)
        pprint(self.data)

    #Returns the grid's id based on the given coordinates
    def idFromCoordinates(self,x,y):
        for grid in self.data:
            if( grid["xmin"] <= x <= grid["xmax"] and grid["ymin"] <= y <= grid["ymax"] ) :
                return grid["id"]

def processOneTweet(map,inputLine):
    #Remove newline character and , to process json
    inputLine = inputLine.rstrip(",\n")
    #Test if legitimate JSON line
    try:
        json_txt = json.loads(inputLine)
    except:
        #Exit function if invalid json
        return

    #Check if we have coordinates
    if (json_txt["doc"]["coordinates"]["coordinates"] is not None):
        x_input = json_txt["doc"]["coordinates"]["coordinates"][0]
        y_input = json_txt["doc"]["coordinates"]["coordinates"][1]    
    return map.idFromCoordinates(x_input,y_input)

def processTwitterData(map,twitterFilePath,grid_counts):
    with open(twitterFilePath,'r',encoding='utf8') as twitterFile:  
        line = twitterFile.readline()
        lineNumber = 1
        total_json_read_with_coordinates = 0
        while line:
            #Distribute line processing using the modulo operation
            if( lineNumber % comm.Get_size() == rank):
                id = processOneTweet(map,line)
                if( id is not None ):
                    total_json_read_with_coordinates += 1
                    grid_counts[id] += 1
            line = twitterFile.readline()
            lineNumber += 1

                

    #Reduce all sums of json lines to all_json_coordinates from all processes
    all_json_coordinates = comm.allreduce(total_json_read_with_coordinates)
    print(rank,"has total",total_json_read_with_coordinates,"out of",all_json_coordinates)

def printFinalResults(grid_counts):
    pprint(grid_counts)

def main():
    #Class containing boundaries for each grid and it's methods
    map = mapData(mapFilePath)

    #Store all grid counts here
    grid_counts = Counter({g["id"] : 0 for g in map.data})

    #Process each line in the twitter file
    processTwitterData(map,twitterFilePath,grid_counts)

    

if __name__ == "__main__":
    main()


