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
            
            #Maximum Map Boundaries
            self.min_x = min([grid["xmin"] for grid in self.data])
            self.max_x = max([grid["xmax"] for grid in self.data])
            self.min_y = min([grid["ymin"] for grid in self.data])
            self.max_y = max([grid["ymax"] for grid in self.data])

    def printMap(self):
        pprint(self.data)

    #Returns the grid's id based on the given coordinates
    def idFromCoordinates(self,x,y):
        #Remove tweets beyond map boundaries
        if x < self.min_x or x > self.max_x or y < self.min_y or y > self.max_y:
           return None

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

    #Reduce sum operation for counter module (for MPI)
    def sumCounter(counterOne,counterTwo,datatype):
        for item in counterOne:
            counterOne[item] += counterTwo[item]
        return counterOne

    with open(twitterFilePath,'r',encoding='utf8') as twitterFile:  
        line = twitterFile.readline()
        lineNumber = 1
        #total_json_read_with_coordinates = 0
        while line:
            #Distribute line processing using the modulo operation
            if( lineNumber % comm.Get_size() == rank):
                id = processOneTweet(map,line)
                if( id is not None ):
                    #total_json_read_with_coordinates += 1
                    grid_counts[id] += 1
            line = twitterFile.readline()
            lineNumber += 1
            
    #Reduce all sums of json lines to all_json_coordinates from all processes
    #all_json_coordinates = comm.allreduce(total_json_read_with_coordinates)
    #print(rank,"has total",total_json_read_with_coordinates,"out of",all_json_coordinates)

    #Creating an MPI sum operation (sum is a comutative operation)
    counterSumOperation = MPI.Op.Create(sumCounter, commute=True)
    all_grid_counts = comm.allreduce(grid_counts, op=counterSumOperation)
    
    #MASTER RANK prints final collected grid count output
    if (rank == 0):
        print("Final Counts for Tweets in Grids:")
        pprint(all_grid_counts)

def main():
    #Class containing boundaries for each grid and it's methods
    map = mapData(mapFilePath)
    #Store all grid counts in a counter map object
    grid_counts = Counter({grid["id"] : 0 for grid in map.data})
    #Process each line in the twitter file
    processTwitterData(map,twitterFilePath,grid_counts)
    

    

if __name__ == "__main__":
    main()


