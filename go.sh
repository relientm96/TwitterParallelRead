#!/bin/bash

echo "Running on 1 Node 1 Core" 
time mpiexec -n 1 python assignment_one.py

echo "Running on 1 Node 4 Cores"
time mpiexec -n 4 python assignment_one.py

echo "Running on 1 Node 8 Cores"
time mpiexec -n 8 python assignment_one.py
