import os
import sys
import numpy as np
import matplotlib.pyplot as plt



class DataProcessor:
     def __init__(self, data):
        self.data = data
        
    def convert_data(self):
        time = np.array([row[0] for row in self.data])
        values_1 = np.array([row[1] for row in self.data])
        values_2 = np.array([row[2] for row in self.data])

        return time, values_1, values_2
    
class Plotter:
    
    def __init__(self, time, value_1, value_2, field_name, year_name):
        self.time = time
        self.value_1 = value_1
        self.value_2 = value_2
        self.field_name = field_name
        self.year_name = year_name
    
    
    