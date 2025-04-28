import sys
import numpy as np
import pandas as pd
import statsmodels as sm
import sklearn
import scipy as sp
%matplotlib inline
import matplotlib.pyplot as plt
import math
import random
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
# Set the random seed such that the results are replicable.

random.seed(256)

# Load the data
url = "https://raw.githubusercontent.com/sytong12/SEEM3650project/refs/heads/main/aggregated_hourly_data.csv"
df_BH = pd.read_csv(url)

df_BH.head()
