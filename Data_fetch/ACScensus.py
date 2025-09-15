# NY state, 5 counties, ACS (2020-2024) 
## includes: population, race, income, housing, age.

# %% step 1: import libraries
import csv
import os
import sys
import time
import json
from pathlib import Path
from urllib.parse import urlencode
import urllib.request
import pandas as pd # type: ignore
