import pandas as pd
import numpy as np
import cPickle as pickle
from os.path import join, exists

orig = pd.DataFrame.from_csv(join("data", "summary.csv"))

# Split the full HR stream into 3 sections, and record mean and standard deviation for each one
def calc_hr_stats(act_id):
    with open(join("data", "{0}.p".format(act_id)), "rb") as f:
        data = pickle.load(f)
    third = len(data) / 3
    result = {}
    for dtype in ("heartrate",):
        for i, start in enumerate((0, third, 2 * third), start=1):
            block = data[dtype][start: start + third]
            result["{0}_{1}-mean".format(dtype, i)] = np.mean(block)
            result["{0}_{1}-std".format(dtype, i)] = np.std(block)
    return result

r = calc_hr_stats(orig.iloc[0].id)

orig = orig.join(pd.DataFrame(map(calc_hr_stats, orig.id), index=orig.index))
print "Added HR stats"

# Insert columns for: days since last race, days until next race
orig["days_since_race"] = [None] * len(orig)
orig["days_until_race"] = [None] * len(orig)

# Insert a column for tagging each run (injured, fit, etc)
orig["state"] = [None] * len(orig)

# Calculate various rolling volume measures: 30 day sum, 7 day sum, exponentially weighted rolling mean
orig["rolling_distance_30d"] = pd.rolling_sum(orig.distance[::-1], 30)
orig["rolling_distance_7d"] = pd.rolling_sum(orig.distance[::-1], 7)
orig["ewma_distance_30d"] = pd.ewma(orig.distance[::-1], halflife=30)
orig["ewma_distance_7d"] = pd.ewma(orig.distance[::-1], halflife=7)

# Load the external weight file, and insert a column for that too
weights = pd.DataFrame.from_csv(join("data", "smooth_weights.csv"))
weights = weights.reindex(index=orig.index, method="ffill")

orig = orig.join(weights)

# Save everything
orig.to_csv(join("data", "observations.csv"))