import pandas as pd
from os.path import join
from collections import defaultdict
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

root_dir = "MAF"
s = pd.DataFrame.from_csv(join(root_dir, "summary.csv"))

# Want to get the coordinates into a big matrix
cols = dict()
all_lats = set()
all_lngs = set()
rids = []
names = []
raw_data = dict()

# Load everything first, and work out how many total observations we have
# We can probably downsample really, don't need quite so much data (I wouldn't have thought)
def make_keys(prefix, idx):
    return ["{0}_{1}".format(prefix, i) for i in idx]

for row in s.iterrows():
    item = row[1]

    print "Loading {0} ({1})".format(item["name"], item.id)
    detail = pd.read_json(join(root_dir, "detail", "{0}.json".format(item.id)))
    rids.append(item.id)
    names.append(item["name"])

    raw_data[item.id] = detail

    lat_idx = make_keys("lat", detail.lat.index)
    all_lats.update(lat_idx)

    lng_idx = make_keys("lng", detail.lng.index)
    all_lngs.update(lng_idx)

all_keys = cols.keys()

obs_index = np.arange(0, len(all_lats))
all_lats_s = make_keys("lat", obs_index)
all_lngs_s = make_keys("lng", obs_index)
all_dist_s = make_keys("dist", obs_index)

for k in list(all_lats_s) + list(all_lngs_s) + list(all_dist_s):
    cols[k] = []

print "Preparing a matrix of observations"
# Now populate everything
for rid in rids:
    detail = raw_data[rid]

    detail.sort_index(inplace=True)

    # We want to stretch everything out so that the matrix isn't sparse
    # Figure out how much to space out the points
    factor = len(all_lats) / len(detail.lat.index)

    expanded_index = [i * factor for i in detail.lat.index]
    e_lats = np.interp(obs_index, expanded_index, detail.lat)
    e_lngs = np.interp(obs_index, expanded_index, detail.lng)
    e_dists = np.interp(obs_index, expanded_index, detail.distance)

    # Columns are the sample indices
    # lat1, lat2, lat3, ..., long1, long2, long3, ..
    for lid, val in zip(all_lats_s, e_lats):
        cols[lid].append(val)

    for lid, val in zip(all_lngs_s, e_lngs):
        cols[lid].append(val)

    for lid, val in zip(all_dist_s, e_dists):
        cols[lid].append(val)

    # # Fill in missing observations so we maintain shape
    # for k in all_lats - set(lat_idx):
    #     cols[k].append(0)

    # lng_idx = make_keys("lng", detail.lng.index)
    # for lid in detail.lng.index:
    #     cols["lng_{0}".format(lid)].append(detail.lng[lid])

    # # Fill in missing observations so we maintain shape
    # for k in all_lngs - set(lng_idx):
    #     cols[k].append(0)

# Still getting some runs clustered wrongly, ones that should be on their own.
# Maybe downsample the data?  Add in the distance as a feature, but it needs to have some weight, so
# maybe the downsampling will work/help?


if True:
    cols["name"] = names

    print len(cols.keys()), len(cols.values()), len(rids)
    print len(cols.values()[1])
    obs = pd.DataFrame(data=cols, index=rids)
    print obs

    obs.to_csv(join(root_dir, "cluster_data.csv"))

    # model = KMeans(X=obs)
    # print model.fit()