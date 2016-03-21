from stravalib.client import Client
from stravalib import unithelper
import pandas as pd
import cPickle as pickle
from os.path import join, exists
from os import makedirs, getcwd
from datetime import datetime

client = Client()
client.access_token = "8ad469fc76e136a0d5a40f0ad10ea0e8282ef6e5"

me = client.get_athlete()

print me.email

def to_miles(dist):
    return float(unithelper.miles(dist))

def to_mph(mps):
    return float(unithelper.mph(act.average_speed))

def echo(v):
    return v

#TODO Convert moving time properly, want in a timedelta really

#TODO To do proper clustering, need columns as lat then long, and rows as runs
# but how to deal with runs of different lengths?
# I think all streams have the same number of points?  
# Which isn't going to be great, but givem that different routes, even if stretched out/shrunk, have different shapes
# then distinct ones should stay that way, so might be ok
# For clustering to work properly, need the same number of features....

count = 0
total = 0

data = []
start_dates = []
save_fields = {"type": echo,
               "average_heartrate": echo,
               "kudos_count": echo,
               "name": echo, 
               "distance": to_miles,
               "average_speed": to_mph, # want mins per mile
               "moving_time": echo,
               "total_elevation_gain": float,
               "suffer_score": echo,
               "calories": echo,
               "id": echo}

stream_filter = ["latlng", "altitude", "heartrate", "velocity_smooth", "moving", "grade_smooth"]

this_dir = getcwd()
output_dir = join(this_dir, "MAF")
output_detail_dir = join(output_dir, "detail")
from_date = datetime(2016, 2, 27)

if not exists(output_detail_dir):
  print "Creating {0}".format(output_detail_dir)
  makedirs(output_detail_dir)


# Download some activities
print "Downloading activities from {0:%d %b %Y}".format(from_date)
acts = client.get_activities(after=from_date)

for act in acts:
    total += 1

    if act.type != "Run" or act.average_heartrate is None:
        continue

    count += 1

    # Get the full data streams
    streams = client.get_activity_streams(act.id, types=stream_filter)
    sdf = pd.DataFrame(dict((stype, stream.data) for (stype, stream) in streams.iteritems()))

    if "latlng" in stream_filter:
      sdf["lat"] = [a[0] for a in sdf.latlng]
      sdf["lng"] = [a[1] for a in sdf.latlng]
      del sdf["latlng"]

    detail_fname = join(output_detail_dir, "{0}.json".format(act.id))
    sdf.to_json(detail_fname)

    # with open(join(output_detail_dir, "{0}.p".format(act.id)), "wb") as f:
    #     pickle.dump(sdf, f, 2)

    print "{0} on {1:%d %b %Y} [kudos {2}]".format(act.name, act.start_date, act.kudos_count)
    print "\tHR: {0}".format(act.average_heartrate)
    print "\tDistance: {0}".format(unithelper.miles(act.distance))
    print "\tSpeed: {0}".format(unithelper.mph(act.average_speed))

    item = dict((k, func(getattr(act, k, None))) for k, func in save_fields.iteritems())
    data.append(item)
    start_dates.append(act.start_date)

df = pd.DataFrame(data=data, index=start_dates)

summary_fname = join(output_dir, "summary.json")
df.to_json(summary_fname)

# with open(join(output_dir, "summary.p"), "wb") as f:
#     pickle.dump(df, f, 2)

df.to_csv(join(output_dir, "summary.csv"))

print "Found {0} suitable runs, out of {1}".format(count, total)
