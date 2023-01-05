import calendar
import json
import os
import pytz
import re
import requests

from absl import app
from absl import flags
from datetime import datetime, timedelta

FLAGS = flags.FLAGS

# Globally defined
flags.DEFINE_string(
    "input_directory", "",
    help="Path to directory containing the Records.json file, default to current.")
flags.DEFINE_string(
    "output_directory", "",
    help="Where to output the results, default to input.")
flags.DEFINE_string(
    "start_date", "2022-01-01", 
    help="yyyy-mm-dd in UTC, default to 2022-01-01, doesn't check if the value makes sense.")
flags.DEFINE_string("end_date", None, 
    help="yyyy-mm-dd in UTC. default to none. doesn't check if the value makes sense.")
# TODO Use 'local' for time local to the GPS coordinates.
flags.DEFINE_string(
    "timezone", "US/Pacific",
    help="The timezone the output will convert to. Default US/Pacific.")
flags.DEFINE_integer(
    "accuracy", 50,
    help="Accuracy as reported by Google in meters. Integer. Default threshold 50m.")
flags.DEFINE_boolean(
    "create_dataset", False,
    help="Boolean. Whether to create a dataset in Mapbox. Requires --username and --token if True.")
flags.DEFINE_string(
    "username", None,
    help="Mapbox username.")
flags.DEFINE_string(
    "token", None,
    help="Mapbox secret token with Dataset write access.")

def main(argv):
  # Format input directory
  input_directory = ""
  if FLAGS.input_directory is not None:
    input_directory = FLAGS.input_directory
    if input_directory.endswith("/"):
      input_directory += "/"

  # Get start and end times
  start_date = datetime.strptime(FLAGS.start_date, "%Y-%m-%d")
  end_date = datetime.utcnow()
  if FLAGS.end_date is not None:
    end_date = datetime.strptime(FLAGS.end_date, "%Y-%m-%d")

  # Load the main location records
  with open(FLAGS.input_directory + "Records.json") as location_json:
    location_history = json.loads(location_json.read())

  # Load the semantic history
  semantic_directory = input_directory + "Semantic Location History/"
  semantic_histories = []
  semantic_years_list = os.listdir(semantic_directory)

  for year_folder in semantic_years_list:
    if (re.search("20[0-2][0-9]", year_folder) is None or 
        int(year_folder) < start_date.year or int(year_folder) > end_date.year):
      continue
    monthly_path = semantic_directory + year_folder
    monthly_json_list = os.listdir(monthly_path)
    for json_path in monthly_json_list:
      month = list(calendar.month_name).index(re.split(r"_|\.", json_path)[1].title())
      if ((int(year_folder) == start_date.year and month < start_date.month) or 
          (int(year_folder) == end_date.year and month > end_date.month)):
        continue
      with open(semantic_directory + year_folder + "/" + json_path) as semantic_json:
        semantic_histories.append(json.loads(semantic_json.read()))

  # Filter the semantic histories for Motorcycle entries
  journeys = []
  for semantic_history in semantic_histories:
    for timeline_event in semantic_history["timelineObjects"]:
      if "activitySegment" in timeline_event:
        activity = timeline_event["activitySegment"]
        if activity["activityType"] == "MOTORCYCLING":
          start = datetime.fromisoformat(activity["duration"]["startTimestamp"][:-1])
          end = datetime.fromisoformat(activity["duration"]["endTimestamp"][:-1])
          if (start < start_date or end > end_date):
            continue
          # Create a Journey for each entry
          journeys.append(dict(startTime=start, endTime=end, waypoints=[]))

  # Load new timezone so we don't use UTC
  utc_timezone = pytz.utc
  new_timezone = pytz.timezone("US/Pacific")

  # Get points from location history and append to the appropriate journey
  for location in location_history["locations"]:
    timestamp = datetime.fromisoformat(location["timestamp"][:-1])
    if (timestamp < start_date or timestamp > end_date or
        ("deviceTag" in location and location["deviceTag"] == 464913864) or
        "latitudeE7" not in location or "longitudeE7" not in location or
        "accuracy" not in location or location["accuracy"] > FLAGS.accuracy):
      continue
    for journey in journeys:
      if (timestamp > (journey["startTime"] + timedelta(minutes=-5)) and
          timestamp < (journey["endTime"] + timedelta(minutes=5))):
        zoned_time = utc_timezone.localize(timestamp).astimezone(new_timezone) 
        latlon = dict(
            lat=location["latitudeE7"] / 10000000,
            lon=location["longitudeE7"] / 10000000,
            time=zoned_time)
        journey["waypoints"].append(latlon)

  # Sort journeys by time
  journeys.sort(key=lambda item: item["startTime"])

  # Create features for upload
  features = []
  id = 1
  for journey in journeys:
    journey["waypoints"].sort(key=lambda item: item["time"])
    # Create coordinates
    coordinates = []
    for waypoint in journey["waypoints"]:
      coordinates.append([waypoint["lon"], waypoint["lat"]])
    feature = dict(
      id = str(id),
      type = "Feature",
      properties = dict(
        ranking = 0,
        startDate = utc_timezone.localize(journey["startTime"]).astimezone(new_timezone).isoformat(),
        endDate = utc_timezone.localize(journey["endTime"]).astimezone(new_timezone).isoformat()
        ),
      geometry = dict(
        type = "LineString",
        coordinates = coordinates
        )
      )
    features.append(feature)
    id += 1

  # Create the GeoJSON
  time_now = datetime.now().isoformat()
  geojson = dict(
    type = "FeatureCollection",
    name = "moto_routes_" + datetime.now().isoformat(),
    features = features
    )

  # Write the output to output directory
  with open(FLAGS.output_directory + "output.geojson", "w", encoding='utf-8') as f:
    json.dump(geojson, f, ensure_ascii=False, indent = 2)

  if FLAGS.create_dataset:
    if FLAGS.username is None or FLAGS.token is None:
      print("Dataset creation was requested but username and token were not provided.")
    else:
      base_url = 'https://api.mapbox.com/datasets/v1/' + FLAGS.username 
      headers = {'Content-Type': 'application/json'}
      creation_request = dict(
        name = "moto_routes_" + time_now,
        description = "Google location history exported motorcycle routes for " + FLAGS.username
        )
      create_result_raw = requests.post(base_url + "?access_token=" + FLAGS.token, data=str(creation_request), headers=headers)
      print(create_result_raw)
      create_result = create_result_raw.json()
      upload_url = base_url + '/' + create_result["id"] + '/features/'
      for feature in features:
        feature_data = json.dump(feature)
        upload_result = requests.post(upload_url + feature["id"] + + "?access_token=" + FLAGS.token, data=str(feature_data), headers=headers)


if __name__ == "__main__":
  # `app.run` calls `sys.exit`
  app.run(main)
