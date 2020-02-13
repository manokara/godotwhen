#!/usr/bin/env python3
import pystache as mustache
from urllib.request import urlopen
import flask
import json
from random import random
import time
import datetime
import threading
import os

from common import eprint, MAX_COUNT
import database


def get_port():
    try:
        port = int(os.environ["PORT"])

        if port > 0xFFFF:
            raise ValueError

        if port < 0:
            raise ValueError

        return port
    except (KeyError, ValueError):
        return 5000


MINUTE = 60
HOUR = 60*MINUTE
DAY = HOUR*24
POOL_TIME = 6*HOUR
INFINITY = "∞"
DATE_FORMAT = "%B %d %Y"

PORT = get_port()
GODOTVER = "4.0"
MILESTONE = 9
API_URL = ("https://api.github.com/repos/godotengine/godot/"
           "milestones/{}".format(MILESTONE))
MOCK = "MOCK" in os.environ
MOCK_BUFFER = [
    (349, 3214),
    (350, 3216),
    (348, 3218),
    (346, 3220),
    (350, 3222),
    (352, 3224)
]
MOODS = [
    "gdthinking",
    "gdangry",
    "thonking",
]
# First index is mood
TITLES = [
    (0, "When Will Godot {} Release?"),
    (0, "Godot {}?"),
    (0, "How Long To Wait For Godot?"),
    (1, "GIVE ME GODOT {}!!!11"),
    (1, "GODOT. {}. WHEN."),
    (1, "I Want Godot {}!"),
    (2, "Will Godot {} Ever Be Finished?"),
    (2, "I Think Godot {} Is An Illusion"),
    (2, "Godot {}? What's That?"),
]

mock_index = 0
ut_buffer = []
update_run = True
update_timer = 0
update_thread = None
startup_date = datetime.date.today()
count_buffer = []
last_prediction = {
    "timestamp": startup_date,
    "issue_count": [0, 0],
    "predict": INFINITY,
    "date": INFINITY
}
__time = ["seconds", "minutes", "hours", "days", "months"]

with open("index.mustache") as f:
    template = f.read()


def fmt_time(seconds):
    result = seconds
    lt = [60, 60, 24, 30]
    level = 0

    try:
        while result//lt[level]:
            result /= lt[level]
            level += 1
    except IndexError:
        pass

    result = round(result)
    unit = __time[level]
    if result == 1:
        unit = unit[0:-1]

    return "{} {}".format(result, unit)


def get_milestone_data():
    try:
        with urlopen(API_URL) as response:
            return json.loads(response.read().decode("utf8"))
    except Exception as e:
        print("Get milestone error: {}".format(str(e)))

    return None


def calculate_time():
    last_group = count_buffer[0]
    deltas = [[], []]

    for cg in count_buffer[1:]:
        deltas[0].append(abs(cg[0]-last_group[0]))
        deltas[1].append(abs(cg[1]-last_group[1]))
        last_group = cg

    openi = int(sum(deltas[0])/len(count_buffer))
    closedi = int(sum(deltas[1])/len(count_buffer))
    openclose = abs(closedi-openi)

    if openclose > 0:
        predict = int((count_buffer[-1][0]+openclose)/openclose)*POOL_TIME
    else:
        predict = 0

    print("Diffs: {}, {}, {}".format(openclose, openi, closedi))
    print("Time: {}".format(fmt_time(predict)))

    if predict > 0:
        date = last_prediction["timestamp"]+datetime.timedelta(seconds=predict)
        date = date.strftime(DATE_FORMAT)
    else:
        date = INFINITY

    last_prediction["issue_count"] = [openi, closedi]
    last_prediction["predict"] = fmt_time(predict) if predict > 0 else INFINITY
    last_prediction["date"] = date


def create_app():
    app = flask.Flask(__name__)

    def thread_control():
        global update_timer

        print("Start update thread")
        while update_run:
            if update_timer == 0:
                update_prediction()

            time.sleep(1)
            update_timer += 1
            if update_timer == (5 if MOCK else POOL_TIME):
                update_timer = 0

        print("End update thread")

    def update_prediction():
        global thread_lock, last_prediction, count_buffer
        global mock_index

        print("Updating prediction")
        print("===================================")

        timestamp = datetime.datetime.utcfromtimestamp(time.time())
        if MOCK:
            print("Mock")
            milestone = {
                "open_issues": MOCK_BUFFER[mock_index][0],
                "closed_issues": MOCK_BUFFER[mock_index][1]
            }
            mock_index += 1
            if mock_index == len(MOCK_BUFFER):
                mock_index = 0
        else:
            milestone = get_milestone_data()
            if milestone is None:
                return
        issue_count = milestone["open_issues"], milestone["closed_issues"]
        print("Got milestone data: {}".format(issue_count))

        count_buffer.append(issue_count)

        if database.CONNECTION:
            database.store_count(*issue_count)

        if len(count_buffer) > MAX_COUNT:
            count_buffer.pop(0)

        print("Buffer size: {}".format(len(count_buffer)))
        last_prediction["timestamp"] = timestamp

        if len(count_buffer) >= 2:
            calculate_time()

        print("===================================")

    @app.route("/")
    def hello():
        k = int(random()*len(TITLES)-1)
        title = TITLES[k]
        mood = MOODS[title[0]]
        title = title[1]

        if "{}" in title:
            title = title.format(GODOTVER)

        ctx = {
            "mood": mood,
            "title": title,
            "timespan": fmt_time(POOL_TIME),
            "open_issues": last_prediction["issue_count"][0],
            "closed_issues": last_prediction["issue_count"][1],
            "predict": last_prediction["predict"],
            "date": last_prediction["date"],
            "version": GODOTVER,
        }

        return mustache.render(template, ctx)

    @app.route("/date")
    def date():
        return last_prediction["date"]

    global update_thread
    update_thread = threading.Thread(target=thread_control)
    update_thread.start()

    return app


def main():
    print("Buffer capacity: {}".format(MAX_COUNT))
    print("Interval: {}".format(fmt_time(POOL_TIME)))

    try:
        try:
            global count_buffer

            db_url = os.environ["DATABASE_URL"]
            database.connect(db_url)
            count_buffer = list(database.fetch_counts())
        except KeyError:
            print("DATABASE_URL not set, not connecting to DB...")
        except ValueError:
            eprint("ERROR: Invalid format for DATABASE_URL")
            return

        create_app().run(host="0.0.0.0", port=PORT)
        print("Finished run")
    except PermissionError:
        eprint("ERROR: Permission denied while binding to port {}"
               .format(PORT))
    finally:
        global update_run

        update_run = False
        update_thread.join()


if __name__ == "__main__":
    main()
