import pystache as mustache
import urllib.request as urlreq
import flask
import json
import time
import datetime
import threading
import atexit

POOL_TIME = 60
INFINITY = "∞"
API_URL = "https://api.github.com/repos/godotengine/godot/milestones/4"
DATE_FORMAT = "%B %d %Y"
MAX_COUNT = 6

control_run = True
control_thread = None
update_thread = threading.Thread()
thread_lock = threading.Lock()
startup_date = datetime.date.today()
count_buffer = []
last_prediction = {
    "timestamp": startup_date,
    "issue_count": [0, 0],
    "hours": 0,
    "date": INFINITY
}


with open("index.mustache") as f:
    template = f.read()

def get_milestone_data():
    try:
        with urlreq.urlopen(API_URL) as response:
            return json.loads(response.read())
    except Exception as e:
        print("Get milestone error: {}".format(str(e)))

    return None

def calculate_hours():
    last_group = count_buffer[1]
    diffs = []
    deltas = []
    hours = 0

    for cg in count_buffer[1:]:
        op = cg[0]-last_group[0]
        closed = cg[1]-last_group[1]
        diffs.append([op, closed])
        deltas.append(closed*2-op) # Closed weighs 2
        last_group = cg

    avg = int(sum(deltas)/len(count_buffer))
    open_avg = int(sum([x[0] for x in diffs])/len(diffs))
    closed_avg = int(sum([x[1] for x in diffs])/len(diffs))
    print("Averages: {}, {}, {}".format(avg, open_avg, closed_avg))
    date = last_prediction["timestamp"]+datetime.timedelta(hours=hours)
    last_prediction["issue_count"] = [open_avg, closed_avg]
    last_prediction["hours"] = hours
    last_prediction["date"] = date.strftime(DATE_FORMAT)

def create_app():
    app = flask.Flask(__name__)

    def start_thread():
        global update_thread
        update_thread = threading.Timer(POOL_TIME, update_prediction, ())
        update_thread.start()
        print("Start update thread")

    def stop_thread():
        global update_thread, control_thread
        control_run = False
        update_thread.cancel()
        control_thread.join()
        print("Stopped threads")

    def thread_control():
        global update_thread, control_run
        print("Start control thread")
        while control_run:
            if not update_thread.is_alive():
                start_thread()
        print("End control thread")

    def update_prediction():
        global update_thread, thread_lock, last_prediction, count_buffer

        with thread_lock:
            print("Updating prediction")
            timestamp = datetime.datetime.utcfromtimestamp(time.time())
            milestone = get_milestone_data()
            if milestone is None: return
            issue_count = milestone["open_issues"], milestone["closed_issues"]
            print("Got milestone")

            count_buffer.append(issue_count)
            if len(count_buffer) > MAX_COUNT:
                count_buffer.pop(0)

            print("Buffer size: {}".format(len(count_buffer)))
            last_prediction["timestamp"] = timestamp
            if len(count_buffer) >= 2:
                calculate_hours()

    @app.route("/")
    def hello():
        global template
        ctx = {
            "open_issues": last_prediction["issue_count"][0],
            "closed_issues": last_prediction["issue_count"][1],
            "hours": last_prediction["hours"],
            "date": last_prediction["date"]
        }

        return mustache.render(template, ctx)

    @app.route("/date")
    def date():
        return ctx["date"]

    global control_thread, update_thread
    control_thread = threading.Thread(target=thread_control)
    control_thread.start()
    atexit.register(stop_thread)

    return app

create_app().run()
