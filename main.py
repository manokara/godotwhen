import pystache as mustache
import urllib.request as urlreq
import flask
import json
import time
import datetime
import threading
import atexit

POOL_TIME = 5
INFINITY = "∞"
API_URL = "https://api.github.com/repos/godotengine/godot/milestones/4"
DATE_FORMAT = "%B %d %Y"
MAX_COUNT = 30
MOCK_BUFFER = [
    (342, 3183),
    (346, 3192),
    (330, 3196),
    (360, 3207),
    (355, 3218),
    (360, 3230)
]

is_mock = True
mock_index = 0
update_run = True
update_timer = 0
update_thread = None
thread_lock = threading.Lock()
startup_date = datetime.date.today()
count_buffer = []
last_prediction = {
    "timestamp": startup_date,
    "issue_count": [0, 0],
    "days": 0,
    "date": INFINITY
}
__time = ["seconds", "minutes", "hours", "days"]


with open("index.mustache") as f:
    template = f.read()

def fmt_time(seconds):
    result = seconds
    level = 0

    while result//60:
        if level == 2:
            result //= 24
        elif level == 3:
            result //= 30
        else:
            result //= 60
        level += 1

    return "{} {}".format(result, __time[level])

def get_milestone_data():
    try:
        with urlreq.urlopen(API_URL) as response:
            return json.loads(response.read())
    except Exception as e:
        print("Get milestone error: {}".format(str(e)))

    return None

def calculate_days():
    last_group = count_buffer[0]
    deltas = [[], []]

    for cg in count_buffer[1:]:
        deltas[0].append(abs(cg[0]-last_group[0]))
        deltas[1].append(abs(cg[1]-last_group[1]))

    openi = int(sum(deltas[0])/len(count_buffer))
    closedi = int(sum(deltas[1])/len(count_buffer))
    openclose = closedi-openi
    days = int((count_buffer[-1][0]+openclose)/openclose)
    print("Diffs: {}, {}, {}".format(openclose, openi, closedi))
    print("Days: {}".format(days))
    
    if days > 0:
        date = last_prediction["timestamp"]+datetime.timedelta(days=days)
        date = date.strftime(DATE_FORMAT)
    else:
        date = INFINITY

    last_prediction["issue_count"] = [openi, closedi]
    last_prediction["days"] = days
    last_prediction["date"] = date

def create_app():
    app = flask.Flask(__name__)

    def stop_thread():
        global update_thread
        update_run = False
        print("Stopped update thread")

    def thread_control():
        global update_thread, update_timer

        print("Start update thread")
        while update_run:
            if update_timer == 0:
                update_prediction()

            time.sleep(1)
            update_timer += 1
            if update_timer == POOL_TIME:
                update_timer = 0

        print("End update thread")

    def update_prediction():
        global update_thread, thread_lock, last_prediction, count_buffer
        global mock_index

        print("Updating prediction")
        timestamp = datetime.datetime.utcfromtimestamp(time.time())
        if is_mock:
            print("Mock")
            milestone = {"open_issues": MOCK_BUFFER[mock_index][0], "closed_issues": MOCK_BUFFER[mock_index][1]}
            mock_index += 1
            if mock_index == len(MOCK_BUFFER):
                mock_index = 0
        else:
            milestone = get_milestone_data()
            if milestone is None: return
        issue_count = milestone["open_issues"], milestone["closed_issues"]
        print("Got milestone data: {}".format(issue_count))

        count_buffer.append(issue_count)
        if len(count_buffer) > MAX_COUNT:
            count_buffer.pop(0)

        print("Buffer size: {}".format(len(count_buffer)))
        last_prediction["timestamp"] = timestamp
        if len(count_buffer) >= 2:
            calculate_days()

    @app.route("/")
    def hello():
        global template
        ctx = {
            "open_issues": last_prediction["issue_count"][0],
            "closed_issues": last_prediction["issue_count"][1],
            "days": last_prediction["days"],
            "date": last_prediction["date"]
        }

        return mustache.render(template, ctx)

    @app.route("/date")
    def date():
        return last_prediction["date"]

    global update_thread
    update_thread = threading.Thread(target=thread_control)
    update_thread.start()
    atexit.register(stop_thread)

    return app

create_app().run()
