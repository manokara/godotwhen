import psycopg2
from glob import glob
from common import eprint, MAX_COUNT
from urllib.parse import urlparse

CONNECTION = None
QUERY_SELECT_MIGRATIONS = \
    "SELECT id, timestamp FROM godotwhen_migrations ORDER BY id DESC;"
QUERY_DELETE_MIGRATIONS = "DELETE FROM godotwhen_migrations WHERE id IN ({});"
QUERY_DROP_MIGRATIONS = "DROP TABLE IF EXISTS godotwhen_migrations;"
QUERY_CREATE_MIGRATIONS = \
    "CREATE TABLE IF NOT EXISTS godotwhen_migrations ( \
        id serial PRIMARY KEY NOT NULL, \
        timestamp varchar(12) NOT NULL \
    );"
QUERY_INSERT_MIGRATION = \
    "INSERT INTO godotwhen_migrations (timestamp) VALUES ('{}');"
QUERY_DELETE_MIGRATION = \
    "DELETE FROM godotwhen_migrations WHERE id = {};"
QUERY_SELECT_LAST_MIGRATION = \
    "SELECT id, timestamp FROM godotwhen_migrations ORDER BY id DESC LIMIT 1;"
QUERY_BUFFER_LENGTH = "SELECT COUNT(*) FROM godotwhen_issuecount;"
QUERY_INSERT_COUNT = \
    "INSERT INTO godotwhen_issuecount (open, closed) VALUES ({}, {});"
QUERY_TRIM_BUFFER = \
    "DELETE FROM godotwhen_issuecount WHERE id in ( \
        SELECT id FROM godotwhen_issuecount \
        ORDER BY id ASC \
        LIMIT {} \
    )"
QUERY_SELECT_BUFFER = "SELECT open, closed FROM godotwhen_issuecount;"


def validate_url(url):
    url = urlparse(url)

    try:
        assert url.scheme == "postgres"
        assert url.hostname is not None
        assert url.username is not None
        assert url.password is not None
        assert url.path or None is not None
        assert len(url.path) > 1

        return True
    except AssertionError:
        return False


def load_migration(timestamp, up=True):
    path = glob("migrations/{}-*/{}.sql".format(timestamp,
                                                "up" if up else "down"))[0]

    with open(path) as f:
        return f.read()


def connect(url):
    import psycopg2
    global CONNECTION

    try:
        if not validate_url(url):
            raise ValueError("Invalid database URL")

        url = urlparse(url)

        CONNECTION = psycopg2.connect(
            host=url.hostname,
            user=url.username,
            password=url.password,
            dbname=url.path.split("/")[1],
        )

        return True
    except psycopg2.DatabaseError as e:
        eprint("Error connecting: {}".format(str(e)))
        CONNECTION = None

        return False


def close():
    global CONNECTION
    assert CONNECTION is not None, "Connection is not defined"

    CONNECTION.close()
    CONNECTION = None


def init():
    global CONNECTION
    assert CONNECTION is not None, "Connection is not defined"

    try:
        with CONNECTION.cursor() as cursor:
            cursor.execute(QUERY_CREATE_MIGRATIONS)

            cursor.execute(QUERY_SELECT_MIGRATIONS)
            result = cursor.fetchall()

            if result:
                ids, timestamps = zip(*result)

                for timestamp in timestamps:
                    print("Migrating {} down...".format(timestamp))
                    query = load_migration(timestamp, False)
                    cursor.execute(query)

                cursor.execute(QUERY_DELETE_MIGRATIONS
                               .format(",".join(map(str, ids))))

            for path in sorted(glob("migrations/*/up.sql")):
                timestamp = path.split("/")[1][:12]

                with open(path) as f:
                    query = f.read()

                print("Migrating {} up".format(timestamp))
                cursor.execute(query)
                cursor.execute(QUERY_INSERT_MIGRATION.format(timestamp))

            CONNECTION.commit()

    except psycopg2.DatabaseError as e:
        eprint("Database error: {}".format(str(e)))
        CONNECTION = None


def store_count(opened, closed):
    global CONNECTION
    assert CONNECTION is not None, "Connection is not defined"

    try:
        with CONNECTION.cursor() as cursor:
            cursor.execute(QUERY_BUFFER_LENGTH)
            length = cursor.fetchone()[0]

            if length >= MAX_COUNT:
                diff = length - MAX_COUNT + 1
                cursor.execute(QUERY_TRIM_BUFFER.format(diff))

            cursor.execute(QUERY_INSERT_COUNT.format(opened, closed))
            CONNECTION.commit()
    except psycopg2.DatabaseError as e:
        eprint("Database error: {}".format(str(e)))
        CONNECTION = None


def fetch_counts():
    global CONNECTION
    assert CONNECTION is not None, "Connection is not defined"

    try:
        with CONNECTION.cursor() as cursor:
            cursor.execute(QUERY_SELECT_BUFFER)
            return cursor.fetchall()
    except psycopg2.DatabaseError as e:
        eprint("Database error: {}".format(str(e)))
        CONNECTION = None


def migrate_up():
    global CONNECTION
    assert CONNECTION is not None, "Connection is not defined"

    migrations = list(map(lambda m: m.split("/")[1][:12],
                      sorted(glob("migrations/*"))))

    try:
        with CONNECTION.cursor() as cursor:
            cursor.execute(QUERY_SELECT_LAST_MIGRATION)
            _, timestamp = cursor.fetchone()

            try:
                index = migrations.index(timestamp)

                if len(migrations) - 1 > index:
                    new_migration = migrations[index + 1]
                    query = load_migration(new_migration)

                    cursor.execute(query)
                    cursor.execute(QUERY_INSERT_MIGRATION
                                   .format(new_migration))
                    CONNECTION.commit()
                else:
                    print("No migrations available")
            except ValueError:
                eprint("Couldn't migrate: Current timestamp in database "
                       "doesn't exist! Fix the table manually")
                return
    except psycopg2.DatabaseError as e:
        eprint("Database error: {}".format(str(e)))
        CONNECTION = None


def migrate_down():
    global CONNECTION
    assert CONNECTION is not None, "Connection is not defined"

    migrations = list(map(lambda m: m.split("/")[1][:12],
                      sorted(glob("migrations/*"))))

    try:
        with CONNECTION.cursor() as cursor:
            cursor.execute(QUERY_SELECT_LAST_MIGRATION)
            id_, timestamp = cursor.fetchone()

            try:
                index = migrations.index(timestamp)

                if index > 0:
                    new_migration = migrations[index - 1]
                    query = load_migration(new_migration)

                    cursor.execute(query)
                    cursor.execute(QUERY_DELETE_MIGRATION.format(id_))
                    CONNECTION.commit()
                else:
                    print("No migrations available")

            except ValueError:
                eprint("Couldn't migrate: Current timestamp in database "
                       "doesn't exist! Fix the table manually")
                return
    except psycopg2.DatabaseError as e:
        eprint("Database error: {}".format(str(e)))
        CONNECTION = None


def main():
    from argparse import ArgumentParser
    import os

    try:
        DATABASE_URL = os.environ["DATABASE_URL"]

        if not validate_url(DATABASE_URL):
            raise ValueError
    except (KeyError, ValueError):
        eprint("DATABASE_URL invalid or not set")
        return

    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    _ = subparsers.add_parser("init")

    p_migrate = subparsers.add_parser("migrate")
    p_migrate.add_argument("direction", choices=["up", "down"])

    p_store = subparsers.add_parser("store")
    p_store.add_argument("opened", type=int)
    p_store.add_argument("closed", type=int)

    _ = subparsers.add_parser("fetch")

    res = parser.parse_args()
    connect(DATABASE_URL)

    if res.cmd == "init":
        init()
    elif res.cmd == "migrate":
        if res.direction == "up":
            migrate_up()
        else:
            migrate_down()
    elif res.cmd == "store":
        store_count(res.opened, res.closed)
    elif res.cmd == "fetch":
        counts = fetch_counts()
        print("\n".join(map(lambda x: "- " + str(x), counts)))


if __name__ == "__main__":
    main()
