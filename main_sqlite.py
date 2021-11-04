import wikipedia
from bs4 import BeautifulSoup
import threading

from wikipedia.exceptions import WikipediaException
import database_sqlite as database
import datetime
import queue
import logging
import sys
from contextlib import suppress
import urllib3


def write_html(html, file_name):
    f = open(file_name, "w", encoding="utf-8")
    f.write(str(html))
    f.close()


def parse_page(page_name, database_q, page_q):

    if not database.page_exists(page_name) and not (0, page_name) in database_q.queue:
        database_q.put((0, page_name))

    page = None
    page_name_url = page_name.replace(" ", "_")
    # get page
    page = wikipedia.page(page_name_url, auto_suggest=False)

    # convert page to beautiful soup
    soup = BeautifulSoup(page.html(), features="html5lib")

    # isolate the infobox sub-lists from the page
    infobox = soup.find_all("td", {"class": "infobox-full-data"})

    # make sure this is correct and not missing edge cases
    if len(infobox) != 3:
        return

    influences = infobox[1]
    influenced = infobox[2]

    # TODO filter out help pages
    influenceslist = []
    for item in influences.find_all("a"):
        # check if it is not a citation
        if item.has_attr("title"):
            influenceslist.append(item["title"])
    # print(influencelist)

    influencedlist = []
    for item in influenced.find_all("a"):
        if item.has_attr("title"):
            influencedlist.append(item["title"])
    # print(influencedlist)

    for item in influencedlist:

        # might need locks if the scheduler terminates the thread in between queue puts
        if not database.page_exists(item) and not (0, item) in database_q.queue:
            database_q.put((0, item))
            page_q.put(item)

        if (
            not database.influenced_exist(page_name, item)
            and not (1, page_name, item) in database_q.queue
        ):
            database_q.put((1, page_name, item))

    for item in influenceslist:
        if not database.page_exists(item) and not (0, item) in database_q.queue:
            database_q.put((0, item))
            page_q.put(item)

        if (
            not database.influenced_exist(item, page_name)
            and not (1, item, page_name) in database_q.queue
        ):
            database_q.put((1, item, page_name))


"""Unfortunately it looks like sqlite3 does not support multiple concurrent database connections 
making writes, so only one worker can be used instead of a pool of workers. 
Not a big deal since the bottleneck is the wikipedia API requests. """


def database_worker(q):
    while True:
        try:
            item = q.get()
            logging.info(
                f"Started saving {item} with {q.unfinished_tasks} unfinished tasks"
            )
            if item[0] == 0 and not database.page_exists(item[1]):
                database.insert_page(item[1])
            elif item[0] == 1 and not database.influenced_exist(item[1], item[2]):
                database.insert_influenced(item[1], item[2])
            logging.info(
                f"Finished saving {item} with {q.unfinished_tasks} unfinished tasks"
            )
            q.task_done()
        except Exception as e:
            logging.exception(e)


def page_worker(database_q, page_q):
    while True:
        item = page_q.get()
        logging.info(
            f"Starting page: {item} with {page_q.unfinished_tasks} unfinished tasks"
        )
        try:
            parse_page(item, database_q, page_q)
        except (wikipedia.exceptions.PageError, KeyError) as e:
            logging.exception(e)
            continue
        except (WindowsError,) as e:
            logging.exception(e)
            page_q.put(item)
            continue
        except (
            ConnectionError,
            TimeoutError,
            urllib3.exceptions.NewConnectionError,
            urllib3.exceptions.MaxRetryError,
            urllib3.exceptions.TimeoutError,
            urllib3.exceptions.RequestError,
            urllib3.exceptions.PoolError,
            urllib3.exceptions.HTTPError,
            wikipedia.exceptions.HTTPTimeoutError,
        ) as e:
            logging.exception(e)
            page_q.put(item)
            continue
        logging.info(
            f"Finished page: {item}  with {page_q.unfinished_tasks} unfinished tasks"
        )
        page_q.task_done()


if __name__ == "__main__":

    if "init" in sys.argv:
        database.init()

    logging.basicConfig(
        filename="main.log", encoding="utf-8", filemode="w", level=logging.INFO
    )

    wikipedia.set_rate_limiting(True, min_wait=datetime.timedelta(0, 0, 500000))

    # Enable WAL
    with database.create_connection() as conn:
        conn.execute("pragma journal_mode=wal")

    database_q = queue.PriorityQueue()
    page_q = queue.Queue()

    threading.Thread(target=database_worker, args=(database_q,), daemon=True).start()

    # Seems to be near the wikipedia request limit due to multiple pages being able to be called
    for x in range(30):
        threading.Thread(
            target=page_worker,
            args=(
                database_q,
                page_q,
            ),
            daemon=True,
        ).start()

    parse_page("Plato", database_q, page_q)
    print("Running... This may take a while")
    while True:
        with page_q.mutex, database_q.mutex:
            if page_q.unfinished_tasks == 0 and database_q.unfinished_tasks == 0:
                break
        page_q.join()
        database_q.join()

    print("All work completed")
