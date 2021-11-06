import wikipedia
from bs4 import BeautifulSoup
import threading
import utils.database as db
import datetime
import queue
import logging
import urllib3


class scraper:
    def __init__(self, database):
        self.database_q = queue.PriorityQueue()
        self.parse_q = queue.Queue()
        self.database = database
        logging.basicConfig(
            filename="main.log", encoding="utf-8", filemode="w", level=logging.INFO
        )
        wikipedia.set_rate_limiting(True, min_wait=datetime.timedelta(0, 0, 1000000))

    # TODO handle wikipedia redirects, pages can be repeated on different names
    def parse_page_recursive(self, page_name):

        # if the page does not exists in the database or queue, add it to the queue
        if (
            not self.database.page_exists(page_name)
            and not (0, page_name) in self.database_q.queue
        ):
            self.database_q.put((0, page_name))

        # replace whitespace in the url with underscores
        page_name_url = page_name.replace(" ", "_")

        # get the wiki page
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
            # TODO check if it is not a citation
            if item.has_attr("title"):
                influenceslist.append(item["title"])

        influencedlist = []
        for item in influenced.find_all("a"):
            if item.has_attr("title"):
                influencedlist.append(item["title"])

        # might need locks if the scheduler terminates the thread in between queue puts
        for item in influencedlist:
            # if the page does not exists in the database or queue, add it to the queue, add the page to the parse queue
            if (
                not self.database.page_exists(item)
                and not (0, item) in self.database_q.queue
            ):
                self.database_q.put((0, item))
                self.parse_q.put(item)
            # if the edge does not exists in the database or queue, add it to the database queue
            if (
                not self.database.influenced_exist(page_name, item)
                and not (1, page_name, item) in self.database_q.queue
            ):
                self.database_q.put((1, page_name, item))

        for item in influenceslist:
            # if the page does not exists in the database or queue, add it to the queue, add the page to the parse queue
            if (
                not self.database.page_exists(item)
                and not (0, item) in self.database_q.queue
            ):
                self.database_q.put((0, item))
                self.parse_q.put(item)
            # if the edge does not exists in the database or queue, add it to the database queue
            if (
                not self.database.influenced_exist(item, page_name)
                and not (1, item, page_name) in self.database_q.queue
            ):
                self.database_q.put((1, item, page_name))

    """Unfortunately it looks like sqlite3 does not support multiple concurrent database connections 
    making writes, so only one worker can be used instead of a pool of workers. 
    Not a big deal since the bottleneck is the wikipedia API requests. """

    """Performance gains/loss for multithreading has not been tested. 
    However due to being I/O bound due to wikipedia api requests it should lead to a speed increase. """

    """TODO use async instead?"""

    def __database_worker(self):
        while True:
            try:
                item = self.database_q.get(timeout=30)
                logging.info(
                    f"Started saving {item} with {self.database_q.unfinished_tasks} unfinished tasks"
                )
                if item[0] == 0 and not self.database.page_exists(item[1]):
                    self.database.insert_page(item[1])
                elif item[0] == 1 and not self.database.influenced_exist(
                    item[1], item[2]
                ):
                    self.database.insert_influenced(item[1], item[2])
                self.database_q.task_done()
                logging.info(
                    f"Finished saving {item} with {self.database_q.unfinished_tasks} unfinished tasks"
                )
            except queue.Empty:
                return

    # Exceptions are caught here so that the page is added to the end of the queue, exceptions may be caused by rate limiting.
    def __page_worker(self, thread_id):
        while True:
            try:
                item = self.parse_q.get(timeout=30)
                logging.info(
                    f"Thread {thread_id} Starting page: {item} with {self.parse_q.unfinished_tasks} unfinished tasks"
                )
                self.parse_page_recursive(item)
            except queue.Empty:
                return
            except (wikipedia.exceptions.WikipediaException, KeyError) as e:
                logging.exception(e)
            except (WindowsError) as e:
                logging.exception(e)
                self.parse_q.put(item)
            except (
                urllib3.exceptions.NewConnectionError,
                urllib3.exceptions.MaxRetryError,
                urllib3.exceptions.TimeoutError,
                urllib3.exceptions.RequestError,
                urllib3.exceptions.PoolError,
                urllib3.exceptions.HTTPError,
                wikipedia.exceptions.HTTPTimeoutError,
                ConnectionError,
                TimeoutError,
            ) as e:
                logging.exception("Connection error:" + e)
                self.parse_q.put(item)
            self.parse_q.task_done()
            logging.info(
                f"Thread {thread_id} Finished page: {item}  with {self.parse_q.unfinished_tasks} unfinished tasks\n Queue contents:{self.parse_q.queue}"
            )

    def run(self):

        d = threading.Thread(target=self.__database_worker, args=(), daemon=True)
        d.start()

        # Seems to be near the wikipedia request limit
        threads = []
        for x in range(10):
            thread = threading.Thread(
                target=self.__page_worker,
                args=(x,),
                daemon=True,
            )
            threads.append(thread)
            thread.start()

        print("Running... This will take a while")

        self.parse_page_recursive("Plato")

        for (
            idx,
            thread,
        ) in enumerate(threads):
            print(idx)
            thread.join()

        print("Parsers done")
        d.join()
        print("Database done")
        print("All work completed")
