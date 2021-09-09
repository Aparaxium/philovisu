import wikipedia
from bs4 import BeautifulSoup as bs
import threading
import database
import datetime

# TODO? parameterized queries might have fixed it?
# if a page contains ' escape it \'

# TODO gephi may not require node degrees

# TODO global hashmap instead of database?

def write_html(html, file_name):
    f = open(file_name, 'w', encoding="utf-8")
    f.write(str(html))
    f.close()

def parse_page(page_name):
    print(page_name)
    page = None

    if not database.check_page_exist(page_name):
        database.insert_page(page_name)
        #print("Added {} to database".format(page_name))

    try:
        # get page
        page = wikipedia.page(page_name, auto_suggest=False)
    except:
        return

    # convert page to beautiful soup
    soup = bs(page.html(), features="html5lib")

    # isolate the infobox sub-lists from the page
    infobox = soup.find_all("td", {"class": "infobox-full-data"})

    # make sure this is correct and not missing edge cases
    if len(infobox) != 3:
        return

    influences = infobox[1]
    influenced = infobox[2]

    '''
    #
    write_html('infobox.html', infobox)
    write_html('influences.html', influences)
    write_html('influenced.html', influenced)
    '''

    # TODO filter out help pages
    influencelist = []
    for item in influences.find_all("a"):
        # check if it is not a citation
        if item.has_attr('title'):
            influencelist.append(item["title"])
    # print(influencelist)

    influencedlist = []
    for item in influenced.find_all("a"):
        if item.has_attr('title'):
            influencedlist.append(item["title"])
    # print(influencedlist)

    for item in influencedlist:
        if not database.check_page_exist(item):
            database.insert_page(item)
        if not database.check_influenced_exist(page_name, item):
            database.insert_influenced(page_name, item)
            t1 = threading.Thread(target=parse_page, args=(item,))
            t1.start()

    for item in influencelist:
        if not database.check_page_exist(item):
            database.insert_page(item)
        if not database.check_influenced_exist(item, page_name):
            database.insert_influenced(item, page_name)
            t2 = threading.Thread(target=parse_page, args=(item,))
            t2.start()

    return

if __name__ == '__main__':
    # rate limited above wikipedia's recommendation just to be safe.
    wikipedia.set_rate_limiting(
        True, min_wait=datetime.timedelta(0, 0, 300000))
    parse_page("Plato")
