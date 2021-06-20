import time
from scrapetwitter import generate_pull


def scheduler():
    while True:
        generate_pull()
        time.sleep(120)
