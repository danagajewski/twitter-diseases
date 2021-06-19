import time
from scrapetwitter import generate_pull

def scheduler():
    while True:
        time.sleep(900)
        generate_pull()
        time.sleep(120)
