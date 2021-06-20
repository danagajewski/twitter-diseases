import tweepy
import pandas as pd
import csv
import time

from updatedb import update_schema

api_key = 'xoKAcU26sn5h2cx3yVQXXCImd'
api_secret = '3J9Incp6bGEHgbgX4KyfM3smeoCbu8BF8quJq4m3T5xd0yac1d'
api_token = '4861209350-wCmUne0GhT1MzIYh3eBvKONKVRxkZwC66Wl3T4f'
api_token_secret = 'DInxBITXoHesAFnro327FT8FhROZhwDy5HeaVALTXcapF'

geo_dict = {'boston': '42.3601 71.0589,10km',
            'san_francisco': '37.7749, 122.4194, 10km'}


def scrape(db, words, date_since, numtweet, hashtag_id):
    ''' Function to scrape tweets from Twitter, and store unique results in the db

    :param db (DataFrame): Our storage, a dataframe containting all tweets saved
    :param words (string): The hashtag we want to search for this function
    :param date_since (str) (yyyy-mm-dd) : The day we want to search back to for our twitter scrape
    :param numtweet (int): number of tweets desired
    :return: Null - we will just be adding onto the given db file
    '''
    # using .Cursor() to search through twitter for the required tweets, allowing us to get more than 100
    tweets = tweepy.Cursor(api.search, q=words, lang="en",
                           since=date_since, tweet_mode='extended').items(numtweet)

    # .Cursor() returns an iterable object. Each item in
    # the iterator has various attributes that you can access to
    # get information about each tweet
    list_tweets = [tweet for tweet in tweets]

    # we will iterate over each tweet in the list for extracting information about each tweet into db
    for tweet in list_tweets:
        # first check if tweet is in db already, if it isnt - put it in our db
        #db = pd.DataFrame(columns=['username', 'description', 'text', 'hashtags', 'date', 'hashtag_pulled', 'tweet_id', 'disease'])
        username = tweet.user.screen_name
        description = tweet.user.description
        #location = tweet.user.location
        hashtags = tweet.entities['hashtags']
        date = tweet.created_at
        hashtag_pulled = words
        tweet_id = str(tweet.id_str)


        # Retweets can be distinguished by a retweeted_status attribute,
        # in case it is an invalid reference, except block will be executed
        try:
            text = tweet.retweeted_status.full_text
        except AttributeError:
            text = tweet.full_text

        # code for making hashtags into text , kept if needed
        hashtext = list()
        for j in range(0, len(hashtags)):
            hashtext.append(hashtags[j]['text'])

        # Here we are appending all the extracted information in the DataFrame
        # if the tweet has at least 1 hashtag
        if len(hashtags) > 0:
            ith_tweet = [username, description, text, date, hashtag_pulled, tweet_id, hashtag_id]
            db.loc[len(db)] = ith_tweet




def get_hashtags_from_file(filename):
    ''' Retrieve list of hashtags desired from file

    :param filename (str): Filename string to get desired list of hashtags
    :return hashtags_from_file (list): list of hashtags to be pulled
    '''

    file = open(filename, mode='r', encoding='utf-8-sig')
    csv_reader = csv.reader(file)

    hashtags_from_file = []

    for row in csv_reader:
        hashtags_from_file.append((row[0], row[1]))

    print(hashtags_from_file)

    return hashtags_from_file
# Enter your own credentials obtained
# from your developer account

api_key = 'xoKAcU26sn5h2cx3yVQXXCImd'
api_secret = '3J9Incp6bGEHgbgX4KyfM3smeoCbu8BF8quJq4m3T5xd0yac1d'
api_token = '4861209350-wCmUne0GhT1MzIYh3eBvKONKVRxkZwC66Wl3T4f'
api_token_secret = 'DInxBITXoHesAFnro327FT8FhROZhwDy5HeaVALTXcapF'



consumer_key = "xoKAcU26sn5h2cx3yVQXXCImd"
consumer_secret = "3J9Incp6bGEHgbgX4KyfM3smeoCbu8BF8quJq4m3T5xd0yac1d"
access_key = "4861209350-wCmUne0GhT1MzIYh3eBvKONKVRxkZwC66Wl3T4f"
access_secret = "DInxBITXoHesAFnro327FT8FhROZhwDy5HeaVALTXcapF"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)
filename = 'scraped_tweets.csv'



date_since = '2021-06-10'
# # number of tweets you want to extract in one run
numtweet = 1000
# scrape(db, words, date_since, numtweet)
# print('Scraping has completed!')


def generate_pull():

    hashtags_to_scrape = get_hashtags_from_file('/Users/danagajewski/Documents/Summer2021/'
                                                'CS3200/Project/Python/hashtags_to_pull.csv')

    for hashtag in hashtags_to_scrape:
        print(hashtag)
        db = pd.DataFrame(
            columns=['username', 'description', 'text', 'date', 'hashtag_pulled', 'tweet_id', 'hashtag_id'])
        scrape(db, hashtag[1], date_since, numtweet, hashtag[0])

        update_schema('twit', db)

        time.sleep(450)

        #append to end of csv

    db.to_csv(filename, a=True, index=False, header=False)

    return db

# we will save our database as a CSV file.
#db.to_csv(filename)

