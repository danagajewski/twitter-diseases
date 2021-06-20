import mysql.connector
from datetime import datetime
import random


def update_schema(schema, data):
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="22dollars",
        database=schema
    )

    twt_to_add = ''

    for idx, row in data.iterrows():

        new_timestamp = datetime.strftime(row[3],'%Y-%m-%d %H:%M:%S') # datetime.strptime(row[3],'%a %b %d %H:%M:%S +0000 %Y'),
        twt_to_add = twt_to_add + "({}, '{}', '{}', '{}', {}, '{}'),".format(row[6], row[2].replace("'", ""),
                                                                             new_timestamp, row[0],
                                                                             random.randint(1, 148), row[5])

    cursor = db.cursor()

    query1 = "insert into tweet (hashtag_id, text, datetime, username, location_id," \
             " twitter_ident) values {};".format(twt_to_add[:-1])

    cursor.execute(query1)

    db.commit()
