import tweepy
import io
import config
import json
import psycopg2

### Connect to twitter API
auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_token, config.access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
client = tweepy.Client(consumer_key=config.consumer_key, consumer_secret=config.consumer_secret,access_token=config.access_token,access_token_secret=config.access_token_secret,wait_on_rate_limit=True)

##Get popular tweet 
popular_tweets = api.search_tweets(q='*', result_type='popular', count=5, lang='en')

##connect to postgresql
connection = psycopg2.connect(user="postgres",
                                  password="blueprint23",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
cursor = connection.cursor()

###insert data to postgresql
for i in popular_tweets:
    empty_json = {"id":i.id,"tweet":i.text, "retweet":i.retweet_count, "created_at":i.created_at}   
    query_sql = """INSERT INTO popular_tweets SELECT * from json_populate_recordset (NULL::popular_tweets, '{}');""".format(empty_json)
    cursor.execute(query_sql)

connection.commit()
connection.close()





