from numpy import full
import tweepy
import configparser
import pandas as pd
import mysql.connector


# read configs
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']

access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']


# authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)


API = tweepy.API(auth, wait_on_rate_limit=True)

# TWITTER_USER Table

NBA_sql = mysql.connector.connect(host = 'localhost', user = 'root',passwd = 'Vikrant@123', database = 'NBA')
mycursor = NBA_sql.cursor()

user_df = pd.read_csv('user_df.csv')

for i,row in user_df.iterrows():   
    sql = "INSERT INTO NBA.TWITTER_USER values (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql,tuple(row))
    print("Record inserted")
    NBA_sql.commit()

twitter_handles =['NBA','KingJames','StephenCurry30','kobebryant','ESPNNBA']

#Tweets_Details Table

main_df = pd.DataFrame(columns = ['tweet_id','twitter_handle','date_time','tweet_source', 'tweet_text' ])
for twitter_handle in twitter_handles:
  df = pd.DataFrame(columns = ['tweet_id','twitter_handle','date_time','tweet_source', 'tweet_text' ])
  tweets = API.user_timeline(screen_name=twitter_handle, 
                           # 200 is the maximum allowed count
                           count=10,
                           include_rts = True,
                           # Necessary to keep full_text 
                           # otherwise only the first 140 words are extracted
                           tweet_mode = 'extended'
                           )
  for i in range(len(tweets)):
    df.loc[i] = [tweets[i].id_str, twitter_handle, tweets[i].source, tweets[i].created_at, tweets[i].full_text]
  main_df = pd.concat([main_df, df], axis=0)



for i,row in main_df.iterrows():   
    sql = "INSERT INTO NBA.Tweets_Details values (%s,%s,%s,%s,%s)"
    mycursor.execute(sql,tuple(row))
    print("Record inserted")
    NBA_sql.commit()

main_df.to_csv(r'C:\Users\Vikrant Satish Pawar\Downloads\Tweepy\Tweets_Details.csv')

# tags table Table

hash_df = pd.DataFrame(columns = ['tweet_id','hashtags'])
for twitter_handle in twitter_handles:
  i=0
  df = pd.DataFrame(columns = ['tweet_id','hashtags'])
  tweets = API.user_timeline(screen_name=twitter_handle, 
                           count=10,
                           include_rts = True,
                           tweet_mode = 'extended'
                           )
  for i in range(len(tweets)):
    if not tweets[i].entities.get('hashtags'):
      tweet_hashtags = "None"
    else: 
      tag_dict = tweets[i].entities.get('hashtags')
      tweet_hashtags = tag_dict[0]['text']
    df.loc[i] = [tweets[i].id_str, tweet_hashtags]
  
    hash_df = pd.concat([hash_df, df], axis=0)

hash_df.reset_index(drop=True, inplace=True)

for i,row in hash_df.iterrows():   
    sql = "INSERT INTO NBA.Tweet_Tags values (%s,%s)"
    mycursor.execute(sql,tuple(row))
    print("Record inserted")
    NBA_sql.commit()

hash_df.to_csv(r'C:\Users\Vikrant Satish Pawar\Downloads\Tweepy\Tweet_Tags.csv')