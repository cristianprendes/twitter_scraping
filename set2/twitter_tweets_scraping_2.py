import os
import csv
import time
import json
import requests
import random
import pandas as pd
from pathlib import Path
from random import randint
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine


try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

POSTGRESQL_USER = os.getenv("POSTGRESQL_USER")
POSTGRESQL_PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
POSTGRESQL_HOST_IP = os.getenv("POSTGRESQL_HOST_IP")
POSTGRESQL_PORT = os.getenv("POSTGRESQL_PORT")
POSTGRESQL_DATABASE = os.getenv("POSTGRESQL_DATABASE")

def get_connection():
    connect = create_engine(
        'postgresql://' + POSTGRESQL_USER + ':' + POSTGRESQL_PASSWORD + '@' + POSTGRESQL_HOST_IP + ':' + POSTGRESQL_PORT + '/' + POSTGRESQL_DATABASE,echo=False)
    return connect

def main():
  script_dir = os.path.dirname(os.path.abspath(__file__))
  csv_file_path = os.path.join(script_dir, 'twitter_account_ids_set2.csv')
  
  with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
      account_name = row['account_name']
      account_id = row['account_id']
      count = count+1

      retry_times = 0
      while retry_times < 3:
        try:
          url = f"https://twitter.com/i/api/graphql/9zyyd1hebl7oNWIPdA8HRw/UserTweets?variables=%7B%22userId%22%3A%22{account_id}%22%2C%22count%22%3A20%2C%22includePromotedContent%22%3Atrue%2C%22withQuickPromoteEligibilityTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Atrue%7D&features=%7B%22rweb_tipjar_consumption_enabled%22%3Atrue%2C%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Atrue%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22communities_web_enable_tweet_community_results_fetch%22%3Atrue%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22articles_preview_enabled%22%3Atrue%2C%22tweetypie_unmention_optimization_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Atrue%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22creator_subscriptions_quote_tweet_preview_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_media_interstitial_enabled%22%3Atrue%2C%22rweb_video_timestamps_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D&fieldToggles=%7B%22withArticlePlainText%22%3Afalse%7D"
          headers_1 = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,en-IN;q=0.8',
            'authorization': '',
            'content-type': 'application/json',
            'cookie': '',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
            'x-csrf-token': ''
          }
          headers_2 = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.7',
            'authorization': '',
            'content-type': 'application/json',
            'cookie': '',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'x-csrf-token': ''
          }
          headers_3 = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': '',
            'content-type': 'application/json',
            'cookie': '',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'x-csrf-token': ''
          }
          headers_4 = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': '',
            'content-type': 'application/json',
            'cookie': '',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'x-csrf-token': ''
          }
          
          headers_list = [headers_1,headers_2,headers_3,headers_4]
          header_index = (count // 50) % len(headers_list)
          headers = headers_list[header_index]

          proxy_list = ['country-us','country-gb','country-in']
          proxy_country = random.choice(proxy_list)
          proxy = os.getenv("DATACENTER_PROXY_US").replace('country-us',proxy_country)
          proxies = {
            'http':  proxy,
            'https': proxy
          }
          
          response = requests.request("GET", url, headers=headers, proxies=proxies.copy())
          print(f'{count} - {account_name} - status_code = {response.status_code} --> tweets API URL')  
          print(f'x-csrf-token = {headers.get("x-csrf-token")}')
          
          if response.status_code == 200:
            time.sleep(randint(1, 2))
            parse_tweets_details(response, account_name)
            break
          elif response.status_code == 429:
              time.sleep(120)
              retry_times = retry_times + 1
              print(f"retrying {retry_times} time due to 429 response")
              if retry_times >= 3:
                with open('error_429_tweet_accounts_2.txt','a') as f:
                  f.write(f'{response.status_code} status_code, {count} - {account_name} - {datetime.fromtimestamp(time.time())}'+'\n')
          else:
            with open('error_tweet_accounts_2.txt','a') as f:
              f.write(f'{response.status_code} status_code, {count} - {account_name} - {datetime.fromtimestamp(time.time())}'+'\n')
            break        

        except Exception as e:
          print(f'error --> {e}\n')
          time.sleep(10)
          retry_times = retry_times + 1
          print(f"retrying {retry_times} time due to exception")
          if retry_times >= 3:
            with open('error_tweet_accounts_exception_2.txt','a') as f:
              f.write(f'{count} - {account_name} - {datetime.fromtimestamp(time.time())}'+'\n')    

      if count < 1000 and count % 50 == 0:
        print(f"Reached {count} accounts, sleeping for 2 minutes...\n")
        time.sleep(120)

def parse_tweets_details(response, account_name):
  engine = get_connection()  
  json_data = json.loads(response.text)
  data_list = json_data.get('data',{}).get('user',{}).get('result',{}).get('timeline_v2',{}).get('timeline',{}).get('instructions',[])
  for data in data_list:
    if data.get('type') == "TimelineAddEntries":
      for entry in data.get('entries'):
        if "tweet" in entry.get('entryId') and not 'promoted-tweet' in entry.get('entryId'):
          created_datetime_str = entry.get('content',{}).get('itemContent',{}).get('tweet_results',{}).get('result',{}).get('legacy',{}).get('created_at','')
          if created_datetime_str:
            current_datetime = datetime.utcnow()
            created_datetime = datetime.strptime(created_datetime_str, '%a %b %d %H:%M:%S %z %Y')
            if created_datetime.date() == current_datetime.date():
              item = {}
              item['account_url'] = f'https://twitter.com/{account_name}'
              item['account_name'] = entry.get('content',{}).get('itemContent',{}).get('tweet_results',{}).get('result',{}).get('core',{}).get('user_results',{}).get('result',{}).get('legacy',{}).get('name','')
              item['user_name'] = account_name
              item['tweet_date'] = current_datetime.strftime('%Y-%m-%d')
              retweet = entry.get('content',{}).get('itemContent',{}).get('tweet_results',{}).get('result',{}).get('legacy',{}).get('retweeted_status_result',{})
              if retweet:
                item['tweet'] = retweet.get('result',{}).get('legacy',{}).get('full_text','')
              else:  
                item['tweet'] = entry.get('content',{}).get('itemContent',{}).get('tweet_results',{}).get('result',{}).get('legacy',{}).get('full_text','')
              item['created_datetime'] = created_datetime_str
              item['inserted_datetime'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

              df = pd.DataFrame([item])
              print(f'Pushing {account_name} tweet into database')
              df.to_sql('twitterfeed_tweets_content_stgg', schema ='public', con=engine, chunksize=100, method='multi', index=False, if_exists='append')
            else:
              continue     

if __name__ == '__main__':
  session_start_time = datetime.fromtimestamp(time.time())
  print(f"session_start_time = {session_start_time}")
  main()
  session_end_time = datetime.fromtimestamp(time.time())
  print(f"session_end_time = {session_end_time}")
  print(f"total_time = {session_end_time - session_start_time}")
  print("-----------------------------------------------------\n")