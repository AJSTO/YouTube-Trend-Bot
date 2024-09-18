import functions_framework

import datetime
import requests
import os
from requests_oauthlib import OAuth1

from google.cloud import bigquery
from google.auth import default

POLISH_SYMBOLS = "ąćęłńóśźż"
ENGLISH_EQUIVALENTS = "acelnoszz"
UNBOLDED_SYMBOLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
BOLDED_SYMBOLS = "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"

API_URL = "https://api.twitter.com/2/tweets"

# Load Twitter API configuration from environment variables
API_KEY = os.getenv('API_KEY')
API_KEY_SECRET = os.getenv('API_KEY_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

# Load BigQuery configuration from environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_CHANNEL_INFO = os.getenv('TABLE_CHANNEL_INFO')
TABLE_CATEGORIES_NAME = os.getenv('TABLE_CATEGORIES_NAME')
TABLE_DAILY_TOP_VIDEOS = os.getenv('TABLE_DAILY_TOP_VIDEOS')


# Use Application Default Credentials (ADC)
credentials, project = default()
CLIENT_BQ = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def format_views(x):
    """
    Custom formatter function for number of views.

    Parameters:
        x (float): The tick value.

    Returns:
        str: The formatted views.
    """
    suffixes = ['', ' k', ' M', ' B']  # Suffixes for thousands, millions, billions
    suffix_idx = 0
    while abs(x) >= 1000 and suffix_idx < len(suffixes) - 1:
        x /= 1000.0
        suffix_idx += 1
    if x % 1 == 0:
        return f'{int(x):.0f}{suffixes[suffix_idx]}'
    else:
        return f'{x:.1f}{suffixes[suffix_idx]}'


@functions_framework.http
def tweet_daily_top(request):
    """
    Retrieve daily top videos from the database, generate a tweet with the top video details, and post it on Twitter.

    This function retrieves the daily top videos from the database and creates a tweet with the top video details,
    including the category name, video title, video ID, and number of views. The tweet is then posted on Twitter.

    Returns:
        str: A message indicating success or failure.
    """
    query = f"""
    SELECT * 
    FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_DAILY_TOP_VIDEOS}` AS dtv
    LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CATEGORIES_NAME}` AS cn
    ON CAST(dtv.video_category_id AS STRING) = CAST(cn.category_id AS STRING)
    LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}` AS ci
    ON CAST(ci.channel_id AS STRING) = CAST(dtv.channel_id AS STRING)
    WHERE 
    DATE(video_captured_at) = DATE('{(datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')}')
    AND default_audio_language = 'pl'
    """

    top_daily_query = CLIENT_BQ.query(query)
    df_top_daily = top_daily_query.to_dataframe()

    # Drop duplicate rows based on 'category_name' column and keep the first occurrence (highest value)
    df_top_daily = df_top_daily.drop_duplicates(subset='category_name', keep='first')
    df_top_daily = df_top_daily.sort_values('video_views', ascending=False)

    n = 1
    for index, row in df_top_daily.iterrows():
        tweet_output = f"""
#YT_DAILY_TOP w kategorii #{row['category_name'].replace(' ', '_').replace('&', 'and')}
Film: {row['video_title'].translate(str.maketrans(POLISH_SYMBOLS, ENGLISH_EQUIVALENTS)).translate(str.maketrans(UNBOLDED_SYMBOLS, BOLDED_SYMBOLS))}
Views: {format_views(row['video_views'])}
#youtube #top #{row['channel_name'].replace(' ', '_')}
https://www.youtube.com/watch?v={row['video_id']}
        """

        oauth = OAuth1(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        response = requests.post(API_URL, json={"text": tweet_output}, auth=oauth)

        if response.status_code == 201:
            print(f"{n}. tweet posted successfully!")
            n += 1
        else:
            print('Failed to post tweet:', response.status_code, response.text)

        if n > 6:
            break

    return "Completed tweet posting.", 200