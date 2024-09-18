import functions_framework

import os
import re
import requests

import tweepy
from requests_oauthlib import OAuth1


import matplotlib.pyplot as plt
from wordcloud import WordCloud

from google.cloud import bigquery
from google.auth import default


# Load Twitter API configuration from environment variables
API_KEY = os.getenv('API_KEY')
API_KEY_SECRET = os.getenv('API_KEY_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

API_URL = "https://api.twitter.com/2/tweets"

# Load BigQuery configuration from environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_CHANNEL_INFO = os.getenv('TABLE_CHANNEL_INFO')
TABLE_CATEGORIES_NAME = os.getenv('TABLE_CATEGORIES_NAME')
TABLE_DAILY_TOP_VIDEOS = os.getenv('TABLE_DAILY_TOP_VIDEOS')


# Use Application Default Credentials (ADC)
credentials, project = default()
CLIENT_BQ = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def get_top_categories_weekly():
    """
    Retrieve the top categories based on their occurrences in the daily top videos dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the top categories and their corresponding occurrences.
    """
    query = f"""
    SELECT
        cn.category_name, count(*) as occurrences
    FROM
        `{PROJECT_ID}.{DATASET_NAME}.{TABLE_DAILY_TOP_VIDEOS}` as dtv
    LEFT JOIN
        `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CATEGORIES_NAME}` AS cn
    ON
        CAST(dtv.video_category_id AS STRING) = CAST(cn.category_id AS STRING)
    WHERE
        DATE(video_captured_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY
        cn.category_name;
    """

    top_categories = CLIENT_BQ.query(query)

    top_categories_df = top_categories.to_dataframe()

    top_categories_df = top_categories_df.sort_values('occurrences', ascending=False)

    return top_categories_df.dropna(subset=['category_name'])


def generate_categories_wordcloud(df):
    """
    Generate and save a word cloud of the categories based on their occurrences and add a title to the plot.

    Args:
        df (pandas.DataFrame): DataFrame containing the category name and occurrences.

    Returns:
        None
    """
    # Create a dictionary of word frequencies
    category_frequencies = dict(zip([category.replace('_', ' ') for category in df.category_name], df.occurrences))

    # Generate the word cloud with a specific background color and white text color
    wordcloud = WordCloud(
        background_color="#007ea7",  # Background color
        color_func=lambda *args, **kwargs: "#ffffff",  # Force word color to white
        random_state=42,
        # font_path=font_path,  # Remove font path if it's causing an error
    ).generate_from_frequencies(frequencies=category_frequencies)

    # Create a figure with a background color
    fig, ax = plt.subplots(figsize=(8, 6), facecolor='#007ea7')

    # Display the word cloud without axis and with adjusted spacing
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    plt.tight_layout()

    # Add the title to the word cloud with larger font size
    plt.title("Najpopularniejsze kategorie w tym tygodniu",
              fontsize=30,  # Larger font size
              color='#ccdbdc',
              fontweight='bold')  # Use default font for title

    # Save the word cloud as an image with the specified background color
    plt.savefig('categories_wordcloud.png', dpi=300, facecolor='#007ea7', bbox_inches='tight')

    # Show the plot
    plt.show()


def tweet_image(image_path, caption):
    """
    Uploads an image to Twitter and posts a tweet with the uploaded image and a caption.

    Args:
        image_path (str): The file path of the image to upload.
        caption (str): The text to include in the tweet.

    Returns:
        None
    """
    tweepy_auth = tweepy.OAuth1UserHandler(
        API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
    )
    tweepy_api = tweepy.API(tweepy_auth)
    post = tweepy_api.simple_upload(image_path)
    text = str(post)
    media_id = re.search("media_id=(.+?),", text).group(1)
    payload = {
        "media": {"media_ids": [media_id]},
        "text": caption
    }

    oauth = OAuth1(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    response = requests.post(API_URL, json=payload, auth=oauth)



@functions_framework.http
def hello_http(request):
    """
    HTTP Cloud Function to generate a word cloud of top YouTube categories and tweet it.

    Args:
        request (flask.Request): The request object for the HTTP function.

    Returns:
        flask.Response: A response confirming the success of the function.
    """
    try:
        # Get top categories from BigQuery
        df = get_top_categories_weekly()

        # Generate and save the word cloud
        generate_categories_wordcloud(df)

        # Tweet the generated word cloud image with a caption
        tweet_image("categories_wordcloud.png", "Najpopularniejsze kategorie na Polskim YT w tym tygodniu")

        # Return a success message as an HTTP response
        return "Word cloud generated and tweeted successfully", 200

    except Exception as e:
        # Return an error message and status code 500 for server errors
        return f"An error occurred: {str(e)}", 500
