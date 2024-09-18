import os
import re
import time
import datetime
import ssl
import requests

import tweepy
from requests_oauthlib import OAuth1

import seaborn as sns
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib.ticker import FuncFormatter

from PIL import Image, ImageDraw

from google.cloud import bigquery
from google.auth import default

import functions_framework


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


def format_tick_labels(x, pos):
    """
    Custom formatter function for tick labels.

    Parameters:
        x (float): The tick value.
        pos (int): The tick position.

    Returns:
        str: The formatted tick label.
    """
    suffixes = ['', 'k', 'M', 'B']  # Suffixes for thousands, millions, billions
    suffix_idx = 0
    while abs(x) >= 1000 and suffix_idx < len(suffixes)-1:
        x /= 1000.0
        suffix_idx += 1
    if x % 1 == 0:
        return f'{int(x):.0f}{suffixes[suffix_idx]}'
    else:
        return f'{x:.2f}{suffixes[suffix_idx]}'


def download_image(url, channel_name):
    """
    Downloads an image from the specified URL and saves it locally with the channel name.

    Args:
        url (str): The URL of the image to download.
        channel_name (str): The name of the channel to use as the file name for the downloaded image.

    Returns:
        None: The image is saved to disk with the filename format '{channel_name}.jpg'.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    response = requests.get(url, verify=False)
    with open(f'{channel_name}.jpg', 'wb') as f:
        f.write(response.content)

def get_image(channel_name):
    """
    Loads an image from the local file system based on the channel name.

    Args:
        channel_name (str): The name of the channel used to locate the image file (expects '{channel_name}.jpg').

    Returns:
        np.ndarray: The image as a NumPy array.
    """
    path = f"{channel_name}.jpg"
    im = plt.imread(path)
    return im

def create_inscribed_circle_image(image):
    """
    Creates a circular (inscribed) version of the given square image, making non-circular areas transparent.

    Args:
        image (np.ndarray): The input image as a NumPy array in the format (height, width, channels).

    Returns:
        np.ndarray: A new image as a NumPy array with the circular region preserved and the non-circular region transparent.
    """
    height, width, _ = image.shape
    radius = min(height, width) // 2
    center = (width // 2, height // 2)

    # Sprawdzanie, czy obraz jest w zakresie [0, 1] i normalizacja
    if image.max() <= 1:
        image = (image * 255).astype(np.uint8)

    # Konwertowanie obrazu do obiektu PIL
    pil_image = Image.fromarray(image)
    mask = Image.new('L', (width, height), 0)
    draw = ImageDraw.Draw(mask)

    # Rysowanie koła na masce
    draw.ellipse((center[0] - radius, center[1] - radius, center[0] + radius, center[1] + radius), fill=255)

    # Tworzenie nowego obrazu z przezroczystością
    transparent_image = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    transparent_image.paste(pil_image.convert('RGBA'), mask=mask)

    # Konwertowanie z powrotem do tablicy NumPy
    circle_image = np.array(transparent_image)

    return circle_image

def offset_image(coord, channel_name, width_of_bar, ax):
    """
    Places an image next to a bar on a bar plot at a specific coordinate.

    Args:
        coord (float): The y-coordinate of the bar (e.g., index or position on the y-axis).
        channel_name (str): The name of the channel used to locate and load the image.
        width_of_bar (float): The width or position of the bar to determine where the image will be placed.
        ax (matplotlib.axes.Axes): The matplotlib axes object to which the image will be added.

    Returns:
        None: The image is added to the plot next to the bar.
    """
    img = get_image(channel_name)
    img = create_inscribed_circle_image(img)
    im = OffsetImage(img, zoom=0.15)
    im.image.axes = ax

    ab = AnnotationBbox(im, (0 + width_of_bar, coord), xybox=(0, 0.), frameon=False,
                        xycoords='data', boxcoords="offset points", pad=0)

    ax.add_artist(ab)


def get_top_views_increase():
    """
    Retrieve the top channels with the highest views increase in the current day compared to the previous day.

    Returns:
        pandas.core.frame.DataFrame: A DataFrame containing the top channels with their channel ID, name,
        logo URL, and views difference between the current day and the previous day.
    """
    query = f"""
    SELECT
      channel_id,
      channel_name,
      channel_logo_url,
      (
        SELECT MAX(total_views)
        FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}`
        WHERE channel_id = t.channel_id
          AND DATE(updated_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      ) - (
        SELECT MAX(total_views)
        FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}`
        WHERE channel_id = t.channel_id
          AND DATE(updated_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      ) AS views_difference
    FROM
      `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}` AS t
    WHERE
        channel_market = 'PL'
    GROUP BY
      channel_id, channel_name, channel_logo_url;
    """

    week_views_increase_query = CLIENT_BQ.query(query)
    week_views_increase_df = week_views_increase_query.to_dataframe()

    week_views_increase_df = week_views_increase_df.sort_values('views_difference', ascending=False)
    week_views_increase_df = week_views_increase_df[:5]

    return week_views_increase_df


def get_top_subs_increase():
    """
    Retrieve the top channels with the highest increase in subscriber count over the past 7 days.

    Returns:
        pandas.core.frame.DataFrame: A DataFrame containing the top channels with their channel ID, name,
        logo URL, subscriber count difference between the current day and 7 days ago, and the percentage increase
        in subscribers.
    """
    query = f"""
    SELECT
        channel_id,
        channel_name,
        channel_logo_url,
        (
            SELECT MAX(channel_subs)
            FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}`
            WHERE channel_id = t.channel_id
            AND DATE(updated_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        ) - (
            SELECT MAX(channel_subs)
            FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}`
            WHERE channel_id = t.channel_id
            AND DATE(updated_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ) AS subs_difference
    FROM
        `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}` AS t
    WHERE
        channel_market = 'PL'
    GROUP BY
        channel_id, channel_name, channel_logo_url;
    """

    week_subs_increase_query = CLIENT_BQ.query(query)
    week_subs_increase_df = week_subs_increase_query.to_dataframe()

    week_subs_increase_df = week_subs_increase_df.sort_values('subs_difference', ascending=False)
    week_subs_increase_df = week_subs_increase_df[:5]

    return week_subs_increase_df


def generate_views_barplot(df):
    """
    Generate a bar plot showing the highest weekly increase in views for each channel.

    Args:
        df (pandas.DataFrame): The DataFrame containing channel information with views difference.

    Returns:
        None
    """
    # Create images for youtube channels
    for index, row in df.iterrows():
        download_image(row['channel_logo_url'], row['channel_name'])

    # Background and color palette
    background_color = "#007ea7"  # Kolor tła
    sns.set_style("darkgrid", {"axes.facecolor": background_color})
    color_palette = sns.color_palette(["#003249"])

    # Font parameters
    plt.rcParams['font.family'] = 'monospace'
    plt.rcParams['font.size'] = 10
    plt.rcParams['text.color'] = '#ccdbdc'

    # Background color
    fig, ax = plt.subplots(figsize=(8, 6), facecolor=background_color)

    # Create bar plot
    ax = sns.barplot(x='views_difference', y='channel_name', data=df, palette=color_palette)

    # Set y ticks
    ax.set_yticks(ax.get_yticks())

    # Separate channel names
    labels = ax.get_yticklabels()
    new_labels = []
    for label in labels:
        words = label.get_text().split()
        new_label = '\n'.join(words)
        new_labels.append(new_label)

    # Set new label for y
    ax.set_yticklabels(new_labels, fontsize=8, fontweight='bold', color='#ccdbdc')

    # Set padding on y
    ax.tick_params(axis='y', which='major', pad=0)

    # Hide y axis name
    ax.set_ylabel('')

    # Hide x axis name
    ax.set_xlabel('')

    # X axis lim
    plt.xlim(0, max(list(df.views_difference)) * 1.15)

    # Add bolding to xlabel
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=10, fontweight='bold', color='#ccdbdc')

    formatter = FuncFormatter(format_tick_labels)
    ax.xaxis.set_major_formatter(formatter)

    # Delete chart frame
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Set grid color
    ax.xaxis.grid(True, color="#1a659e")

    # Set bar patches
    for patch in ax.patches:
        patch.set_edgecolor("#124559")
        patch.set_linewidth(1.2)


    # Set chart background color
    fig.patch.set_facecolor(background_color)


    # Pin youtube channel logo
    for i, (c, v) in enumerate(zip(list(df.channel_name), list(df.views_difference))):
        offset_image(i, c, v, ax)



    # Set chart title
    plt.title("Najwyższy tygodniowy wzrost wyświetleń", fontsize=16, fontweight='bold', color='#ccdbdc')

    # Save chart
    plt.savefig('barplot_views.png', dpi=300, bbox_inches='tight')

    # Close
    plt.close('all')



def generate_subs_barplot(df):
    """
    Generate a bar plot showing the highest weekly increase in subscribers for each channel.

    Args:
        df (pandas.DataFrame): The DataFrame containing channel information with subscriber difference.

    Returns:
        None
    """
    # Create images for youtube channels
    for index, row in df.iterrows():
        download_image(row['channel_logo_url'], row['channel_name'])

    # Background and color palette
    background_color = "#007ea7"  # Kolor tła
    sns.set_style("darkgrid", {"axes.facecolor": background_color})
    color_palette = sns.color_palette(["#003249"])

    # Font parameters
    plt.rcParams['font.family'] = 'monospace'
    plt.rcParams['font.size'] = 10
    plt.rcParams['text.color'] = '#ccdbdc'

    # Background color
    fig, ax = plt.subplots(figsize=(8, 6), facecolor=background_color)

    # Create bar plot
    ax = sns.barplot(x='subs_difference', y='channel_name', data=df, palette=color_palette)

    # Set y ticks
    ax.set_yticks(ax.get_yticks())

    # Separate channel names
    labels = ax.get_yticklabels()
    new_labels = []
    for label in labels:
        words = label.get_text().split()
        new_label = '\n'.join(words)
        new_labels.append(new_label)

    # Set new label for y
    ax.set_yticklabels(new_labels, fontsize=8, fontweight='bold', color='#ccdbdc')

    # Set padding on y
    ax.tick_params(axis='y', which='major', pad=0)

    # Hide y axis name
    ax.set_ylabel('')

    # Hide x axis name
    ax.set_xlabel('')

    # X axis lim
    plt.xlim(0, max(list(df.subs_difference)) * 1.15)

    # Add bolding to xlabel
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=10, fontweight='bold', color='#ccdbdc')

    formatter = FuncFormatter(format_tick_labels)
    ax.xaxis.set_major_formatter(formatter)

    # Delete chart frame
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Set grid color
    ax.xaxis.grid(True, color="#1a659e")

    # Set bar patches
    for patch in ax.patches:
        patch.set_edgecolor("#124559")
        patch.set_linewidth(1.2)


    # Set chart background color
    fig.patch.set_facecolor(background_color)


    # Pin youtube channel logo
    for i, (c, v) in enumerate(zip(list(df.channel_name), list(df.subs_difference))):
        offset_image(i, c, v, ax)



    # Set chart title
    plt.title("Najwyższy tygodniowy wzrost subskrybentów", fontsize=16, fontweight='bold', color='#ccdbdc')

    # Save chart
    plt.savefig('barplot_subs.png', dpi=300, bbox_inches='tight')

    # Close
    plt.close('all')


def tweet_image(image_path, caption):
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
    HTTP Cloud Function to generate bar plots for the highest weekly growth in YouTube views and subscribers,
    and tweet the images.

    Args:
        request (flask.Request): The request object for the HTTP function.

    Returns:
        flask.Response: A response confirming the success of the function.
    """
    try:
        # Get top channels with the highest increase in views and subscribers
        df = get_top_views_increase()
        df1 = get_top_subs_increase()

        # Generate and save bar plots for views and subscribers growth
        generate_views_barplot(df)
        generate_subs_barplot(df1)

        # Define date range for the tweet caption (replace _date_range with your actual logic)
        today = datetime.date.today()

        week_before = today - datetime.timedelta(days=7)

        date_format = "%d.%m.%Y"
        _date_range = f"{week_before.strftime(date_format)} - {today.strftime(date_format)}"


        # Tweet the generated bar plot for top views increase
        tweet_image("barplot_views.png", f"Najwyższy tygodniowy wzrost wyświetleń na Polskim YT ({_date_range})")

        # Tweet the generated bar plot for top subscribers increase
        tweet_image("barplot_subs.png", f"Najwyższy tygodniowy wzrost subskrybentów na Polskim YT ({_date_range})")

        # Return a success message as an HTTP response
        return "Bar plots generated and tweeted successfully", 200

    except Exception as e:
        # Return an error message and status code 500 for server errors
        return f"An error occurred: {str(e)}", 500

        return "Word cloud generated and tweeted successfully", 200

    except Exception as e:
        # Return an error message and status code 500 for server errors
        return f"An error occurred: {str(e)}", 500