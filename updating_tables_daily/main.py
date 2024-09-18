import functions_framework
import os
import time
import pandas as pd
import pandas_gbq
from datetime import datetime
from dateutil import parser

from google.cloud import bigquery
from googleapiclient.discovery import build
from google.auth import default

# Import your schema and method from your package
from yt_config.schemas import (
    CHANNEL_INFO_SCHEMA,
    CHANNEL_INFO_CLUSTERING,
    DAILY_TOP_VIDEOS_SCHEMA,
    DAILY_TOP_VIDEOS_CLUSTERING,
)
from yt_config.methods import convert_duration_to_seconds

# Load configuration from environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_CHANNEL_INFO = os.getenv('TABLE_CHANNEL_INFO')
TABLE_DAILY_TOP_VIDEOS = os.getenv('TABLE_DAILY_TOP_VIDEOS')

# Define constants
REGION_CODE = 'PL'
NUM_OF_TOP_VIDEOS_TO_RECEIVE = 100

# Use Application Default Credentials (ADC)
credentials, project = default()

CLIENT_BQ = bigquery.Client(credentials=credentials, project=PROJECT_ID)
CLIENT_YT = build("youtube", "v3", credentials=credentials)


# Function to fetch categories
def get_categories(region: str) -> pd.DataFrame:
    categories_response = CLIENT_YT.videoCategories().list(
        part='snippet',
        regionCode=region
    ).execute()

    categories_lst = []
    for category in categories_response['items']:
        category_id = category['id']
        category_name = category['snippet']['title']
        categories_lst.append((category_id, category_name))
    return pd.DataFrame(categories_lst, columns=['category_id', 'category_name'])


# Function to get top daily videos
def get_top_daily_videos(num_of_videos: int, region: str) -> pd.DataFrame:
    request = CLIENT_YT.videos().list(
        part="snippet,contentDetails,statistics",
        chart="mostPopular",
        regionCode=region,
        maxResults=num_of_videos
    )
    response = request.execute()
    items = response["items"]
    video_data = []
    for item in items:
        video_id = item["id"]
        kind = item["kind"]
        live_broadcast = item["snippet"]["liveBroadcastContent"]
        channel_id = item["snippet"]["channelId"]
        video_category_id = item["snippet"]["categoryId"]
        video_title = item["snippet"]["title"]
        video_description = item["snippet"]["description"]
        default_language = item["snippet"].get("defaultLanguage", "")
        default_audio_language = item["snippet"].get("defaultAudioLanguage", "")
        video_published = datetime.fromisoformat(item["snippet"]["publishedAt"][:-1])
        video_duration = convert_duration_to_seconds(item["contentDetails"]["duration"])
        video_views = int(item["statistics"]["viewCount"])
        video_likes = int(item["statistics"].get("likeCount", 0))
        video_comments = int(item["statistics"].get("commentCount", 0))

        video_data.append({
            "video_id": video_id,
            "kind": kind,
            "live_broadcast": live_broadcast,
            "channel_id": channel_id,
            "video_category_id": video_category_id,
            "video_title": video_title,
            "video_description": video_description,
            "default_language": default_language,
            "default_audio_language": default_audio_language,
            "video_published": video_published,
            "video_duration": video_duration,
            "video_views": video_views,
            "video_likes": video_likes,
            "video_comments": video_comments,
            "video_captured_at": pd.Timestamp.now().date(),
        })

    return pd.DataFrame(video_data)


# Function to get channel info
def get_channel_info(today_channel_ids: set):
    query = f"""
        SELECT DISTINCT
            channel_id
        FROM 
            `{PROJECT_ID}.{DATASET_NAME}.{TABLE_CHANNEL_INFO}`
        ;
        """
    channel_ids_from_bq = CLIENT_BQ.query(query)
    channel_id_set = set(row['channel_id'] for row in channel_ids_from_bq)
    channels_id = today_channel_ids.union(channel_id_set)

    channels_data = []
    for id in channels_id:
        request = CLIENT_YT.channels().list(part="snippet,statistics", id=id)
        response = request.execute()
        channel_info = response["items"][0]
        channel_name = channel_info['snippet']['title']
        channel_id = channel_info['id']
        kind = channel_info['kind']
        channel_published = parser.isoparse(channel_info['snippet']['publishedAt'])
        channel_logo_url = channel_info['snippet']['thumbnails']['medium']['url']
        total_views = int(channel_info['statistics']['viewCount'])
        channel_market = channel_info['snippet'].get("country", 'None')
        channel_subs = int(channel_info['statistics']['subscriberCount'])
        channel_videos = int(channel_info['statistics']['videoCount'])
        channel_description = channel_info['snippet']['description']

        channels_data.append({
            "channel_id": channel_id,
            "channel_name": channel_name,
            "kind": kind,
            "channel_published": channel_published,
            "channel_logo_url": channel_logo_url,
            "total_views": total_views,
            "channel_market": channel_market,
            "channel_subs": channel_subs,
            "channel_videos": channel_videos,
            "channel_description": channel_description,
            "updated_at": pd.Timestamp.now().date()
        })
    return pd.DataFrame(channels_data)


# Function to create BQ table if not exists
def create_bq_table(dataset_name: str, table_name: str, schema: list, clustering: list):
    try:
        CLIENT_BQ.get_table(f"{PROJECT_ID}.{dataset_name}.{table_name}")
    except:
        table = bigquery.Table(f"{PROJECT_ID}.{dataset_name}.{table_name}", schema=schema)
        table = CLIENT_BQ.create_table(table)
        table.clustering_fields = clustering


# Function to upload DataFrame to BQ
def upload_dataframe(df: pd.DataFrame, dataset_name: str, table_name: str, operation_type: str) -> None:
    pandas_gbq.to_gbq(
        df,
        f'{dataset_name}.{table_name}',
        project_id=PROJECT_ID,
        if_exists=operation_type,
        credentials=credentials,
    )


# Cloud Function entry point for HTTP requests
@functions_framework.http
def youtube_data_pipeline(request):
    """
    HTTP Cloud Function for executing the YouTube data pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        Response with execution time.
    """
    start_time = time.time()

    try:
        # Create or ensure the BigQuery tables exist
        create_bq_table(DATASET_NAME, TABLE_DAILY_TOP_VIDEOS, DAILY_TOP_VIDEOS_SCHEMA, DAILY_TOP_VIDEOS_CLUSTERING)
        top_daily_videos = get_top_daily_videos(NUM_OF_TOP_VIDEOS_TO_RECEIVE, REGION_CODE)
        upload_dataframe(top_daily_videos, DATASET_NAME, TABLE_DAILY_TOP_VIDEOS, "append")

        create_bq_table(DATASET_NAME, TABLE_CHANNEL_INFO, CHANNEL_INFO_SCHEMA, CHANNEL_INFO_CLUSTERING)
        channel_info = get_channel_info(set(top_daily_videos.channel_id))
        upload_dataframe(channel_info, DATASET_NAME, TABLE_CHANNEL_INFO, "append")

        end_time = time.time()
        elapsed_time = end_time - start_time
        return f"Data pipeline executed successfully in {elapsed_time:.2f} seconds.", 200
    except Exception as e:
        return f"Error during execution: {str(e)}", 500
