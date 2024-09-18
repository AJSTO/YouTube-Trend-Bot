"""
This file contains the schema definitions for BigQuery tables used in the project.

It defines the schema structures for the following tables:
- CHANNEL_INFO_SCHEMA: Schema for the channel information table.
- CATEGORIES_NAME_SCHEMA: Schema for the category name table.
- CHANNEL_CATEGORIES_SCHEMA: Schema for the channel categories table.
- DAILY_TOP_VIDEOS_SCHEMA: Schema for the daily top videos table.

Each schema is defined as a list of dictionaries, where each dictionary represents
a field in the table with properties such as name, type, and mode. The file also includes
definitions for clustering columns where applicable.

This file serves as a central reference for the schemas used in the project and provides
a clear structure for the data stored in the corresponding BigQuery tables.

Note: This file does not contain any executable code. It solely serves as a reference
for schema definitions and should be imported and used in other modules or scripts
where the schema information is required.
"""

CHANNEL_INFO_SCHEMA = [
    {"name": "channel_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "channel_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "kind", "type": "STRING", "mode": "REQUIRED"},
    {"name": "channel_published", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "channel_logo_url", "type": "STRING", "mode": "REQUIRED"},
    {"name": "total_views", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "channel_market", "type": "STRING", "mode": "REQUIRED"},
    {"name": "channel_subs", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "channel_videos", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "channel_description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "updated_at", "type": "DATE", "mode": "REQUIRED"},
]
CHANNEL_INFO_CLUSTERING = ["updated_at", ]

CATEGORIES_NAME_SCHEMA = [
    {"name": "category_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "category_name", "type": "STRING", "mode": "REQUIRED"},
]

CHANNEL_CATEGORIES_SCHEMA = [
    {"name": "channel_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "video_category_id", "type": "STRING", "mode": "REQUIRED"},
]
CHANNEL_CATEGORIES_CLUSTERING = ["channel_id", ]

DAILY_TOP_VIDEOS_SCHEMA = [
    {"name": "video_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "kind", "type": "STRING", "mode": "REQUIRED"},
    {"name": "live_broadcast", "type": "STRING", "mode": "REQUIRED"},
    {"name": "channel_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "video_category_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "video_title", "type": "STRING", "mode": "REQUIRED"},
    {"name": "video_description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "default_language", "type": "STRING", "mode": "REQUIRED"},
    {"name": "default_audio_language", "type": "STRING", "mode": "REQUIRED"},
    {"name": "video_published", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "video_duration", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "video_views", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "video_likes", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "video_comments", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "video_captured_at", "type": "DATE", "mode": "REQUIRED"},
]
DAILY_TOP_VIDEOS_CLUSTERING = ["channel_id", "video_category_id", "video_captured_at"]