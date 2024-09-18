# 🎬 Cloud Run Function `tweet_daily_top` to tweet daily top videos on Polish Youtube

Google Cloud Function `tweet_daily_top` retrieves the daily top videos from YouTube, generates a tweet with the video details, and posts it on Twitter. The function is scheduled to run daily via Google Cloud Scheduler.

### Overview
Function performs the following tasks:

* Queries a BigQuery dataset for the top YouTube videos from the previous day.
* Formats the video data and generates a tweet.
* Posts the tweet to Twitter using the Twitter API.

### Prerequisites

* **Google Cloud Project**: You need a Google Cloud Project with billing enabled.
* **BigQuery Dataset**: Your project must have the necessary BigQuery datasets and tables (yt_channel_info, categories_name, and yt_daily_top_videos).
* **Twitter Developer Account**: You need Twitter API credentials (API Key, API Key Secret, Access Token, Access Token Secret).

### Setup

#### 1. Configure Environment Variables

Set the necessary environment variables for your function. You can set these variables directly in your Google Cloud Function configuration or locally using a `.env` file.

```env
# Twitter API credentials
API_KEY=your_twitter_api_key
API_KEY_SECRET=your_twitter_api_key_secret
ACCESS_TOKEN=your_twitter_access_token
ACCESS_TOKEN_SECRET=your_twitter_access_token_secret

# Google Cloud Project and BigQuery details
PROJECT_ID=your_google_cloud_project_id
DATASET_NAME=your_bigquery_dataset_name
TABLE_CHANNEL_INFO=your_channel_info_table_name
TABLE_CATEGORIES_NAME=your_categories_table_name
TABLE_DAILY_TOP_VIDEOS=your_daily_top_videos_table_name
```

#### 2. Deploy the Google Cloud Function

```bash
gcloud functions deploy tweet_daily_top \
  --runtime python310 \
  --trigger-http \
  --region=us-central1 \
  --allow-unauthenticated
```

#### 3. Grant Invoke Permission

```bash
gcloud functions add-iam-policy-binding tweet_daily_top \
  --region="us-central1" \
  --member="SERVICE_ACCOUNT_NAME" \
  --role="roles/cloudfunctions.invoker"
```

#### 4. Grant Cloud Scheduler Permission

```bash
gcloud functions add-iam-policy-binding tweet_daily_top \
  --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
  --role="roles/cloudscheduler.admin" \
  --region=us-central1
```

#### 5. Schedule the Function with Cloud Scheduler

```bash
gcloud scheduler jobs create http tweet-daily-top \
  --schedule="YOUR_CRON_EXPRESSION" \
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/tweet_daily_top" \
  --http-method=POST \
  --time-zone="YOUR_TIME_ZONE"
```