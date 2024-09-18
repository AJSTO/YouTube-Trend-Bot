# 🎬 Cloud Function `tweet_top_categories_weekly` to Tweet Weekly Top YouTube Categories

The `tweet_top_categories_weekly` Google Cloud Function retrieves the most popular YouTube categories in Poland over the past week, generates a word cloud, and posts it to Twitter. This function can be scheduled to run weekly using Google Cloud Scheduler.

### Overview
This function performs the following tasks:

* Queries a BigQuery dataset for the top YouTube video categories from the past week.
* Generates a word cloud based on the categories and their occurrences.
* Posts the word cloud to Twitter using the Twitter API.

### Prerequisites

Before deploying the function, ensure you have the following:

* **Google Cloud Project**: A Google Cloud Project with billing enabled.
* **BigQuery Dataset**: A BigQuery dataset with the necessary tables (yt_channel_info, categories_name, and yt_daily_top_videos).
* **Twitter Developer Account**: Twitter API credentials (API Key, API Key Secret, Access Token, Access Token Secret).
* **Google Cloud Storage** (optional, if you want to store images before tweeting).

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
gcloud functions deploy tweet_top_categories_weekly \
  --runtime python310 \
  --trigger-http \
  --region=us-central1 \
  --allow-unauthenticated
```

#### 3. Grant Invoke Permission

```bash
gcloud functions add-iam-policy-binding tweet_top_categories_weekly \
  --region="us-central1" \
  --member="SERVICE_ACCOUNT_NAME" \
  --role="roles/cloudfunctions.invoker"
```

#### 4. Grant Cloud Scheduler Permission

```bash
gcloud functions add-iam-policy-binding tweet_top_categories_weekly \
  --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
  --role="roles/cloudscheduler.admin" \
  --region=us-central1
```

#### 5. Schedule the Function with Cloud Scheduler

```bash
gcloud scheduler jobs create http tweet-top-categories-weekly \
  --schedule="0 12 * * 7" \  # Weekly schedule on Sunday at 12 AM
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/tweet_top_categories_weekly" \
  --http-method=POST \
  --time-zone="Europe/Warsaw"
```