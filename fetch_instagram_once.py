import os
import csv
import logging
import gspread
import pytz
from datetime import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials
import requests

# ====== 設定 ======
HASHTAG = '華蔵寺庭園'
INSTAGRAM_BUSINESS_ID = '17841413261363491'
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
CSV_PATH = 'log.csv'
SPREADSHEET_NAME = 'InstaContestLogTopMedia'
GOOGLE_CREDENTIALS_PATH = 'client_secret.json'
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
# ==================

logging.basicConfig(
    filename='log.txt',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
)
def log(message):
    print(message)
    logging.info(message)

def notify_slack(message):
    if not SLACK_WEBHOOK_URL:
        log("Slack URLが設定されていません")
        return
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json={'text': message})
        if response.status_code != 200:
            log(f"Slack通知エラー: {response.status_code} {response.text}")
    except Exception as e:
        log(f"Slack通知例外: {e}")

def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_PATH, scope)
    return gspread.authorize(creds)

def instagram_api(url):
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_hashtag_id():
    url = f"https://graph.facebook.com/v23.0/ig_hashtag_search?user_id={INSTAGRAM_BUSINESS_ID}&q={HASHTAG}&access_token={ACCESS_TOKEN}"
    data = instagram_api(url)
    hashtag_id = data['data'][0]['id']
    log(f"Hashtag ID for '{HASHTAG}' is {hashtag_id}")
    return hashtag_id

def get_hashtag_id_safe():
    try:
        return get_hashtag_id()
    except Exception as e:
        log(f"ハッシュタグ取得失敗（投稿ゼロの可能性）: {e}")
        return None

def fetch_posts():
    hashtag_id = get_hashtag_id_safe()
    if not hashtag_id:
        return []
    url = f"https://graph.facebook.com/v23.0/{hashtag_id}/top_media?user_id={INSTAGRAM_BUSINESS_ID}&fields=id,timestamp,media_url,like_count,comments_count,permalink,caption&limit=50&access_token={ACCESS_TOKEN}"
    posts = []
    while url:
        data = instagram_api(url)
        posts.extend(data.get('data', []))
        url = data.get('paging', {}).get('next')
    return posts

def load_existing_ids(sheet):
    """スプレッドシート上のIDを取得"""
    return set(sheet.col_values(3)[1:])  # 3列目がID

def save_to_gsheet(post, sheet, fetch_time):
    """画像は保存せず、media_urlだけスプレッドシートに追記"""
    timestamp_jst = dt.strptime(post['timestamp'], '%Y-%m-%dT%H:%M:%S%z').astimezone(pytz.timezone('Asia/Tokyo'))
    timestamp_str = timestamp_jst.strftime('%Y-%m-%d %H:%M:%S')
    # 4列目ファイル名は「post_id.jpeg」の形式で作る
    file_name = f"{post['id']}.jpeg"
    if sheet.row_count == 0 or sheet.cell(1,1).value is None:
        sheet.append_row(['fetch_time','timestamp','id','filename','like_count','comment_count','caption','permalink','media_url'])
    sheet.append_row([
        fetch_time,
        timestamp_str,
        post['id'],
        file_name,
        post.get('like_count',0),
        post.get('comments_count',0),
        post.get('caption',''),
        post['permalink'],
        post['media_url']
    ])
    log(f"→ スプレッドシートに保存: {post['id']}")

def job():
    log("===== ジョブ開始 =====")
    try:
        gc = get_gspread_client()
        sheet = gc.open(SPREADSHEET_NAME).sheet1
        posts = fetch_posts()
        if not posts:
            log("投稿なし")
            return
        existing_ids = load_existing_ids(sheet)
        fetch_time = dt.now(pytz.timezone('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
        new_count = 0
        for post in posts:
            if post['id'] in existing_ids:
                continue
            save_to_gsheet(post, sheet, fetch_time)
            new_count += 1
        log(f"===== ジョブ終了: 新規取得 {new_count} 件 =====")
        notify_slack(f"✅ Instagram Fetcher 完了！ 新規取得 {new_count} 件")
    except Exception as e:
        log(f"ジョブ処理中のエラー: {e}")
        notify_slack(f"❌ Instagram Fetcher エラー: {e}")

if __name__ == "__main__":
    log("Instagram Fetcher started.")
    notify_slack("🚀 Instagram Fetcher 起動しました！")
    job()
