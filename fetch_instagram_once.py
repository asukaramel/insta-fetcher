import requests
import os
import csv
import gspread
import logging
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime as dt
import pytz
import time
import re

# ====== 設定項目 ======
HASHTAG = '内々神社'
INSTAGRAM_BUSINESS_ID = '17841413261363491'
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
CSV_PATH = 'log.csv'
SPREADSHEET_NAME = 'InstaContestLogTopMedia'
GOOGLE_CREDENTIALS_PATH = 'client_secret.json'
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
# =======================

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
    payload = {'text': message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
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
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        log(f"HTTP Error {response.status_code} : {response.text}")
        raise
    except Exception as e:
        log(f"Unexpected error: {e}")
        raise

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

    url = (
        f"https://graph.facebook.com/v23.0/{hashtag_id}/top_media"
        f"?user_id={INSTAGRAM_BUSINESS_ID}&fields=id,timestamp,media_url,like_count,comments_count,permalink,caption"
        f"&limit=50&access_token={ACCESS_TOKEN}"
    )
    posts = []
    while url:
        data = instagram_api(url)
        posts.extend(data.get('data', []))
        url = data.get('paging', {}).get('next')
        if url:
            time.sleep(1)
    return posts

def load_existing_ids():
    if not os.path.exists(CSV_PATH):
        return set()
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        return set(row[1] for row in reader)

def save_to_csv(post, file_name, timestamp_str, fetch_time):
    is_new = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(['fetch_time','timestamp', 'id', 'filename', 'like_count', 'comment_count', 'caption', 'permalink', 'media_url'])
        writer.writerow([
            fetch_time,
            timestamp_str,
            post['id'],
            file_name,
            post.get('like_count', 0),
            post.get('comments_count', 0),
            post.get('caption', ''),
            post['permalink'],
            post['media_url'],
        ])

def get_next_file_number(sheet):
    try:
        filenames = sheet.col_values(4)[1:]
        numbers = []
        for name in filenames:
            match = re.match(r'utsutsu_(\d+)\.jpeg', name)
            if match:
                numbers.append(int(match.group(1)))
        return max(numbers) + 1 if numbers else 1
    except Exception as e:
        log(f"スプレッドシートからファイル名取得失敗: {e}")
        return 1

def job():
    log("===== ジョブ開始 =====")
    try:
        posts = fetch_posts()
        if not posts:
            log("投稿がまだありません。")
            return

        existing_ids = load_existing_ids()
        gc = get_gspread_client()
        sheet = gc.open(SPREADSHEET_NAME).sheet1
        existing_ids_gsheet = set(sheet.col_values(3)[1:])
        file_counter = get_next_file_number(sheet)
        jst = pytz.timezone('Asia/Tokyo')
        fetch_time = dt.now(tz=jst).strftime('%Y-%m-%d %H:%M:%S')
        new_count = 0

        rows_to_append = []

        for post in posts:
            if post['id'] in existing_ids or post['id'] in existing_ids_gsheet:
                continue

            timestamp_utc = dt.strptime(post['timestamp'], '%Y-%m-%dT%H:%M:%S%z')
            timestamp_jst = timestamp_utc.astimezone(jst)
            timestamp_str = timestamp_jst.strftime('%Y-%m-%d %H:%M:%S')
            file_name = f'utsutsu_{file_counter}.jpeg'
            file_counter += 1

            # CSVは従来通り
            save_to_csv(post, file_name, timestamp_str, fetch_time)

            # Sheets用にまとめる
            rows_to_append.append([
                fetch_time,
                timestamp_str,
                post['id'],
                file_name,
                post.get('like_count', 0),
                post.get('comments_count', 0),
                post.get('caption', ''),
                post['permalink'],
                post['media_url'],
            ])
            new_count += 1

        # Sheetsにまとめて書き込み
        if rows_to_append:
            if sheet.row_count == 0 or sheet.cell(1,1).value is None:
                sheet.append_row(['fetch_time','timestamp','id','filename','like_count','comment_count','caption','permalink','media_url'])
            sheet.append_rows(rows_to_append)
            log(f"→ スプレッドシートにまとめて {len(rows_to_append)} 件保存")

        log(f"===== ジョブ終了: 新規取得 {new_count} 件 =====")
        notify_slack(f"✅ Instagram Fetcher 完了！ 新規取得 {new_count} 件 ( {dt.now().strftime('%Y-%m-%d %H:%M:%S')} )")

    except Exception as e:
        log(f"ジョブ処理中のエラー: {e}")
        notify_slack(f"❌ Instagram Fetcher エラー: {e}")

if __name__ == "__main__":
    log("Instagram Fetcher started.")
    notify_slack("🚀 Instagram Fetcher 起動しました！")
    job()
