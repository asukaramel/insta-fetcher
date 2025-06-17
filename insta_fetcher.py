import requests
import os
import csv
import datetime
import urllib.request
import gspread
import re
import logging
from oauth2client.service_account import ServiceAccountCredentials
import schedule
import time
from datetime import datetime as dt
import pytz

# ====== 設定項目 ======
HASHTAG = '徳川園'
INSTAGRAM_BUSINESS_ID = '17841413261363491'
ACCESS_TOKEN = 'EAAH9Mdud0HQBOZCXyCReEgIcpZAHTijRi4GGWyWZAKNBofp7kxjm3L6b4ZCWbFEbTXEHkd7RvnWYzIROHOzhyk5nOXyorNcmm6vjNHJapx1XibLNkW2uJJ9ZCgrysb0JkGM76V5V1UzOWKcmOW5UXU9UdroO8Sz9EgDKZCLELUjpIwjHXGvqvwzBZCwBLPNatT4XrZBti4Ai'
SAVE_DIR = '/Users/asuka_2752/Documents/INSTA/Images'  # ローカルの保存パス
CSV_PATH = '/Users/asuka_2752/Documents/INSTA/log.csv'
SPREADSHEET_NAME = 'InstaContestLog'
GOOGLE_CREDENTIALS_PATH = 'client_secret.json'  # 認証ファイルのパス
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'  # ← あなたのSlack Webhook URLにする
# =======================

# ✅ ログ設定
logging.basicConfig(
    filename='log.txt',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
)

def log(message):
    print(message)
    logging.info(message)

# ✅ Slack通知
def notify_slack(message):
    payload = {'text': message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code != 200:
            log(f"Slack通知エラー: {response.status_code} {response.text}")
    except Exception as e:
        log(f"Slack通知例外: {e}")

# （以降は同じ）
# Google Sheets 認証
def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    return gspread.authorize(creds)

# Instagram APIリクエスト
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

# ハッシュタグIDを取得
def get_hashtag_id():
    url = f"https://graph.facebook.com/v23.0/ig_hashtag_search?user_id={INSTAGRAM_BUSINESS_ID}&q={HASHTAG}&access_token={ACCESS_TOKEN}"
    data = instagram_api(url)
    hashtag_id = data['data'][0]['id']
    log(f"Hashtag ID for '{HASHTAG}' is {hashtag_id}")
    return hashtag_id


# 投稿データを全件取得（ページネーション対応）
def fetch_posts():
    hashtag_id = get_hashtag_id()
    url = (
        f"https://graph.facebook.com/v23.0/{hashtag_id}/recent_media"
        f"?user_id={INSTAGRAM_BUSINESS_ID}&fields=id,timestamp,media_url,like_count,comments_count,permalink,caption"
        f"&limit=50&access_token={ACCESS_TOKEN}"
    )
    posts = []
    while url:
        data = instagram_api(url)
        posts.extend(data['data'])
        url = data.get('paging', {}).get('next')
        time.sleep(1)  # ← ここで1秒待つ（必要に応じて秒数調整）
    return posts

# 画像を保存
def download_image(url, path):
    urllib.request.urlretrieve(url, path)

# CSV に記録済み ID をロード
def load_existing_ids():
    if not os.path.exists(CSV_PATH):
        return set()
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return set(row[1] for row in csv.reader(f))[1:]

# CSV に保存
def save_to_csv(post, file_name, timestamp_str):
    is_new = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(['timestamp', 'id', 'filename', 'like_count', 'comment_count', 'caption', 'permalink', 'media_url'])
        writer.writerow([
            timestamp_str,
            post['id'],
            file_name,
            post.get('like_count', 0),
            post.get('comments_count', 0),
            post.get('caption', ''),
            post['permalink'],
            post['media_url'],
        ])

# Google スプレッドシートに保存
def save_to_gsheet(post, file_name, timestamp_str, sheet):
    # ヘッダー行がなければ追加
    if sheet.row_count == 0 or sheet.cell(1, 1).value is None:
        sheet.append_row(['timestamp', 'id', 'filename', 'like_count', 'comment_count', 'caption', 'permalink', 'media_url'])

    # ※ existing_ids_gsheet のチェックは job() 側でやっているのでここは不要！
    sheet.append_row([
        timestamp_str,
        post['id'],
        file_name,
        post.get('like_count', 0),
        post.get('comments_count', 0),
        post.get('caption', ''),
        post['permalink'],
        post['media_url'],
    ])
    log(f"→ スプレッドシートに保存: {post['id']}")


# ✅ スプレッドシートから画像番号取得
def get_next_file_number_from_sheet(sheet):
    file_names = sheet.col_values(3)  # 3列目が filename 列
    numbers = []
    for name in file_names:
        match = re.match(r'tokugawa_(\d+)\.jpeg', name)
        if match:
            numbers.append(int(match.group(1)))
    return max(numbers) + 1 if numbers else 1

# メイン処理
def job():
    try:
        log("===== ジョブ開始 =====")
        os.makedirs(SAVE_DIR, exist_ok=True)
        existing_ids = load_existing_ids()
        posts = fetch_posts()
        gc = get_gspread_client()
        sheet = gc.open(SPREADSHEET_NAME).sheet1
        existing_ids_gsheet = sheet.col_values(2)
        file_counter = get_next_file_number()
        jst = pytz.timezone('Asia/Tokyo')
        new_count = 0

        for post in posts:
            if post['id'] in existing_ids or post['id'] in existing_ids_gsheet:
                continue

            timestamp_utc = dt.strptime(post['timestamp'], '%Y-%m-%dT%H:%M:%S%z')
            timestamp_jst = timestamp_utc.astimezone(jst)
            timestamp_str = timestamp_jst.strftime('%Y-%m-%d %H:%M:%S')
            file_name = f'tokugawa_{file_counter}.jpeg'
            file_counter += 1
            image_path = os.path.join(SAVE_DIR, file_name)
            download_image(post['media_url'], image_path)
            save_to_csv(post, file_name, timestamp_str)
            save_to_gsheet(post, file_name, timestamp_str, sheet)
            log(f"[NEW] {post['id']} → {file_name}")
            new_count += 1

        msg = f"✅ Instagram Fetcher 完了！ 新規取得 {new_count} 件 ({dt.now().strftime('%Y-%m-%d %H:%M:%S')})"
        log(msg)
        notify_slack(msg)
        log("===== ジョブ終了 =====")

    except Exception as e:
        error_message = f"❌ Instagram Fetcher エラー発生！\n{str(e)}\n{traceback.format_exc()}"
        log(error_message)
        notify_slack(error_message)

# スケジュール実行
schedule.every(6).hours.do(job)

if __name__ == "__main__":
    log("Instagram Fetcher started. Press Ctrl+C to stop.")
    notify_slack("🚀 Instagram Fetcher 起動しました！")
    job()  # 最初に一度実行
    while True:
        schedule.run_pending()
        time.sleep(60)