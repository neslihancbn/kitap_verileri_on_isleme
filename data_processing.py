import pandas as pd
import os
import requests
import time
import random
from tqdm import tqdm
import re

# Gerekli kütüphanelerin kontrolü
try:
    from fuzzywuzzy import fuzz
except ImportError:
    import subprocess

    subprocess.run(["pip", "install", "fuzzywuzzy python-Levenshtein"], check=True)
    from fuzzywuzzy import fuzz

# -------------------- KONFİGÜRASYON --------------------
MAX_SUMMARIES = 1000  # Alınacak maksimum özet sayısı
REQUEST_DELAY = (0.5, 1.5)  # Min-max istek arası bekleme süresi (saniye)
TIMEOUT = 20  # API timeout süresi (saniye)
RETRY_COUNT = 3  # Başarısız istekler için yeniden deneme sayısı
RATE_LIMIT_RETRY = 60  # Rate limit için bekleyecek saniye


# -------------------------------------------------------

def log(message, message_type="INFO"):
    """Renkli ve formatlı log mesajları"""
    colors = {
        "INFO": "\033[94m", "SUCCESS": "\033[92m",
        "WARNING": "\033[93m", "ERROR": "\033[91m",
        "END": "\033[0m"
    }
    icons = {
        "INFO": "ℹ️", "SUCCESS": "✅",
        "WARNING": "⚠️", "ERROR": "❌"
    }
    print(f"{colors.get(message_type, '')}{icons.get(message_type, '')} {message}{colors['END']}")


def standardize_isbn(isbn):
    """ISBN'yi standart formata dönüştürür"""
    if pd.isna(isbn):
        return None
    return ''.join(filter(str.isdigit, str(isbn)))


def check_data_files(folder_path):
    """Gerekli veri dosyalarını kontrol eder"""
    required_files = ['books.csv', 'ratings.csv', 'book_tags.csv', 'tags.csv']
    missing_files = [f for f in required_files if not os.path.exists(os.path.join(folder_path, f))]

    if missing_files:
        log(f"Eksik dosyalar: {', '.join(missing_files)}", "ERROR")
        return False
    return True


def load_data(folder_path):
    """Veri setlerini yükler ve temel kontroller yapar"""
    try:
        if not check_data_files(folder_path):
            return None, None, None, None

        books_df = pd.read_csv(os.path.join(folder_path, 'books.csv'))
        ratings_df = pd.read_csv(os.path.join(folder_path, 'ratings.csv'))
        book_tags_df = pd.read_csv(os.path.join(folder_path, 'book_tags.csv'))
        tags_df = pd.read_csv(os.path.join(folder_path, 'tags.csv'))

        log("Veri setleri başarıyla yüklendi:", "SUCCESS")
        log(f"- Books: {len(books_df)} kayıt", "INFO")
        log(f"- Ratings: {len(ratings_df)} kayıt", "INFO")
        log(f"- Book Tags: {len(book_tags_df)} kayıt", "INFO")
        log(f"- Tags: {len(tags_df)} kayıt", "INFO")

        # ISBN standardizasyonu
        books_df['isbn'] = books_df['isbn13'].astype(str).apply(standardize_isbn)

        return books_df, ratings_df, book_tags_df, tags_df

    except Exception as e:
        log(f"Veri yükleme hatası: {str(e)}", "ERROR")
        return None, None, None, None


def clean_books_data(books_df):
    """Kitap verilerini temizler ve optimize eder"""
    try:
        cols_to_keep = ['book_id', 'title', 'authors', 'isbn', 'average_rating']
        books_clean = books_df[[col for col in cols_to_keep if col in books_df.columns]].copy()

        books_clean['authors'] = books_clean['authors'].str.lower().str.strip().fillna('unknown')
        books_clean['title'] = books_clean['title'].str.strip()
        books_clean['average_rating'] = pd.to_numeric(
            books_clean['average_rating'], errors='coerce').fillna(3.0)
        books_clean['isbn'] = books_clean['isbn'].apply(standardize_isbn)

        return books_clean.dropna(subset=['title', 'authors'])
    except Exception as e:
        log(f"Veri temizleme hatası: {str(e)}", "ERROR")
        return books_df


def add_tags_to_books(books_df, book_tags_df, tags_df):
    """Kitaplara etiket bilgilerini ekler"""
    try:
        book_tags_merged = pd.merge(book_tags_df, tags_df, on='tag_id', how='left')
        top_tags = (book_tags_merged.sort_values(['goodreads_book_id', 'count'],
                                                 ascending=[True, False])
                    .groupby('goodreads_book_id').head(5))

        top_tags['tags_combined'] = top_tags.groupby('goodreads_book_id')['tag_name'].transform(
            lambda x: ', '.join(x))

        tags_final = top_tags.drop_duplicates('goodreads_book_id')[
            ['goodreads_book_id', 'tags_combined']]

        books_with_tags = pd.merge(
            books_df, tags_final,
            left_on='book_id', right_on='goodreads_book_id', how='left')

        books_with_tags['main_genre'] = books_with_tags['tags_combined'].apply(
            lambda x: x.split(',')[0].strip() if isinstance(x, str) else 'Unknown')

        return books_with_tags.drop(columns=['goodreads_book_id'])
    except Exception as e:
        log(f"Etiket ekleme hatası: {str(e)}", "ERROR")
        return books_df


def fetch_by_isbn(isbn):
    """ISBN ile Open Library'den kitap bilgisi alır"""
    if not isbn or pd.isna(isbn):
        return None

    try:
        response = requests.get(
            f"https://openlibrary.org/isbn/{isbn}.json",
            timeout=TIMEOUT,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        )

        if response.status_code == 404:
            return None

        if response.status_code == 429:  # Rate limit
            log(f"Rate limit aşıldı. {RATE_LIMIT_RETRY} saniye bekleniyor...", "WARNING")
            time.sleep(RATE_LIMIT_RETRY)
            return fetch_by_isbn(isbn)  # Retry

        response.raise_for_status()

        data = response.json()
        description = data.get('description', '')

        # Farklı description formatlarını işle
        if isinstance(description, dict):
            description = description.get('value', '')
        elif isinstance(description, list):
            description = ' '.join(description)

        return description[:2000] if description else None

    except requests.exceptions.RequestException as e:
        if isinstance(e, requests.exceptions.Timeout):
            log(f"ISBN zaman aşımı: {isbn}", "WARNING")
        elif not isinstance(e, requests.exceptions.HTTPError):
            log(f"ISBN hatası ({isbn}): {str(e)}", "WARNING")
        return None


def fetch_book_summary(title, author=None, isbn=None):
    """Kitap özetini çeşitli yöntemlerle almaya çalışır"""
    # Önce ISBN ile deneyelim
    if isbn and pd.notna(isbn):
        isbn_result = fetch_by_isbn(isbn)
        if isbn_result:
            return isbn_result, 'isbn_search'

    # Open Library search API
    base_url = "https://openlibrary.org/search.json"
    params = {'title': title, 'fields': 'title,author_name,description', 'limit': 3}
    if author:
        params['author'] = author.split(',')[0].strip()

    best_match = None
    best_score = 0

    for attempt in range(RETRY_COUNT):
        try:
            time.sleep(random.uniform(*REQUEST_DELAY) * (attempt + 1))

            response = requests.get(
                base_url, params=params,
                timeout=TIMEOUT,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            )

            if response.status_code == 429:  # Rate limit
                log(f"Rate limit aşıldı. {RATE_LIMIT_RETRY} saniye bekleniyor...", "WARNING")
                time.sleep(RATE_LIMIT_RETRY)
                continue  # Retry

            if response.status_code == 404:
                continue
            response.raise_for_status()

            data = response.json()
            if data.get('numFound', 0) == 0:
                continue

            # En iyi eşleşmeyi bul
            for doc in data['docs']:
                current_title = doc.get('title', '')
                authors = doc.get('author_name', [])

                title_score = fuzz.ratio(title.lower(), current_title.lower())
                author_score = max([fuzz.ratio(author.lower(), a.lower())
                                    for a in authors], default=0) if author else 0
                combined_score = 0.7 * title_score + 0.3 * author_score

                if combined_score > best_score:
                    best_score = combined_score
                    best_match = doc

            # Kaliteli eşleşme bulunduysa
            if best_score >= 75 and best_match:
                description = best_match.get('description', '')

                if isinstance(description, dict):
                    description = description.get('value', '')
                elif isinstance(description, list):
                    description = ' '.join(description)

                if description:
                    return description[:2000], 'title_search'

        except requests.exceptions.RequestException as e:
            if attempt == RETRY_COUNT - 1:  # Son deneme
                if isinstance(e, requests.exceptions.Timeout):
                    log(f"'{title}' için zaman aşımı", "WARNING")
                elif isinstance(e, requests.exceptions.HTTPError):
                    if e.response.status_code != 404:
                        log(f"'{title}' için HTTP {e.response.status_code}", "WARNING")
            continue

    return None, 'not_found'


def process_book_summaries(books_df, cache_path, max_summaries=MAX_SUMMARIES):
    """Kitap özetlerini toplu şekilde işler ve kaldığı yerden devam eder"""
    # Cache dosyasını yükle veya oluştur
    if os.path.exists(cache_path):
        cache_df = pd.read_csv(cache_path)
        log(f"Önbellek dosyası bulundu: {len(cache_df)} kayıt", "INFO")
    else:
        cache_df = pd.DataFrame(columns=['book_id', 'title', 'authors', 'isbn', 'summary', 'source'])
        log("Yeni önbellek dosyası oluşturuldu", "INFO")

    # Mevcut özet sayısını kontrol et
    current_summary_count = len(cache_df)
    if current_summary_count >= max_summaries:
        log(f"Zaten {current_summary_count} özet mevcut (Maksimum: {max_summaries})", "INFO")
        return pd.merge(books_df, cache_df, on='book_id', how='left')

    # İşlenecek kitapları belirle
    remaining_summaries = max_summaries - current_summary_count
    books_to_process = books_df[~books_df['book_id'].isin(cache_df['book_id'])]
    books_to_process = books_to_process.sample(min(remaining_summaries, len(books_to_process)))

    log(f"Alınacak yeni özet sayısı: {len(books_to_process)}", "INFO")
    log(f"Toplam hedef: {current_summary_count + len(books_to_process)}/{max_summaries}", "INFO")

    new_entries = []

    # İlerleme çubuğu
    with tqdm(total=len(books_to_process), desc="📚 Özetler alınıyor", unit="kitap") as pbar:
        for _, row in books_to_process.iterrows():
            summary, source = fetch_book_summary(
                title=row['title'],
                author=row['authors'],
                isbn=row['isbn']
            )

            if summary:
                new_entries.append({
                    'book_id': row['book_id'],
                    'title': row['title'],
                    'authors': row['authors'],
                    'isbn': row['isbn'],
                    'summary': summary,
                    'source': source
                })

            pbar.update(1)

    # Yeni kayıtları ekle
    if new_entries:
        new_df = pd.DataFrame(new_entries)
        updated_cache = pd.concat([cache_df, new_df]).drop_duplicates('book_id')
        updated_cache.to_csv(cache_path, index=False)
        log(f"{len(new_df)} yeni özet eklendi", "SUCCESS")
    else:
        log("Yeni özet eklenmedi", "WARNING")

    # Tüm veriyi birleştir
    final_cache = pd.read_csv(cache_path)
    result = pd.merge(books_df, final_cache, on='book_id', how='left')

    # İstatistikleri göster
    log("\n📊 Özet Dağılımı:", "INFO")
    print(result['source'].value_counts(dropna=False))
    log(f"Toplam {len(final_cache)}/{max_summaries} özet alındı", "SUCCESS")

    return result


def save_results(final_books_df, folder_path):
    """Sonuçları kaydeder"""
    os.makedirs(folder_path, exist_ok=True)
    output_path = os.path.join(folder_path, 'books_with_summaries.csv')

    # Sadece özeti olanları kaydet
    books_with_summaries = final_books_df[final_books_df['summary'].notna()]
    books_with_summaries.to_csv(output_path, index=False)

    log(f"\n✅ Sonuçlar kaydedildi: {output_path}", "SUCCESS")
    log(f"📚 Toplam {len(books_with_summaries)} kitap kaydedildi", "INFO")


def main():
    # Yapılandırma
    DATA_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    CACHE_FILE = "book_summaries_cache.csv"

    # Klasör yoksa oluştur
    os.makedirs(DATA_FOLDER, exist_ok=True)

    log("Program başlatılıyor...", "INFO")

    # Verileri yükle
    books_df, _, book_tags_df, tags_df = load_data(DATA_FOLDER)
    if books_df is None:
        return

    # Verileri temizle
    books_clean = clean_books_data(books_df)
    books_with_tags = add_tags_to_books(books_clean, book_tags_df, tags_df)

    # Özetleri al (kaldığı yerden devam eder)
    cache_path = os.path.join(DATA_FOLDER, CACHE_FILE)
    final_books = process_book_summaries(books_with_tags, cache_path)

    # Sonucu kaydet
    save_results(final_books, DATA_FOLDER)


if __name__ == "__main__":
    main()