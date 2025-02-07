import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import time

def scrape_peninsula_news():
    base_url = "https://thepeninsulaqatar.com/category/qatar/news?page="
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    # Calculate date threshold (3 months ago)
    cutoff_date = datetime.now() - timedelta(days=90)
    articles = []
    page = 1
    stop_scraping = False

    while not stop_scraping:
        url = base_url + str(page)
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve page {page}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        news_items = soup.find_all('div', class_='col-sm-6 item')
        
        if not news_items:
            break

        for item in news_items:
            # Extract date
            date_span = item.find('span')
            if not date_span:
                continue
                
            date_str = date_span.text.split(' - ')[0].strip()
            try:
                article_date = datetime.strptime(date_str, "%d %b %Y")
            except ValueError:
                continue

            if article_date < cutoff_date:
                stop_scraping = True
                break

            # Extract other elements
            title = item.find('a', class_='title').text.strip()
            summary = item.find('p', class_='search').text.strip()
            article_url = item.find('a', class_='photo')['href']
            
            articles.append({
                'headline': title,
                'date': article_date.strftime("%Y-%m-%d"),
                'summary': summary,
                'url': f"https://thepeninsulaqatar.com{article_url}"
            })

        print(f"Processed page {page} - Found {len(articles)} articles so far")
        page += 1
        time.sleep(1)  # Be polite with delay between requests

    # Save to JSON
    with open('peninsula_news.json', 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    print(f"Scraping complete. Saved {len(articles)} articles to peninsula_news.json")

if __name__ == "__main__":
    scrape_peninsula_news()