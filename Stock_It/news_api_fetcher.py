import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from config import Config

class NewsAPIFetcher:
    def __init__(self):
        Config.validate_keys()
        self.api_key = Config.NEWS_API_KEY
        self.base_url = Config.NEWS_API_BASE_URL
    
    def get_everything_news(self, query, from_date=None, to_date=None, 
                           sources=None, language='en', sort_by='publishedAt', page_size=100):
        url = f"{self.base_url}/everything"
        
        params = {
            'q': query,
            'apiKey': self.api_key,
            'language': language,
            'sortBy': sort_by,
            'pageSize': page_size
        }
        
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        if sources:
            params['sources'] = sources
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != 'ok':
                print(f"API Error: {data.get('message', 'Unknown error')}")
                return None
            
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
    
    def get_stock_related_news(self, stock_symbol, company_name=None, days_back=7):
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = f'"{stock_symbol}" OR "{company_name}"' if company_name else f'"{stock_symbol}"'
        sources = Config.DEFAULT_NEWS_SOURCES
        
        return self.get_everything_news(
            query=query,
            from_date=from_date,
            to_date=to_date,
            sources=sources,
            sort_by='publishedAt'
        )
    
    def get_top_headlines(self, category='business', country='us', sources=None, page_size=100):
        url = f"{self.base_url}/top-headlines"
        
        params = {
            'apiKey': self.api_key,
            'pageSize': page_size
        }
        
        if category:
            params['category'] = category
        if country:
            params['country'] = country
        if sources:
            params['sources'] = sources
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != 'ok':
                print(f"API Error: {data.get('message', 'Unknown error')}")
                return None
            
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
    
    def format_news_to_dataframe(self, news_data):
        if not news_data or 'articles' not in news_data:
            return None
        
        articles = news_data['articles']
        if not articles:
            return None
        
        formatted_articles = []
        for article in articles:
            formatted_article = {
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'published_at': article.get('publishedAt', ''),
                'source_name': article.get('source', {}).get('name', ''),
                'author': article.get('author', ''),
                'content': article.get('content', '')
            }
            formatted_articles.append(formatted_article)
        
        return pd.DataFrame(formatted_articles)
    
    def save_news_to_csv(self, news_df, filename):
        try:
            news_df.to_csv(filename, index=False)
            print(f"News data saved to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    def get_financial_market_news(self, days_back=3):
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        query = 'stock market OR financial OR economy OR trading OR investment'
        sources = Config.DEFAULT_NEWS_SOURCES
        
        return self.get_everything_news(
            query=query,
            from_date=from_date,
            to_date=to_date,
            sources=sources,
            sort_by='publishedAt'
        )

if __name__ == "__main__":
    news_fetcher = NewsAPIFetcher()
    
    stock_symbol = "AAPL"
    company_name = "Apple"
    
    print(f"Fetching news for {stock_symbol} ({company_name})...")
    stock_news = news_fetcher.get_stock_related_news(stock_symbol, company_name, days_back=7)
    
    if stock_news:
        news_df = news_fetcher.format_news_to_dataframe(stock_news)
        if news_df is not None and not news_df.empty:
            print(f"Found {len(news_df)} articles")
            print("\nLatest headlines:")
            for idx, row in news_df.head().iterrows():
                print(f"- {row['title']}")
            
            news_fetcher.save_news_to_csv(news_df, f"{stock_symbol}_news.csv")
        else:
            print("No articles found or failed to format data")
    else:
        print("Failed to fetch news data")
    
    print("\nFetching general financial market news...")
    market_news = news_fetcher.get_financial_market_news(days_back=3)
    
    if market_news:
        market_df = news_fetcher.format_news_to_dataframe(market_news)
        if market_df is not None and not market_df.empty:
            print(f"Found {len(market_df)} market news articles")
            news_fetcher.save_news_to_csv(market_df, "financial_market_news.csv")
        else:
            print("No market news found")
    else:
        print("Failed to fetch market news")