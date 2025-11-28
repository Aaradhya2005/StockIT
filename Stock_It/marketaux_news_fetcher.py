import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Optional, List, Dict

# Load environment variables
load_dotenv()

class MarketauxNewsFetcher:
    def __init__(self):
        self.api_key = os.getenv("MARKETAUX_API_KEY")
        self.base_url = "https://api.marketaux.com/v1/news/all"
        if not self.api_key:
            raise ValueError("❌ Missing MARKETAUX_API_KEY in .env file")

    def get_everything_news(self, query: str = None, from_date: str = None, to_date: str = None,
                           sources: str = None, language: str = 'en', sort_by: str = 'published_at',
                           page_size: int = 100) -> Optional[Dict]:
        """
        Get news articles matching a query (compatible with NewsAPIFetcher interface).
        For Marketaux, query can be a stock symbol or keywords.
        """
        try:
            params = {
                "api_token": self.api_key,
                "language": language,
                "limit": min(page_size, 100),  # Marketaux max is 100
            }
            
            # If query contains a stock symbol, use symbols parameter
            # Otherwise use keywords parameter
            if query:
                # Check if query looks like a stock symbol (uppercase, short)
                query_upper = query.strip().upper()
                if len(query_upper) <= 5 and query_upper.isalpha():
                    params["symbols"] = query_upper
                else:
                    params["keywords"] = query
            
            if from_date:
                params["published_after"] = from_date
            if to_date:
                params["published_before"] = to_date
            
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Format response to match NewsAPI structure for compatibility
            if "data" in data and data["data"]:
                formatted_data = {
                    "status": "ok",
                    "totalResults": len(data["data"]),
                    "articles": []
                }
                
                for article in data["data"]:
                    description = article.get("description", "") or ""
                    snippet = article.get("snippet", "") or ""
                    # Combine description and snippet for content
                    content = f"{description} {snippet}".strip() if description or snippet else ""
                    
                    formatted_article = {
                        "title": article.get("title", ""),
                        "description": description,
                        "url": article.get("url", ""),
                        "publishedAt": article.get("published_at", ""),
                        "source": {
                            "name": article.get("source", "Unknown")
                        },
                        "author": None,  # Marketaux doesn't provide author
                        "content": content
                    }
                    formatted_data["articles"].append(formatted_article)
                
                return formatted_data
            else:
                return {"status": "ok", "totalResults": 0, "articles": []}
                
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except Exception as e:
            print(f"Error fetching news: {e}")
            return None

    def get_top_headlines(self, category: str = 'business', country: str = 'us',
                          sources: str = None, page_size: int = 100) -> Optional[Dict]:
        """
        Get top headlines (compatible with NewsAPIFetcher interface).
        For Marketaux, we fetch general market news.
        """
        try:
            params = {
                "api_token": self.api_key,
                "language": "en",
                "limit": min(page_size, 100),
            }
            
            # Marketaux doesn't have category filter, but we can use keywords
            if category == 'business':
                params["keywords"] = "stock market,finance,business,economy"
            
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Format response to match NewsAPI structure
            if "data" in data and data["data"]:
                formatted_data = {
                    "status": "ok",
                    "totalResults": len(data["data"]),
                    "articles": []
                }
                
                for article in data["data"]:
                    formatted_article = {
                        "title": article.get("title", ""),
                        "description": article.get("description", ""),
                        "url": article.get("url", ""),
                        "publishedAt": article.get("published_at", ""),
                        "source": {
                            "name": article.get("source", "Unknown")
                        },
                        "author": None,
                        "content": article.get("description", "") or article.get("snippet", "")
                    }
                    formatted_data["articles"].append(formatted_article)
                
                return formatted_data
            else:
                return {"status": "ok", "totalResults": 0, "articles": []}
                
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
        except Exception as e:
            print(f"Error fetching headlines: {e}")
            return None

    def format_news_to_dataframe(self, news_data: Dict) -> Optional[pd.DataFrame]:
        """
        Format news data to DataFrame (compatible with NewsAPIFetcher interface).
        """
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
                'publishedAt': article.get('publishedAt', ''),
                'source_name': article.get('source', {}).get('name', '') if isinstance(article.get('source'), dict) else article.get('source', 'Unknown'),
                'author': article.get('author', ''),
                'content': article.get('content', '') or article.get('description', '')
            }
            formatted_articles.append(formatted_article)
        
        return pd.DataFrame(formatted_articles)

    def get_stock_related_news(self, stock_symbol: str, company_name: str = None, days_back: int = 7) -> Optional[Dict]:
        """
        Get stock-related news (compatible with NewsAPIFetcher interface).
        """
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        return self.get_everything_news(
            query=stock_symbol,
            from_date=from_date,
            to_date=to_date,
            page_size=50
        )

    def get_financial_market_news(self, days_back: int = 3) -> Optional[Dict]:
        """
        Get financial market news (compatible with NewsAPIFetcher interface).
        """
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        return self.get_everything_news(
            query="stock market OR financial OR economy OR trading OR investment",
            from_date=from_date,
            to_date=to_date,
            page_size=50
        )

    def save_news_to_csv(self, news_df: pd.DataFrame, filename: str):
        """Save news DataFrame to CSV file."""
        try:
            news_df.to_csv(filename, index=False)
            print(f"News data saved to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")

    def fetch_news(self, symbols: List[str], limit: int = 5):
        """
        Original method: Fetches stock-related news articles for given symbols.
        :param symbols: List of ticker symbols, e.g. ['AAPL', 'TSLA']
        :param limit: Number of articles per symbol
        """
        all_articles = []
        for symbol in symbols:
            params = {
                "api_token": self.api_key,
                "symbols": symbol,
                "limit": limit,
                "language": "en",
                "filter_entities": True,
            }

            print(f"\n📰 Fetching news for {symbol} ...")
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if "data" in data and data["data"]:
                    for article in data["data"]:
                        all_articles.append({
                            "symbol": symbol,
                            "title": article.get("title"),
                            "description": article.get("description"),
                            "snippet": article.get("snippet"),
                            "url": article.get("url"),
                            "source": article.get("source"),
                            "published_at": article.get("published_at"),
                            "entities": ", ".join([e["name"] for e in article.get("entities", [])]) if article.get("entities") else None
                        })
                    print(f"✅ {len(data['data'])} articles fetched for {symbol}")
                else:
                    print(f"⚠️ No news found for {symbol}")

            except Exception as e:
                print(f"❌ Error fetching {symbol}: {e}")

        # Save results
        if all_articles:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"marketaux_news_{timestamp}.csv"
            df = pd.DataFrame(all_articles)
            df.to_csv(output_file, index=False)
            print(f"\n🗂️ News saved to {output_file}")
        else:
            print("\n⚠️ No articles fetched. Check API key or limits.")


if __name__ == "__main__":
    fetcher = MarketauxNewsFetcher()
    
    # Prompt the user for stock symbols
    symbols_input = input("Enter stock symbols separated by commas (e.g., AAPL,MSFT,TSLA): ")
    symbols = [symbol.strip().upper() for symbol in symbols_input.split(',') if symbol.strip()]
    
    if symbols:
        fetcher.fetch_news(symbols, limit=5)
    else:
        print("No symbols entered. Exiting.")