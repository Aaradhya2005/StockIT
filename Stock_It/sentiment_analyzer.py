"""
Sentiment Analysis module for financial news articles.
Supports multiple sentiment analysis models including TextBlob and VADER.
"""

from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
from typing import Dict, Tuple

class SentimentAnalyzer:
    """Sentiment analysis for financial news articles."""
    
    def __init__(self):
        self.vader_analyzer = SentimentIntensityAnalyzer()
    
    def clean_text(self, text: str) -> str:
        """Clean and preprocess text for sentiment analysis."""
        if not text:
            return ""
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def analyze_with_textblob(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using TextBlob.
        
        Returns:
            Dict with sentiment_score (-1 to 1), confidence_score (0 to 1), and sentiment_label
        """
        cleaned_text = self.clean_text(text)
        if not cleaned_text:
            return {
                'sentiment_score': 0.0,
                'confidence_score': 0.0,
                'sentiment_label': 'neutral'
            }
        
        blob = TextBlob(cleaned_text)
        polarity = blob.sentiment.polarity  # -1 to 1
        subjectivity = blob.sentiment.subjectivity  # 0 to 1
        
        # Convert subjectivity to confidence (higher subjectivity = lower confidence)
        confidence = 1.0 - subjectivity
        
        # Determine sentiment label
        if polarity > 0.1:
            label = 'positive'
        elif polarity < -0.1:
            label = 'negative'
        else:
            label = 'neutral'
        
        return {
            'sentiment_score': round(polarity, 4),
            'confidence_score': round(confidence, 4),
            'sentiment_label': label
        }
    
    def analyze_with_vader(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment using VADER (Valence Aware Dictionary and sEntiment Reasoner).
        
        Returns:
            Dict with sentiment_score (-1 to 1), confidence_score (0 to 1), and sentiment_label
        """
        cleaned_text = self.clean_text(text)
        if not cleaned_text:
            return {
                'sentiment_score': 0.0,
                'confidence_score': 0.0,
                'sentiment_label': 'neutral'
            }
        
        scores = self.vader_analyzer.polarity_scores(cleaned_text)
        
        # VADER returns compound score (-1 to 1)
        compound_score = scores['compound']
        
        # Calculate confidence based on the strength of positive/negative scores
        pos_score = scores['pos']
        neg_score = scores['neg']
        neu_score = scores['neu']
        
        # Higher confidence when sentiment is more extreme
        confidence = max(pos_score, neg_score)
        
        # Determine sentiment label based on compound score
        if compound_score >= 0.05:
            label = 'positive'
        elif compound_score <= -0.05:
            label = 'negative'
        else:
            label = 'neutral'
        
        return {
            'sentiment_score': round(compound_score, 4),
            'confidence_score': round(confidence, 4),
            'sentiment_label': label
        }
    
    def analyze_financial_sentiment(self, text: str, model: str = 'vader') -> Dict[str, float]:
        """
        Analyze sentiment with financial context awareness.
        
        Args:
            text: Text to analyze
            model: 'textblob' or 'vader'
        
        Returns:
            Dict with sentiment analysis results
        """
        # Financial keywords that might affect sentiment
        positive_financial_words = [
            'profit', 'growth', 'increase', 'rise', 'gain', 'bull', 'bullish',
            'upgrade', 'beat', 'exceed', 'strong', 'robust', 'outperform'
        ]
        
        negative_financial_words = [
            'loss', 'decline', 'decrease', 'fall', 'drop', 'bear', 'bearish',
            'downgrade', 'miss', 'weak', 'poor', 'underperform', 'recession'
        ]
        
        if model.lower() == 'textblob':
            result = self.analyze_with_textblob(text)
        elif model.lower() == 'vader':
            result = self.analyze_with_vader(text)
        else:
            raise ValueError(f"Unsupported model: {model}")
        
        # Adjust sentiment based on financial keywords
        text_lower = text.lower()
        financial_adjustment = 0.0
        
        for word in positive_financial_words:
            if word in text_lower:
                financial_adjustment += 0.1
        
        for word in negative_financial_words:
            if word in text_lower:
                financial_adjustment -= 0.1
        
        # Apply adjustment but keep within bounds
        adjusted_score = result['sentiment_score'] + financial_adjustment
        adjusted_score = max(-1.0, min(1.0, adjusted_score))
        
        # Update label based on adjusted score
        if adjusted_score > 0.1:
            adjusted_label = 'positive'
        elif adjusted_score < -0.1:
            adjusted_label = 'negative'
        else:
            adjusted_label = 'neutral'
        
        return {
            'sentiment_score': round(adjusted_score, 4),
            'confidence_score': result['confidence_score'],
            'sentiment_label': adjusted_label
        }
    
    def batch_analyze(self, texts: list, model: str = 'vader') -> list:
        """
        Analyze sentiment for multiple texts.
        
        Args:
            texts: List of texts to analyze
            model: 'textblob' or 'vader'
        
        Returns:
            List of sentiment analysis results
        """
        results = []
        for text in texts:
            try:
                result = self.analyze_financial_sentiment(text, model)
                results.append(result)
            except Exception as e:
                print(f"Error analyzing text: {e}")
                results.append({
                    'sentiment_score': 0.0,
                    'confidence_score': 0.0,
                    'sentiment_label': 'neutral'
                })
        
        return results

# Example usage
if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    
    # Test with financial news examples
    test_texts = [
        "Apple stock surges 5% after beating earnings expectations with strong iPhone sales.",
        "Tesla shares plummet following disappointing quarterly results and production delays.",
        "The market remains stable with mixed signals from various sectors.",
        "Amazon reports record profits driven by cloud computing growth."
    ]
    
    print("Sentiment Analysis Results:")
    print("=" * 50)
    
    for i, text in enumerate(test_texts, 1):
        print(f"\nText {i}: {text}")
        
        # Analyze with VADER
        vader_result = analyzer.analyze_financial_sentiment(text, 'vader')
        print(f"VADER: {vader_result}")
        
        # Analyze with TextBlob
        textblob_result = analyzer.analyze_financial_sentiment(text, 'textblob')
        print(f"TextBlob: {textblob_result}")