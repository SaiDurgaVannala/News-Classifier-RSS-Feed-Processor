

# main_app.py
import feedparser
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
from dateutil import parser as date_parser
from bs4 import BeautifulSoup

nltk.download('punkt')
nltk.download('stopwords')


# Initialize NLTK
stop_words = set(stopwords.words('english'))

# Database Configuration
Base = declarative_base()
engine = create_engine('postgresql://postgres:password@localhost/rss_feeds', echo=True)

# Article Model
class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)
    title = Column(String)
    content = Column(Text)
    pub_date = Column(DateTime)
    source_url = Column(String)
    category = Column(String)

# Create tables
Base.metadata.create_all(engine)

# Function to categorize article
def categorize_article(title, content):
    # Combine title and content for text processing
    text_to_process = title + ' ' + content

    # Tokenize the text
    tokens = word_tokenize(text_to_process)

    # Remove stop words and perform stemming
    filtered_tokens = [word.lower() for word in tokens if word.isalnum() and word.lower() not in stop_words]

    # Implement custom categorization logic based on keywords
    if any(keyword in filtered_tokens for keyword in ['terrorism', 'protest', 'political', 'unrest', 'riot']):
        return 'Terrorism/Protest/Political Unrest/Riot'
    elif any(keyword in filtered_tokens for keyword in ['positive', 'uplifting']):
        return 'Positive/Uplifting'
    elif any(keyword in filtered_tokens for keyword in ['natural', 'disaster']):
        return 'Natural Disasters'
    else:
        return 'Others'

# Function to update category in the database
def update_category(session, article_id, category):
    article = session.query(Article).get(article_id)
    if article:
        article.category = category
        session.commit()

# Function to fetch and process RSS feeds
def fetch_and_process_feeds():
    rss_feeds = [
        "http://rss.cnn.com/rss/cnn_topstories.rss",
        "http://qz.com/feed",
        "http://feeds.foxnews.com/foxnews/politics",
        # "http://feeds.reuters.com/reuters/businessNews",
        "http://feeds.feedburner.com/NewshourWorld",
        "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
    ]

    for feed_url in rss_feeds:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            article_id = entry.id
            title = entry.title
            if 'content' in entry and len(entry['content']) > 0 and 'value' in entry['content'][0]:
                content = BeautifulSoup(entry['content'][0]['value'], 'html.parser').get_text()
            elif 'summary' in entry:
                content = entry.summary
            else:
                content = ''
            if 'published' in entry: 
                pub_date = date_parser.parse(entry.published)
            else:
                pub_date= None
            source_url = entry.link

            # Check for duplicate articles
            session = Session()
            existing_article = session.query(Article).get(article_id)
            if not existing_article:
                # Insert new article into the database
                new_article = Article(id=article_id, title=title, content=content,
                                      pub_date=pub_date, source_url=source_url)
                session.add(new_article)
                session.commit()

                # Process the article and update the category
                category = categorize_article(title, content)
                update_category(session, article_id, category)

            # Close the session
            session.close()

if __name__ == "__main__":
    Session = sessionmaker(bind=engine)

    # Fetch and process RSS feeds
    fetch_and_process_feeds()

