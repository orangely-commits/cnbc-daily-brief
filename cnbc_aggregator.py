import requests
from bs4 import BeautifulSoup
import feedparser
from youtube_transcript_api import YouTubeTranscriptApi
import pandas as pd
from datetime import datetime
import time
import random
import re

class CNBCAggregator:
    def __init__(self):
        # Configuration for headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        self.data_store = []

    def log(self, message):
        """Simple logger with timestamp"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    # ------------------------------------------------------------------
    # MODULE 1: Web Scraper (Public News)
    # ------------------------------------------------------------------
    def fetch_web_news(self):
        self.log("Starting Web Scraper module...")
        url = "https://www.cnbc.com/finance/"
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                self.log(f"Failed to fetch website. Status: {response.status_code}")
                return

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find news cards (Note: Selectors change over time, these are common classes)
            # Targeting 'Card-title' is usually reliable on CNBC
            articles = soup.select('.Card-title') 

            count = 0
            for article in articles[:15]: # Limit to top 15 to avoid spam
                try:
                    title = article.get_text(strip=True)
                    link = article.get('href')
                    
                    # Handle relative URLs
                    if link and not link.startswith('http'):
                        link = f"https://www.cnbc.com{link}"

                    self.data_store.append({
                        'Source_Type': 'Web News',
                        'Timestamp': datetime.now().strftime('%Y-%m-%d'),
                        'Headline': title,
                        'Snippet': 'N/A (Click link for full text)',
                        'Link': link
                    })
                    count += 1
                except Exception as e:
                    continue
            
            self.log(f"Successfully scraped {count} web articles.")
            
        except Exception as e:
            self.log(f"Error in Web Scraper: {e}")

    # ------------------------------------------------------------------
    # MODULE 2: YouTube Intelligence (Video Transcripts)
    # ------------------------------------------------------------------
    def fetch_youtube_intelligence(self):
        """
        Uses RSS to find recent videos, then fetches transcripts via unofficial API.
        Target: CNBC Television Channel
        """
        self.log("Starting YouTube Intelligence module...")
        
        # CNBC Television Channel RSS Feed
        # Channel ID: UCrp_UI8XtuYfpiqluWLD7Lw (CNBC Television)
        rss_url = "https://www.youtube.com/feeds/videos.xml?channel_id=UCrp_UI8XtuYfpiqluWLD7Lw"
        
        feed = feedparser.parse(rss_url)
        
        keywords = ['cramer', 'morning', 'club', 'investing', 'stock']
        
        for entry in feed.entries[:5]: # Check last 5 videos
            video_id = entry.yt_videoid
            title = entry.title.lower()
            
            # Filter for relevant content
            if any(k in title for k in keywords):
                try:
                    # Attempt to fetch transcript
                    transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
                    
                    # Combine first 500 chars of transcript as a snippet
                    full_text = " ".join([t['text'] for t in transcript_list])
                    snippet = full_text[:500] + "..."
                    
                    self.data_store.append({
                        'Source_Type': 'YouTube Video',
                        'Timestamp': entry.published,
                        'Headline': entry.title,
                        'Snippet': snippet, # Real spoken text from the video
                        'Link': entry.link
                    })
                    self.log(f"Retrieved transcript for: {entry.title[:30]}...")
                    
                    # Polite delay
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    # Often videos don't have captions immediately
                    self.log(f"No transcript available for {video_id}: {str(e)[:50]}")

    # ------------------------------------------------------------------
    # MODULE 3: Podcast Intelligence (Audio RSS)
    # ------------------------------------------------------------------
    def fetch_podcast_intelligence(self):
        self.log("Starting Podcast Intelligence module...")
        
        # "Squawk on the Street" RSS Feed (Simplecast)
        rss_url = "https://feeds.simplecast.com/GcylmXl7"
        
        try:
            feed = feedparser.parse(rss_url)
            
            for entry in feed.entries[:3]: # Get last 3 episodes
                published_date = entry.get('published', datetime.now().strftime('%Y-%m-%d'))
                
                # Podcast descriptions often contain stock tickers/summaries
                summary = BeautifulSoup(entry.description, "html.parser").get_text()[:400] + "..."
                
                self.data_store.append({
                    'Source_Type': 'Podcast RSS',
                    'Timestamp': published_date,
                    'Headline': entry.title,
                    'Snippet': summary,
                    'Link': entry.link
                })
                
            self.log(f"Parsed {min(3, len(feed.entries))} podcast episodes.")
            
        except Exception as e:
            self.log(f"Error in Podcast module: {e}")

    # ------------------------------------------------------------------
    # MAIN EXECUTION
    # ------------------------------------------------------------------
    def run(self):
        print("--- Initiating CNBC Market Intelligence ---\n")
        
        # Run all modules
        self.fetch_web_news()
        time.sleep(1)
        self.fetch_youtube_intelligence()
        time.sleep(1)
        self.fetch_podcast_intelligence()
        
        # Save to CSV
        if self.data_store:
            df = pd.DataFrame(self.data_store)
            
            # Clean up timestamp format slightly
            filename = f"cnbc_intelligence_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename, index=False)
            
            print(f"\nSUCCESS: Data saved to {filename}")
            print("\nSample Data:")
            print(df[['Source_Type', 'Headline']].head())
        else:
            print("\nWARNING: No data collected from any source.")

if __name__ == "__main__":
    aggregator = CNBCAggregator()
    aggregator.run()
