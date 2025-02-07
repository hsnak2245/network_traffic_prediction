import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime, timedelta
import random
import logging
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
from groq import Groq
import ipaddress
import csv
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='qatar_analysis.log'
)

class QatarEventScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

    def fetch_page(self, url):
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            return None

    def parse_event(self, event_block):
        try:
            event_data = {
                'event_name': '',
                'date_range': '',
                'location': '',
                'description': '',
                'event_type': '',
                'time_range': '',
                'url': ''
            }

            # Extract event details
            title_elem = event_block.find('a', class_='article-block__title')
            if title_elem:
                event_data['event_name'] = title_elem.text.strip()
                event_data['url'] = 'https://www.iloveqatar.net' + title_elem['href']

            # Extract other elements
            desc_elem = event_block.find('div', class_='article-block__text')
            if desc_elem:
                event_data['description'] = desc_elem.text.strip()

            event_details = event_block.find('div', class_='top-slider-content-event')
            if event_details:
                for detail in event_details.find_all('div'):
                    if '_place' in detail.get('class', []):
                        event_data['location'] = detail.text.strip()
                    elif '_date' in detail.get('class', []):
                        event_data['date_range'] = detail.text.strip()
                    elif '_time' in detail.get('class', []):
                        event_data['time_range'] = detail.text.strip()

            return event_data

        except Exception as e:
            logging.error(f"Error parsing event block: {str(e)}")
            return None

    def scrape_events(self):
        base_url = "https://www.iloveqatar.net/events/p{}"
        all_events = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for page_num in range(1, 100):
                url = base_url.format(page_num)
                logging.info(f"Scraping page {page_num}")
                
                content = self.fetch_page(url)
                if not content:
                    continue

                soup = BeautifulSoup(content, 'html.parser')
                event_blocks = soup.find_all('div', class_='article-block _events')
                
                for event_block in event_blocks:
                    event_data = self.parse_event(event_block)
                    if event_data:
                        all_events.append(event_data)
                
                time.sleep(random.uniform(1, 3))

        return all_events

class GroqEventAnalyzer:
    def __init__(self, api_key):
        self.client = Groq(api_key=api_key)

    def analyze_event_impact(self, event):
        prompt = f"""
        Analyze the following event and provide a network traffic impact score between 1.0 and 2.0:
        Event Name: {event['event_name']}
        Description: {event['description']}
        Location: {event['location']}
        Time: {event['time_range']}

        Consider factors like:
        - Expected attendance
        - Digital engagement likelihood
        - Streaming requirements
        - Social media activity potential
        
        Return only the numerical score.
        """

        try:
            completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="mixtral-8x7b-32768",
                temperature=0.3
            )
            score = float(completion.choices[0].message.content.strip())
            return min(max(score, 1.0), 2.0)  # Ensure score is between 1.0 and 2.0
        except Exception as e:
            logging.error(f"Error getting Groq analysis: {str(e)}")
            return 1.0

class NetworkTrafficGenerator:
    def __init__(self):
        self.ip_ranges = {
            'qatar_isps': ['178.152.0.0/15', '37.208.0.0/13', '82.148.96.0/19'],
            'data_centers': ['185.176.0.0/16', '157.167.0.0/16'],
            'cdns': ['104.16.0.0/12', '199.27.128.0/21']
        }
        
        self.protocols = {'TCP': 0.75, 'UDP': 0.20, 'ICMP': 0.05}
        self.common_ports = {
            'HTTP': 80, 'HTTPS': 443, 'DNS': 53,
            'STREAMING': 1935, 'GAMING': 3074
        }

    def generate_ip(self, ip_range):
        network = ipaddress.ip_network(np.random.choice(self.ip_ranges[ip_range]))
        host = np.random.randint(0, network.num_addresses - 1)
        return str(network[host])

    def generate_traffic_metrics(self, timestamp, event_impact):
        base_bytes = np.random.lognormal(mean=10, sigma=1) * 1000
        base_packets = max(1, int(base_bytes / 1460))
        base_duration = np.random.lognormal(mean=2, sigma=0.5) / 1000
        
        hour = timestamp.hour
        time_multiplier = 1.5 if 9 <= hour <= 17 else (1.3 if 18 <= hour <= 23 else 0.7)
        final_multiplier = time_multiplier * event_impact
        
        return {
            'bytes': int(base_bytes * final_multiplier),
            'packets': int(base_packets * final_multiplier),
            'duration': round(base_duration * final_multiplier, 3),
            'latency': round(np.random.lognormal(mean=2, sigma=0.5), 3),
            'packet_loss': round(np.random.beta(1, 30) * 100, 4)
        }

    def generate_traffic_data(self, events, start_date, end_date):
        current = start_date
        all_records = []
        
        with tqdm(total=(end_date - start_date).days * 24) as pbar:
            while current < end_date:
                # Find relevant events for current timestamp
                relevant_events = [e for e in events if e['date_range'].startswith(current.strftime('%Y-%m-%d'))]
                event_impact = max([e.get('impact_score', 1.0) for e in relevant_events], default=1.0)
                
                # Generate hourly records
                num_records = np.random.randint(300, 2000)
                for _ in range(num_records):
                    metrics = self.generate_traffic_metrics(current, event_impact)
                    record = {
                        'timestamp': current.strftime('%Y-%m-%d %H:%M:%S'),
                        'source_ip': self.generate_ip('qatar_isps'),
                        'dest_ip': self.generate_ip('cdns'),
                        'source_port': np.random.randint(1024, 65535),
                        'dest_port': np.random.choice(list(self.common_ports.values())),
                        'protocol': np.random.choice(
                            list(self.protocols.keys()),
                            p=list(self.protocols.values())
                        ),
                        **metrics
                    }
                    all_records.append(record)
                
                current += timedelta(hours=1)
                pbar.update(1)
                
                # Save batch every 24 hours
                if len(all_records) >= 50000:
                    self.save_batch(all_records, current.strftime('%Y%m%d'))
                    all_records = []
        
        return all_records

    def save_batch(self, records, batch_id):
        filename = f'traffic_data_batch_{batch_id}.csv'
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

def main():
    # Initialize components
    scraper = QatarEventScraper()
    analyzer = GroqEventAnalyzer(api_key="your_groq_api_key")
    traffic_gen = NetworkTrafficGenerator()

    # Scrape events
    logging.info("Starting event scraping")
    events = scraper.scrape_events()
    
    # Analyze events with Groq
    logging.info("Analyzing events with Groq")
    for event in events:
        event['impact_score'] = analyzer.analyze_event_impact(event)
    
    # Save processed events
    with open('processed_events.json', 'w') as f:
        json.dump(events, f, indent=2)
    
    # Generate traffic data
    logging.info("Generating network traffic data")
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 2, 1)
    traffic_data = traffic_gen.generate_traffic_data(events, start_date, end_date)
    
    logging.info("Analysis complete")

if __name__ == "__main__":
    main()