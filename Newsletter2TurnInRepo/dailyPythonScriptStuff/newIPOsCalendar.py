#!/usr/bin/env python3
"""Scrape IPO calendar from IPOScoop"""

import requests
from bs4 import BeautifulSoup
import csv
import os
import sys
from datetime import datetime
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class IPOScraper:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.url = "https://www.iposcoop.com/ipo-calendar/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    def scrape_ipo_data(self) -> List[Dict]:
        try:
            response = self.session.get(self.url, headers=self.headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            if not table:
                return []
            rows = table.find_all('tr')[1:]
            ipo_data = []
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 9:
                    try:
                        company_cell = cells[0].find('a')
                        company = company_cell.get_text(strip=True) if company_cell else cells[0].get_text(strip=True)
                        symbol_cell = cells[1].find('a')
                        symbol = symbol_cell.get_text(strip=True) if symbol_cell else cells[1].get_text(strip=True)
                        lead_managers = cells[2].get_text(strip=True)
                        shares = cells[3].get_text(strip=True)
                        price_low = cells[4].get_text(strip=True)
                        price_high = cells[5].get_text(strip=True)
                        est_volume = cells[6].get_text(strip=True)
                        expected_trade = cells[7].get_text(strip=True)
                        ipo_entry = {
                            'Company': company,
                            'Symbol': symbol,
                            'Lead Managers': lead_managers,
                            'Shares (Millions)': shares,
                            'Price Low': price_low,
                            'Price High': price_high,
                            'Est. $ Volume': est_volume,
                            'Expected to Trade': expected_trade,
                            'Scraped Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        ipo_data.append(ipo_entry)
                    except Exception as e:
                        continue
            return ipo_data
        except requests.RequestException as e:
            return []
        except Exception as e:
            return []
    def save_to_csv(self, new_data: List[Dict]) -> bool:
        try:
            if not new_data:
                fieldnames = [
                    'Company', 'Symbol', 'Lead Managers', 'Shares (Millions)',
                    'Price Low', 'Price High', 'Est. $ Volume', 'Expected to Trade', 'Scraped Date'
                ]
                with open(self.csv_path, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                return True
            fieldnames = [
                'Company', 'Symbol', 'Lead Managers', 'Shares (Millions)',
                'Price Low', 'Price High', 'Est. $ Volume', 'Expected to Trade', 'Scraped Date'
            ]
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(new_data)
            return True
        except Exception as e:
            return False
    def run(self) -> bool:
        try:
            new_data = self.scrape_ipo_data()
            success = self.save_to_csv(new_data)
            return success
        except Exception as e:
            return False

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'CSVs', 'NewIPOCalendar.csv')
    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    except Exception as e:
        sys.exit(1)
    try:
        scraper = IPOScraper(csv_path)
        success = scraper.run()
        if success:
            print("IPO scraping completed successfully")
            sys.exit(0)
        else:
            print("IPO scraping failed")
            sys.exit(1)
    except Exception as e:
        print(f"IPO scraping failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
