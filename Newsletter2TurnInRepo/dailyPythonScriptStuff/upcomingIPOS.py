#!/usr/bin/env python3
"""Scrape upcoming IPOs from IPOScoop"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Set
import re
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class IPOScraper:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.url = "https://www.iposcoop.com/ipo-calendar/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.existing_entries: Set[str] = set()
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    def load_existing_data(self) -> List[Dict]:
        existing_data = []
        if os.path.exists(self.csv_path):
            try:
                with open(self.csv_path, 'r', newline='', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        existing_data.append(row)
                        unique_id = f"{row.get('Company', '').strip()}_{row.get('Symbol', '').strip()}"
                        self.existing_entries.add(unique_id)
            except Exception as e:
                pass
        return existing_data
    def parse_date(self, date_str: str) -> datetime:
        if not date_str or date_str.strip() == '':
            return datetime.max
        date_str = date_str.strip()
        date_patterns = [
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            r'(\d{1,2})/(\d{1,2})/(\d{2})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
        ]
        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    try:
                        if len(groups[2]) == 2:
                            year = int('20' + groups[2]) if int(groups[2]) < 50 else int('19' + groups[2])
                        else:
                            year = int(groups[2])
                        month = int(groups[0])
                        day = int(groups[1])
                        return datetime(year, month, day)
                    except ValueError:
                        continue
        year_match = re.search(r'(\d{4})', date_str)
        if year_match:
            year = int(year_match.group(1))
            return datetime(year, 1, 1)
        return datetime.max
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
                        unique_id = f"{company}_{symbol}"
                        if unique_id not in self.existing_entries:
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
                            self.existing_entries.add(unique_id)
                    except Exception as e:
                        continue
            return ipo_data
        except requests.RequestException as e:
            return []
        except Exception as e:
            return []
    def filter_old_entries(self, data: List[Dict]) -> List[Dict]:
        current_date = datetime.now()
        cutoff_date = current_date - timedelta(days=7)
        filtered_data = []
        for entry in data:
            expected_trade_date = self.parse_date(entry.get('Expected to Trade', ''))
            if expected_trade_date == datetime.max or expected_trade_date >= cutoff_date:
                filtered_data.append(entry)
        return filtered_data
    def save_to_csv(self, new_data: List[Dict], existing_data: List[Dict]) -> bool:
        try:
            all_data = existing_data + new_data
            if not all_data:
                return True
            filtered_data = self.filter_old_entries(all_data)
            filtered_data.sort(key=lambda x: self.parse_date(x.get('Expected to Trade', '')))
            fieldnames = [
                'Company', 'Symbol', 'Lead Managers', 'Shares (Millions)',
                'Price Low', 'Price High', 'Est. $ Volume', 'Expected to Trade', 'Scraped Date'
            ]
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(filtered_data)
            return True
        except Exception as e:
            return False
    def run(self) -> bool:
        try:
            existing_data = self.load_existing_data()
            new_data = self.scrape_ipo_data()
            success = self.save_to_csv(new_data, existing_data)
            return success
        except Exception as e:
            return False

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'CSVs', 'upcomingIPOS.csv')
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
