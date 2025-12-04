#!/usr/bin/env python3
"""Process recent IPOs and get Polygon data"""

import csv
import os
import sys
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RecentIPOProcessor:
    def __init__(self, upcoming_csv_path: str, recent_csv_path: str, polygon_api_key: str):
        self.upcoming_csv_path = upcoming_csv_path
        self.recent_csv_path = recent_csv_path
        self.polygon_api_key = polygon_api_key
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.existing_entries: Dict[str, Dict] = {}
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
    def is_recent_date(self, date_str: str) -> bool:
        expected_date = self.parse_date(date_str)
        if expected_date == datetime.max:
            return False
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        expected_date_only = expected_date.date()
        return expected_date_only == today or expected_date_only == tomorrow
    def load_upcoming_data(self) -> List[Dict]:
        if not os.path.exists(self.upcoming_csv_path):
            return []
        recent_entries = []
        try:
            with open(self.upcoming_csv_path, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    expected_trade = row.get('Expected to Trade', '').strip()
                    if self.is_recent_date(expected_trade):
                        recent_entries.append(row)
            return recent_entries
        except Exception as e:
            return []
    def load_existing_recent_data(self) -> Dict[str, Dict]:
        existing_data = {}
        if os.path.exists(self.recent_csv_path):
            try:
                with open(self.recent_csv_path, 'r', newline='', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        symbol = row.get('Symbol', '').strip()
                        if symbol:
                            existing_data[symbol] = row
            except Exception as e:
                pass
        return existing_data
    def query_polygon_snapshot(self, symbol: str) -> Optional[Dict]:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        params = {'apikey': self.polygon_api_key}
        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            if data.get('status') == 'OK' and 'ticker' in data:
                return data['ticker']
            else:
                return None
        except requests.exceptions.RequestException as e:
            return None
    def query_polygon_reference(self, symbol: str) -> Optional[Dict]:
        url = "https://api.polygon.io/v3/reference/tickers"
        params = {
            'ticker': symbol,
            'apikey': self.polygon_api_key
        }
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get('status') == 'OK' and data.get('results'):
                return data['results'][0]
            else:
                return None
        except requests.exceptions.RequestException as e:
            return None
    def enrich_with_polygon_data(self, entries: List[Dict]) -> List[Dict]:
        enriched_entries = []
        for entry in entries:
            symbol = entry.get('Symbol', '').strip()
            if not symbol:
                continue
            snapshot_data = self.query_polygon_snapshot(symbol)
            reference_data = self.query_polygon_reference(symbol)
            enriched_entry = entry.copy()
            if snapshot_data:
                enriched_entry.update({
                    'polygon_market_status': snapshot_data.get('market_status', ''),
                    'polygon_last_quote_price': snapshot_data.get('last_quote', {}).get('P', ''),
                    'polygon_last_quote_size': snapshot_data.get('last_quote', {}).get('S', ''),
                    'polygon_last_trade_price': snapshot_data.get('last_trade', {}).get('p', ''),
                    'polygon_last_trade_size': snapshot_data.get('last_trade', {}).get('s', ''),
                    'polygon_min_price': snapshot_data.get('min', {}).get('p', ''),
                    'polygon_min_volume': snapshot_data.get('min', {}).get('v', ''),
                    'polygon_max_price': snapshot_data.get('max', {}).get('p', ''),
                    'polygon_max_volume': snapshot_data.get('max', {}).get('v', ''),
                    'polygon_prev_close': snapshot_data.get('prev_day', {}).get('c', ''),
                    'polygon_open': snapshot_data.get('open', ''),
                    'polygon_high': snapshot_data.get('high', ''),
                    'polygon_low': snapshot_data.get('low', ''),
                    'polygon_volume': snapshot_data.get('volume', ''),
                    'polygon_vwap': snapshot_data.get('vw', ''),
                })
            if reference_data:
                enriched_entry.update({
                    'polygon_name': reference_data.get('name', ''),
                    'polygon_market': reference_data.get('market', ''),
                    'polygon_locale': reference_data.get('locale', ''),
                    'polygon_primary_exchange': reference_data.get('primary_exchange', ''),
                    'polygon_type': reference_data.get('type', ''),
                    'polygon_active': reference_data.get('active', ''),
                    'polygon_currency_name': reference_data.get('currency_name', ''),
                    'polygon_cik': reference_data.get('cik', ''),
                    'polygon_composite_figi': reference_data.get('composite_figi', ''),
                    'polygon_share_class_figi': reference_data.get('share_class_figi', ''),
                    'polygon_last_updated_utc': reference_data.get('last_updated_utc', ''),
                    'polygon_delisted_utc': reference_data.get('delisted_utc', ''),
                })
            enriched_entry['processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            enriched_entries.append(enriched_entry)
            time.sleep(0.1)
        return enriched_entries
    def filter_old_entries(self, data: List[Dict]) -> List[Dict]:
        current_date = datetime.now()
        cutoff_date = current_date - timedelta(days=90)
        filtered_data = []
        for entry in data:
            expected_trade_date = self.parse_date(entry.get('Expected to Trade', ''))
            if expected_trade_date == datetime.max or expected_trade_date >= cutoff_date:
                filtered_data.append(entry)
        return filtered_data
    def save_recent_data(self, new_entries: List[Dict], existing_entries: Dict[str, Dict]) -> bool:
        try:
            all_entries = list(existing_entries.values())
            for entry in new_entries:
                symbol = entry.get('Symbol', '').strip()
                if symbol:
                    all_entries.append(entry)
            unique_entries = {}
            for entry in all_entries:
                symbol = entry.get('Symbol', '').strip()
                if symbol:
                    unique_entries[symbol] = entry
            final_entries = list(unique_entries.values())
            filtered_entries = self.filter_old_entries(final_entries)
            filtered_entries.sort(key=lambda x: self.parse_date(x.get('Expected to Trade', '')), reverse=True)
            if filtered_entries:
                all_fieldnames = [
                    'Company', 'Symbol', 'Lead Managers', 'Shares (Millions)',
                    'Price Low', 'Price High', 'Est. $ Volume', 'Expected to Trade', 'Scraped Date',
                    'polygon_market_status', 'polygon_last_quote_price', 'polygon_last_quote_size',
                    'polygon_last_trade_price', 'polygon_last_trade_size', 'polygon_min_price',
                    'polygon_min_volume', 'polygon_max_price', 'polygon_max_volume',
                    'polygon_prev_close', 'polygon_open', 'polygon_high', 'polygon_low',
                    'polygon_volume', 'polygon_vwap', 'polygon_name', 'polygon_market',
                    'polygon_locale', 'polygon_primary_exchange', 'polygon_type', 'polygon_active',
                    'polygon_currency_name', 'polygon_cik', 'polygon_composite_figi',
                    'polygon_share_class_figi', 'polygon_last_updated_utc', 'polygon_delisted_utc',
                    'processed_date'
                ]
                with open(self.recent_csv_path, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=all_fieldnames)
                    writer.writeheader()
                    writer.writerows(filtered_entries)
            else:
                with open(self.recent_csv_path, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow(['Company', 'Symbol', 'Expected to Trade', 'processed_date'])
            return True
        except Exception as e:
            return False
    def run(self) -> bool:
        try:
            recent_entries = self.load_upcoming_data()
            if not recent_entries:
                return True
            existing_entries = self.load_existing_recent_data()
            enriched_entries = self.enrich_with_polygon_data(recent_entries)
            success = self.save_recent_data(enriched_entries, existing_entries)
            return success
        except Exception as e:
            return False

def main():
    POLYGON_API_KEY = "XXoIaNK7KzjMgpZQsMIOfpF_LutcAuLsWG"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    upcoming_csv_path = os.path.join(script_dir, 'CSVs', 'upcomingIPOS.csv')
    recent_csv_path = os.path.join(script_dir, 'CSVs', 'recentIPOS.csv')
    try:
        os.makedirs(os.path.dirname(recent_csv_path), exist_ok=True)
    except Exception as e:
        sys.exit(1)
    try:
        processor = RecentIPOProcessor(upcoming_csv_path, recent_csv_path, POLYGON_API_KEY)
        success = processor.run()
        if success:
            print("Recent IPO processing completed successfully")
            sys.exit(0)
        else:
            print("Recent IPO processing failed")
            sys.exit(1)
    except Exception as e:
        print(f"Recent IPO processing failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
