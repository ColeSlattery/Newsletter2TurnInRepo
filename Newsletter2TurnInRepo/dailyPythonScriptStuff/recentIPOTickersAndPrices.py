#!/usr/bin/env python3
"""Get prices for recent IPO tickers"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time
import os

class PolygonPriceFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        self.session = requests.Session()
    def get_ticker_price(self, ticker: str, max_retries: int = 2) -> Optional[float]:
        url = f"{self.base_url}/{ticker}"
        params = {'apikey': self.api_key}
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                data = response.json()
                if data.get('status') == 'OK' and 'ticker' in data:
                    ticker_data = data['ticker']
                    if 'min' in ticker_data and 'c' in ticker_data['min']:
                        return float(ticker_data['min']['c'])
                    elif 'day' in ticker_data and 'c' in ticker_data['day']:
                        return float(ticker_data['day']['c'])
                    else:
                        return None
                else:
                    return None
            except requests.exceptions.RequestException as e:
                return None
            except (ValueError, KeyError) as e:
                return None
        return None

class RecentIPOPriceUpdater:
    def __init__(self, polygon_api_key: str):
        self.polygon_fetcher = PolygonPriceFetcher(polygon_api_key)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.recent_ipo_csv_path = os.path.join(script_dir, "CSVs", "recentIPOS.csv")
        self.output_csv_path = os.path.join(script_dir, "CSVs", "recentIPOTickersAndPrices.csv")
        try:
            os.makedirs(os.path.dirname(self.recent_ipo_csv_path), exist_ok=True)
            os.makedirs(os.path.dirname(self.output_csv_path), exist_ok=True)
        except Exception as e:
            pass
    def load_existing_tickers(self) -> List[str]:
        try:
            if os.path.exists(self.output_csv_path) and os.path.getsize(self.output_csv_path) > 0:
                df = pd.read_csv(self.output_csv_path)
                tickers = df['ticker'].tolist()
                return tickers
            else:
                return []
        except Exception as e:
            return []
    def get_tickers_from_recent_ipo(self, limit: int = None) -> List[str]:
        try:
            df_recent = pd.read_csv(self.recent_ipo_csv_path)
            existing_tickers = set(self.load_existing_tickers())
            all_tickers = df_recent['Symbol'].dropna().tolist()
            new_tickers = [ticker for ticker in all_tickers if ticker not in existing_tickers]
            seen = set()
            unique_new_tickers = []
            for ticker in new_tickers:
                if ticker not in seen:
                    seen.add(ticker)
                    unique_new_tickers.append(ticker)
            if limit:
                unique_new_tickers = unique_new_tickers[:limit]
            return unique_new_tickers
        except Exception as e:
            return []
    def get_missing_tickers_from_upcoming(self) -> List[str]:
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            upcoming_csv_path = os.path.join(script_dir, "CSVs", "upcomingIPOS.csv")
            if not os.path.exists(upcoming_csv_path):
                return []
            df_upcoming = pd.read_csv(upcoming_csv_path)
            existing_tickers = set(self.load_existing_tickers())
            all_upcoming_tickers = df_upcoming['Symbol'].dropna().tolist()
            missing_tickers = []
            for ticker in all_upcoming_tickers:
                if ticker not in existing_tickers:
                    ticker_rows = df_upcoming[df_upcoming['Symbol'] == ticker]
                    if not ticker_rows.empty:
                        scraped_date_str = ticker_rows.iloc[0].get('Scraped Date', '')
                        if scraped_date_str:
                            try:
                                scraped_date = datetime.strptime(scraped_date_str, '%Y-%m-%d %H:%M:%S')
                                days_ago = (datetime.now() - scraped_date).days
                                if days_ago <= 7:
                                    missing_tickers.append(ticker)
                            except ValueError:
                                missing_tickers.append(ticker)
            return missing_tickers
        except Exception as e:
            return []
    def fetch_prices_for_tickers(self, tickers: List[str]) -> Dict[str, float]:
        prices = {}
        failed_tickers = []
        for i, ticker in enumerate(tickers):
            price = self.polygon_fetcher.get_ticker_price(ticker)
            if price is not None:
                prices[ticker] = price
            else:
                failed_tickers.append(ticker)
            time.sleep(0.1)
        return prices
    def update_csv_with_new_tickers(self, ticker_prices: Dict[str, float]):
        try:
            new_rows = []
            for ticker, price in ticker_prices.items():
                row_data = {
                    'ticker': ticker,
                    '31DaysAgo_price': '',
                    '30DaysAgo_price': '',
                    '29DaysAgo_price': '',
                    '28DaysAgo_price': '',
                    '27DaysAgo_price': '',
                    '26DaysAgo_price': '',
                    '25DaysAgo_price': '',
                    '24DaysAgo_price': '',
                    '23DaysAgo_price': '',
                    '22DaysAgo_price': '',
                    '21DaysAgo_price': '',
                    '20DaysAgo_price': '',
                    '19DaysAgo_price': '',
                    '18DaysAgo_price': '',
                    '17DaysAgo_price': '',
                    '16DaysAgo_price': '',
                    '15DaysAgo_price': '',
                    '14DaysAgo_price': '',
                    '13DaysAgo_price': '',
                    '12DaysAgo_price': '',
                    '11DaysAgo_price': '',
                    '10DaysAgo_price': '',
                    '9DaysAgo_price': '',
                    '8DaysAgo_price': '',
                    '7DaysAgo_price': '',
                    '6DaysAgo_price': '',
                    '5DaysAgo_price': '',
                    '4DaysAgo_price': '',
                    '3DaysAgo_price': '',
                    '2DaysAgo_price': '',
                    '1DaysAgo_price': '',
                    'Today_price': price,
                    'Day_move': 0.0,
                    'Week_move': 0.0,
                    'Month_move': 0.0
                }
                new_rows.append(row_data)
            if os.path.exists(self.output_csv_path) and os.path.getsize(self.output_csv_path) > 0:
                df_existing = pd.read_csv(self.output_csv_path)
            else:
                columns = ['ticker'] + [f'{i}DaysAgo_price' for i in range(31, 0, -1)] + ['Today_price', 'Day_move', 'Week_move', 'Month_move']
                df_existing = pd.DataFrame(columns=columns)
            df_new = pd.DataFrame(new_rows)
            df_updated = pd.concat([df_existing, df_new], ignore_index=True)
            df_updated.to_csv(self.output_csv_path, index=False)
        except Exception as e:
            raise
    def roll_prices_for_existing_tickers(self):
        try:
            if not os.path.exists(self.output_csv_path) or os.path.getsize(self.output_csv_path) == 0:
                return
            df = pd.read_csv(self.output_csv_path)
            existing_tickers = df['ticker'].tolist()
            ticker_prices = self.fetch_prices_for_tickers(existing_tickers)
            for index, row in df.iterrows():
                ticker = row['ticker']
                if ticker in ticker_prices:
                    current_price = ticker_prices[ticker]
                    previous_today_price = row['Today_price']
                    if pd.notna(previous_today_price) and previous_today_price != 0:
                        day_move = current_price - previous_today_price
                    else:
                        day_move = 0.0
                    week_ago_price = row['7DaysAgo_price']
                    if pd.notna(week_ago_price) and week_ago_price != 0:
                        week_move = current_price - week_ago_price
                    else:
                        week_move = 0.0
                    month_ago_price = row['30DaysAgo_price']
                    if pd.notna(month_ago_price) and month_ago_price != 0:
                        month_move = current_price - month_ago_price
                    else:
                        month_move = 0.0
                    for i in range(31, 1, -1):
                        df.at[index, f'{i}DaysAgo_price'] = row[f'{i-1}DaysAgo_price']
                    df.at[index, '1DaysAgo_price'] = previous_today_price
                    df.at[index, 'Today_price'] = current_price
                    df.at[index, 'Day_move'] = day_move
                    df.at[index, 'Week_move'] = week_move
                    df.at[index, 'Month_move'] = month_move
            df.to_csv(self.output_csv_path, index=False)
        except Exception as e:
            raise
    def run_daily_update(self, test_mode: bool = False, test_limit: int = 100):
        try:
            existing_tickers_before = self.load_existing_tickers()
            if test_mode:
                new_tickers = self.get_tickers_from_recent_ipo(limit=test_limit)
            else:
                new_tickers = self.get_tickers_from_recent_ipo()
            missing_tickers = self.get_missing_tickers_from_upcoming()
            if missing_tickers:
                new_tickers.extend(missing_tickers)
                new_tickers = list(set(new_tickers))
            if new_tickers:
                new_ticker_prices = self.fetch_prices_for_tickers(new_tickers)
                if new_ticker_prices:
                    self.update_csv_with_new_tickers(new_ticker_prices)
            if existing_tickers_before:
                self.roll_prices_for_existing_tickers()
        except Exception as e:
            raise

def main():
    POLYGON_API_KEY = "oIaNK7KzjMgpZQsMIOfpF_LutcAuLsWG"
    updater = RecentIPOPriceUpdater(POLYGON_API_KEY)
    updater.run_daily_update(test_mode=False)

def run_production():
    POLYGON_API_KEY = "oIaNK7KzjMgpZQsMIOfpF_LutcAuLsWG"
    updater = RecentIPOPriceUpdater(POLYGON_API_KEY)
    updater.run_daily_update(test_mode=False)

def test_script():
    print("Testing recent IPO script functionality...")
    test_api_key = "test_key"
    updater = RecentIPOPriceUpdater(test_api_key)
    existing_tickers = updater.load_existing_tickers()
    print(f"Found {len(existing_tickers)} existing tickers: {existing_tickers[:5]}...")
    new_tickers = updater.get_tickers_from_recent_ipo(limit=5)
    print(f"Found {len(new_tickers)} new tickers: {new_tickers}")
    print("Recent IPO script structure test completed successfully!")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_script()
        elif sys.argv[1] == "production":
            run_production()
        elif sys.argv[1] == "main":
            main()
        else:
            print("Usage: python recentIPOTickersAndPrices.py [test|production|main]")
            print("  test       - Test script structure without API calls")
            print("  production - Run full update for all tickers")
            print("  main       - Run full update for all tickers")
    else:
        main()
