#!/usr/bin/env python3
"""Daily script to pull SEC filings and get Polygon data"""

import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os
import time

class PolygonSnapshotFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        self.session = requests.Session()
    def get_ticker_snapshot(self, ticker: str) -> Optional[Dict]:
        url = f"{self.base_url}/{ticker}"
        params = {'apikey': self.api_key}
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

class PolygonTickerFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v3/reference/tickers"
        self.session = requests.Session()
    def get_ticker_info(self, ticker: str) -> Optional[Dict]:
        url = f"{self.base_url}/{ticker}"
        params = {'apikey': self.api_key}
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get('status') == 'OK' and 'results' in data:
                return data['results']
            else:
                return None
        except requests.exceptions.RequestException as e:
            return None
    def process_tickers(self, tickers: List[str]) -> List[Dict]:
        results = []
        for ticker in tickers:
            ticker_info = self.get_ticker_info(ticker)
            if ticker_info:
                results.append(ticker_info)
        return results

class DailyScriptPuller:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.sec-api.io"
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.rolling_file = os.path.join(script_dir, "CSVs", "workingRolling.csv")
        self.polygon_api_key = "publicccccc"
    def get_sec_filings_data(self, start_date: str, end_date: str, max_results: int = 10000) -> pd.DataFrame:
        try:
            all_filings = []
            offset = 0
            batch_size = 50
            while offset < max_results:
                payload = {
                    "query": f"filedAt:[{start_date} TO {end_date}] AND formType:S-1",
                    "from": offset,
                    "size": batch_size
                }
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': self.api_key
                }
                response = requests.post(f"{self.base_url}/form-s1-424b4", json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                filings = data.get('data', [])
                if not filings:
                    break
                all_filings.extend(filings)
                offset += batch_size
                if len(filings) < batch_size:
                    break
            if not all_filings:
                return pd.DataFrame()
            df = pd.DataFrame(all_filings)
            return df
        except Exception as e:
            return pd.DataFrame()
    def filter_sec_data_by_ticker(self, sec_df: pd.DataFrame) -> pd.DataFrame:
        if sec_df.empty:
            return pd.DataFrame()
        ticker_mask = sec_df['tickers'].notna() & (sec_df['tickers'] != '[]')
        filtered_df = sec_df[ticker_mask].copy()
        return filtered_df
    def get_ticker_info(self, ticker: str) -> Optional[Dict]:
        try:
            ticker_fetcher = PolygonTickerFetcher(self.polygon_api_key)
            snapshot_fetcher = PolygonSnapshotFetcher(self.polygon_api_key)
            ticker_info = ticker_fetcher.get_ticker_info(ticker)
            snapshot_info = snapshot_fetcher.get_ticker_snapshot(ticker)
            price_data = {}
            if snapshot_info:
                if 'min' in snapshot_info:
                    min_data = snapshot_info['min']
                    price_data = {
                        'current_price': min_data.get('c', ''),
                        'open': min_data.get('o', ''),
                        'day_low': min_data.get('l', ''),
                        'day_high': min_data.get('h', ''),
                    }
                elif 'day' in snapshot_info:
                    day_data = snapshot_info['day']
                    price_data = {
                        'current_price': day_data.get('c', ''),
                        'open': day_data.get('o', ''),
                        'day_low': day_data.get('l', ''),
                        'day_high': day_data.get('h', ''),
                    }
            company_data = {}
            if ticker_info:
                company_data = {
                    'symbol': ticker_info.get('ticker', ''),
                    'name': ticker_info.get('name', ''),
                    'short_name': ticker_info.get('name', ''),
                    'exchange': ticker_info.get('primary_exchange', ''),
                    'sector': ticker_info.get('sic_description', ''),
                    'industry': ticker_info.get('sic_description', ''),
                    'market_cap': ticker_info.get('market_cap', ''),
                    'employees_yahoo': ticker_info.get('total_employees', ''),
                    'website': ticker_info.get('homepage_url', ''),
                    'business_summary': ticker_info.get('description', '')
                }
            ticker_data = {
                'ticker': ticker,
                'symbol': company_data.get('symbol', ''),
                'name': company_data.get('name', ''),
                'short_name': company_data.get('short_name', ''),
                'exchange': company_data.get('exchange', ''),
                'sector': company_data.get('sector', ''),
                'industry': company_data.get('industry', ''),
                'current_price': price_data.get('current_price', ''),
                'open': price_data.get('open', ''),
                'day_low': price_data.get('day_low', ''),
                'day_high': price_data.get('day_high', ''),
                'market_cap': company_data.get('market_cap', ''),
                'debt_to_equity': '',
                'current_ratio': '',
                'quick_ratio': '',
                'book_value': '',
                '52_week_change': '',
                'employees_yahoo': company_data.get('employees_yahoo', ''),
                'website': company_data.get('website', ''),
                'business_summary': company_data.get('business_summary', '')
            }
            has_data = any([
                ticker_data['current_price'],
                ticker_data['name'],
                ticker_data['market_cap'],
                ticker_data['business_summary']
            ])
            if not has_data:
                return None
            return ticker_data
        except Exception as e:
            return None
    def process_polygon_tickers(self, tickers: List[str], delay: float = 0.1) -> pd.DataFrame:
        results = []
        for i, ticker in enumerate(tickers, 1):
            ticker_info = self.get_ticker_info(ticker)
            if ticker_info:
                results.append(ticker_info)
            if i < len(tickers):
                time.sleep(delay)
        if results:
            df = pd.DataFrame(results)
            return df
        else:
            return pd.DataFrame()
    def get_polygon_finance_data(self, sec_df: pd.DataFrame) -> pd.DataFrame:
        if sec_df.empty:
            return pd.DataFrame()
        tickers = []
        for _, row in sec_df.iterrows():
            ticker_value = row['tickers']
            try:
                is_valid = (ticker_value is not None and
                           str(ticker_value).strip() not in ['[]', 'nan', 'None', ''] and
                           str(ticker_value).strip() != 'nan')
                if is_valid:
                    import json
                    import ast
                    ticker_data = row['tickers']
                    if isinstance(ticker_data, str):
                        try:
                            ticker_list = json.loads(ticker_data)
                        except json.JSONDecodeError:
                            try:
                                ticker_list = ast.literal_eval(ticker_data)
                            except (ValueError, SyntaxError):
                                continue
                    elif isinstance(ticker_data, list):
                        ticker_list = ticker_data
                    else:
                        continue
                    for ticker_info in ticker_list:
                        if isinstance(ticker_info, dict) and 'ticker' in ticker_info:
                            tickers.append(ticker_info['ticker'])
                        elif isinstance(ticker_info, str):
                            tickers.append(ticker_info)
            except (KeyError, TypeError, AttributeError) as e:
                continue
        unique_tickers = list(set(tickers))
        if not unique_tickers:
            return pd.DataFrame()
        return self.process_polygon_tickers(unique_tickers)
    def combine_sec_and_yahoo_data(self, sec_df: pd.DataFrame, yahoo_df: pd.DataFrame) -> pd.DataFrame:
        if sec_df.empty and yahoo_df.empty:
            return pd.DataFrame()
        if sec_df.empty:
            return yahoo_df
        if yahoo_df.empty:
            return sec_df
        try:
            sec_tickers = []
            for _, row in sec_df.iterrows():
                ticker_value = row['tickers']
                try:
                    is_valid = (ticker_value is not None and 
                               str(ticker_value).strip() not in ['[]', 'nan', 'None', ''] and
                               str(ticker_value).strip() != 'nan')
                    if is_valid:
                        try:
                            import json
                            import ast
                            ticker_data = row['tickers']
                            if isinstance(ticker_data, str):
                                try:
                                    ticker_list = json.loads(ticker_data)
                                except json.JSONDecodeError:
                                    try:
                                        ticker_list = ast.literal_eval(ticker_data)
                                    except (ValueError, SyntaxError):
                                        sec_tickers.append('')
                                        continue
                            elif isinstance(ticker_data, list):
                                ticker_list = ticker_data
                            else:
                                sec_tickers.append('')
                                continue
                            if ticker_list and isinstance(ticker_list[0], dict) and 'ticker' in ticker_list[0]:
                                sec_tickers.append(ticker_list[0]['ticker'])
                            elif ticker_list and isinstance(ticker_list[0], str):
                                sec_tickers.append(ticker_list[0])
                            else:
                                sec_tickers.append('')
                        except (KeyError, IndexError, TypeError, AttributeError):
                            sec_tickers.append('')
                    else:
                        sec_tickers.append('')
                except (KeyError, TypeError, AttributeError):
                    sec_tickers.append('')
            if len(sec_tickers) != len(sec_df):
                while len(sec_tickers) < len(sec_df):
                    sec_tickers.append('')
                sec_tickers = sec_tickers[:len(sec_df)]
            sec_df_copy = sec_df.copy()
            sec_df_copy['ticker'] = sec_tickers
            combined_df = pd.merge(sec_df_copy, yahoo_df, on='ticker', how='outer', suffixes=('', '_yahoo'))
            if 'ticker' in combined_df.columns:
                duplicate_count = combined_df.duplicated(subset=['ticker'], keep='first').sum()
                if duplicate_count > 0:
                    combined_df = combined_df.drop_duplicates(subset=['ticker'], keep='first')
            return combined_df
        except Exception as e:
            return pd.DataFrame()
    def get_polygon_data(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        if combined_df.empty:
            return combined_df
        if not self.polygon_api_key:
            return combined_df
        tickers = combined_df['ticker'].dropna().unique().tolist()
        valid_tickers = []
        for t in tickers:
            if t and t.strip():
                if not (t.startswith('001-') or t.startswith('000-') or len(t) > 15):
                    valid_tickers.append(t.strip())
        if not valid_tickers:
            return combined_df
        polygon_fetcher = PolygonTickerFetcher(self.polygon_api_key)
        polygon_data = polygon_fetcher.process_tickers(valid_tickers)
        if not polygon_data:
            return combined_df
        polygon_df = pd.DataFrame(polygon_data)
        polygon_columns = {}
        for col in polygon_df.columns:
            if col != 'ticker':
                polygon_columns[col] = f'polygon_{col}'
        polygon_df = polygon_df.rename(columns=polygon_columns)
        if 'ticker' in polygon_df.columns:
            final_df = pd.merge(combined_df, polygon_df, on='ticker', how='left')
            return final_df
        else:
            return combined_df
    def update_rolling_data(self, new_data: pd.DataFrame) -> pd.DataFrame:
        if new_data.empty:
            return self.load_existing_rolling_data()
        existing_df = self.load_existing_rolling_data()
        if existing_df.empty:
            return new_data
        existing_tickers = set(existing_df['ticker'].dropna().unique()) if 'ticker' in existing_df.columns else set()
        new_tickers = set(new_data['ticker'].dropna().unique()) if 'ticker' in new_data.columns else set()
        truly_new_tickers = new_tickers - existing_tickers
        if truly_new_tickers:
            new_rows = new_data[new_data['ticker'].isin(truly_new_tickers)].copy()
            combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
        else:
            combined_df = existing_df
        if 'ticker' in combined_df.columns:
            if 'filedAt' in combined_df.columns:
                combined_df['filedAt'] = pd.to_datetime(combined_df['filedAt'], errors='coerce')
                combined_df = combined_df.sort_values(['ticker', 'filedAt'], ascending=[True, False])
                duplicate_count = combined_df.duplicated(subset=['ticker'], keep='first').sum()
                if duplicate_count > 0:
                    combined_df = combined_df.drop_duplicates(subset=['ticker'], keep='first')
            else:
                duplicate_count = combined_df.duplicated(subset=['ticker'], keep='first').sum()
                if duplicate_count > 0:
                    combined_df = combined_df.drop_duplicates(subset=['ticker'], keep='first')
        if 'polygon_list_date' in combined_df.columns:
            combined_df['polygon_list_date'] = pd.to_datetime(combined_df['polygon_list_date'], errors='coerce')
            combined_df = combined_df.sort_values('polygon_list_date', ascending=False, na_position='last')
        return combined_df
    def load_existing_rolling_data(self) -> pd.DataFrame:
        if os.path.exists(self.rolling_file):
            file_size = os.path.getsize(self.rolling_file)
            if file_size > 1:
                try:
                    df = pd.read_csv(self.rolling_file)
                    return df
                except Exception as e:
                    return pd.DataFrame()
            else:
                return pd.DataFrame()
        else:
            return pd.DataFrame()
    def save_rolling_data(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        try:
            desired_columns = [
                'ticker', 'tickers', 'symbol', 'name', 'short_name', 'exchange', 'sector', 'industry',
                'current_price', 'open', 'day_low', 'day_high', 'market_cap', 'debt_to_equity', 
                'current_ratio', 'quick_ratio', 'book_value', '52_week_change', 'accessionNo', 'auditors',
                'employees', 'filedAt', 'filingUrl', 'formType', 'lawFirms', 'proceedsBeforeExpenses',
                'publicOfferingPrice', 'securities', 'underwriters', 'employees_yahoo', 'website',
                'business_summary', 'polygon_market_cap', 'polygon_share_class_shares_outstanding',
                'polygon_weighted_shares_outstanding', 'polygon_round_lot', 'polygon_ticker_root',
                'polygon_sic_description', 'polygon_total_employees', 'polygon_list_date', 'polygon_description'
            ]
            available_columns = [col for col in desired_columns if col in df.columns]
            df_filtered = df[available_columns].copy()
            if 'polygon_list_date' in df_filtered.columns:
                df_filtered['polygon_list_date'] = pd.to_datetime(df_filtered['polygon_list_date'], errors='coerce')
                df_filtered = df_filtered.sort_values('polygon_list_date', ascending=False, na_position='last')
            os.makedirs(os.path.dirname(self.rolling_file), exist_ok=True)
            df_filtered.to_csv(self.rolling_file, index=False)
        except Exception as e:
            pass
    def run_daily_pipeline(self) -> None:
        try:
            today = datetime.now()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
            sec_df = self.get_sec_filings_data(start_date, end_date)
            if sec_df.empty:
                return
            sec_filtered_df = self.filter_sec_data_by_ticker(sec_df)
            if sec_filtered_df.empty:
                return
            polygon_finance_df = self.get_polygon_finance_data(sec_filtered_df)
            if polygon_finance_df.empty:
                yahoo_df = pd.DataFrame(columns=['ticker', 'symbol', 'name', 'short_name', 'exchange', 
                                               'sector', 'industry', 'current_price', 'open', 'day_low', 
                                               'day_high', 'market_cap', 'debt_to_equity', 'current_ratio', 
                                               'quick_ratio', 'book_value', '52_week_change', 
                                               'employees_yahoo', 'website', 'business_summary'])
            else:
                yahoo_df = polygon_finance_df
            combined_df = self.combine_sec_and_yahoo_data(sec_filtered_df, yahoo_df)
            if combined_df.empty:
                return
            final_df = self.get_polygon_data(combined_df)
            if final_df.empty:
                return
            updated_rolling_df = self.update_rolling_data(final_df)
            self.save_rolling_data(updated_rolling_df)
        except Exception as e:
            pass

def main():
    try:
        api_key = "PUBLIC"
        puller = DailyScriptPuller(api_key)
        puller.run_daily_pipeline()
        return 0
    except Exception as e:
        return 1

if __name__ == "__main__":
    exit(main())
