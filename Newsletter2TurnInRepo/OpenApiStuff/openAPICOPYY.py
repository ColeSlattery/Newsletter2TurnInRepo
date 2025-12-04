#!/usr/bin/env python3
"""
OpenAI API Script for Top Performing IPO Financial Snapshots
Generates comprehensive financial snapshots for the top 5 companies with highest positive week_move
Now includes web search functionality for real-time information gathering with increased token capacity
"""

import csv
import os
import sys
import time
import requests
from datetime import datetime
from openai import OpenAI

# Add current directory to Python path for cron job execution
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Hardcoded API Key
API_KEY = "TESTTTTTTTTTTTT-KoA"

# Initialize OpenAI client
client = OpenAI(api_key=API_KEY)

def read_top_performing_ipos():
    """Read recent IPO data and filter for top 5 companies with highest positive week_move"""
    recent_ipos = []
    
    # First, create a mapping of ticker symbols to company names
    ticker_to_company = {}
    company_csv_path = "CSVs/recentIPOS.csv"
    
    try:
        with open(company_csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['Symbol'] and row['Company']:
                    ticker_to_company[row['Symbol'].strip()] = row['Company'].strip()
    except FileNotFoundError:
        print(f"Warning: Could not find {company_csv_path}")
    except Exception as e:
        print(f"Warning: Error reading company CSV file: {e}")
    
    # Read from recentIPOTickersAndPrices.csv
    csv_path = "CSVs/recentIPOTickersAndPrices.csv"
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['ticker'] and row['ticker'].strip():  # Skip empty rows
                    try:
                        week_move = float(row['Week_move']) if row['Week_move'] else 0.0
                        # Include all companies (not just positive week_move)
                        ticker = row['ticker'].strip()
                        company_name = ticker_to_company.get(ticker, ticker)  # Use company name if available, fallback to ticker
                        
                        recent_ipos.append({
                            'ticker': ticker,
                            'company_name': company_name,
                            'week_move': week_move,
                            'today_price': float(row['Today_price']) if row['Today_price'] else 0.0,
                            'day_move': float(row['Day_move']) if row['Day_move'] else 0.0,
                            'month_move': float(row['Month_move']) if row['Month_move'] else 0.0
                        })
                    except (ValueError, TypeError):
                        # Skip rows with invalid numeric data
                        continue
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path}")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []
    
    # Sort by week_move in descending order and take top 5
    recent_ipos.sort(key=lambda x: x['week_move'], reverse=True)
    top_5_ipos = recent_ipos[:5]
    
    print(f"Found {len(recent_ipos)} companies total")
    print(f"Top 5 performers by week_move:")
    for i, ipo in enumerate(top_5_ipos, 1):
        sign = "+" if ipo['week_move'] >= 0 else ""
        print(f"  {i}. {ipo['company_name']} ({ipo['ticker']}): {sign}{ipo['week_move']:.4f}")
    
    return top_5_ipos

def web_search(query):
    """Perform web search using DuckDuckGo API"""
    try:
        # Using DuckDuckGo Instant Answer API for web search
        url = "https://api.duckduckgo.com/"
        params = {
            'q': query,
            'format': 'json',
            'no_html': '1',
            'skip_disambig': '1'
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        # Extract relevant information
        result = ""
        if data.get('Abstract'):
            result += f"Summary: {data['Abstract']}\n"
        if data.get('AbstractText'):
            result += f"Details: {data['AbstractText']}\n"
        if data.get('RelatedTopics'):
            topics = data['RelatedTopics'][:3]  # Limit to first 3 topics
            for topic in topics:
                if isinstance(topic, dict) and topic.get('Text'):
                    result += f"Related: {topic['Text']}\n"
        
        return result if result else f"No specific information found for: {query}"
        
    except Exception as e:
        return f"Web search failed: {str(e)}"

def get_financial_snapshot(company_name, ticker_symbol, week_move, today_price):
    """Get financial snapshot from OpenAI API with web search enabled"""
    sign = "+" if week_move >= 0 else ""
    prompt = f"Provide a comprehensive financial snapshot and current update on activity for the company that recently IPOd - {company_name}. The stock has moved {sign}{week_move:.4f} this week and is currently trading at ${today_price:.2f}. Include recent news, market sentiment, social media buzz, analyst opinions, and any significant developments. You do not need to mention that the company recently IPOd."
    
    # Define web search tool for real-time information gathering
    # This enables the AI to search for current news, financial updates, and market sentiment
    web_search_tool = {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for current information about a company, including recent news, financial updates, and market sentiment",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query to find current information about the company"
                    }
                },
                "required": ["query"]
            }
        }
    }
    
    # List of models to try in order of preference
    models_to_try = ["gpt-5", "gpt-4o", "gpt-4", "gpt-3.5-turbo"]
    
    for model in models_to_try:
        try:
            print(f"Trying model: {model}")
            
            # Check if model supports tools (web search)
            if model in ["gpt-5", "gpt-4o"]:
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a financial analyst providing comprehensive, accurate financial snapshots of recently IPO'd companies. Focus on current financial status, recent activity, market sentiment, and social media buzz. Use web search to find the most current information about the company. Provide detailed analysis while keeping responses informative and engaging."},
                        {"role": "user", "content": prompt}
                    ],
                    tools=[web_search_tool],
                    tool_choice="auto",
                    max_tokens=1000,  # Increased tokens for more comprehensive response
                    temperature=0.2  # Lower temperature for more factual responses
                )
                
                # Handle function calls if the AI decides to use web search
                message = response.choices[0].message
                if message.tool_calls:
                    # Process tool calls
                    messages = [
                        {"role": "system", "content": "You are a financial analyst providing comprehensive, accurate financial snapshots of recently IPO'd companies. Focus on current financial status, recent activity, market sentiment, and social media buzz. Provide detailed analysis while keeping responses informative and engaging."},
                        {"role": "user", "content": prompt}
                    ]
                    
                    # Add the assistant's message with tool calls
                    messages.append({
                        "role": "assistant",
                        "content": message.content,
                        "tool_calls": message.tool_calls
                    })
                    
                    # Execute tool calls
                    for tool_call in message.tool_calls:
                        if tool_call.function.name == "web_search":
                            import json
                            args = json.loads(tool_call.function.arguments)
                            search_result = web_search(args["query"])
                            
                            messages.append({
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": search_result
                            })
                    
                    # Get final response with search results
                    final_response = client.chat.completions.create(
                        model=model,
                        messages=messages,
                        max_tokens=1000,
                        temperature=0.2
                    )
                    
                    print(f"Successfully used model: {model} with web search")
                    return final_response.choices[0].message.content.strip()
                else:
                    print(f"Successfully used model: {model}")
                    return message.content.strip()
            else:
                # Fallback for models that don't support tools
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are a financial analyst providing comprehensive, accurate financial snapshots of recently IPO'd companies. Focus on current financial status, recent activity, market sentiment, and social media buzz. Provide detailed analysis while keeping responses informative and engaging."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=1000,  # Increased tokens for more comprehensive response
                    temperature=0.2  # Lower temperature for more factual responses
                )
                
                print(f"Successfully used model: {model}")
                return response.choices[0].message.content.strip()
            
        except Exception as model_error:
            print(f"Model {model} failed: {model_error}")
            continue
    
    # If all models fail
    print(f"All models failed for {company_name}")
    return f"Error: Could not generate financial snapshot for {company_name} using any available model"

def save_responses_to_csv(responses_data, filename):
    """Save all responses to a CSV file"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Write header
            writer.writerow(['Company Name', 'Ticker', 'GPTResponse', 'Date Pulled'])
            
            # Write data rows
            for data in responses_data:
                writer.writerow([
                    data['company_name'],
                    data['ticker'],
                    data['gpt_response'],
                    data['date_pulled']
                ])
        
        print(f"Responses saved to {filename}")
        return True
    
    except Exception as e:
        print(f"Error saving to CSV file: {e}")
        return False

def main():
    """Main function to orchestrate the process"""
    try:
        print(f"Starting Top Performing IPO Financial Snapshot Generation at {datetime.now()}")
        
        # Change to script directory for cron job compatibility
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        print(f"Working directory: {os.getcwd()}")
        
        # Read top performing IPO data
        print("Reading top performing IPO data...")
        top_ipos = read_top_performing_ipos()
        
        if not top_ipos:
            print("No top performing IPO data found. Exiting.")
            return
        
        print(f"\nProcessing top {len(top_ipos)} performing IPOs...")
        
        # Generate financial snapshots
        responses_data = []
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for i, ipo in enumerate(top_ipos, 1):
            ticker = ipo['ticker']
            company_name = ipo['company_name']
            week_move = ipo['week_move']
            today_price = ipo['today_price']
            
            sign = "+" if week_move >= 0 else ""
            print(f"Processing {i}/{len(top_ipos)}: {company_name} ({ticker}) ({sign}{week_move:.4f} week move)")
            
            # Get financial snapshot
            gpt_response = get_financial_snapshot(company_name, ticker, week_move, today_price)
            
            # Store data for CSV
            responses_data.append({
                'company_name': company_name,
                'ticker': ticker,
                'gpt_response': gpt_response,
                'date_pulled': current_date
            })
            
            # Add delay to avoid rate limiting
            if i < len(top_ipos):
                time.sleep(1)
    
        # Save responses to CSV file
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"CSVs/gptSummary{current_date}.csv"
        
        if save_responses_to_csv(responses_data, filename):
            print(f"\nProcess completed successfully!")
            print(f"Generated snapshots for {len(responses_data)} top performing companies")
            print(f"Results saved to: {filename}")
        else:
            print("Error saving results to CSV file")
            
    except Exception as e:
        print(f"Error in main execution: {e}")
        print(f"Error occurred at: {datetime.now()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
