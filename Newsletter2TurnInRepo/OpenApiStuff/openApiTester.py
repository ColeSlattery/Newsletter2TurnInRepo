from openai import OpenAI
import requests
from bs4 import BeautifulSoup

# Hardcoded API key
api_key = "PublicTESTTTTTTTTTTT"

def search_ipo_data():
    """Search for recent NYSE IPO data"""
    try:
        # Search for recent NYSE IPOs
        search_url = "https://www.renaissancecapital.com/IPO-Center/Recent-IPOs"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Extract IPO data (this would need to be customized based on the actual HTML structure)
            return "Found IPO data from Renaissance Capital"
        else:
            return "Could not access IPO data source"
    except Exception as e:
        return f"Error fetching data: {str(e)}"

# Get web data first
web_data = search_ipo_data()

client = OpenAI(api_key=api_key)

response = client.chat.completions.create(
    model="gpt-5-mini",
    messages=[{"role": "user", "content": f"Here's some web data I found: {web_data}. Now list the last 10 IPOs on the NYSE and give me the details of the IPOs. List in an ordered list. The details should include ticker, company name, IPO date, offering price, and first-day % change. Format as a plain table."}]
)

print(response.choices[0].message.content)