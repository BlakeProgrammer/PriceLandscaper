import json
import time
import random
import requests
from bs4 import BeautifulSoup
import numpy as np
from concurrent.futures import ThreadPoolExecutor

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# --- zipcode scraper
def get_listing_urls_for_zip(all_urls, zipcode):
    page = 1
    
    # Get page 1 URLs first as our reference
    first_page_urls = None
    
    while True:
        if page == 1:
            url = f'https://www.redfin.com/zipcode/{zipcode}'
        else:
            url = f'https://www.redfin.com/zipcode/{zipcode}/page-{page}'
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        urls = []
        scripts = soup.find_all('script', type='application/ld+json')
        for s in scripts:
            try:
                data = json.loads(s.string)
                if isinstance(data, list):
                    for item in data:
                        if 'url' in item and 'address' in item:
                            urls.append(item['url'])
            except:
                pass
        
        print(f"Zip {zipcode} page {page}: found {len(urls)} listings")
        
        if page == 1:
            first_page_urls = set(urls)
        else:
            # If urls overlap with page 1, we've been redirected back
            if set(urls) == first_page_urls or len(urls) == 0:
                print(f"Zip {zipcode}: reached end at page {page}, did not include those duplicates")
                break
        
        all_urls.extend(urls)
        page += 1
        # Polite delay - random wait between 1 and half a second
        time.sleep(random.uniform(0.5, 1))
    
    return all_urls

# --- STAGE 1: Collect listing URLs and append to input array ---
def get_listing_urls(urls_array, zipcode):
    page = 1
    first_page_urls = set()
    
    while True:
        # 1. Build URL
        url = f'https://www.redfin.com/zipcode/{zipcode}' if page == 1 else f'https://www.redfin.com/zipcode/{zipcode}/page-{page}'
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 2. Extract ONLY this page's URLs
        current_page_urls = []
        scripts = soup.find_all('script', type='application/ld+json')
        for s in scripts:
            try:
                data = json.loads(s.string)
                if isinstance(data, list):
                    for item in data:
                        if 'url' in item and 'address' in item:
                            current_page_urls.append(item['url'])
            except:
                continue

        # 3. Check Stop Conditions
        if not current_page_urls:
            print(f"Zip {zipcode}: No listings found on page {page}. Stopping.")
            break
            
        current_page_set = set(current_page_urls)

        if page == 1:
            first_page_urls = current_page_set
        else:
            # If this page is identical to page 1, Redfin redirected us back to the start
            if current_page_set == first_page_urls:
                print(f"Zip {zipcode}: Redirection detected at page {page}. Reached the end.")
                break
        
        # 4. Add to the main list and increment
        urls_array.extend(current_page_urls)
        print(f"Zip {zipcode} page {page}: added {len(current_page_urls)} listings")
        
        page += 1
        time.sleep(random.uniform(1, 2)) # Important to stay unblocked!

    return urls_array

# --- STAGE 2: Scrape individual listing data ---
def get_listing_data(url):
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        scripts = soup.find_all('script', type='application/ld+json')
        for s in scripts:
            try:
                data = json.loads(s.string)
                if isinstance(data, dict) and 'offers' in data and 'mainEntity' in data:
                    return {
                        'price': data['offers']['price'],
                        'latitude': data['mainEntity']['geo']['latitude'],
                        'longitude': data['mainEntity']['geo']['longitude'],
                        'sqft': data['mainEntity']['floorSize']['value'],
                        'address': data['mainEntity']['address']['streetAddress'],
                        'city': data['mainEntity']['address']['addressLocality'],
                        'zip': data['mainEntity']['address']['postalCode'],
                        'url': data['url'],
                        'price_per_sqft': data['offers']['price'] / data['mainEntity']['floorSize']['value'],
                        'image': data.get('image', {}).get('url', None)
                    }
            except:
                pass
    except Exception as e:
        print(f"Failed on {url}: {e}")
    return None

# --- MAIN ---
print("Stage 1: Collecting listing URLs...")
urls = []
get_listing_urls(urls, '37072')
get_listing_urls(urls, '37073')
get_listing_urls(urls, '37074')
print(f"Found {len(urls)} listings")

# only taking first ** listings
urls = urls[:100]
print(f"Scraping {len(urls)} listings...")

listings = []
for i, url in enumerate(urls):
    print(f"Scraping {i+1}/{len(urls)}: {url}")
    data = get_listing_data(url)
    if data:
        listings.append(data)
    
    # Polite delay - random wait between 1 and half a second
    time.sleep(random.uniform(0.5, 1))

# filter listings out of 2nd std dev
prices = [l['price'] for l in listings]
mean = np.mean(prices)
std = np.std(prices)

lower_bound = mean - (2 * std)
upper_bound = mean + (2 * std)

filtered = [l for l in listings if lower_bound <= l['price'] <= upper_bound]

print(f"Mean price: ${mean:,.0f}")
print(f"Standard deviation: ${std:,.0f}")
print(f"Acceptable range: ${lower_bound:,.0f} — ${upper_bound:,.0f}")
print(f"Removed {len(listings) - len(filtered)} outliers")
print(f"Kept {len(filtered)} listings")

# save to file
with open('PriceLandscaper/listings.json', 'w') as f:
    json.dump(filtered, f, indent=2)

print(f"Done! Saved {len(filtered)} listings to listings.json")