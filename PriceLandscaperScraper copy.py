import json
import time
import random
import requests
from bs4 import BeautifulSoup
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

ZIPCODES = [37010, 37012, 37013, 37014, 37015, 37016, 37018, 37019, 37020, 37022, 37023, 37025, 37026, 37027, 37028, 37029, 37030, 37031, 37032, 37033, 37034, 37035, 37036, 37037, 37040, 37042, 37043, 37046, 37047, 37048, 37049, 37050, 37051, 37052, 37055, 37057, 37058, 37059, 37060, 37061, 37062, 37064, 37066, 37067, 37069, 37072, 37073, 37074, 37075, 37076, 37078, 37079, 37080, 37082, 37083, 37085, 37086, 37087, 37090, 37091, 37095, 37096, 37097, 37098, 37101, 37110, 37115, 37118, 37122, 37127, 37128, 37129, 37130, 37134, 37135, 37137, 37138, 37140, 37141, 37142, 37143, 37144, 37145, 37146, 37148, 37149, 37150, 37151, 37153, 37160, 37165, 37166, 37167, 37171, 37172, 37174, 37175, 37178, 37179, 37180, 37181, 37183, 37184, 37185, 37186, 37187, 37188, 37189, 37190, 37191, 37201, 37203, 37204, 37205, 37206, 37207, 37208, 37209, 37210, 37211, 37212, 37213, 37214, 37215, 37216, 37217, 37218, 37219, 37220, 37221, 37228, 37238, 37301, 37302, 37303, 37305, 37306, 37307, 37308, 37309, 37310, 37311, 37312, 37313, 37315, 37316, 37317, 37318, 37321, 37322, 37323, 37324, 37325, 37326, 37327, 37328, 37329, 37330, 37331, 37332, 37333, 37334, 37335, 37336, 37337, 37338, 37339, 37340, 37341, 37342, 37343, 37345, 37347, 37348, 37350, 37351, 37352, 37353, 37354, 37355, 37356, 37357, 37359, 37360, 37361, 37362, 37363, 37365, 37366, 37367, 37369, 37370, 37373, 37374, 37375, 37376, 37377, 37379, 37380, 37381, 37385, 37387, 37388, 37391, 37394, 37396, 37397, 37398, 37402, 37403, 37404, 37405, 37406, 37407, 37408, 37409, 37410, 37411, 37412, 37415, 37416, 37419, 37421, 37450, 37601, 37604, 37615, 37616, 37617, 37618, 37620, 37640, 37641, 37642, 37643, 37645, 37650, 37656, 37657, 37658, 37659, 37660, 37663, 37664, 37665, 37680, 37681, 37682, 37683, 37686, 37687, 37688, 37690, 37691, 37692, 37694, 37701, 37705, 37708, 37709, 37710, 37711, 37713, 37714, 37715, 37716, 37719, 37721, 37722, 37723, 37724, 37725, 37726, 37727, 37729, 37730, 37731, 37732, 37737, 37738, 37742, 37743, 37745, 37748, 37752, 37753, 37754, 37755, 37756, 37757, 37760, 37762, 37763, 37764, 37765, 37766, 37769, 37770, 37771, 37772, 37774, 37777, 37779, 37801, 37803, 37804, 37806, 37807, 37809, 37810, 37811, 37813, 37814, 37818, 37819, 37820, 37821, 37825, 37826, 37828, 37829, 37830, 37840, 37841, 37843, 37845, 37846, 37847, 37848, 37849, 37852, 37853, 37854, 37857, 37860, 37861, 37862, 37863, 37865, 37866, 37869, 37870, 37871, 37872, 37873, 37874, 37876, 37877, 37878, 37879, 37880, 37881, 37882, 37885, 37886, 37887, 37888, 37890, 37891, 37892, 37902, 37909, 37912, 37914, 37915, 37916, 37917, 37918, 37919, 37920, 37921, 37922, 37923, 37924, 37929, 37931, 37932, 37934, 37938, 38001, 38002, 38004, 38006, 38007, 38008, 38011, 38012, 38015, 38016, 38017, 38018, 38019, 38021, 38023, 38024, 38028, 38029, 38030, 38034, 38036, 38037, 38039, 38040, 38041, 38042, 38044, 38046, 38047, 38049, 38050, 38052, 38053, 38054, 38057, 38058, 38059, 38060, 38061, 38063, 38066, 38067, 38068, 38069, 38070, 38075, 38076, 38077, 38079, 38080, 38103, 38104, 38105, 38106, 38107, 38108, 38109, 38111, 38112, 38113, 38114, 38115, 38116, 38117, 38118, 38119, 38120, 38122, 38125, 38126, 38127, 38128, 38131, 38132, 38133, 38134, 38135, 38138, 38139, 38141, 38201, 38220, 38221, 38222, 38224, 38225, 38226, 38229, 38230, 38231, 38232, 38233, 38235, 38236, 38237, 38240, 38241, 38242, 38251, 38253, 38255, 38256, 38257, 38258, 38259, 38260, 38261, 38301, 38305, 38310, 38311, 38313, 38315, 38316, 38317, 38318, 38320, 38321, 38326, 38327, 38328, 38329, 38330, 38332, 38333, 38334, 38337, 38338, 38339, 38340, 38341, 38342, 38343, 38344, 38345, 38347, 38348, 38351, 38352, 38355, 38356, 38357, 38358, 38359, 38361, 38362, 38363, 38365, 38366, 38367, 38368, 38369, 38370, 38371, 38372, 38374, 38375, 38376, 38379, 38380, 38381, 38382, 38387, 38388, 38389, 38390, 38391, 38392, 38401, 38425, 38449, 38450, 38451, 38452, 38453, 38454, 38455, 38456, 38457, 38459, 38460, 38461, 38462, 38463, 38464, 38468, 38469, 38471, 38472, 38473, 38474, 38475, 38476, 38477, 38478, 38481, 38482, 38483, 38485, 38486, 38487, 38488, 38501, 38503, 38504, 38506, 38541, 38542, 38543, 38544, 38545, 38547, 38548, 38549, 38550, 38551, 38552, 38553, 38554, 38555, 38556, 38558, 38559, 38560, 38562, 38563, 38564, 38565, 38567, 38568, 38569, 38570, 38571, 38572, 38573, 38574, 38575, 38577, 38578, 38579, 38580, 38581, 38582, 38583, 38585, 38587, 38588, 38589, 37132, 37383, 38152, 38505, 37232, 37614, 37733, 37851, 38254]

# --- begin timer
start_time = time.time()
# --- STAGE 1: Collect listing URLs for zipcodes ---
def get_listing_urls(zipcode):
    urls = []
    page = 1
    first_page_urls = None

    while True:
        url = f'https://www.redfin.com/zipcode/{zipcode}' if page == 1 else f'https://www.redfin.com/zipcode/{zipcode}/page-{page}'
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Zip {zipcode} page {page}: request failed ({e}), skipping")
            break

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

        if not current_page_urls:
            print(f"Zip {zipcode}: no listings on page {page}, stopping")
            break

        current_page_set = set(current_page_urls)

        if page == 1:
            first_page_urls = current_page_set
        elif current_page_set == first_page_urls:
            print(f"Zip {zipcode}: redirect detected at page {page}, stopping")
            break

        urls.extend(current_page_urls)
        print(f"Zip {zipcode} page {page}: added {len(current_page_urls)} listings")
        page += 1
        time.sleep(random.uniform(1, 2))

    return urls

# --- STAGE 2: Scrape individual listing data ---
def get_listing_data(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
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

# --- STAGE 1 MAIN: Iterate all zipcodes ---
print("Stage 1: Collecting listing URLs across all zipcodes...")
all_urls = []

for zipcode in ZIPCODES:
    zip_urls = get_listing_urls(zipcode)
    all_urls.extend(zip_urls)
    time.sleep(random.uniform(1, 2))  # polite delay between zipcodes

# Deduplicate
all_urls = list(set(all_urls))
print(f"Total unique listing URLs: {len(all_urls)}")

# --- STAGE 2 MAIN: Scrape listings in parallel ---
print("Stage 2: Scraping listing data...")
listings = []

# MAX_WORKERS controls parallelism - keep low to avoid getting blocked
MAX_WORKERS = 4

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(get_listing_data, url): url for url in all_urls}
    for i, future in enumerate(as_completed(futures)):
        result = future.result()
        if result:
            listings.append(result)
        print(f"Progress: {i+1}/{len(all_urls)} ({len(listings)} successful)")
        time.sleep(random.uniform(0.5, 1))

# --- Filter outliers ---
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

with open('PriceLandscaper/listings.json', 'w') as f:
    json.dump(filtered, f, indent=2)

print(f"Done! Saved {len(filtered)} listings to listings.json")
#remember to change what is fetched in index.html

end_time = time.time()
elapsed = end_time - start_time
formatted = str(datetime.timedelta(seconds=int(elapsed)))
print(f"Total scraping time: {formatted}")