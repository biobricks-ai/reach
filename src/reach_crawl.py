import re, requests, pandas as pd, time, os, glob
import pyarrow as pa, pyarrow.parquet as pq
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
from urllib.parse import urlparse

base_url = 'https://echa.europa.eu'

# TOO MANY LINKS just getting some predefined ones now
# def get_hrefs(bpage):
#     links = bpage.find_all('a')
#     mkref = lambda l: urlparse(l.get('href'))._replace(fragment='',query='').geturl() if l.get('href') else None
#     reftxt = [(mkref(link),link.text.strip()) for link in links]
#     hrefs = [h for h in reftxt if h[0] and h[0].startswith('/registration-dossier')]
#     hrefs = [base_url + href[0] for href in hrefs]
#     return sorted(list(set(hrefs)))

def get_hrefs(ecid):
    ecidurl = base_url + f'/registration-dossier/-/registered-dossier/{ecid}/'
    substance_identity = ecidurl + '1/1'
    ghs = ecidurl + '2/1'
    return [substance_identity, ghs]
    
    
df = pd.read_excel('downloads/reach.xlsx',sheet_name=0)
urls = df['Factsheet URL']

# FIRST LOOP GET BASE PAGES
for url in tqdm(urls):
    bref = url[22:] # base relative reference
    ecid = urlparse(url).path.split('/')[-1]
    path = f'cache/reach/{ecid}.html'
    
    # if path exists read from path, otherwise get url
    if os.path.exists(path):
        continue
    
    page = requests.get(url).text
    page = re.sub('\n+', '\n', page)
    
    with open(path, 'w', encoding='utf-8') as file:
        _ = file.write(page)
    
    time.sleep(0.1)


# SECOND LOOP GET SUBPAGES
pages = glob.glob('cache/reach/*.html')
pbar = tqdm(pages)
for page in pbar:
    pbar.set_description(page)
    ecid = page.split('/')[-1].split('.')[0]
    hrefs = get_hrefs(ecid)
    
    path = [h[65:] for h in hrefs]
    path = [p.replace('/','-') + ".html" for p in path]
    path = ['cache/reach/subpages/' + p for p in path]
    
    href_path = list(zip(hrefs, path))
    href_path = [(h, p) for h, p in href_path if not os.path.exists(p)]
    
    for href, path in href_path:
        hpage = requests.get(href, timeout=60).text
        hpage = re.sub('\n+', '\n', hpage)
        with open(path, 'w', encoding='utf-8') as file:
            _ = file.write(hpage)
        time.sleep(0.1)

# THIRD LOOP BUILD DATAFRAME ===================================================
def get_id(idpg):
    html = open(idpg, 'r', encoding='utf-8').read()
    soup = bs(html, 'html.parser')
    
    # Find all 'div' elements with the id 'SectionContent'
    header = soup.find_all('h3', {'id': 'sIdentification'})
    iddiv = header[0].find_next_sibling('div')
    
    # remove all images
    imgs = iddiv.find_all('img')
    for img in imgs: _ = img.extract()
    
    dt = [x.string.strip() for x in iddiv.find_all('dt')]
    dt = [x.replace(':',"").replace(" ","_").lower() for x in dt]
    dd = [x.string for x in iddiv.find_all('dd')]
    dd = [x.strip() if x else None for x in dd] 
    
    return [x for x in zip(dt,dd)]

def get_haz(hzpg):
    html = open(hzpg, 'r', encoding='utf-8').read()
    soup = bs(html, 'html.parser')
    cont = soup.find('div', {'id': 'SectionContent'})
    
    # remove all images
    imgs = cont.find_all('img')
    for img in imgs: _ = img.extract()
    
    haz = re.compile(r'^H[0-9]{3}')
    def categorize(ddstr):
        if ddstr is None: 
            return None
        elif ddstr == "hazard class not applicable":
            return None
        elif ddstr == "data lacking":
            return None
        elif ddstr == "data conclusive but not sufficient for classification":
            return "negative"
        elif haz.match(ddstr):
            return haz.match(ddstr).group()
        else:
            return None
        
    data = []
    for h5 in cont.find_all('h5'):
        nt = h5.find_next_sibling('dl')
        if nt:
            for dt, dd in zip(nt.find_all('dt'), nt.find_all('dd')):
                cat = categorize(dd.string)
                data.append((h5.text.strip(), cat))
    
    return data

def process_page(page):
    try:
        ecid = page.split('/')[-1].split('.')[0]
        
        idpg = get_id('cache/reach/subpages/' + ecid + '-1-1.html')
        idpg = [(ecid, x[0], x[1]) for x in idpg if x[1] is not None]
        idpg = list(set(idpg))
        idf = pd.DataFrame(idpg, columns=['ecid','property','value'])
        
        hzpg = 'cache/reach/subpages/' + ecid + '-2-1.html'
        hzpg = get_haz(hzpg)
        hzpg = [(ecid, x[0].lower(), x[1]) for x in hzpg if x[1] is not None]
        hzpg = list(set(hzpg))
        
        hdf = pd.DataFrame(hzpg, columns=['ecid','property','value'])
        
        return pd.concat([idf, hdf], ignore_index=True)
    except Exception as e:
        print(f"An error occurred with page {page}: {str(e)}. Skipping this page.")
        return pd.DataFrame(columns=['ecid','property','value'])

def process_pages(pages):
    dfs = []
    for page in pages:
        try:
            df = process_page(page)
            dfs.append(df)
        except Exception as e:
            print(f"An error occurred with page {page}: {str(e)}. Skipping this page.")
    pdf = pd.concat(dfs, ignore_index=True)
    return pdf

import random, numpy as np, multiprocessing as mp
pages = glob.glob('cache/reach/*.html')
num_pages = len(pages)
num_procs = 50
chunk_size = num_pages // num_procs
chunks = np.array_split(pages, num_pages // chunk_size)

pool = mp.Pool(processes=num_procs)
result = pool.map(process_pages, chunks)
pool.close()
pool.join()

pdf = pd.concat(result, ignore_index=True)
pdf.to_csv('cache/reach.csv', index=False)

# create partitioned hazards
def partition_pdf(pdf):
    properties = pdf['property'].unique()
    idproperties = ['ec_name', 'iupac_name', 'origin', 'ec_number',
       'molecular_formula', 'cas_number', 'state_form', 'display_name',
       'composition']
    badprops = ['description','typical_specific_surface_area']
    pdf = pdf[~pdf['property'].isin(badprops)]
    hazproperties = [x for x in properties if x not in idproperties]
    
    hazdf = pdf[pdf['property'].isin(hazproperties)]
    
    # filter to rows where value is 'negative' or starts with 'H'
    hazdf = hazdf[hazdf['value'].str.startswith('H') | (hazdf['value'] == 'negative')]
    hazvals = hazdf.groupby('property')['value'].unique()
    
    new_rows = []
    
    for _, row in tqdm(hazdf.iterrows()):
        ecid = row['ecid']
        property_ = row['property']
        value = row['value']
        
        possible_values = hazvals[property_]
        # replace 'negative' possible_value with property
        possible_values = [property_ if x == 'negative' else x for x in possible_values]
        
        if value == 'negative':
            new_rows.extend([(ecid, v, 'negative') for v in possible_values])
        else:
            new_rows.extend([(ecid, v, 'positive' if v == value else 'negative') for v in possible_values])
    
    newhazdf = pd.DataFrame(new_rows, columns=['ecid', 'property', 'value'])
    idf = pdf[pdf['property'].isin(idproperties)]
    newpdf = pd.concat([idf, newhazdf], ignore_index=True)
    return newpdf

partitioned_pdf = partition_pdf(pdf)
table = pa.Table.from_pandas(partitioned_pdf)
pq.write_table(table, 'brick/reach_crawl.parquet')
