import re
import time
import requests
from bs4 import BeautifulSoup
import mysql.connector

start = time.time()

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="MyPassword123#@!",
    database="maia_scrape",
    auth_plugin="mysql_native_password"
)

TABLE_NAME = "All_In_One"

session = requests.Session()
baseUrl = "http://www.nyconnects.ny.gov"

mycursor1 = mydb.cursor()
mycursor1.execute(f'SHOW TABLES LIKE "{TABLE_NAME}";')
existing_table = mycursor1.fetchall()

if (len(existing_table) == 0):
    mycursor1.execute(f"""
    CREATE TABLE {TABLE_NAME} (Category VARCHAR(1000), SubCategory VARCHAR(1000),
    ProviderName VARCHAR(1000), ProgramName TEXT, ProgramDescription TEXT,
    ProviderTelephone TEXT, AddressLine1 TEXT, PostalCode VARCHAR(20));
    """)

mainLinks = [
    "http://www.nyconnects.ny.gov/browse-search/topic/Basic-Needs",
    "http://www.nyconnects.ny.gov/browse-search/topic/Health-Care-Services-and-Supports",
    "http://www.nyconnects.ny.gov/browse-search/topic/Income-Support(or)Employment",
    "http://www.nyconnects.ny.gov/browse-search/topic/Community-Engagement",
    "http://www.nyconnects.ny.gov/browse-search/topic/Legal-and-Government-Services"
]

for eachLink in mainLinks:
    r1 = requests.get(eachLink)
    soup = BeautifulSoup(r1.content, 'html.parser')
    Category = re.findall(r'(?<=<li class="last">)[^<>]+?(?=<\/li>)', str(r1.content))[0]

    html = soup.find_all('ul', id=re.compile(r'ctl00_MainContent_BrowseCategory\w+'))
    list_string = str(html)

    soup = BeautifulSoup(list_string, 'html.parser')
    a_link = soup.find_all('a', {
        'id': re.compile(r'ctl00_MainContent_BrowseCategory\w+?_2'),
        'href': re.compile(r'(?<=\/browse-search\/category\/).+')
    })

    titles = soup.find_all('h3', {"id": re.compile(r'ctl00_MainContent_BrowseCategoryH3\d+')})

    index = 0
    for link in a_link:
        key = str(re.findall('(?<=<a href="/browse-search/category/).+(?=" id)', str(link))[0])
        index_item = str(titles[index])
        r1 = requests.get(eachLink)
        SubCategory = re.sub('(?<=&)amp;', '', str(re.findall(r'(?<=">).+(?=<\/h3>)', index_item)[0]))
        index = index + 1
        session.get(f'http://www.nyconnects.ny.gov/browse-search/category/{key}')
        cookieDictStr = str(session.cookies.get_dict())

        replace_comma = re.sub(',', ';', cookieDictStr)
        remove_curls = re.sub(r'[\{\}]', '', replace_comma)
        replace_colons = re.sub(r':\s*', '=', remove_curls)
        remove_inverted_comma = re.sub('\'', '', replace_colons)
        refined_cookie = remove_inverted_comma

        refined_headers = {
            "Host": "www.nyconnects.ny.gov",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/114.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "http://www.nyconnects.ny.gov/results",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cookie": refined_cookie,
            "Upgrade-Insecure-Requests": "1"
        }

        response = requests.get("http://www.nyconnects.ny.gov/results?page=1&pageSize=5000&sortby=ProgramNameAsc&focus=CWSortOptionDropDownList", headers=refined_headers)

        soup = BeautifulSoup(response.content, 'html.parser')

        html = soup.find_all("div", class_="optiondetails resultitem")

        row_count = 0

        for i in html:

            strIed = str(i)
            ProviderName = None
            ProgramName = None
            ProgramDescription = None
            ProviderTelephone = None
            AddressLine1 = None
            PostalCode = None

            if re.search('(?<=<div data-itemid="ResultGroupTitle" data-itemvalue=").+?(?=">)', strIed):
                ProviderName = re.sub('(?<=&)amp;', '', re.findall(
                    '(?<=<div data-itemid="ResultGroupTitle" data-itemvalue=").+?(?=">)', strIed)[0])

            if re.search(r'(?<=<a href="\/services\/).+?(?=">)', strIed):
                ProgramName = re.search(r'(?<=">).+?(?=<\/a>)', re.sub('(?<=&)amp;', '', re.sub(
                    r'\n|\s{2,}', '', re.findall(r'(?<=<a href="\/services\/).+?(?=<\/h2>)', strIed, re.DOTALL)[0]))).group(0)

            if re.search('(?<=<div data-itemid="ServiceDescription" data-itemvalue=").+?(?=">)', strIed):
                ProgramDescription = re.sub('&quot;', '"', re.sub(r'&lt;br&gt;', r' \n ', re.sub('(?<=&)amp;', '', 
                    re.findall('(?<=<div data-itemid="ServiceDescription" data-itemvalue=").+?(?=">)', strIed)[0])))

            if re.search('(?<=<div class="result-telephone" data-itemid="ServiceTelephone" data-itemvalue=").+?(?=">)', strIed):
                ProviderTelephone = re.findall(
                    '(?<=<div class="result-telephone" data-itemid="ServiceTelephone" data-itemvalue=").+?(?=">)', strIed)[0]

            if re.search('(?<=<div data-itemid="ProviderAddress" data-itemvalue=").+?(?=">)', strIed):
                AddressLine1 = re.sub('(?<=&)amp;', '', re.findall(
                    '(?<=<div data-itemid="ProviderAddress" data-itemvalue=").+?(?=">)', strIed)[0])

                if re.search(r'\d+-?$', AddressLine1):
                    PostalCode = re.findall(r'[0-9-]+\s*$', AddressLine1)[0]

            mycursor = mydb.cursor()
            sql = f"INSERT INTO {TABLE_NAME}" + " " + \
                "(Category, SubCategory, ProviderName, ProgramName, ProgramDescription, ProviderTelephone, AddressLine1, PostalCode)" + \
                " " + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Category, SubCategory, ProviderName, ProgramName, ProgramDescription, ProviderTelephone, AddressLine1, PostalCode)
            mycursor.execute(sql, val)
            mydb.commit()
            row_count += 1
            print(row_count, "record inserted.")

        row_count = 0

        print(f"\nINSERTED ALL ENTRIES IN '{SubCategory}' TO '{TABLE_NAME}' TABLE !!!\n")

print("SCRAPPING COMPLETED SUCCESSFULLY.")

end = time.time()

print(f"total-time: {(end - start) / 60} minutes")
