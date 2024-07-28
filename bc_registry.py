import gzip
import hashlib
import os
import requests
import pymysql
from lxml import html
from pymysql.constants import CLIENT
from db_maker import bc_registry_create_query, pincodes_create_query, new_bc_registry_create_query
import time
from sys import argv

start_time = time.time()

start = argv[1]
end = argv[2]


def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Directory {path} created")


class Scraper:
    def __init__(self):
        self.session = requests.Session()  # Initialize session in the Scraper class
        self.client = pymysql.connect(
            host='localhost',
            user='root',
            database='bc_registry_db',
            password='actowiz',
            charset='utf8mb4',
            autocommit=True
        )
        if self.client.open:
            print('Database connection Successful!')
        else:
            print('Database connection Un-Successful.')
        self.cursor = self.client.cursor()

        # Creating Saved Pages Directory for this Project if not Exists
        project_name = 'Bc_Registry'

        self.project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
        ensure_directory_exists(path=self.project_files_dir)

    def req_sender(self, url: str, method: str, headers, data, params=None):
        _response = self.session.request(method=method, url=url, params=params, headers=headers, data=data)
        if _response.status_code != 200:
            print(f"HTTP Request Status code: {_response.status_code}")  # HTTP response error
            return None
        else:
            return _response  # Request Successful

    def page_checker(self, url: str, method: str, path: str, headers, data, params=None, pincode_=None) -> str:
        page_hash = hashlib.sha256(url.encode()).hexdigest()
        file_path = os.path.join(path, f"{page_hash}_{pincode_}.html.gz")
        ensure_directory_exists(path)
        ensure_directory_exists(os.path.join(os.getcwd(), 'pincodes_pages'))
        if os.path.exists(file_path):
            print("Page exists, Reading it...")
            print(f'File name is : {file_path}')
            with gzip.open(file_path, 'rb') as file:
                html_text = file.read().decode(errors='backslashreplace')
            return html_text
        else:
            print("Page does not exist, Sending Request...")
            _response = self.req_sender(url=url, method=method, headers=headers, data=data, params=params)
            print(f'File name is : {file_path}')
            if _response is not None:
                with gzip.open(file_path, 'wb') as file:
                    file.write(_response.content)
                    print("Page Saved")
                return _response.text

    def db_schema_creater(self):
        self.cursor.execute(bc_registry_create_query)
        print('BC REGISTRY Table created!')
        self.cursor.execute(pincodes_create_query)
        print('PINCODES Table created!')
        self.cursor.execute(new_bc_registry_create_query)
        print('PINCODES Table created!')

    def pincodes_fetcher(self, table_name: str, status_column: str):
        select_query = f'''SELECT * FROM {table_name} WHERE {status_column} = 'pending' and id between {start} and {end};'''
        print(select_query)
        self.cursor.execute(select_query)
        pincodes_data = self.cursor.fetchall()
        print(pincodes_data)
        return pincodes_data

    def bc_data_fetcher(self):
        self.db_schema_creater()
        payload_pincodes_list = self.pincodes_fetcher(table_name='new_pincodes', status_column='pincode_status')
        print(payload_pincodes_list)
        num = 0
        for pincode in payload_pincodes_list:
            pincode_id = pincode[0]
            pincode = pincode[1]

            headers = {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Content-type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www.bcregistry.org.in',
                'Referer': 'https://www.bcregistry.org.in/iba/home/HomeAction.do?doBCPortal=yes',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            }

            data = {
                'type': '3',
            }

            captcha_response = self.session.request(
                method='POST',
                url='https://www.bcregistry.org.in/iba/ajax/home/captchasession.jsp',
                headers=headers,
                data=data,
            )

            captcha_code = captcha_response.text.strip()

            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www.bcregistry.org.in',
                'Referer': 'https://www.bcregistry.org.in/iba/home/HomeAction.do?doBCPortal=yes',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            }

            params = {
                'doBCPortal': 'yes',
            }

            data = {
                'doBCPortal': 'yes',
                'searchType': '4',
                'currentPincode': '',
                'bcRequestId': '',
                'pincode': pincode,
                'subDistrict': '',
                'stateId': '-1',
                'districtId': '-1',
                'bankId': '-1',
                'userName': '',
                'userEmail': '',
                'userMobile': '',
                'remarks': '',
                'cap_reg': '',
                'cap_search': captcha_code
            }

            html_text = self.page_checker(
                url='https://www.bcregistry.org.in/iba/home/HomeAction.do',
                method='POST',
                params=params,
                headers=headers,
                data=data,
                path=os.path.join(self.project_files_dir, 'Pincodes_Pages'),
                pincode_=pincode
            )

            parsed_html = html.fromstring(html_text)
            bc_list = parsed_html.xpath('//td[@class="bc_name"]')
            for this_bc in bc_list:
                bc_name = this_bc.xpath('.//text()')[0][2:].replace('\\', '').replace('"', '').replace("'", '')
                bc_contact_no = this_bc.xpath('./following-sibling::td[1]/text()')[0]
                bc_bank_name = this_bc.xpath('./following-sibling::td[3]/text()')[0]

                bc_insert_query = f'''INSERT INTO new_bc_registry (BC_Name, Mobile_No, Pincode, Bank_Name)
                                      VALUES ('{bc_name}', {bc_contact_no}, {pincode}, '{bc_bank_name}');'''
                print(bc_insert_query)
                self.cursor.execute(bc_insert_query)

            print(f'{pincode} Done.')

            update_query = f'''UPDATE new_pincodes
                               SET pincode_status = 'Done'
                               WHERE id = '{pincode_id}';'''
            self.cursor.execute(update_query)
            print(f'{num}th Done')
            num += 1
            print('-' * 30)

        self.client.close()  # Ensure the database connection is closed properly


print(Scraper().bc_data_fetcher())
end_time = time.time()
print(end_time - start_time)
