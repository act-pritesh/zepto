import datetime
import json
import os
import random
import time

from evpn import ExpressVpnApi
import lxml.html
import pymysql
import scrapy


from scrapy.crawler import CrawlerProcess


class ZeptoSpiderSpider(scrapy.Spider):
    name = "zpt"


    fetch_table='fetch_leftover_product'
    create_and_insert_table='zepto_data_master_2_data_left'

    def __init__(self, start_id, end_id):
        super(ZeptoSpiderSpider, self).__init__()

        self.start_id = start_id
        self.end_id = end_id

        #MySQL database credentials
        self.db_conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='zepto',
            charset='utf8mb4'
        )
        self.cursor = self.db_conn.cursor()
        # Create the table if it doesn't exist
        self.create_scraped_data_table()

    def start_requests(self):
        cookies = {
            'unique_browser_id': '6320098320329916',
            '_gcl_au': '1.1.51708254.1734423278',
            '_ga': 'GA1.1.696673055.1734423279',
            '_fbp': 'fb.1.1734423279029.18584957053118149',
            'maxWeightLimitCart': '25000',
            'latitude': '23.022505',
            'longitude': '72.5713621',
            '_ga_37QQVCR1ZS': 'GS1.1.1735710120.6.0.1735710120.60.0.0',
            '_ga_52LKG2B3L1': 'GS1.1.1735709899.16.1.1735711019.60.0.107590341',
            'mp_dcc8757645c1c32f4481b555710c7039_mixpanel': '%7B%22distinct_id%22%3A%20%22%24device%3A193d3aef6158f1-0af350e2c649b8-26011851-e1000-193d3aef6158f1%22%2C%22%24device_id%22%3A%20%22193d3aef6158f1-0af350e2c649b8-26011851-e1000-193d3aef6158f1%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D',
            'store-info': '%7B%22storeServiceableResponseV2%22%3A%5B%7B%22serviceable%22%3Atrue%2C%22storeId%22%3A%22dc62c1e8-21cf-4d94-920d-e5e08a110322%22%2C%22storeConstruct%22%3A%22PRIMARY_STORE%22%7D%5D%2C%22primaryStoreInfo%22%3A%7B%22name%22%3A%22AHM-Navrangpura%22%2C%22isOnline%22%3Atrue%2C%22openTime%22%3A%2206%3A00%3A00.422918%22%2C%22closeTime%22%3A%2202%3A00%3A00.422934%22%2C%22id%22%3A%22dc62c1e8-21cf-4d94-920d-e5e08a110322%22%2C%22cityId%22%3A%2272fb38ea-adb8-468a-9d7b-be46ae9d7fc1%22%2C%22city%22%3A%7B%22name%22%3A%22Ahmedabad%22%2C%22state%22%3A%22Gujarat%22%2C%22country%22%3A%22India%22%7D%2C%22sellerInfo%22%3A%7B%22name%22%3A%22Drogheria%20Sellers%20Private%20Limited%22%2C%22address%22%3A%22NB%201001%20%26%20NB%201002%2C%20North%20Block%2C%20Empire%20Tower%2C%20Cloud%20City%2C%20Plot%20Gut%20No%2031%2C%20Mouje%20Ejthan%2C%20Airoli%2C%20Mumbai%2C%20Navi%20Mumbai%20Municipal%20Corporation%20(Thane%20Zone-2)%2C%20Maharashtra-400708%22%2C%22fssaiNo%22%3A11522998001570%2C%22showSellerInfo%22%3Afalse%2C%22juspayMerchantId%22%3A%22zepto_drogheria%22%2C%22juspayAndroidClientId%22%3A%22geddit_android%22%2C%22juspayIosClientId%22%3A%22geddit_ios%22%7D%2C%22isOtofEnabled%22%3Afalse%2C%22noBagDeliveryEnabled%22%3Atrue%2C%22isNoBagDeliveryNew%22%3Atrue%2C%22noBagDeliveryDefaultOptStatus%22%3Atrue%2C%22standStillMode%22%3Afalse%2C%22raining%22%3Afalse%2C%22isFullNightDeliveryEnabled%22%3Atrue%2C%22issueAtStore%22%3Afalse%2C%22takingOrders%22%3Atrue%2C%22cartV3Enabled%22%3Atrue%2C%22cartV2Enabled%22%3Afalse%2C%22initiateSdkNewFlow%22%3Atrue%7D%2C%22etaServiceableInfo%22%3A%5B%7B%22storeId%22%3A%22dc62c1e8-21cf-4d94-920d-e5e08a110322%22%2C%22etaInMinutes%22%3A%2212%22%2C%22deliverableType%22%3A%22OPEN%22%2C%22deliverableSubtype%22%3A%22ETA_NORMAL%22%2C%22isDeliverable%22%3Atrue%7D%5D%7D',
            'csrfSecret': '6g_-2RhUCQE',
            'XSRF-TOKEN': 'NYbUqd53QwIyRUqQoykT2%3ADF9r1dAHcK_fmpv7_HxwbXnen1o.AqVJTJqJLzZG5hgBZ9LbYHcY%2F5L6lTa4Ra4A7whWTfw',
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            # 'cookie': 'unique_browser_id=6320098320329916; _gcl_au=1.1.51708254.1734423278; _ga=GA1.1.696673055.1734423279; _fbp=fb.1.1734423279029.18584957053118149; maxWeightLimitCart=25000; _ga_37QQVCR1ZS=GS1.1.1734423446.1.0.1734423446.60.0.0; latitude=22.9937992; longitude=72.5012954; mp_dcc8757645c1c32f4481b555710c7039_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A193d3aef6158f1-0af350e2c649b8-26011851-e1000-193d3aef6158f1%22%2C%22%24device_id%22%3A%20%22193d3aef6158f1-0af350e2c649b8-26011851-e1000-193d3aef6158f1%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; _ga_52LKG2B3L1=GS1.1.1734447756.6.1.1734449034.59.0.207351802; store-info=%7B%22storeServiceableResponseV2%22%3A%5B%7B%22serviceable%22%3Atrue%2C%22storeId%22%3A%22863ee650-1df1-418c-a988-2a64f2c555e3%22%2C%22storeConstruct%22%3A%22PRIMARY_STORE%22%7D%5D%2C%22primaryStoreInfo%22%3A%7B%22name%22%3A%22AHM-Makarba%22%2C%22isOnline%22%3Atrue%2C%22openTime%22%3A%2206%3A00%3A00.248019%22%2C%22closeTime%22%3A%2202%3A00%3A00.248039%22%2C%22id%22%3A%22863ee650-1df1-418c-a988-2a64f2c555e3%22%2C%22cityId%22%3A%2272fb38ea-adb8-468a-9d7b-be46ae9d7fc1%22%2C%22city%22%3A%7B%22name%22%3A%22Ahmedabad%22%2C%22state%22%3A%22Gujarat%22%2C%22country%22%3A%22India%22%7D%2C%22sellerInfo%22%3A%7B%22name%22%3A%22Drogheria%20Sellers%20Private%20Limited%22%2C%22address%22%3A%22NB%201001%20%26%20NB%201002%2C%20North%20Block%2C%20Empire%20Tower%2C%20Cloud%20City%2C%20Plot%20Gut%20No%2031%2C%20Mouje%20Ejthan%2C%20Airoli%2C%20Mumbai%2C%20Navi%20Mumbai%20Municipal%20Corporation%20(Thane%20Zone-2)%2C%20Maharashtra-400708%22%2C%22fssaiNo%22%3A11522998001570%2C%22showSellerInfo%22%3Afalse%2C%22juspayMerchantId%22%3A%22zepto_drogheria%22%2C%22juspayAndroidClientId%22%3A%22geddit_android%22%2C%22juspayIosClientId%22%3A%22geddit_ios%22%7D%2C%22isOtofEnabled%22%3Afalse%2C%22noBagDeliveryEnabled%22%3Atrue%2C%22isNoBagDeliveryNew%22%3Atrue%2C%22noBagDeliveryDefaultOptStatus%22%3Atrue%2C%22standStillMode%22%3Afalse%2C%22raining%22%3Afalse%2C%22isFullNightDeliveryEnabled%22%3Afalse%2C%22issueAtStore%22%3Afalse%2C%22takingOrders%22%3Atrue%2C%22cartV3Enabled%22%3Atrue%2C%22cartV2Enabled%22%3Afalse%2C%22initiateSdkNewFlow%22%3Atrue%7D%2C%22etaServiceableInfo%22%3A%5B%7B%22storeId%22%3A%22863ee650-1df1-418c-a988-2a64f2c555e3%22%2C%22etaInMinutes%22%3A%2211%22%2C%22deliverableType%22%3A%22OPEN%22%2C%22deliverableSubtype%22%3A%22ETA_NORMAL%22%2C%22isDeliverable%22%3Atrue%7D%5D%7D; csrfSecret=NuD2PPt4ZP8; XSRF-TOKEN=ez55CLRg7YkoYozEOFfqG%3Arqwe5Y9OUoH20xvK-z8vKXsrmZA.wedCmfgbOX3soyc2cOVK%2FLRpsR0MiR8s3gmWwd8fNpY',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }
        # Fetch URLs with status 'pending' from the database
        self.cursor.execute(f"SELECT id, product_url FROM {self.fetch_table}  WHERE status = 'pending' and id between {self.start_id} and {self.end_id}")
        rows = self.cursor.fetchall()
        request_count = 0  # To track the number of requests made
        # Loop through rows and yield requests for each URL
        random_number = random.randint(10000000, 99999999)
        # username = f'actowiz-stc-US'
        username = f'actowiz-res-US-any-sid-{random_number}'
        password = 'yYNSa0hbTdfNUIh'
        server = 'gw.ntnt.io'
        port = '5959'
        # proxy = {
        #     'http': f'http://{username}:{password}@{server}:{port}',
        #     'https': f'http://{username}:{password}@{server}:{port}'
        # }
        for row in rows:
            id, url = row
            # max_requests_before_vpn_change = 500  # Change VPN after this many requests
            # if request_count==max_requests_before_vpn_change:
            #     with ExpressVpnApi() as api:
            #
            #         with open(r"C:\Users\Actowiz\Desktop\Pritesh_project\zepto\zepto\us.json", "r") as file:
            #             data = json.load(file)
            #
            #         if isinstance(data, list):
            #             loc = random.choice(data)
            #             print("Selected Location:", loc)
            #             api.connect(loc["id"])
            #             time.sleep(6)
            #             request_count=0
            #         else:
            #             print("The JSON data is not a list.")
            # else:
            #     request_count +=1

            yield scrapy.Request(url,
                                 headers=headers,
                                 cookies=cookies,
                                 callback=self.parse,
                                 meta={
                                     'id': id,
                                     'impersonate': 'edge99',
                                     'proxy':f'http://{username}:{password}@{server}:{port}'
                                       })

    def create_scraped_data_table(self):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.create_and_insert_table} (
            ID INT AUTO_INCREMENT PRIMARY KEY,
            ProductCode TEXT ,
            ProductURL TEXT ,
            ProductName TEXT,
            Image TEXT,
            Quantity TEXT,
            MRP TEXT,
            SellingPrice TEXT,
            Discount TEXT,
            StockAvailability TEXT,
            Categories TEXT,
            Rating TEXT,
            Reviews TEXT,
            Pincode TEXT,
            DeliveryTime TEXT,
            City TEXT,
            Date VARCHAR(255)
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")


    def get_availability(self,response):
        parsed = lxml.html.fromstring(response.text)
        availability = parsed.xpath('//button[@aria-label="Increase quantity by 1"]/@disabled | //button[@aria-label="Notify Me"]')
        if not availability:
            return 'In Stock'
        else:
            return 'Out of stock'

    # Parsing the response and updating the status
    def parse(self, response):
        # Get the ID from the meta attribute
        record_id = response.meta['id']

        # Process the response (example: extract title)
        product_code = response.url.rstrip('/').split('/')[-1]
        filename = f"{product_code}.html"
        # Define the path where the response will be saved
        save_directory = r"C:\Users\Actowiz\Desktop\zepto_response_2"

        # Ensure the save directory exists
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)

        # Full path to save the file
        file_path = os.path.join(save_directory, filename)

        # Save the response to the specified path
        with open(file_path, 'wb') as f:
            f.write(response.body)
        self.log(f"Saved response to {file_path}")
        product_url=response.url
        product_name=response.xpath('//span[@class="text-sm font-semibold leading-[14px] text-[#101418]"]/text()').get('')
        if not product_name or product_name.strip() == '':
            self.update_status_to_404(record_id)
            self.log(f"Product not found for {product_url}. Status updated to 404.")
            return  # Exit the parse function, no further processing needed # Exit the parse function, no further processing needed

        product_img=response.xpath('//div[@class="relative aspect-square h-full w-full snap-center"]/img[1]/@src').get(' ')
        quantity=response.xpath('//p[@class="mt-2 text-sm leading-4 text-[#757C8D]"]/span/text()').get('')
        mrp=response.xpath('//span[@class="line-through font-bold"]/text()').get('')
        mrp_final=mrp.replace('₹','')
        selling_price=response.xpath('//span[@class="text-[32px] font-medium leading-[30px] text-[#262A33]"]/text()').get('')
        selling_price_final=selling_price.replace('₹','')
        discount=response.xpath('//p[@class="text-[14px] font-semibold leading-[21.6px] tracking-[-0.24px] text-[#079761]"]//text()').getall()
        final_dic=''.join(discount)
        stock_availability=self.get_availability(response)
        category = response.xpath('//span[@class="flex shrink-0 items-center gap-2"]//a//text()').getall()

        # Extract the product name
        name = response.xpath('//span[@class="text-sm font-semibold leading-[14px] text-[#101418]"]/text()').get()

        # Combine categories and name
        formatted_category = ' | '.join(category + [name])
        rating=''
        reviews=''
        pincode=''
        delivery_time=''
        city=''
        date1=datetime.date.today()
        # Format the date in "DD-MM-YYYY" format
        formatted_date = date1.strftime("%d-%m-%Y")
        print(product_code, product_url, product_name, product_img, quantity, mrp_final, selling_price_final, final_dic, stock_availability, formatted_category, rating, reviews, pincode, delivery_time, city, formatted_date)

        # # Insert the scraped data into the new table
        self.insert_scraped_data(product_code, product_url, product_name, product_img, quantity, mrp_final, selling_price_final, final_dic, stock_availability, formatted_category, rating, reviews, pincode, delivery_time, city, formatted_date)
        #
        # # Now update the database to mark the URL as "done"
        self.update_status(record_id)

# Function to insert the scraped data into the new table
    def insert_scraped_data(self, product_code, product_url, product_name, product_img, quantity, mrp, selling_price, discount, stock_availability, category, rating, reviews, pincode, delivery_time, city, date):
        # SQL query to insert data into the scraped_data table
        insert_query =f"""
        INSERT INTO {self.create_and_insert_table} (
            ProductCode, ProductURL, ProductName, Image, Quantity, MRP, SellingPrice, Discount,
            StockAvailability, Categories, Rating, Reviews, Pincode, DeliveryTime, City, Date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(insert_query, (
            product_code, product_url, product_name, product_img, quantity, mrp, selling_price, discount,
            stock_availability, category, rating, reviews, pincode, delivery_time, city, date
        ))
        self.db_conn.commit()


    # Function to update the status to 'done'
    def update_status(self, record_id):
        # Update the status in the database to 'done' after scraping the URL
        self.cursor.execute(f"UPDATE {self.fetch_table} SET status = 'done' WHERE id = %s", (record_id,))
        self.db_conn.commit()

    def update_status_to_404(self, record_id):
        # Update the status in the database to '404' when product name is not found
        update_query = f"UPDATE {self.fetch_table} SET status = '404' WHERE id = %s"
        self.cursor.execute(update_query, (record_id,))
        self.db_conn.commit()

    # Close the database connection when the spider is closed
    def close(self, reason):
        self.cursor.close()
        self.db_conn.close()

if __name__=="__main__":
    process = CrawlerProcess()
    process.crawl(ZeptoSpiderSpider)  # Pass arguments if needed
    process.start()