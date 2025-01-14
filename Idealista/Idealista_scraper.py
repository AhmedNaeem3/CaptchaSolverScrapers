import os
import csv
import json
import pytz
import logging
import asyncio
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from scrapfly import ScrapeConfig, ScrapflyClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()


class IdealistaScraper:
    HEADERS = {
        "connection": "keep-alive"
    }
    SCRAPE_DATE_TIME = datetime.now(tz=pytz.utc).strftime("%d%m%Y_%H%M")

    def __init__(self):
        self.scrapfly = ScrapflyClient(key=self.get_api_key())
        self.base_url = "{base_url}"
        self.search_url = "{search_url}"
        self.scraped_data = []
        self.sleep = 1

    @staticmethod
    def get_api_key():
        """Retrieve the API key from the environment variables."""
        api_key = os.getenv('API_KEY')
        if not api_key:
            raise ValueError("API_KEY not found in environment variables")
        return api_key

    async def scrape_page(self):
        url = self.search_url.format(
            base_url=self.base_url
        )
        try:
            # Scrape the main page
            scrape_config = ScrapeConfig(
                url=url,
                country="PT",
                headers=self.HEADERS,
                asp=True,
                cache=True,
                cache_ttl=86400
            )
            response = await self.scrapfly.async_scrape(scrape_config)
            soup = BeautifulSoup(response.content, "html.parser")
            regions = soup.select(".locations-list a")

            for region in regions:
                region_url = region.get('href')
                region_complete_url = f"{self.base_url}{region_url}"
                region_name = region.text

                logger.info(f"Results for region: {region_name}")
                logger.info(f"Requesting to region url: {region_complete_url}")

                await asyncio.sleep(self.sleep)
                await self.scrape_region_page(region_complete_url)

        except Exception as e:
            logger.error(f"An error occurred while processing {url}: {e}")

    async def scrape_region_page(self, url):
        try:

            scrape_config = ScrapeConfig(
                url=url,
                country="PT",
                headers=self.HEADERS,
                asp=True,
                cache=True,
                cache_ttl=86400
            )
            response = await self.scrapfly.async_scrape(scrape_config)
            soup = BeautifulSoup(response.content, "html.parser")

            view_all_products = soup.select_one(".container .title a")
            if view_all_products:
                listing_page_url = view_all_products.get('href')
                listing_url = f"{self.base_url}{listing_page_url}"

                scrape_config = ScrapeConfig(
                    url=listing_url,
                    country="PT",
                    headers=self.HEADERS,
                    asp=True,
                    cache=True,
                    cache_ttl=86400
                )
                response = await self.scrapfly.async_scrape(scrape_config)
                soup = BeautifulSoup(response.content, "html.parser")

            property_listings = soup.select('.item-multimedia')
            await self.scrape_listing_urls(property_listings)

            next_page = soup.select_one(".pagination .next a")
            if next_page:
                next_page_url = f"{self.base_url}{next_page.get('href')}"
                logger.info(f"Requesting to url {next_page_url}")
                await asyncio.sleep(self.sleep)
                await self.scrape_region_page(next_page_url)

        except Exception as e:
            logger.error(f"An error occurred while scraping the region {url}: {e}")

    async def scrape_listing_urls(self, property_listings):

        for property in property_listings:
            try:
                land_type = None
                property_details = property.select(".item-detail-char")
                if len(property_details) > 1:
                    land_type = property_details[1].text

                if not land_type:
                    continue
                if "Não urbanizável" in land_type:
                    continue

                property_price = property.select_one(".price-row .item-price").text.strip()
                property_price = property_price.split("€")[0].replace('.', '')

                if int(property_price) > 120000:
                    continue

                url = property.select_one(".item-link").get('href')
                property_url = f"{self.base_url}{url}"

                if any(data["Property_Url"] == property_url for data in self.scraped_data):
                    logger.info(f"Skipping already scraped property: {property_url}")
                    continue

                scrape_config = ScrapeConfig(
                    url=property_url,
                    country="PT",
                    headers=self.HEADERS,
                    asp=True,
                    cache=True,
                    cache_ttl=86400
                )
                response = await self.scrapfly.async_scrape(scrape_config)

                # Check if the response status is not 200
                if response.status_code != 200:
                    logger.error(f"Error: Received status code {response.status_code} for URL: {property_url}")
                    continue

                soup = BeautifulSoup(response.content, "html.parser")
                await self.parse_products(soup, property_url, land_type)

            except Exception as e:
                logger.error(f"An error occurred while processing: {e}")

    async def parse_products(self, soup, url, type_of_land):

        particular_property = soup.select_one(".professional-name")
        if particular_property:
            if "Particular" != particular_property.text.strip():
                logger.info(f"Skipping not a particular property: {url}")
                return

        property_features = soup.select(".details-property-feature-one li")
        buildable_area = [
            feature.text.split("edificável")[1].strip()
            for feature in property_features
            if "Superfície edificável" in feature.text
        ]
        if buildable_area:
            buildable_area = buildable_area[0].split("m²")[0].strip()
            check_buildable_area = buildable_area
            if "." in check_buildable_area:
                check_buildable_area = float(check_buildable_area)
            if int(check_buildable_area) < 70:
                logger.info(f"Skipping buildable area of property less than 70: {url}")
                return
        else:
            buildable_area = None
        property_url = url
        listing_reference = url.split("imovel/")[1].split("/")[0]
        land_type = type_of_land
        property_details = soup.select_one(".detail-info")
        property_name = property_details.select_one(".main-info__title-main").text.strip()
        total_land_area = property_details.select_one(".info-features").text.strip()
        total_land_area = total_land_area.split("m²")[0].strip().replace(".", ",")

        property_price = soup.select_one("#mortgages .toggle-price")
        price = property_price.select_one(".flex-feature").text.strip()
        price = price.split("€")[0].strip().replace(".", ",")
        price_per = property_price.select(".squaredmeterprice")[1].text.strip()
        price_per = price_per.split("€/m²")[0]

        location_elements = soup.select("#mapWrapper")
        location = ", ".join([element.text.strip() for element in location_elements])

        contact_number1 = None
        contact_number2 = None
        contact_info_container = soup.select("#contact-phones-container")
        if contact_info_container:
            path = f"/pt/ajax/ads/{listing_reference}/contact-phones"
            encoded_path = quote(path)
            contact_info_url = f"https://www.idealista.pt{encoded_path}?dummy=1"
            try:
                scrape_config = ScrapeConfig(
                    url=contact_info_url,
                    country="PT",
                    headers={
                        "connection":"keep-alive"
                    },
                    asp=True,
                    cache=True,
                    cache_ttl=86400
                )

                response = await self.scrapfly.async_scrape(scrape_config)
                json_data = json.loads(response.content)

                contact_number1 = json_data.get("phone1", {}).get("number")
                contact_number2 = json_data.get("phone2", {})
                if contact_number2:
                    contact_number2 = json_data.get("number")
                else:
                    contact_number2 = None
            except Exception as e:
                logger.error(f"An error occurred while fetching contact information for {property_url}: {e}")

        new_scraped_data = []
        new_dict = {
            "Property_Url":property_url,
            "Listing_Reference":listing_reference,
            "Property_Name": property_name,
            "Property_Price": price,
            "Price_per": price_per,
            "Land_Type": land_type,
            "Location": location,
            "Total_Land_Area": total_land_area,
            "Buildable_Area":buildable_area,
            "Contact_Number1":contact_number1,
            "Contact_Number2":contact_number2
        }

        self.scraped_data.append(new_dict)
        new_scraped_data.append(new_dict)
        self.save_to_csv(new_scraped_data)
        logger.info(f"Results: {new_dict}")

    def save_to_csv(self, scraped_data):

        file_exists = os.path.isfile(f'Scraped_data{self.SCRAPE_DATE_TIME}.csv')
        with open(f'Scraped_data{self.SCRAPE_DATE_TIME}.csv', 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow([
                    "Property_Url", "Listing_Reference", "Property_Name", "Property_Price", "Price_per", "Land_Type",
                    "Location", "Total_Land_Area", "Buildable_Area", "Contact_Number1", "Contact_Number2"
                ])
            for data in scraped_data:
                writer.writerow(data.values())


if __name__ == "__main__":
    scraper = IdealistaScraper()
    asyncio.run(scraper.scrape_page())










