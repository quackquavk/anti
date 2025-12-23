import asyncio
import re
from playwright.async_api import async_playwright
from .extractor import Extractor
from .storage import Storage

class ScraperEngine:
    def __init__(self, headless=False, log_callback=None, result_callback=None):
        self.headless = headless
        self.extractor = Extractor()
        self.storage = Storage()
        self.log_callback = log_callback or print
        self.result_callback = result_callback  # Called after each result with (result, current_count, total)

    async def get_location_coordinates(self, location_name):
        """
        Navigates to Google Maps, searches for the location, and extracts coordinates from the URL.
        Returns: (lat, lon) tuple or None
        """
        async with async_playwright() as p:
            launch_args = ["--disable-gpu", "--disable-dev-shm-usage", "--disable-extensions"]
            browser = await p.chromium.launch(headless=self.headless, args=launch_args)
            page = await browser.new_page()
            
            print(f"Calibrating location: {location_name}...")
            await page.goto("https://www.google.com/maps?hl=en")
            await page.wait_for_selector("input#searchboxinput")
            await page.fill("input#searchboxinput", location_name)
            await page.keyboard.press("Enter")
            
            # Wait for URL to change to contain coordinates '@lat,lon'
            try:
                # Wait up to 10 seconds for the URL to update with coordinates
                # Regex match for @ followed by numbers
                await page.wait_for_url(re.compile(r"@-?\d+\.\d+,-?\d+\.\d+"), timeout=10000)
            except:
                print("  URL didn't update with coordinates clearly. Trying to proceed anyway.")
                await asyncio.sleep(3)

            url = page.url
            await browser.close()
            
            # Extract @lat,lon,zoom
            # Format: .../place/Location/@27.7089603,85.3261328,14z/... or .../search/Location/@27.7,85.3,14z
            match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                print(f"  Found coordinates: {lat}, {lon}")
                return lat, lon
            else:
                print("  Could not extract coordinates from URL.")
                return None

    async def handle_age_gate(self, page):
        """Attempts to click generic age verification / entry buttons."""
        try:
            match_texts = ["enter", "enter site", "yes", "i am 18", "i am 21", "agree", "confirm", "verify", "submit", "accept"]
            for text in match_texts:
                try:
                    xpath = f"//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')] | //a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')] | //input[@type='submit' and contains(translate(@value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]"
                    element = await page.query_selector(xpath)
                    if element and await element.is_visible():
                        await element.click(timeout=2000)
                        await asyncio.sleep(1)
                        return
                except: pass
        except: pass

    async def _get_phone_from_details(self, page):
        """Attempts to extract phone number specifically from the details pane using robust selectors."""
        try:
            phone_btn = await page.query_selector("button[data-item-id^='phone:tel:']")
            if phone_btn:
                text = await phone_btn.inner_text()
                extracted = self.extractor.extract_phone(text)
                if extracted: return extracted
            
            aria_phone_elements = await page.query_selector_all("[aria-label*='Phone'], [aria-label*='फोन']")
            for el in aria_phone_elements:
                label = await el.get_attribute("aria-label")
                extracted = self.extractor.extract_phone(label)
                if extracted: return extracted

            icon_elements = await page.query_selector_all("//*[contains(text(), '')]")
            for el in icon_elements:
                text = await el.inner_text()
                extracted = self.extractor.extract_phone(text)
                if extracted: return extracted
                
            copy_btn = await page.query_selector("button[data-tooltip='Copy phone number']")
            if copy_btn:
                 val = await copy_btn.get_attribute("data-value")
                 if val: return val

            return None
        except Exception as e:
            return None

    async def _process_single_item(self, page, context, el, i, total):
        text = await el.inner_text()
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        name = "Unknown"
        if lines:
            name = lines[0]
            if name == "Sponsored" or name == "Ad":
                if len(lines) > 1:
                    name = lines[1]
                    if len(name) < 3 and len(lines) > 2: name = lines[2]
        
        self.log_callback(f"Processing {i+1}/{total}: {name}")
        
        await el.click()
        await asyncio.sleep(2) 
        
        website = None
        try:
            website_btn = await page.query_selector('a[data-item-id="authority"]')
            if website_btn: website = await website_btn.get_attribute("href")
        except: pass

        phone = None
        try:
            phone = self.extractor.extract_phone(text)
            phone_from_details = await self._get_phone_from_details(page)
            if phone_from_details: phone = phone_from_details
        except: pass
        
        email = None
        website_phone = None
        share_link = None
        
        if website:
            print(f"  Found website: {website}. Visiting...")
            try:
                site_page = await context.new_page()
                try:
                    await site_page.goto(website, timeout=15000)
                    await self.handle_age_gate(site_page)
                    content = await site_page.content()
                    email = self.extractor.extract_email(content)
                    website_phone = self.extractor.extract_phone(content)
                    
                    if not email:
                        contact_links = await site_page.query_selector_all('a[href*="contact" i]')
                        if not contact_links:
                                contact_links = await site_page.query_selector_all('//a[contains(translate(@href, "CONTACT", "contact"), "contact")]')
                        if contact_links:
                            contact_href = await contact_links[0].get_attribute("href")
                            if contact_href:
                                try:
                                    await site_page.goto(contact_href, timeout=10000)
                                    content = await site_page.content()
                                    email = self.extractor.extract_email(content)
                                    if not website_phone: website_phone = self.extractor.extract_phone(content)
                                except: pass
                except: pass
                finally: await site_page.close()
            except: pass
        
        final_phone = phone if phone else website_phone
        if email: self.log_callback(f"  SUCCESS: Email {email}")
        if final_phone: self.log_callback(f"  SUCCESS: Phone {final_phone}")

        try:
            share_btn = await page.query_selector('button[data-value="Share"]')
            if not share_btn: share_btn = await page.query_selector('button[aria-label*="Share"]')
            if not share_btn: share_btn = await page.query_selector('button:has(span[class*="google-symbols"])')

            if share_btn:
                await share_btn.click()
                try:
                    await page.wait_for_selector('input.vrsrZe', timeout=3000)
                    link_input = await page.query_selector('input.vrsrZe')
                    if link_input: share_link = await link_input.get_attribute("value")
                except: pass
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.5)
        except: pass

        return {
            "name": name,
            "website": website,
            "email": email,
            "phone": final_phone,
            "location_link": share_link,
            "raw_text": text[:100]
        }

    async def run(self, search_term, total, lat=None, lon=None, zoom=None):
        async with async_playwright() as p:
            launch_args = ["--disable-gpu", "--disable-dev-shm-usage", "--disable-extensions"]
            browser = await p.chromium.launch(headless=self.headless, args=launch_args)
            context = await browser.new_context()
            page = await context.new_page()
            
            # Construct URL with coordinates if provided
            if lat and lon and zoom:
                # url = f"https://www.google.com/maps/search/{search_term}/@{lat},{lon},{zoom}z/data=!3m1!4b1?hl=en"
                # Using simple search URL with viewport
                url = f"https://www.google.com/maps/search/{search_term}/@{lat},{lon},{zoom}z?hl=en"
                self.log_callback(f"Navigating to grid point: {lat}, {lon} (Zoom: {zoom})")
            else:
                url = "https://www.google.com/maps?hl=en"
                print(f"Navigating to Google Maps for: {search_term}")
            
            await page.goto(url)
            
            # If standard search, we need to type and enter
            if not (lat and lon and zoom):
                await page.wait_for_selector("input#searchboxinput")
                await page.fill("input#searchboxinput", search_term)
                await page.keyboard.press("Enter")
                self.log_callback("Searching...")

            # Wait for results feed
            try:
                await page.wait_for_selector('div[role="feed"]', timeout=10000)
            except:
                print("Could not find results feed. Trying to wait for generic results.")
                await asyncio.sleep(5)

            feed_selector = 'div[role="feed"]'
            results = []
            
            self.log_callback(f"Scraping up to {total} results...")
            
            no_change_counter = 0
            previous_count = 0
            
            while len(results) < total:
                elements = await page.query_selector_all('div[role="article"]')
                current_count = len(elements)
                
                if current_count == previous_count:
                    no_change_counter += 1
                else:
                    no_change_counter = 0
                
                previous_count = current_count
                
                if no_change_counter >= 5:
                    self.log_callback("  End of list or stuck.")
                    break

                # Scroll down
                try:
                    await page.evaluate(f"const feed = document.querySelector('{feed_selector}'); if(feed) {{ feed.scrollTop = feed.scrollHeight; }}")
                except Exception: pass
                    
                await asyncio.sleep(2)
                
                if len(elements) >= total:
                    break
            
            elements = await page.query_selector_all('div[role="article"]')
            
            for i, el in enumerate(elements[:total]):
                # Wrap the processing of each item in a timeout to prevent getting stuck
                try:
                    result = await asyncio.wait_for(self._process_single_item(page, context, el, i, total), timeout=60)
                    if result:
                        results.append(result)
                        if self.result_callback:
                            self.result_callback(result, len(results), total)

                except asyncio.TimeoutError:
                    self.log_callback(f"  !!! TIMEOUT processing item {i+1}. skipping...")
                    # Cleanup: Close any extra pages that might have been opened
                    for p in context.pages:
                        if p != page:
                            try:
                                await p.close()
                            except: pass

                except Exception as e:
                    self.log_callback(f"Error processing item {i}: {e}")

            await browser.close()
            return results
