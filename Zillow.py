import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
import random
import time
from datetime import datetime
import os
import re
from typing import List, Dict, Tuple, Optional

# Set up enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zillow_debug_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ZillowDebugScraper:
    def __init__(self):
        self.all_results = []
        self.processed_locations = []
        self.skipped_cities = []

        # Province to abbreviation mapping
        self.province_abbreviations = {
            'alberta': 'ab', 'british columbia': 'bc', 'manitoba': 'mb',
            'new brunswick': 'nb', 'newfoundland and labrador': 'nl',
            'nova scotia': 'ns', 'ontario': 'on', 'prince edward island': 'pe',
            'quebec': 'qc', 'saskatchewan': 'sk', 'northwest territories': 'nt',
            'nunavut': 'nu', 'yukon': 'yt'
        }

    async def create_browser(self):
        """Create browser for manual CAPTCHA solving"""
        playwright = await async_playwright().start()

        browser = await playwright.chromium.launch(
            headless=False,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
        )

        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

        # Anti-detection
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        return playwright, browser, context

    def load_locations_from_file(self, file_path: str) -> List[Dict]:
        """Load cities and states from input file"""
        logger.info(f"Loading locations from: {file_path}")

        try:
            file_path = file_path.strip('"\'')

            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel files.")

            logger.info(f"Available columns in file: {list(df.columns)}")

            required_columns = {'state': 'Province', 'city': 'City Name'}
            missing_columns = []

            for internal_name, actual_name in required_columns.items():
                if actual_name not in df.columns:
                    missing_columns.append(actual_name)

            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            locations = []
            for _, row in df.iterrows():
                city = row['City Name']
                province = row['Province']

                if pd.isna(city) or pd.isna(province) or str(city).strip() == '' or str(province).strip() == '':
                    continue

                province_abbr = self.get_province_abbreviation(str(province).strip())

                locations.append({
                    'city': str(city).strip(),
                    'state': str(province).strip(),
                    'state_abbr': province_abbr,
                    'search_url': f"https://www.zillow.com/homes/{city.lower().replace(' ', '-').replace(',', '').replace('.', '')}-{province_abbr.lower()}_rb/"
                })

            logger.info(f"Loaded {len(locations)} locations from file")
            return locations

        except Exception as e:
            logger.error(f"Failed to load locations file: {e}")
            return []

    def get_province_abbreviation(self, province_name: str) -> str:
        """Convert full province name to abbreviation"""
        province_lower = province_name.lower().strip()

        if province_lower in self.province_abbreviations:
            return self.province_abbreviations[province_lower]

        variations = {
            'b.c.': 'bc', 'b c': 'bc', 'bc': 'bc',
            'ont.': 'on', 'ont': 'on', 'queb.': 'qc', 'queb': 'qc',
            'alb.': 'ab', 'alb': 'ab', 'man.': 'mb', 'man': 'mb',
            'sask.': 'sk', 'sask': 'sk'
        }

        if province_lower in variations:
            return variations[province_lower]

        logger.warning(f"Unknown province: {province_name}, using 'on' as default")
        return 'on'

    async def wait_for_manual_captcha_solution(self, page, location_name):
        """Wait for user to manually solve the CAPTCHA"""
        logger.info(f"🔍 CHECKING FOR CAPTCHA FOR: {location_name}")

        max_wait_time = 300
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            current_url = page.url
            page_content = await page.content()

            # Check if we're on CAPTCHA page
            if 'Press & Hold to confirm you are' in page_content:
                logger.info("🚨 PERIMETERX CAPTCHA DETECTED!")
                logger.info("👆 PLEASE MANUALLY SOLVE THE CAPTCHA NOW")
                logger.info("⏳ Waiting for you to solve the CAPTCHA...")

                try:
                    await page.wait_for_navigation(timeout=120000)
                    logger.info("✅ CAPTCHA APPEARS TO BE SOLVED! Continuing...")
                    return True
                except Exception as e:
                    logger.info("⏰ Still waiting for CAPTCHA solution...")
                    await page.wait_for_timeout(5000)
                    continue

            # Check if we successfully passed CAPTCHA and have search results
            elif await self.has_search_results(page):
                logger.info("✅ Already on search results page - no CAPTCHA needed")
                return True

            else:
                await page.wait_for_timeout(5000)
                continue

        logger.error("⏰ Timeout waiting for CAPTCHA solution")
        return False

    async def has_search_results(self, page):
        """Check if we're on a page with search results"""
        try:
            # Check for actual property cards
            listings = await page.query_selector_all('[data-testid="property-card"]')
            if listings and len(listings) > 0:
                return True

            # Alternative selectors
            alt_listings = await page.query_selector_all('.property-card, .list-card, [class*="PropertyCard"]')
            if alt_listings and len(alt_listings) > 0:
                return True

            return False
        except:
            return False

    async def extract_listings_debug(self, page):
        """Debug extraction with detailed logging"""
        clean_listings = []

        try:
            # Wait for listings to load
            await page.wait_for_timeout(5000)

            # Get property cards
            listings = await page.query_selector_all('[data-testid="property-card"]')
            logger.info(f"🔍 Found {len(listings)} property cards")

            if not listings:
                logger.warning("❌ No property cards found")
                return []

            for i, listing in enumerate(listings):
                try:
                    logger.info(f"  📝 Processing listing {i+1}/{len(listings)}")

                    # Get full text for processing
                    full_text = await listing.text_content()
                    logger.info(f"  📄 FULL TEXT: {full_text}")

                    # Parse the text to extract data
                    listing_data = await self.parse_listing_text_debug(full_text, i+1)

                    if listing_data:
                        logger.info(f"  ✅ SUCCESS: {listing_data.get('address', 'No address')} | {listing_data.get('price', 'No price')} | {listing_data.get('beds', 'No beds')} beds | {listing_data.get('baths', 'No baths')} baths")
                        clean_listings.append(listing_data)
                    else:
                        logger.info(f"  ❌ FAILED: Could not parse listing data")

                except Exception as e:
                    logger.error(f"  💥 ERROR: {e}")
                    continue

            logger.info(f"🎯 Extraction result: {len(clean_listings)}/{len(listings)} listings")
            return clean_listings

        except Exception as e:
            logger.error(f"Error extracting listings: {e}")
            return []

    async def parse_listing_text_debug(self, full_text, listing_num):
        """Debug parsing with detailed logging"""
        try:
            logger.info(f"    🔍 [Listing {listing_num}] Starting text parsing...")

            if not full_text:
                logger.info(f"    ❌ [Listing {listing_num}] No text content")
                return None

            # Clean up the text by removing newlines and extra spaces
            text = ' '.join(full_text.split())

            # The junk text usually appears after the core details.
            # We can often find a reliable word like 'Show more' to split the string
            # and only keep the important part.
            if 'Show more' in text:
                text = text.split('Show more')[0]

            # Also remove other known junk patterns (like navigation, image indicators)
            junk_patterns = [
                'Save this home', 'Previous photo', 'Next photo',
                'Use arrow keys to navigate', r'Image \d+ of \d+', 'Loading',
                'Press & Hold'
            ]
            for pattern in junk_patterns:
                text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()

            logger.info(f"    📝 [Listing {listing_num}] Cleaned Text for Parsing: {text}")

            # --- Improved Parsing Logic on the cleaned text ---

            # Extract price
            price_match = re.search(r'(C?\$|CAD)\s*([\d,]+\.?\d*)', text)
            price = "Price not listed"
            if price_match:
                price = f"${price_match.group(2).replace(',', '')}" # Remove comma for clean number
                logger.info(f"    💰 [Listing {listing_num}] Price found: {price}")
            else:
                logger.info(f"    ❌ [Listing {listing_num}] No price pattern found")

            # Extract beds (more specific regex to avoid parts of address)
            beds = "Not specified"
            beds_match = re.search(r'(\d+)\s*(?:bds?|beds?|bd)', text, re.IGNORECASE)
            if beds_match:
                bed_count = int(beds_match.group(1))
                if bed_count > 0 and bed_count < 20: # Sanity check for bed count
                    beds = str(bed_count)
                    logger.info(f"    🛏️ [Listing {listing_num}] Beds found: {beds}")
                else:
                    logger.info(f"    ⚠️ [Listing {listing_num}] Suspicious bed count ({bed_count}), setting to 'Not specified'")
            elif 'studio' in text.lower():
                beds = "Studio"
                logger.info(f"    🛏️ [Listing {listing_num}] Studio found")
            else:
                logger.info(f"    ❌ [Listing {listing_num}] No beds pattern found")

            # Extract baths (more specific regex to avoid parts of address)
            baths = "Not specified"
            baths_match = re.search(r'(\d+\.?\d*)\s*(?:ba|baths?)', text, re.IGNORECASE)
            if baths_match:
                bath_count = float(baths_match.group(1))
                if bath_count > 0 and bath_count < 20: # Sanity check for bath count
                    baths = str(bath_count)
                    logger.info(f"    🚿 [Listing {listing_num}] Baths found: {baths}")
                else:
                    logger.info(f"    ⚠️ [Listing {listing_num}] Suspicious bath count ({bath_count}), setting to 'Not specified'")
            else:
                logger.info(f"    ❌ [Listing {listing_num}] No baths pattern found")

            # Extract address - the address is usually everything before the MLS ID, price, or bed/bath.
            # We'll try to be more precise to capture only the street address
            address = "Address not found"
            # Look for common address patterns (number, street name, type)
            # This is a bit complex, so we'll try a few variations and clean it up.
            
            # Pattern 1: Starts with a number, ends before a comma or keywords
            address_match = re.search(r'^(\d+\s+.*?)(?:,\s*[A-Z]{2,}\s+[A-Z\d]+|MLS® ID|C\$|\d+\s*bds|\d+\s*ba)', text, re.IGNORECASE)
            if address_match:
                address = address_match.group(1).strip()
                # Remove brokerage info if it's accidentally included at the end
                address = re.sub(r',\s*RE/MAX.*|,\s*ROYAL LEPAGE.*|,\s*THE AGENCY.*|,\s*KELLER WILLIAMS.*|,\s*BOSLEY REAL ESTATE.*|,\s*HAMMOND INTERNATIONAL PROPERTIES.*|,\s*NEST SEEKERS INTERNATIONAL REAL ESTATE.*', '', address, flags=re.IGNORECASE).strip()
                logger.info(f"    🏠 [Listing {listing_num}] Address found (pattern 1): '{address}'")
            else:
                # Fallback: take everything before the first bed/bath/price indicator or MLS
                fallback_address_match = re.search(r'^(.*?)(?=\s*(C?\$|CAD|MLS®|bds?|beds?|bd|ba|baths?))', text, re.IGNORECASE)
                if fallback_address_match:
                    address = fallback_address_match.group(1).strip(', ')
                    # Clean up if brokerage name is at the end
                    address = re.sub(r',\s*RE/MAX.*|,\s*ROYAL LEPAGE.*|,\s*THE AGENCY.*|,\s*KELLER WILLIAMS.*|,\s*BOSLEY REAL ESTATE.*|,\s*HAMMOND INTERNATIONAL PROPERTIES.*|,\s*NEST SEEKERS INTERNATIONAL REAL ESTATE.*', '', address, flags=re.IGNORECASE).strip()
                    logger.info(f"    🏠 [Listing {listing_num}] Address found (fallback): '{address}'")
                else:
                    logger.info(f"    ❌ [Listing {listing_num}] No robust address pattern found.")
            
            # Extract property name (address before the first comma)
            property_name = address.split(',')[0].strip()
            if not property_name and address != "Address not found":
                property_name = address # If no comma, use the full address
            elif property_name == "Address not found":
                property_name = "Name not available"


            # Extract property type
            property_type = "Property"
            if 'townhouse' in text.lower():
                property_type = "Townhouse"
            elif 'condo' in text.lower():
                property_type = "Condo"
            elif 'house' in text.lower():
                property_type = "House"
            elif 'apartment' in text.lower():
                property_type = "Apartment"
            elif 'lot / land' in text.lower() or 'sqft lot' in text.lower():
                property_type = "Lot / Land"
            logger.info(f"    🏡 [Listing {listing_num}] Property type: {property_type}")

            # Extract MLS ID
            mls_match = re.search(r'MLS® ID #([A-Z0-9]+)', text)
            mls_id = mls_match.group(1) if mls_match else "Not available"
            logger.info(f"    📋 [Listing {listing_num}] MLS ID: {mls_id}")

            # For lots, beds/baths are not applicable
            if property_type == "Lot / Land":
                beds = "N/A"
                baths = "N/A"

            listing_data = {
                'property_name': property_name, # New field for property name
                'address': address,
                'price': price,
                'beds': beds,
                'baths': baths,
                'property_type': property_type,
                'mls_id': mls_id
            }

            # Final check to ensure we have a valid listing
            if address == "Address not found" or property_name == "Name not available":
                 logger.info(f"    ❌ [Listing {listing_num}] FINAL CHECK FAILED: Invalid address or property name.")
                 return None

            logger.info(f"    ✅ [Listing {listing_num}] SUCCESSFULLY PARSED")
            return listing_data

        except Exception as e:
            logger.error(f"    💥 [Listing {listing_num}] Parsing error: {e}")
            return None

    def format_for_excel(self, listings, location, page_number):
        """Format listings for Excel output"""
        formatted_data = []
        current_time = datetime.now()

        for listing in listings:
            # Create bed type description
            if listing['beds'] not in ["Not specified", "N/A"] and listing['baths'] not in ["Not specified", "N/A"]:
                bed_type = f"{listing['beds']} Beds, {listing['baths']} Baths"
            elif listing['beds'] not in ["Not specified", "N/A"]:
                bed_type = f"{listing['beds']} Beds"
            elif listing['baths'] not in ["Not specified", "N/A"]:
                bed_type = f"{listing['baths']} Baths" # Only baths
            elif listing['beds'] == "Studio":
                 bed_type = "Studio"
            else:
                bed_type = "Details not specified"


            rent_price = listing['price']
            if rent_price == "Price not available": # Ensure consistency
                 rent_price = "Price not listed"


            formatted_entry = {
                'fld_property_name': listing['property_name'], # Use the new property_name field
                'fld_property_address': listing['address'],
                'fld_state_name': location['state'],
                'fld_city_name': location['city'],
                'fld_bed_type': bed_type,
                'fld_rent': rent_price,
                'fld_property_type': listing['property_type'],
                'fld_mls_id': listing['mls_id'],
                'fld_month_updated_on': current_time.strftime('%B'),
                'fld_year': current_time.year,
                'fld_time': current_time.strftime('%H:%M:%S'),
                'fld_page_number': page_number
            }
            formatted_data.append(formatted_entry)

        return formatted_data

    async def scrape_single_location(self, location: Dict):
        """Scrape a single location with debug parsing"""
        playwright, browser, context = await self.create_browser()
        location_results = []

        try:
            page = await context.new_page()
            page.set_default_timeout(120000)

            logger.info(f"🌐 NAVIGATING TO: {location['search_url']}")
            await page.goto(location['search_url'], wait_until='domcontentloaded')
            await page.wait_for_timeout(5000)

            # Wait for manual CAPTCHA solution
            captcha_solved = await self.wait_for_manual_captcha_solution(page, location['city'])

            if not captcha_solved:
                logger.error(f"❌ CAPTCHA not solved for {location['city']}")
                return []

            # Get pagination information
            total_pages = await self.get_total_pages(page)
            logger.info(f"📄 Found {total_pages} pages for {location['city']}")

            # Scrape all pages
            current_page = 1
            while current_page <= total_pages:
                logger.info(f"   📖 Processing Page {current_page}/{total_pages} for {location['city']}")

                # Extract listings with debug parsing
                listings = await self.extract_listings_debug(page)

                if not listings:
                    logger.info(f"   📭 No listings found on page {current_page}")
                    # If no listings on current page, check if it's the last page or error
                    if current_page == 1 and total_pages > 1:
                        logger.warning("   ⚠️ No listings found on page 1, but multiple pages exist. This might indicate an issue with content loading or detection.")
                    break # Stop if no listings found, assume end of results or error

                logger.info(f"   🏠 Found {len(listings)} listings on page {current_page}")

                # Format data for Excel
                page_data = self.format_for_excel(listings, location, current_page)
                location_results.extend(page_data)

                if current_page >= total_pages:
                    break

                # Navigate to next page
                success = await self.go_to_next_page(page, current_page)
                if not success:
                    logger.info(f"   ➡️ No more pages available for {location['city']}")
                    break

                current_page += 1
                await page.wait_for_timeout(random.uniform(3000, 6000))

            logger.info(f"✅ Completed {location['city']}: {len(location_results)} listings")
            return location_results

        except Exception as e:
            logger.error(f"❌ Scraping failed for {location['city']}: {e}")
            return []
        finally:
            await browser.close()
            await playwright.stop()

    async def get_total_pages(self, page):
        """Extract total number of pages"""
        try:
            # Try multiple pagination selectors
            pagination_selectors = [
                '.search-pagination',
                '[data-testid="search-pagination"]',
                '.pagination',
                '[class*="pagination"]'
            ]

            for selector in pagination_selectors:
                try:
                    pagination_element = await page.query_selector(selector)
                    if pagination_element:
                        pagination_text = await pagination_element.text_content()
                        page_numbers = re.findall(r'\b(\d+)\b', pagination_text)
                        if page_numbers:
                            return max([int(num) for num in page_numbers])
                except:
                    continue

            return 1

        except Exception as e:
            logger.error(f"Error detecting pagination: {e}")
            return 1

    async def go_to_next_page(self, page, current_page):
        """Navigate to next page"""
        next_button_selectors = [
            'a[title="Next page"]',
            '[data-testid="next-page"]',
            '.pagination-next',
            'a:has-text("Next")',
            'button:has-text("Next")'
        ]

        for selector in next_button_selectors:
            try:
                next_btn = await page.query_selector(selector)
                if next_btn and await next_btn.is_visible():
                    await next_btn.scroll_into_view_if_needed()
                    await page.wait_for_timeout(2000)

                    await next_btn.click()
                    await page.wait_for_timeout(5000)

                    # Verify we have new listings by checking URL or page content
                    # Or check for new listings explicitly on the page
                    new_listings = await page.query_selector_all('[data-testid="property-card"]')
                    if new_listings and len(new_listings) > 0:
                        return True
                    else:
                        logger.warning(f"   ⚠️ Next page button clicked but no new listings detected on page {current_page + 1}.")
                        return False
            except Exception as e:
                logger.warning(f"   ⚠️ Attempt to click next page button failed for selector '{selector}': {e}")
                continue

        return False

    async def scrape_all_locations(self, locations_file: str):
        """Main scraping function"""
        logger.info("🚀 STARTING ZILLOW DEBUG SCRAPER")
        logger.info("=" * 80)
        logger.info("DEBUG MODE:")
        logger.info("• SHOWS FULL TEXT of each listing")
        logger.info("• LOGS EVERY STEP of parsing")
        logger.info("• SHOWS EXACTLY WHY parsing fails")
        logger.info("=" * 80)

        locations = self.load_locations_from_file(locations_file)
        if not locations:
            logger.error("❌ No locations found to process")
            return

        total_locations = len(locations)
        logger.info(f"📋 Processing {total_locations} locations")

        for idx, location in enumerate(locations, 1):
            logger.info(f"\n📍 PROCESSING LOCATION {idx}/{total_locations}: {location['city']}, {location['state']}")

            try:
                location_results = await self.scrape_single_location(location)

                if location_results:
                    self.all_results.extend(location_results)
                    self.processed_locations.append({
                        'city': location['city'],
                        'state': location['state'],
                        'listings_count': len(location_results),
                        'status': 'SUCCESS'
                    })
                    logger.info(f"✅ Completed {location['city']}: {len(location_results)} listings")
                else:
                    self.skipped_cities.append({
                        'city': location['city'],
                        'state': location['state'],
                        'reason': 'No listings found or scraping failed'
                    })
                    logger.info(f"📭 Skipped {location['city']}: No listings available or an error occurred")

            except Exception as e:
                logger.error(f"❌ Failed to process {location['city']}: {e}")
                self.skipped_cities.append({
                    'city': location['city'],
                    'state': location['state'],
                    'reason': f'Error: {str(e)}'
                })

            if idx < total_locations:
                delay = random.uniform(10, 20)
                logger.info(f"⏳ Waiting {delay:.1f}s before next location...")
                await asyncio.sleep(delay)

        await self.save_results_to_excel()
        self.print_final_summary()

    async def save_results_to_excel(self):
        """Save results to Excel"""
        if not self.all_results:
            logger.warning("📭 No data to save")
            return

        output_dir = "zillow_debug_results"
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(output_dir, f"Zillow_Debug_Listings_{len(self.all_results)}_listings_{timestamp}.xlsx")

        df = pd.DataFrame(self.all_results)

        # Reorder columns for better readability
        column_order = [
            'fld_property_name', 'fld_property_address', 'fld_state_name', 'fld_city_name',
            'fld_bed_type', 'fld_rent', 'fld_property_type', 'fld_mls_id', 'fld_page_number',
            'fld_month_updated_on', 'fld_year', 'fld_time'
        ]

        # Only include columns that exist in the dataframe
        final_columns = [col for col in column_order if col in df.columns]
        df = df[final_columns]

        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Zillow Listings', index=False)

                # Auto-adjust column widths
                worksheet = writer.sheets['Zillow Listings']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            logger.info(f"💾 DEBUG Excel file saved: {filename}")

        except Exception as e:
            logger.error(f"❌ Excel save failed: {e}")
            # Fallback save
            df.to_excel(filename, index=False)

    def print_final_summary(self):
        """Print summary"""
        print("\n" + "="*80)
        print("🎉 ZILLOW DEBUG SCRAPING COMPLETE")
        print("="*80)

        if self.all_results:
            total_listings = len(self.all_results)
            successful_locations = [loc for loc in self.processed_locations if loc['status'] == 'SUCCESS']

            print(f"📊 TOTAL LISTINGS COLLECTED: {total_listings}")
            print(f"🏙️ SUCCESSFUL CITIES: {len(successful_locations)}")

            if successful_locations:
                print(f"\n✅ SUCCESSFUL LOCATIONS:")
                for loc in successful_locations:
                    print(f"   📍 {loc['city']}, {loc['state']}: {loc['listings_count']} listings")

            # Show sample of collected data
            if self.all_results:
                print(f"\n📊 SAMPLE DATA:")
                sample = self.all_results[:3]  # Show first 3 listings
                for i, listing in enumerate(sample, 1):
                    print(f"   {i}. {listing.get('fld_property_name', 'No address')} | {listing.get('fld_rent', 'No price')} | {listing.get('fld_bed_type', 'No details')}")

        if self.skipped_cities:
            print(f"\n📭 SKIPPED CITIES: {len(self.skipped_cities)}")
            for skipped in self.skipped_cities:
                print(f"   ❌ {skipped['city']}, {skipped['state']}: {skipped['reason']}")

        if not self.all_results and not self.skipped_cities:
            print("📭 NO DATA COLLECTED")

def create_sample_input_file():
    """Create sample file"""
    sample_data = {
        'Province': ['Ontario', 'British Columbia', 'Alberta'],
        'City Name': ['Toronto', 'Vancouver', 'Calgary']
    }
    df = pd.DataFrame(sample_data)
    df.to_csv('sample_locations.csv', index=False)
    print("📝 Created sample_locations.csv for testing")

async def main():
    """Main function"""
    print("="*80)
    print("🏠 ZILLOW DEBUG SCRAPER - SHOWS EVERYTHING")
    print("="*80)
    print("🔧 DEBUG FEATURES:")
    print("   • SHOWS FULL TEXT of each listing")
    print("   • LOGS EVERY PARSING STEP")
    print("   • SHOWS EXACT FAILURE REASONS")
    print("="*80)

    create_sample_input_file()

    locations_file = input("📁 Enter the path to your locations file (CSV/Excel): ").strip()
    locations_file = locations_file.strip('"\'')

    if not os.path.exists(locations_file):
        print(f"❌ File not found: {locations_file}")
        print("📝 A sample file 'sample_locations.csv' has been created.")
        return

    start_time = time.time()
    scraper = ZillowDebugScraper()

    try:
        await scraper.scrape_all_locations(locations_file)
    except Exception as e:
        logger.error(f"💥 Main execution failed: {e}")
    finally:
        end_time = time.time()
        duration_minutes = (end_time - start_time) / 60
        print(f"\n⏱️ Total execution time: {duration_minutes:.1f} minutes")

if __name__ == '__main__':
    asyncio.run(main())