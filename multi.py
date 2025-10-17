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
import json

# Set up enhanced logging without emojis for Windows compatibility
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('apartment_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MultiCityApartmentScraper:
    def __init__(self):
        self.all_results = []
        self.processed_locations = []
        self.skipped_cities = []
        
        # Province to abbreviation mapping
        self.province_abbreviations = {
            'alberta': 'ab',
            'british columbia': 'bc', 
            'manitoba': 'mb',
            'new brunswick': 'nb',
            'newfoundland and labrador': 'nl',
            'nova scotia': 'ns',
            'ontario': 'on',
            'prince edward island': 'pe',
            'quebec': 'qc',
            'saskatchewan': 'sk',
            'northwest territories': 'nt',
            'nunavut': 'nu',
            'yukon': 'yt'
        }
        
    async def stealth_browser(self):
        """Create a stealth browser with enhanced anti-detection"""
        playwright = await async_playwright().start()
        
        browser = await playwright.chromium.launch(
            headless=False,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """)
        
        return playwright, browser, context
    
    def get_province_abbreviation(self, province_name: str) -> str:
        """Convert full province name to abbreviation"""
        province_lower = province_name.lower().strip()
        
        # Direct mapping
        if province_lower in self.province_abbreviations:
            return self.province_abbreviations[province_lower]
        
        # Handle common variations
        variations = {
            'b.c.': 'bc',
            'b c': 'bc',
            'bc': 'bc',
            'ont.': 'on',
            'ont': 'on',
            'queb.': 'qc', 
            'queb': 'qc',
            'alb.': 'ab',
            'alb': 'ab',
            'man.': 'mb',
            'man': 'mb',
            'sask.': 'sk',
            'sask': 'sk'
        }
        
        if province_lower in variations:
            return variations[province_lower]
        
        logger.warning(f"Unknown province: {province_name}, using 'on' as default")
        return 'on'  # Default to Ontario if unknown
    
    def load_locations_from_file(self, file_path: str) -> List[Dict]:
        """Load cities and states from input file with exact column names"""
        logger.info(f"Loading locations from: {file_path}")
        
        try:
            # Clean the file path - remove quotes and strip whitespace
            file_path = file_path.strip('"\'')
            
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel files.")
            
            # Show available columns to help debug
            logger.info(f"Available columns in file: {list(df.columns)}")
            
            # EXACT COLUMN NAMES FROM YOUR FILE
            required_columns = {
                'state': 'Province',
                'city': 'City Name'
            }
            
            # Check if required columns exist
            missing_columns = []
            for internal_name, actual_name in required_columns.items():
                if actual_name not in df.columns:
                    missing_columns.append(actual_name)
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}. Available columns: {list(df.columns)}")
            
            locations = []
            for _, row in df.iterrows():
                city = row['City Name']
                province = row['Province']
                
                # Skip empty rows
                if pd.isna(city) or pd.isna(province) or str(city).strip() == '' or str(province).strip() == '':
                    continue
                
                province_abbr = self.get_province_abbreviation(str(province).strip())
                    
                locations.append({
                    'city': str(city).strip(),
                    'state': str(province).strip(),
                    'state_abbr': province_abbr,
                    'search_url': self.generate_search_url(str(city).strip(), province_abbr)
                })
            
            logger.info(f"Loaded {len(locations)} locations from file")
            return locations
            
        except Exception as e:
            logger.error(f"Failed to load locations file: {e}")
            return []
    
    def generate_search_url(self, city: str, state_abbr: str) -> str:
        """Generate apartments.com search URL using the correct format"""
        # Format: https://www.apartments.com/apartments/{city}-{state_abbr}/
        city_clean = city.lower().replace(' ', '-').replace(',', '').replace("'", "")
        state_clean = state_abbr.lower()
        
        return f"https://www.apartments.com/apartments/{city_clean}-{state_clean}/"
    
    async def scrape_all_locations(self, locations_file: str):
        """Main function to scrape all locations from input file"""
        logger.info("STARTING MULTI-CITY APARTMENT SCRAPER")
        logger.info("=" * 80)
        
        # Load locations
        locations = self.load_locations_from_file(locations_file)
        if not locations:
            logger.error("No locations found to process")
            return
        
        total_locations = len(locations)
        logger.info(f"Processing {total_locations} locations")
        
        # Process each location
        for idx, location in enumerate(locations, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Location {idx}/{total_locations}: {location['city']}, {location['state']} ({location['state_abbr'].upper()})")
            logger.info(f"URL: {location['search_url']}")
            logger.info(f"{'='*60}")
            
            try:
                location_results = await self.scrape_single_location(location)
                
                if location_results:
                    self.all_results.extend(location_results)
                    self.processed_locations.append({
                        'city': location['city'],
                        'state': location['state'],
                        'state_abbr': location['state_abbr'],
                        'listings_count': len(location_results),
                        'status': 'SUCCESS'
                    })
                    logger.info(f"Completed {location['city']}: {len(location_results)} listings")
                else:
                    self.skipped_cities.append({
                        'city': location['city'],
                        'state': location['state'],
                        'state_abbr': location['state_abbr'],
                        'reason': 'No listings found'
                    })
                    logger.info(f"Skipped {location['city']}: No listings available")
                
            except Exception as e:
                logger.error(f"Failed to process {location['city']}: {e}")
                self.skipped_cities.append({
                    'city': location['city'],
                    'state': location['state'],
                    'state_abbr': location['state_abbr'],
                    'reason': f'Error: {str(e)}'
                })
            
            # Delay between locations
            if idx < total_locations:
                delay = random.uniform(5, 10)
                logger.info(f"Waiting {delay:.1f}s before next location...")
                await asyncio.sleep(delay)
        
        # Save final results
        await self.save_complete_data_to_excel()
        
        # Print comprehensive summary
        self.print_final_summary()
    
    async def scrape_single_location(self, location: Dict):
        """Scrape apartments for a single city/state combination"""
        playwright, browser, context = await self.stealth_browser()
        location_results = []
        
        try:
            page = await context.new_page()
            page.set_default_timeout(60000)
            
            logger.info(f"Navigating to {location['search_url']}")
            await page.goto(location['search_url'], wait_until='domcontentloaded')
            await page.wait_for_timeout(5000)
            
            # STRONGER CHECK FOR "NO RESULTS" - Check page content first
            page_content = await page.content()
            
            # Check for various "no results" patterns in the entire page
            no_results_indicators = [
                'no results found',
                'no listings found', 
                '0 results',
                'no exact matches',
                'no properties found',
                'sorry, no results',
                'we couldn\'t find any'
            ]
            
            page_text_lower = page_content.lower()
            if any(indicator in page_text_lower for indicator in no_results_indicators):
                logger.info(f"No results found for {location['city']} - skipping")
                return []
            
            # Check for specific "More Rentals Near" sections that indicate no local results
            if 'more rentals near' in page_text_lower:
                logger.info(f"Only 'More Rentals Near' found for {location['city']} - no local listings, skipping")
                return []
            
            # Check for pagination elements that indicate no results
            no_results_elements = await page.query_selector_all('div.no-return, div.noResults, div.no-results, [class*="no-return"], [class*="no-results"]')
            for element in no_results_elements:
                element_text = await element.text_content()
                if element_text and any(term in element_text.lower() for term in ['no results', 'no listings', '0 results']):
                    logger.info(f"No results element found for {location['city']} - skipping")
                    return []
            
            # Check if page loaded successfully
            page_title = await page.title()
            if '404' in page_title or 'not found' in page_title.lower():
                logger.warning(f"Page not found for {location['city']}, trying alternative URL")
                # Try alternative URL format without state
                alt_url = f"https://www.apartments.com/{location['city'].lower().replace(' ', '-')}/"
                await page.goto(alt_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(5000)
                
                # Check if alternative also failed
                alt_title = await page.title()
                alt_content = await page.content()
                alt_text_lower = alt_content.lower()
                
                if '404' in alt_title or 'not found' in alt_title.lower() or any(indicator in alt_text_lower for indicator in no_results_indicators):
                    logger.info(f"No listings found for {location['city']}")
                    return []
            
            # Get pagination information
            total_pages = await self.get_total_pages(page)
            
            # If no listings found on first page, skip this city
            if total_pages == 0:
                logger.info(f"No listings found for {location['city']}")
                return []
            
            logger.info(f"Found {total_pages} pages for {location['city']}")
            
            # Scrape all pages
            current_page = 1
            while current_page <= total_pages:
                logger.info(f"   Processing Page {current_page}/{total_pages} for {location['city']}")
                
                # Check for "More Rentals Near" sections on current page - these are NOT local listings
                page_content_current = await page.content()
                if 'more rentals near' in page_content_current.lower():
                    logger.info(f"Found 'More Rentals Near' section on page {current_page} - stopping as these are not local listings")
                    break
                
                # Check for "No results" on current page before processing
                no_results_current = await page.query_selector_all('div.no-return, div.noResults, div.no-results, [class*="no-return"], [class*="no-results"]')
                for element in no_results_current:
                    element_text = await element.text_content()
                    if element_text and any(term in element_text.lower() for term in ['no results', 'no listings', '0 results']):
                        logger.info(f"No more results found on page {current_page} - stopping")
                        break
                
                # Extract listings from current page - EXCLUDE "More Rentals Near" sections
                page_listings = await self.extract_current_page_listings(page)
                
                # Filter out "More Rentals Near" listings
                filtered_listings = []
                for listing in page_listings:
                    listing_text = await listing.text_content()
                    if listing_text and 'more rentals near' not in listing_text.lower():
                        filtered_listings.append(listing)
                
                # If no valid listings on current page, stop
                if not filtered_listings:
                    logger.info(f"No valid listings found on page {current_page} - stopping")
                    break
                
                logger.info(f"   Found {len(filtered_listings)} valid listings on page {current_page}")
                
                # Extract data with location info
                page_data = await self.extract_page_data(page, filtered_listings, current_page, location)
                location_results.extend(page_data)
                
                if current_page >= total_pages:
                    break
                
                # Navigate to next page
                success = await self.go_to_next_page(page, current_page)
                if not success:
                    logger.info(f"No more pages available for {location['city']}")
                    break
                
                current_page += 1
                await page.wait_for_timeout(random.uniform(2000, 4000))
            
            logger.info(f"Completed {location['city']}: {len(location_results)} total listings")
            return location_results
            
        except Exception as e:
            logger.error(f"Scraping failed for {location['city']}: {e}")
            try:
                await page.screenshot(path=f'error_{location["city"].replace(" ", "_")}.png')
            except:
                pass
            return []
        finally:
            await browser.close()
            await playwright.stop()
    
    async def get_total_pages(self, page):
        """Extract total number of pages from pagination"""
        try:
            await page.wait_for_timeout(3000)
            
            # First check if there are any listings at all
            listings = await self.extract_current_page_listings(page)
            if not listings:
                return 0
            
            pagination_selectors = [
                '.paging',
                '.pagination', 
                '[data-tracking-label="pagination"]',
                '.pageRange',
                '.searchResults'
            ]
            
            for selector in pagination_selectors:
                try:
                    pagination_element = await page.query_selector(selector)
                    if pagination_element:
                        pagination_text = await pagination_element.text_content()
                        logger.info(f"Pagination text: {pagination_text}")
                        
                        page_matches = re.findall(r'Page\s+(\d+)\s+of\s+(\d+)', pagination_text)
                        if page_matches:
                            return int(page_matches[0][1])
                        
                        of_matches = re.findall(r'of\s+(\d+)', pagination_text)
                        if of_matches:
                            return int(of_matches[0])
                except:
                    continue
            
            # Fallback: check for results count
            page_content = await page.content()
            showing_matches = re.findall(r'Showing\s+\d+\s+of\s+(\d+)\s+Results.*Page\s+\d+\s+of\s+(\d+)', page_content, re.IGNORECASE)
            if showing_matches:
                total_pages = int(showing_matches[0][1])
                return total_pages
            
            # If no pagination found but we have listings, assume 1 page
            if listings:
                return 1
            
            return 0
            
        except Exception as e:
            logger.error(f"Error detecting pagination: {e}")
            return 1
    
    async def extract_current_page_listings(self, page):
        """Extract all listings from current page"""
        listings = []
        primary_selectors = [
            '.placard',
            '[data-tracking-label="property-card"]',
            '.property-item', 
            '.placardContainer',
            '.propertyListing',
            '.listing-tile',
            '.searchListing',
            'article.property',
            'div.property-card'
        ]
        
        for selector in primary_selectors:
            try:
                found = await page.query_selector_all(selector)
                if found:
                    listings.extend(found)
            except:
                continue
        
        # Remove duplicates
        unique_listings = []
        seen_ids = set()
        
        for listing in listings:
            try:
                listing_id = await listing.evaluate("""
                    element => {
                        const rect = element.getBoundingClientRect();
                        const id = element.getAttribute('id') || '';
                        const classes = element.getAttribute('class') || '';
                        const dataLabel = element.getAttribute('data-tracking-label') || '';
                        return id + classes + dataLabel + Math.round(rect.top);
                    }
                """)
                
                if listing_id not in seen_ids:
                    seen_ids.add(listing_id)
                    unique_listings.append(listing)
            except:
                unique_listings.append(listing)
        
        return unique_listings
    
    async def extract_page_data(self, page, listings, page_number, location):
        """Extract data from all listings on current page with enhanced fields"""
        page_data = []
        
        for idx, listing in enumerate(listings):
            try:
                if (idx + 1) % 10 == 0:
                    logger.info(f"   Processing listing {idx + 1}/{len(listings)} on page {page_number}")
                
                if idx % 5 == 0:
                    await listing.scroll_into_view_if_needed()
                    await page.wait_for_timeout(500)
                
                # Extract detailed data with enhanced fields
                data_list = await self.extract_enhanced_listing_info(listing, location)
                
                # Add all entries (multiple bed types and price ranges create multiple entries)
                for data in data_list:
                    # Only add if we have valid data (not empty/error)
                    if data.get('fld_property_name') and data.get('fld_property_name') not in ['Unknown Name', 'Extraction Error']:
                        page_data.append(data)
                
            except Exception as e:
                logger.warning(f"   Failed listing {idx + 1} on page {page_number}: {e}")
                continue
        
        return page_data
    
    async def extract_enhanced_listing_info(self, listing, location):
        """Extract comprehensive listing information - WITH STRICT VALIDATION"""
        data_entries = []
        
        try:
            full_text = await listing.text_content()
            lines = [line.strip() for line in full_text.split('\n') if line.strip()]
            
            # 1. Extract PROPERTY NAME with improved logic
            property_name = await self.extract_with_selectors(listing, [
                'a[data-tracking-label="property-title"]',
                'a.property-title',
                'h1', 'h2', 'h3', 'h4',
                '[class*="title"]',
                '[class*="name"]'
            ])
            
            # 2. Extract ADDRESS with improved selectors
            property_address = await self.extract_with_selectors(listing, [
                '.property-address',
                '.address',
                '[class*="address"]',
                '[data-tracking-label*="address"]',
                '[itemprop="address"]',
                '.location'
            ])
            
            # IMPROVED NAME EXTRACTION
            if not property_name or property_name == 'Unknown Name' or self.looks_like_address(property_name):
                if property_address:
                    clean_name = self.extract_clean_name_from_address(property_address)
                    property_name = clean_name
                else:
                    clean_name = self.extract_name_from_text(full_text)
                    if clean_name:
                        property_name = clean_name
                    else:
                        property_name = 'Unknown Name'
            
            # IMPROVED ADDRESS EXTRACTION
            if not property_address or property_address == 'Address not found':
                property_address = await self.extract_address_alternative(listing, full_text)
            
            # 3. Extract ALL possible combinations of bed types and prices WITH VALIDATION
            all_combinations = await self.extract_all_bed_price_combinations(listing, full_text)
            
            # If no valid combinations found, try fallback method but with strict validation
            if not all_combinations:
                base_data = self.create_base_data(property_name, property_address, location)
                bed_text = await self.extract_clean_beds(listing)
                rent_text = await self.extract_clean_price(listing)
                
                # Only create entries if we have valid data
                if bed_text and rent_text and self.is_valid_price(rent_text):
                    # Handle ranges in bed and rent
                    bed_entries = self.expand_ranges(bed_text, is_bed=True)
                    rent_entries = self.expand_ranges(rent_text, is_bed=False)
                    
                    # Create combinations only with valid prices
                    seen_fallback = set()
                    for bed in bed_entries:
                        for rent in rent_entries:
                            if self.is_valid_price(rent):
                                combo_key = (bed, rent)
                                if combo_key not in seen_fallback:
                                    seen_fallback.add(combo_key)
                                    entry_data = base_data.copy()
                                    entry_data['fld_bed_type'] = bed
                                    entry_data['fld_rent'] = rent
                                    data_entries.append(entry_data)
            else:
                # Create separate entry for each validated bed type and price combination
                seen_main = set()
                for bed_type, rent in all_combinations:
                    combo_key = (bed_type, rent)
                    if combo_key not in seen_main:
                        seen_main.add(combo_key)
                        base_data = self.create_base_data(property_name, property_address, location)
                        base_data['fld_bed_type'] = bed_type
                        base_data['fld_rent'] = rent
                        data_entries.append(base_data)
            
        except Exception as e:
            logger.error(f"Error extracting enhanced listing info: {e}")
            return []
        
        return data_entries
    
    def expand_ranges(self, text: str, is_bed: bool = False) -> List[str]:
        """Expand ranges like '1-2' or 'C$1,200-C$1,500' into individual values"""
        if not text or text in ['Call for Price', 'Call for Details', 'Not available', 'Error']:
            return [text] if text else ['Call for Details' if is_bed else 'Call for Price']
        
        # Handle bed ranges: "1-2 beds" -> ["1 Bed", "2 Bed"]
        if is_bed:
            range_match = re.search(r'(\d+)\s*-\s*(\d+)', text)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2))
                return [f"{i} Bed" for i in range(start, end + 1)]
            
            # Single bed type
            single_match = re.search(r'(\d+)\s*(?:bed|bd|beds|bedroom)', text, re.IGNORECASE)
            if single_match:
                return [f"{single_match.group(1)} Bed"]
            
            # Studio
            if 'studio' in text.lower():
                return ['Studio']
        
        # Handle price ranges: "C$1,200-C$1,500" -> ["C$1,200", "C$1,500"]
        else:
            # Find all individual prices in the range
            price_pattern = r'(?:C\$|CAD?\$?)\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'
            prices = re.findall(price_pattern, text)
            
            if len(prices) >= 2:
                # Return individual prices
                return [f"C${price}" for price in prices]
            elif len(prices) == 1:
                return [f"C${prices[0]}"]
        
        # If no range detected, return original text
        return [text]
    
    def create_base_data(self, property_name: str, property_address: str, location: Dict) -> Dict:
        """Create base data structure for each entry"""
        current_time = datetime.now()
        
        return {
            'fld_property_name': property_name or 'Unknown Name',
            'fld_property_address': property_address or 'Address not found',
            'fld_state_name': location['state'],
            'fld_city_name': location['city'],
            'fld_bed_type': 'Call for Details',  # Will be overwritten
            'fld_rent': 'Call for Price',  # Will be overwritten
            'fld_month_updated_on': current_time.strftime('%B'),
            'fld_year': current_time.year,
            'fld_time': current_time.strftime('%H:%M:%S')
        }
    
    async def extract_all_bed_price_combinations(self, listing, full_text: str) -> List[Tuple[str, str]]:
        """Extract all possible combinations of bed types and prices, expanding ranges - WITH VALIDATION"""
        combinations = []
        seen_combinations = set()  # Track seen combinations to avoid duplicates
        
        try:
            # Method 1: Look for multiple pricing sections
            pricing_sections = await listing.query_selector_all('[data-tracking-label="pricing"], .price-range, .price, .pricing, [class*="price"]')
            
            for section in pricing_sections:
                section_text = await section.text_content()
                if section_text:
                    # Extract bed type and price from this section
                    bed_type = self.extract_bed_type_from_text(section_text)
                    price_text = self.clean_price_text(section_text)
                    
                    # VALIDATE: Only proceed if we have valid bed type and price
                    if bed_type and price_text and self.is_valid_price(price_text):
                        # Expand ranges in both bed type and price
                        expanded_beds = self.expand_ranges(bed_type, is_bed=True)
                        expanded_prices = self.expand_ranges(price_text, is_bed=False)
                        
                        # Create all combinations
                        for bed in expanded_beds:
                            for price in expanded_prices:
                                # Only add if price is valid and combination is unique
                                if self.is_valid_price(price):
                                    combo_key = (bed, price)
                                    if combo_key not in seen_combinations:
                                        seen_combinations.add(combo_key)
                                        combinations.append((bed, price))
            
            # Method 2: Look for unit type sections
            unit_sections = await listing.query_selector_all('.unit-type, .bed-range, .beds, [class*="bed"], [class*="unit"]')
            
            for section in unit_sections:
                section_text = await section.text_content()
                if section_text:
                    # Look for corresponding price nearby
                    bed_type = self.extract_bed_type_from_text(section_text)
                    if bed_type:
                        # Try to find price in parent or sibling elements
                        price_text = await self.find_price_near_element(section)
                        if price_text and self.is_valid_price(price_text):
                            # Expand ranges in both bed type and price
                            expanded_beds = self.expand_ranges(bed_type, is_bed=True)
                            expanded_prices = self.expand_ranges(price_text, is_bed=False)
                            
                            # Create all combinations
                            for bed in expanded_beds:
                                for price in expanded_prices:
                                    # Only add if price is valid and combination is unique
                                    if self.is_valid_price(price):
                                        combo_key = (bed, price)
                                        if combo_key not in seen_combinations:
                                            seen_combinations.add(combo_key)
                                            combinations.append((bed, price))
            
            # Method 3: Parse full text for multiple bed/price patterns
            text_combinations = self.extract_combinations_from_text(full_text)
            for bed, price in text_combinations:
                # VALIDATE prices before expanding
                if self.is_valid_price(price):
                    expanded_beds = self.expand_ranges(bed, is_bed=True)
                    expanded_prices = self.expand_ranges(price, is_bed=False)
                    
                    for exp_bed in expanded_beds:
                        for exp_price in expanded_prices:
                            # Only add if price is valid and combination is unique
                            if self.is_valid_price(exp_price):
                                combo_key = (exp_bed, exp_price)
                                if combo_key not in seen_combinations:
                                    seen_combinations.add(combo_key)
                                    combinations.append((exp_bed, exp_price))
            
            return combinations
            
        except Exception as e:
            logger.debug(f"Error extracting bed-price combinations: {e}")
            return []
    
    def is_valid_price(self, price_text: str) -> bool:
        """Check if price text is valid (not garbage like 's')"""
        if not price_text or price_text in ['Call for Price', 'Call for Details', 'Not available', 'Error']:
            return True  # These are valid placeholder values
        
        # Check for garbage values
        garbage_patterns = [
            r'^[a-zA-Z]$',  # Single letters like 's'
            r'^\d+$',       # Just numbers without currency
            r'^[a-zA-Z]\d*$', # Letter followed by optional numbers
        ]
        
        for pattern in garbage_patterns:
            if re.match(pattern, price_text.strip()):
                return False
        
        # Valid price patterns
        valid_patterns = [
            r'^(?:C\$|\$)\d',
            r'^\d',
            r'Call for',
        ]
        
        for pattern in valid_patterns:
            if re.search(pattern, price_text, re.IGNORECASE):
                return True
        
        return False

    def extract_bed_type_from_text(self, text: str) -> str:
        """Extract bed type from text"""
        if not text:
            return ""
        
        text_lower = text.lower()
        
        # Bed patterns
        bed_patterns = [
            (r'studio', 'Studio'),
            (r'(\d+)\s*-\s*(\d+)\s*(?:bed|bd|beds)', lambda m: f"{m.group(1)}-{m.group(2)} Bed"),
            (r'(\d+)\s*(?:bed|bd|beds)', lambda m: f"{m.group(1)} Bed"),
            (r'(\d+)\s*bedroom', lambda m: f"{m.group(1)} Bedroom"),
        ]
        
        for pattern, replacement in bed_patterns:
            match = re.search(pattern, text_lower, re.IGNORECASE)
            if match:
                if callable(replacement):
                    return replacement(match)
                else:
                    return replacement
        
        return ""
    
    async def find_price_near_element(self, element) -> str:
        """Find price near a given element"""
        try:
            # Look in parent element
            parent = await element.query_selector('xpath=..')
            if parent:
                parent_text = await parent.text_content()
                price = self.clean_price_text(parent_text)
                if price:
                    return price
            
            # Look in next sibling
            next_sibling = await element.query_selector('xpath=following-sibling::*[1]')
            if next_sibling:
                sibling_text = await next_sibling.text_content()
                price = self.clean_price_text(sibling_text)
                if price:
                    return price
            
            return ""
        except:
            return ""
    
    def extract_combinations_from_text(self, text: str) -> List[Tuple[str, str]]:
        """Extract bed type and price combinations from text"""
        combinations = []
        
        # Pattern for "X Bed - $Y" or "X Beds - $Y"
        pattern1 = r'(\d+\s*(?:-\s*\d+)?\s*(?:bed|bd|beds|bedroom)s?)\s*[-–]\s*((?:C\$|\$)\d+(?:,\d{3})*(?:\.\d{2})?(?:\s*-\s*(?:C\$|\$)\d+(?:,\d{3})*(?:\.\d{2})?)*)'
        matches1 = re.findall(pattern1, text, re.IGNORECASE)
        for bed, price in matches1:
            combinations.append((bed.strip(), price.strip()))
        
        # Pattern for "Studio - $X"
        pattern2 = r'(studio)\s*[-–]\s*((?:C\$|\$)\d+(?:,\d{3})*(?:\.\d{2})?(?:\s*-\s*(?:C\$|\$)\d+(?:,\d{3})*(?:\.\d{2})?)*)'
        matches2 = re.findall(pattern2, text, re.IGNORECASE)
        for bed, price in matches2:
            combinations.append(('Studio', price.strip()))
        
        return combinations
    
    def looks_like_address(self, text: str) -> bool:
        """Check if text looks like an address rather than a property name"""
        if not text:
            return False
        
        address_indicators = [
            r'\d+.*\d{5}',  # Contains numbers and zip code
            r'\d+.*(st|street|ave|avenue|rd|road|dr|drive|ln|lane|blvd|boulevard)',
            r'unit\s+\w+',
            r'\d+.*,\s*\w+,\s*\w{2}\s+\w{5,6}',  # Full address format
        ]
        
        text_lower = text.lower()
        for pattern in address_indicators:
            if re.search(pattern, text_lower):
                return True
        return False
    
    def extract_clean_name_from_address(self, address: str) -> str:
        """Extract a clean property name from address"""
        if not address:
            return "Unknown Name"
        
        # Remove unit numbers and identifiers
        clean_address = re.sub(r'unit\s+\w+', '', address, flags=re.IGNORECASE)
        clean_address = re.sub(r'#\w+', '', clean_address)
        clean_address = re.sub(r'id\d+', '', clean_address, flags=re.IGNORECASE)
        
        # Take only the street address part (before first comma)
        if ',' in clean_address:
            street_part = clean_address.split(',')[0].strip()
        else:
            street_part = clean_address
        
        # Clean up extra spaces and return
        return ' '.join(street_part.split())
    
    def extract_name_from_text(self, text: str) -> str:
        """Extract property name from full text as last resort"""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        for line in lines:
            # Skip lines that are clearly prices, beds, or other metadata
            if any(indicator in line.lower() for indicator in ['$', 'bed', 'bath', 'sq', 'contact']):
                continue
            
            # Skip very short lines
            if len(line) < 5:
                continue
                
            # If line doesn't look like an address and is reasonable length, use it
            if not self.looks_like_address(line) and 5 <= len(line) <= 50:
                return line
        
        return ""
    
    async def extract_address_alternative(self, listing, full_text: str) -> str:
        """Alternative methods to extract address"""
        try:
            # Method 1: Look for address patterns in the full text
            address_patterns = [
                r'\d+\s+[\w\s]+,?\s*\w+,?\s*\w{2}\s+\w{5,6}',
                r'\d+\s+[\w\s]+(?:\s+(?:st|street|ave|avenue|rd|road|dr|drive))',
            ]
            
            for pattern in address_patterns:
                matches = re.findall(pattern, full_text)
                if matches:
                    return matches[0]
            
            # Method 2: Look for location elements
            location_selectors = [
                '[data-tracking-label*="address"]',
                '[itemprop="address"]',
                '.location',
                '.property-location'
            ]
            
            for selector in location_selectors:
                element = await listing.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text and text.strip():
                        return text.strip()
            
            return "Address not found"
            
        except:
            return "Address not found"
    
    async def extract_clean_price(self, listing):
        """Extract only price information"""
        try:
            price_selectors = [
                '[data-tracking-label="pricing"]',
                '.price-range',
                '.price',
                '.rent',
                '.pricing',
                '[class*="price"]',
                '[class*="rent"]'
            ]
            
            for selector in price_selectors:
                try:
                    price_element = await listing.query_selector(selector)
                    if price_element:
                        price_text = await price_element.text_content()
                        if price_text and price_text.strip():
                            cleaned_price = self.clean_price_text(price_text)
                            if cleaned_price:
                                return cleaned_price
                except:
                    continue
            
            # Search in full text
            full_text = await listing.text_content()
            lines = [line.strip() for line in full_text.split('\n') if line.strip()]
            
            for line in lines:
                if '$' in line and any(word in line.lower() for word in ['$', 'c$', 'cad', 'price', 'rent']):
                    if not any(bed_word in line.lower() for bed_word in ['bed', 'bath', 'bd', 'ba', 'studio']):
                        cleaned_price = self.clean_price_text(line)
                        if cleaned_price:
                            return cleaned_price
            
            return None
            
        except Exception as e:
            logger.debug(f"Price extraction error: {e}")
            return None
    
    def clean_price_text(self, text):
        """Clean price text"""
        if not text:
            return None
        
        # Remove bed/bath patterns
        text = re.sub(r'\d+\s*-\s*\d+\s*(?:bed|bd|beds|bath|ba|baths)', '', text, flags=re.IGNORECASE)
        text = re.sub(r'studio', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\d+\s*(?:bed|bd|beds|bath|ba|baths)', '', text, flags=re.IGNORECASE)
        
        # Extract price patterns
        price_patterns = [
            r'(?:C\$|CAD?\$?)\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?:\s*-\s*(?:C\$|CAD?\$?)\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?)*',
            r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?:\s*-\s*\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?)*',
            r'Call for (?:Rent|Pricing|Price)',
            r'Rent Specials?',
        ]
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                clean_match = matches[0].strip()
                if clean_match:
                    return clean_match
        
        return text.strip() if text.strip() else None
    
    async def extract_clean_beds(self, listing):
        """Extract only bed information"""
        try:
            bed_selectors = [
                '.bed-range',
                '.beds', 
                '.bedrooms',
                '.unit-type',
                '[class*="bed"]',
                '[data-tracking-label*="bed"]'
            ]
            
            for selector in bed_selectors:
                try:
                    bed_element = await listing.query_selector(selector)
                    if bed_element:
                        bed_text = await bed_element.text_content()
                        if bed_text and bed_text.strip():
                            cleaned_beds = self.clean_bed_text(bed_text)
                            if cleaned_beds:
                                return cleaned_beds
                except:
                    continue
            
            # Search in full text
            full_text = await listing.text_content()
            lines = [line.strip() for line in full_text.split('\n') if line.strip()]
            
            for line in lines:
                line_lower = line.lower()
                if any(keyword in line_lower for keyword in ['studio', 'bed', 'bds', 'bd']):
                    if len(line) < 50:
                        cleaned_beds = self.clean_bed_text(line)
                        if cleaned_beds:
                            return cleaned_beds
            
            return None
            
        except Exception as e:
            logger.debug(f"Bed extraction error: {e}")
            return None
    
    def clean_bed_text(self, text):
        """Clean bed text"""
        if not text:
            return None
        
        # Remove price patterns
        text = re.sub(r'(?:C\$|CAD?\$?)\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?:\s*-\s*(?:C\$|CAD?\$?)\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?)*', '', text)
        text = re.sub(r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?:\s*-\s*\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?)*', '', text)
        
        # Extract bed patterns
        bed_patterns = [
            r'\d+\s*-\s*\d+\s*(?:bed|bd|beds)',
            r'\d+\s*(?:bed|bd|beds)',
            r'studio',
            r'\d+\s*bedroom',
        ]
        
        for pattern in bed_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                clean_match = matches[0].strip()
                if clean_match:
                    return clean_match
        
        return text.strip() if text.strip() else None
    
    async def extract_with_selectors(self, element, selectors):
        """Extract text using multiple selectors"""
        for selector in selectors:
            try:
                found_element = await element.query_selector(selector)
                if found_element:
                    text = await found_element.text_content()
                    if text and text.strip():
                        return text.strip()
            except:
                continue
        return None
    
    async def go_to_next_page(self, page, current_page):
        """Navigate to next page"""
        next_button_selectors = [
            f'a[data-tracking-label="next-page"]',
            f'a.paging__link[data-page="{current_page + 1}"]',
            '.paging .next',
            '.pagination .next', 
            'a:has-text("Next")',
            f'a:has-text("{current_page + 1}")',
            f'button[data-page="{current_page + 1}"]'
        ]
        
        for selector in next_button_selectors:
            try:
                next_btn = await page.query_selector(selector)
                if next_btn and await next_btn.is_visible():
                    await next_btn.scroll_into_view_if_needed()
                    await page.wait_for_timeout(1000)
                    
                    await next_btn.click()
                    await page.wait_for_timeout(3000)
                    
                    # Check if we're still on a valid page (not "no results" or "more rentals near")
                    page_content = await page.content()
                    if 'more rentals near' in page_content.lower():
                        return False
                    
                    # Check for no results
                    no_results = await page.query_selector_all('div.no-return, div.noResults, div.no-results, [class*="no-return"], [class*="no-results"]')
                    for element in no_results:
                        element_text = await element.text_content()
                        if element_text and any(term in element_text.lower() for term in ['no results', 'no listings', '0 results']):
                            return False
                    
                    new_listings = await self.extract_current_page_listings(page)
                    if new_listings:
                        return True
                    else:
                        return False
                        
            except:
                continue
        
        return False
    
    async def save_complete_data_to_excel(self):
        """Save all results to Excel with UPDATED column order (single bed type and rent)"""
        if not self.all_results:
            logger.warning("No data to save")
            return
        
        output_dir = "multi_city_apartment_results"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(output_dir, f"All_Cities_Apartments_{len(self.all_results)}_listings_{timestamp}.xlsx")
        
        df = pd.DataFrame(self.all_results)
        
        # UPDATED COLUMNS - only single bed type and rent
        required_columns = [
            'fld_property_name', 'fld_property_address', 'fld_state_name', 'fld_city_name',
            'fld_bed_type', 'fld_rent',  # SINGLE columns now
            'fld_month_updated_on', 'fld_year', 'fld_time'
        ]
        
        # Filter to only include columns that exist and are in our required list
        final_columns = [col for col in required_columns if col in df.columns]
        
        df = df[final_columns]
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='All Cities Apartments', index=False)
                
                workbook = writer.book
                worksheet = writer.sheets['All Cities Apartments']
                
                # Adjust column widths
                column_widths = {
                    'A': 35,  # fld_property_name
                    'B': 45,  # fld_property_address  
                    'C': 20,  # fld_state_name
                    'D': 20,  # fld_city_name
                    'E': 20,  # fld_bed_type (wider for multiple bed types)
                    'F': 20,  # fld_rent
                    'G': 15,  # fld_month_updated_on
                    'H': 10,  # fld_year
                    'I': 12   # fld_time
                }
                
                for col_letter, width in column_widths.items():
                    if col_letter in worksheet.column_dimensions:
                        worksheet.column_dimensions[col_letter].width = width
                
                worksheet.auto_filter.ref = worksheet.dimensions
                worksheet.freeze_panes = 'A2'
            
            logger.info(f"Excel file saved: {filename}")
            
        except Exception as e:
            logger.error(f"Excel save failed: {e}")
            df.to_excel(filename, index=False)
        
        return filename
    
    def print_final_summary(self):
        """Print comprehensive final summary"""
        print("\n" + "="*80)
        print("MULTI-CITY SCRAPING COMPLETE - FINAL SUMMARY")
        print("="*80)
        
        if self.all_results:
            total_listings = len(self.all_results)
            successful_locations = [loc for loc in self.processed_locations if loc['status'] == 'SUCCESS']
            
            print(f"TOTAL LISTINGS COLLECTED: {total_listings}")
            print(f"SUCCESSFUL CITIES: {len(successful_locations)}")
            
            if successful_locations:
                print(f"\nSUCCESSFUL LOCATIONS:")
                for loc in successful_locations:
                    print(f"   {loc['city']}, {loc['state']}: {loc['listings_count']} listings")
        
        if self.skipped_cities:
            print(f"\nSKIPPED CITIES (No listings available): {len(self.skipped_cities)}")
            for skipped in self.skipped_cities:
                print(f"   {skipped['city']}, {skipped['state']}: {skipped['reason']}")
        
        if not self.all_results and not self.skipped_cities:
            print("NO DATA COLLECTED")
            return
        
        # Data quality report
        if self.all_results:
            beds_found = sum(1 for item in self.all_results if item.get('fld_bed_type') and item['fld_bed_type'] not in ['Call for Details', 'Error'])
            rents_found = sum(1 for item in self.all_results if item.get('fld_rent') and item['fld_rent'] not in ['Call for Price', 'Error'])
            addresses_found = sum(1 for item in self.all_results if item.get('fld_property_address') and 'not found' not in item.get('fld_property_address', '').lower() and 'error' not in item.get('fld_property_address', '').lower())
            
            print(f"\nDATA QUALITY REPORT:")
            print(f"   Prices extracted: {rents_found}/{total_listings} ({rents_found/total_listings*100:.1f}%)")
            print(f"   Bed info extracted: {beds_found}/{total_listings} ({beds_found/total_listings*100:.1f}%)") 
            print(f"   Addresses extracted: {addresses_found}/{total_listings} ({addresses_found/total_listings*100:.1f}%)")
            
            # Show sample data
            if total_listings >= 3:
                print(f"\nSAMPLE DATA (EXPANDED RANGES - One entry per combination):")
                sample_indices = [0, total_listings//2, total_listings-1]
                for i, idx in enumerate(sample_indices):
                    if idx < len(self.all_results):
                        result = self.all_results[idx]
                        print(f"   {i+1}. {result.get('fld_property_name', 'N/A')[:30]}...")
                        print(f"      City: {result.get('fld_city_name', 'N/A')}, State: {result.get('fld_state_name', 'N/A')}")
                        print(f"      Bed: {result.get('fld_bed_type', 'N/A')}")
                        print(f"      Rent: {result.get('fld_rent', 'N/A')}")

def create_sample_input_file():
    """Create a sample input file for testing"""
    sample_data = {
        'Province': ['British Columbia', 'Manitoba', 'New Brunswick'],
        'City Name': ['Saanich', 'Brandon', 'Edmundston']
    }
    df = pd.DataFrame(sample_data)
    df.to_csv('sample_locations.csv', index=False)
    print("Created sample_locations.csv for testing")

async def main():
    """Main execution function"""
    print("="*80)
    print("MULTI-CITY APARTMENT SCRAPER - EXPANDED RANGES VERSION")
    print("="*80)
    print("FEATURES:")
    print("   • EXPANDS price ranges: C$2,294-C$2,295 → 2 separate entries")
    print("   • EXPANDS bed ranges: 1-2 Bed → 2 separate entries") 
    print("   • CREATES all combinations of individual bed types and prices")
    print("="*80)
    
    # Create sample file first
    create_sample_input_file()
    
    # Get input file from user
    locations_file = input("Enter the path to your locations file (CSV/Excel): ").strip()
    
    # Clean the file path - remove quotes and strip whitespace
    locations_file = locations_file.strip('"\'')
    
    if not os.path.exists(locations_file):
        print(f"File not found: {locations_file}")
        print("Please check the path and try again.")
        print(f"A sample file 'sample_locations.csv' has been created in the current directory.")
        
        # Show current directory for reference
        current_dir = os.getcwd()
        print(f"Current directory: {current_dir}")
        return
    
    start_time = time.time()
    scraper = MultiCityApartmentScraper()
    
    try:
        await scraper.scrape_all_locations(locations_file)
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        end_time = time.time()
        duration_minutes = (end_time - start_time) / 60
        print(f"\nTotal execution time: {duration_minutes:.1f} minutes")

if __name__ == '__main__':
    # Run the main function
    asyncio.run(main())