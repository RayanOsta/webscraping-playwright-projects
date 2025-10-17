import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import os

async def scrape_city_apartments(page, province, city, all_property_data):
    """Scrape apartments for a specific city"""
    try:
        # Construct URL based on province and city
        base_url = "https://www.rentfaster.ca"
        # Clean province to get abbreviation (e.g., "Ontario" -> "on")
        province_clean = province.lower().replace(' ', '')
        if 'ontario' in province_clean:
            province_abbr = 'on'
        elif 'britishcolumbia' in province_clean or 'bc' in province_clean:
            province_abbr = 'bc'
        elif 'alberta' in province_clean or 'ab' in province_clean:
            province_abbr = 'ab'
        elif 'manitoba' in province_clean or 'mb' in province_clean:
            province_abbr = 'mb'
        elif 'saskatchewan' in province_clean or 'sk' in province_clean:
            province_abbr = 'sk'
        elif 'quebec' in province_clean or 'qc' in province_clean:
            province_abbr = 'qc'
        else:
            province_abbr = province_clean[:2]  # Use first two letters as fallback
        
        # Clean city name for URL
        city_clean = city.lower().replace(' ', '-')
        main_url = f"{base_url}/{province_abbr}/{city_clean}/"
        print(f"\n{'='*80}")
        print(f"SCRAPING: {city}, {province}")
        print(f"URL: {main_url}")
        print(f"{'='*80}")
        
        await page.goto(main_url, wait_until='domcontentloaded')
        await page.wait_for_timeout(5000)
        
        print("Waiting for apartment listings to load...")
        try:
            await page.wait_for_selector('.listing-item', timeout=10000)
        except:
            print(f"⚠ No listings found for {city}, {province}. Moving to next city.")
            return
        
        listing_items = await page.query_selector_all('.listing-item')
        print(f"Found {len(listing_items)} listing items/cards in {city}")
        
        # NOTE: Change min(3, len(listing_items)) to len(listing_items) for a full scrape
        for i in range(min(3, len(listing_items))): 
            try:
                print(f"\n{'='*60}")
                print(f"PROCESSING APARTMENT {i+1}/{len(listing_items)} in {city}")
                print(f"{'='*60}")
                
                listing_items = await page.query_selector_all('.listing-item')
                if i >= len(listing_items):
                    break
                
                current_item = listing_items[i]
                
                title_element = await current_item.query_selector('.title, h2, h3')
                title_text = await title_element.inner_text() if title_element else f"Property {i+1}"
                print(f"Clicking on: {title_text}")
                
                await current_item.click()
                
                # 1. EXPLICIT WAIT FOR THE APARTMENT NAME ELEMENT TO BE ON THE PAGE
                APARTMENT_NAME_SELECTOR = '.level-item h2.title.dnt'
                try:
                    await page.wait_for_selector(APARTMENT_NAME_SELECTOR, timeout=15000)
                    print("✓ Property page loaded and title element found.")
                except:
                    print("⚠ Property page might not have loaded correctly or title element is missing.")
                
                # EXTRACT PROPERTY NAME
                property_name = "Not found"
                try:
                    # 2. MOST PRECISE SELECTOR COMBINED WITH text_content()
                    name_element = await page.query_selector(APARTMENT_NAME_SELECTOR)
                    if name_element:
                        # Use text_content() to get non-visible text just in case, but selector is key
                        property_name = await name_element.text_content() 
                        property_name = property_name.strip()
                        if property_name and len(property_name) > 3:
                            print(f"✓ Property name: {property_name}")
                    
                    # Fallback selectors (use less precise selectors only if the main one fails)
                    if property_name == "Not found" or "the apartment name" in property_name.lower():
                        name_selectors = ['.level-item h2', '.title.dnt']
                        for selector in name_selectors:
                            name_element = await page.query_selector(selector)
                            if name_element:
                                property_name = await name_element.text_content()
                                property_name = property_name.strip()
                                if property_name and len(property_name) > 3 and "the apartment name" not in property_name.lower():
                                    print(f"✓ Property name found with fallback selector: {selector}")
                                    break
                                    
                except Exception as e:
                    print(f"Error getting property name: {e}")
                
                # EXTRACT PROPERTY ADDRESS (Using the previous logic which seemed fine)
                property_address = "Not found"
                try:
                    address_selectors = [
                        '.has-background-grey-lightest .dnt.is-size-7',
                        '.dnt.is-size-7',
                    ]
                    for selector in address_selectors:
                        address_elements = await page.query_selector_all(selector)
                        for addr_element in address_elements:
                            address_text = await addr_element.inner_text()
                            address_text = address_text.strip()
                            if address_text and len(address_text) > 10:
                                property_address = address_text
                                print(f"✓ Address: {property_address}")
                                break
                        if property_address != "Not found":
                            break
                except Exception as e:
                    print(f"Error getting address: {e}")
                
                state_name = province
                city_name = city
                
                # Base property data (common to all units)
                base_property_data = {
                    'fld_property_name': property_name,
                    'fld_property_address': property_address,
                    'fld_state_name': state_name,
                    'fld_city_name': city_name,
                }
                
                # FIND AND PROCESS APARTMENT CARDS (UNIT TYPES)
                apartment_cards = []
                try:
                    await page.wait_for_selector('.card.block', timeout=8000)
                    apartment_cards = await page.query_selector_all('.card.block')
                    print(f"✓ Found {len(apartment_cards)} apartment unit cards")
                except:
                    print("No apartment cards found")
                
                # EXTRACT DATA FROM EACH APARTMENT CARD
                for j, card in enumerate(apartment_cards):
                    try:
                        # 3. EXTRACTION LOGIC MATCHING YOUR NEW REQUIREMENTS
                        
                        # Click to expand the card if necessary (existing logic)
                        card_header = await card.query_selector('.card-header')
                        is_expanded = True
                        if card_header:
                            is_expanded = await card_header.query_selector('.fa-angle-up')
                            if not is_expanded:
                                await card_header.click()
                                await page.wait_for_timeout(2000)
                            
                            # fld_bed_type: <p class="card-header-title py-0 my-0">
                            bed_type = "Unknown"
                            bed_type_element = await card.query_selector('.card-header-title.py-0.my-0')
                            if bed_type_element:
                                bed_type = await bed_type_element.text_content() 
                                bed_type = bed_type.strip()
                            
                            # Clean bed type name
                            if '1 Bedroom' in bed_type or '1 bed' in bed_type.lower():
                                clean_bed_type = '1 Bed'
                            elif '3 Bedrooms' in bed_type or '3 beds' in bed_type.lower():
                                clean_bed_type = '3 Bed'
                            elif '2 Bedrooms' in bed_type or '2 beds' in bed_type.lower():
                                clean_bed_type = '2 Bed'
                            elif 'Studio' in bed_type or 'Bachelor' in bed_type:
                                clean_bed_type = 'Studio'
                            else:
                                clean_bed_type = bed_type
                            
                            # fld_rent: <li title="Rent" class="dnt has-text-black is-size-5">
                            rent_price = "Not found"
                            try:
                                # Use the most specific selector for the price list item
                                price_element = await card.query_selector('li[title="Rent"].dnt.has-text-black.is-size-5')
                                if price_element:
                                    # Use text_content() to grab all text inside the <li>, which includes the <span>s with the price
                                    rent_text = await price_element.text_content() 
                                    # Clean the text to just get the price portion
                                    import re
                                    match = re.search(r'\$\s*([\d,]+)', rent_text)
                                    if match:
                                        rent_price = "$" + match.group(1)
                                    
                            except Exception as price_error:
                                print(f"    Error extracting price: {price_error}")
                            
                            print(f"    ✓ {clean_bed_type}: {rent_price}")
                            
                            # Create a new row (long format)
                            unit_data = base_property_data.copy()
                            unit_data['fld_bed_type'] = clean_bed_type
                            unit_data['fld_rent'] = rent_price
                            
                            all_property_data.append(unit_data)
                            
                            # Collapse card if we expanded it
                            if not is_expanded:
                                await card_header.click()
                                await page.wait_for_timeout(1000)
                        
                    except Exception as e:
                        print(f"    Error processing card {j+1}: {e}")
                        continue
                
                print(f"✓ Finished scraping units for: {property_name}")
                
                # GO BACK TO MAIN PAGE
                print("Returning to main search page...")
                close_button = await page.query_selector('.delete, .close-button, [class*="close"], [class*="back"]')
                if close_button:
                    await close_button.click()
                    await page.wait_for_timeout(3000)
                else:
                    await page.goto(main_url, wait_until='domcontentloaded')
                    await page.wait_for_timeout(3000)
                
                await page.wait_for_selector('.listing-item', timeout=10000)
                print("✓ Back on main page, ready for next property")
                    
            except Exception as e:
                print(f"❌ Error processing property {i+1} in {city}: {e}")
                import traceback
                traceback.print_exc()
                try:
                    await page.goto(main_url, wait_until='domcontentloaded')
                    await page.wait_for_timeout(3000)
                except:
                    pass
                continue
    
    except Exception as e:
        print(f"❌ Error scraping {city}, {province}: {e}")
        import traceback
        traceback.print_exc()

async def scrape_all_apartments_from_excel(excel_file_path):
    """Main function to scrape apartments for multiple cities from Excel file"""
    
    # Read the Excel file
    try:
        df_input = pd.read_excel(excel_file_path)
        print(f"📊 Loaded Excel file: {excel_file_path}")
        print(f"Found {len(df_input)} rows in the file")
        
        # Show all available columns
        print(f"Available columns in your file: {list(df_input.columns)}")
        
        # Try to detect province and city columns (more flexible detection)
        province_col = None
        city_col = None
        
        for col in df_input.columns:
            col_lower = str(col).lower().strip()
            # Check for province/state columns
            if any(keyword in col_lower for keyword in ['province', 'state', 'territory']):
                province_col = col
            # Check for city columns  
            elif any(keyword in col_lower for keyword in ['city', 'town', 'municipality', 'location']):
                city_col = col
        
        # If automatic detection failed, try direct column names
        if not province_col:
            for col in df_input.columns:
                if 'Province' in str(col):
                    province_col = col
                    break
                    
        if not city_col:
            for col in df_input.columns:
                if 'City' in str(col):
                    city_col = col
                    break
        
        # Final fallback - use first two columns
        if not province_col or not city_col:
            if len(df_input.columns) >= 2:
                province_col = df_input.columns[0]
                city_col = df_input.columns[1]
                print(f"⚠ Using first two columns as fallback: '{province_col}' for province and '{city_col}' for city")
        
        if province_col and city_col:
            print(f"✓ Using '{province_col}' for province and '{city_col}' for city")
            # Filter out any rows with missing values
            df_input = df_input.dropna(subset=[province_col, city_col])
            print(f"✓ Found {len(df_input)} valid rows with both province and city data")
            
            print(f"\nCities to scrape:")
            for index, row in df_input.iterrows():
                province = str(row[province_col]).strip()
                city = str(row[city_col]).strip()
                print(f"  - {city}, {province}")
                
        else:
            print("❌ Could not detect province and city columns.")
            print("Please rename your columns to 'Province' and 'City' and try again.")
            return None
            
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
        return
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        page = await context.new_page()
        
        all_property_data = [] 
        
        try:
            # Process each city in the Excel file
            for index, row in df_input.iterrows():
                province = str(row[province_col]).strip()
                city = str(row[city_col]).strip()
                
                await scrape_city_apartments(page, province, city, all_property_data)
            
            # SAVE RESULTS
            if all_property_data:
                df_output = pd.DataFrame(all_property_data)
                
                # Reorder the columns for final output
                column_order = [
                    'fld_property_name', 'fld_property_address', 'fld_state_name', 
                    'fld_city_name', 'fld_bed_type', 'fld_rent'
                ]
                df_output = df_output.reindex(columns=column_order)
                
                # Generate output filename based on input filename
                input_filename = os.path.splitext(os.path.basename(excel_file_path))[0]
                excel_filename = f"{input_filename}_scraped_results.xlsx"
                df_output.to_excel(excel_filename, index=False)
                
                print(f"\n{'='*80}")
                print(f"🎉 SUCCESS: Scraped {len(all_property_data)} units across all cities!")
                print(f"💾 Saved to: {excel_filename}")
                print(f"{'='*80}")
                
                print("\nScraped Data Summary:")
                print(f"Total properties: {len(all_property_data)}")
                print(f"Cities processed: {df_input[city_col].nunique()}")
                print(f"Provinces processed: {df_input[province_col].nunique()}")
                
                if len(all_property_data) > 0:
                    print("\nFirst few rows of scraped data:")
                    print(df_output.head(10).to_string())
            else:
                print("❌ No data was scraped from any city")
                
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

# Run the scraper
async def main():
    # Get Excel file path from user
    excel_file_path = input("Please enter the path to your Excel file (with province and city columns): ").strip()
    
    # Remove quotes if user dragged and dropped the file
    excel_file_path = excel_file_path.strip('"')
    
    if not os.path.exists(excel_file_path):
        print(f"❌ File not found: {excel_file_path}")
        return
    
    await scrape_all_apartments_from_excel(excel_file_path)

if __name__ == "__main__":
    asyncio.run(main())