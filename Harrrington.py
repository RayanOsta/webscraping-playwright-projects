import asyncio
import os
import uuid
import re
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

# ======================================================
# CONFIG
# ======================================================
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
OUTPUT_FILE = f"HarringtonHousing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

# ======================================================
# HELPERS
# ======================================================
def generate_listing_id():
    return str(uuid.uuid4())[:8]

def map_billing_cycle(label: str) -> str:
    label = label.lower().strip()
    if 'month' in label:
        return 'monthly'
    elif 'week' in label:
        return 'weekly'
    elif 'year' in label:
        return 'annually'
    return label

async def scroll_to_bottom(page):
    prev_height = None
    while True:
        curr_height = await page.evaluate("document.body.scrollHeight")
        if prev_height == curr_height:
            break
        prev_height = curr_height
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(2)

async def extract_address(page):
    # Specifically target the last <p> inside .name-detail (contains full address)
    try:
        await page.wait_for_selector("div.name-detail > p:last-of-type", timeout=5000)
        el = await page.query_selector("div.name-detail > p:last-of-type")
        if el:
            text = (await el.inner_text()).strip()
            if text and len(text) > 10:
                return text
    except:
        pass

    # Try alternative selectors just in case
    selectors = [
        "div.address-detail p",
        "div.detail-info p",
        "div.name-detail p:nth-of-type(2)",
    ]
    for selector in selectors:
        try:
            el = await page.query_selector(selector)
            if el:
                text = (await el.inner_text()).strip()
                if text and len(text) > 10:
                    return text
        except:
            continue

    # Try extracting from JSON-LD if available
    try:
        json_els = await page.query_selector_all('script[type="application/ld+json"]')
        for el in json_els:
            content = await el.inner_text()
            match = re.search(r'"streetAddress"\s*:\s*"([^"]+)"', content)
            if match:
                return match.group(1)
    except:
        pass

    # Fallback: regex for address-like text from page content
    try:
        body_text = await page.content()
        match = re.search(r'\d{2,4}[^<]+(Toronto|Montreal|Ottawa|Vancouver|ON|QC|BC|CA)', body_text, re.IGNORECASE)
        if match:
            return match.group(0).replace('\n', ' ').strip()
    except:
        pass

    return None

# ======================================================
# MAIN SCRAPER
# ======================================================
async def scrape_city(context, city_name: str, province_name: str):
    print(f"\n➡️ Scraping city: {city_name} ({province_name})")
    url = f"https://harringtonhousing.com/{city_name.lower()}/coliving-shared-rooms-on-rent"
    results = []

    page = await context.new_page()

    try:
        await page.goto(url, timeout=60000)
        await scroll_to_bottom(page)

        # Collect all listing URLs first
        link_elements = await page.query_selector_all("div.inner-card a")
        listing_links = []
        for el in link_elements:
            href = await el.get_attribute('href')
            if href and href.startswith('/'):
                listing_links.append(f"https://harringtonhousing.com{href}")
            elif href and href.startswith('http'):
                listing_links.append(href)

        print(f"Found {len(listing_links)} listings in {city_name}.")

        for index, link in enumerate(listing_links, start=1):
            try:
                detail_page = await context.new_page()
                await detail_page.goto(link, timeout=60000)
                await detail_page.wait_for_load_state('domcontentloaded')
                await asyncio.sleep(2)

                # Title
                title_el = await detail_page.query_selector("p.font-16.dark")
                fld_title = (await title_el.inner_text()).strip() if title_el else None

                # Price and billing cycle
                price_el = await detail_page.query_selector("div.price h6")
                cycle_el = await detail_page.query_selector("div.price p.price-label")
                fld_price = (await price_el.inner_text()).strip() if price_el else None
                fld_billing_cycle = map_billing_cycle(await cycle_el.inner_text() if cycle_el else '')

                # Address (robust extraction)
                fld_address = await extract_address(detail_page)

                # Bedrooms & Bathrooms
                icon_boxes = await detail_page.query_selector_all("div.icon-box p.dark")
                fld_bedrooms = fld_bathrooms = None
                if len(icon_boxes) >= 2:
                    fld_bedrooms = (await icon_boxes[0].inner_text()).strip()
                    fld_bathrooms = (await icon_boxes[1].inner_text()).strip()

                # Amenities
                amenity_els = await detail_page.query_selector_all("div.md\\:w-1\\/2 p.dark")
                fld_amenities = ', '.join([await el.inner_text() for el in amenity_els]) if amenity_els else ''

                now = datetime.now()
                result = {
                    'fld_listing_id': generate_listing_id(),
                    'fld_city_name': city_name,
                    'fld_province_name': province_name,
                    'fld_title': fld_title,
                    'fld_address': fld_address,
                    'fld_price': fld_price,
                    'fld_billing_cycle': fld_billing_cycle,
                    'fld_amenities': fld_amenities,
                    'fld_source': link,
                    'fld_month': now.strftime('%B'),
                    'fld_year': now.year,
                    'fld_bedrooms': fld_bedrooms,
                    'fld_bathrooms': fld_bathrooms,
                    'fld_updated_on': now.strftime('%Y-%m-%d %H:%M:%S')
                }

                results.append(result)
                print(f"  ✅ Scraped {index}: {fld_title} | Address: {fld_address}")

                await detail_page.close()

            except Exception as e:
                print(f"  ⚠️ Error scraping listing {index}: {e}")
                try:
                    await detail_page.close()
                except:
                    pass
                continue

    except Exception as e:
        print(f"❌ Failed to scrape {city_name}: {e}")

    finally:
        await page.close()

    return results

# ======================================================
# ENTRY POINT
# ======================================================
async def main():
    input_file = input("Enter the input Excel filename (with city and province columns): ").strip()
    df_input = pd.read_excel(input_file)

    # Detect city/province columns automatically
    city_col = next((col for col in df_input.columns if 'city' in col.lower()), None)
    province_col = next((col for col in df_input.columns if 'province' in col.lower() or 'state' in col.lower()), None)

    if not city_col or not province_col:
        print("❌ Could not detect city or province/state columns. Please check your Excel headers.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)

        all_results = []
        for _, row in df_input.iterrows():
            city = str(row.get(city_col)).strip()
            province = str(row.get(province_col)).strip()
            if not city:
                continue
            city_results = await scrape_city(context, city, province)
            all_results.extend(city_results)

        await browser.close()

    # Save to Excel
    if all_results:
        df_out = pd.DataFrame(all_results)
        df_out.to_excel(OUTPUT_FILE, index=False)
        print(f"\n🎉 Data successfully saved to {OUTPUT_FILE}")
    else:
        print("\n⚠️ No data scraped.")

# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    asyncio.run(main())
