#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper for The Diva Shop collections.

Outputs:
- data/products.csv
- data/products.json

It scrapes the specified collection URLs, paginates through all pages,
extracts product details, images, and variant-like attributes (size, color,
fragrance, format, etc.) inferred from option selectors / titles.

Note: Site structure can change. If selectors fail, adjust CSS selectors
below. This scraper uses only requests + BeautifulSoup, so it's static HTML only.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup
import time


BASE_URL = "https://thedivashop.ng"

# Collections to scrape
COLLECTION_URLS = [
	"https://thedivashop.ng/collections/darling",
	"https://thedivashop.ng/collections/amigos",
	"https://thedivashop.ng/collections/megagrowth",
	"https://thedivashop.ng/collections/tcb-naturals",
	"https://thedivashop.ng/collections/good-knight",
	"https://thedivashop.ng/collections/aer-pocket",
	"https://thedivashop.ng/collections/personal-care",
	"https://thedivashop.ng/collections/sale",
	"https://thedivashop.ng/collections/the-diva-shop-gift-card",
]


HEADERS = {
	"User-Agent": (
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/124.0 Safari/537.36"
	),
	"Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class Variant:
	option_title: Optional[str] = None  # e.g., Size: 250ml, Color: Black
	price: Optional[float] = None
	compare_at_price: Optional[float] = None
	sku: Optional[str] = None
	available: Optional[bool] = None
	options: Dict[str, Optional[str]] = field(default_factory=dict)  # {"Size": "250ml", ...}


@dataclass
class Product:
	collection: str
	title: str
	url: str
	price: Optional[float] = None
	compare_at_price: Optional[float] = None
	currency: Optional[str] = None
	description: Optional[str] = None
	images: List[str] = field(default_factory=list)
	tags: List[str] = field(default_factory=list)
	vendor: Optional[str] = None
	product_type: Optional[str] = None
	variants: List[Variant] = field(default_factory=list)
	option_names: List[str] = field(default_factory=list)  # e.g., ["Size", "Color"]
	option_values: Dict[str, List[str]] = field(default_factory=dict)  # e.g., {"Size": ["250ml","500ml"]}


def ensure_dir(path: str) -> None:
	os.makedirs(path, exist_ok=True)


def fetch(url: str, retries: int = 3, backoff: float = 1.0) -> requests.Response:
	last_exc: Optional[Exception] = None
	for attempt in range(1, retries + 1):
		try:
			resp = requests.get(url, headers=HEADERS, timeout=30)
			resp.raise_for_status()
			# politeness: brief delay
			time.sleep(0.4)
			return resp
		except Exception as e:
			last_exc = e
			if attempt < retries:
				time.sleep(backoff * attempt)
			else:
				raise


def parse_price(text: str) -> Optional[float]:
	if not text:
		return None
	# Remove currency symbols and commas
	t = re.sub(r"[^0-9.,]", "", text)
	# Normalize comma as thousands separator
	t = t.replace(",", "")
	try:
		return float(t)
	except ValueError:
		return None


def full_url(href: str) -> str:
	if href.startswith("http"):
		return href
	return BASE_URL.rstrip("/") + href


def paginate_collection_urls(base_collection_url: str) -> List[str]:
	"""Return all paginated URLs for a collection by following ?page=N links.
	Assumes a Shopify-like theme pagination.
	"""
	urls = []
	page = 1
	while True:
		url = f"{base_collection_url}?page={page}"
		r = fetch(url)
		soup = BeautifulSoup(r.text, "lxml")

		# Heuristic: if no products found and page>1, stop.
		product_cards = soup.select(
			".product-grid .grid__item, .collection .grid__item, .product-card, .product-grid-item"
		)
		if not product_cards and page > 1:
			break

		# Some collections may not use numbered pages for the first page
		if page == 1:
			urls.append(base_collection_url)
		else:
			urls.append(url)

		# Stop if no next link present (for safety)
		next_link = soup.select_one(".pagination a[rel='next'], a.pagination__next")
		if not next_link:
			break
		page += 1

	return urls


def extract_product_links(soup: BeautifulSoup) -> List[str]:
	links = set()
	# Common selectors for Shopify themes
	for a in soup.select(
		"a.product-card, a.full-unstyled-link, a.product-item__title, a.grid-view-item__link, a.product-title"
	):
		href = a.get("href")
		if href and "/products/" in href:
			links.add(full_url(href))
	# Fallback: find any link with /products/
	if not links:
		for a in soup.find_all("a", href=True):
			href = a["href"]
			if "/products/" in href:
				links.add(full_url(href))
	# Normalize links to canonical /products/<handle>
	normed: Set[str] = set()
	for url in links:
		if "/collections/" in url and "/products/" in url:
			handle = url.split("/products/")[-1].split("?")[0].strip("/")
			normed.add(f"{BASE_URL}/products/{handle}")
		else:
			# already product URL
			# strip query params for dedup
			clean = url.split("?")[0]
			normed.add(clean)
	return sorted(normed)


def parse_product_page(collection_name: str, url: str) -> Optional[Product]:
	try:
		r = fetch(url)
	except Exception as e:
		print(f"Failed to fetch product {url}: {e}")
		return None
	soup = BeautifulSoup(r.text, "lxml")

	# Try JSON-LD first (structured data)
	ld_product = None
	for script in soup.find_all("script", type="application/ld+json"):
		try:
			data = json.loads(script.string or "{}")
		except json.JSONDecodeError:
			continue
		if isinstance(data, list):
			for item in data:
				if isinstance(item, dict) and item.get("@type") == "Product":
					ld_product = item
					break
		elif isinstance(data, dict) and data.get("@type") == "Product":
			ld_product = data
		if ld_product:
			break

	# Title
	title = None
	title_el = None
	if ld_product and isinstance(ld_product.get("name"), str):
		title = ld_product.get("name").strip()
	if not title:
		title_el = soup.select_one("h1.product__title, h1.product-title, h1.product-name, h1")
		if title_el:
			title = title_el.get_text(strip=True)

	# Price (current) and compare-at
	price_text = None
	compare_text = None
	currency = None

	if ld_product:
		offers = ld_product.get("offers")
		# offers can be dict or list
		def extract_offer_fields(of):
			return (
				of.get("price") if isinstance(of, dict) else None,
				of.get("priceCurrency") if isinstance(of, dict) else None,
			)
		if isinstance(offers, dict):
			p, cur = extract_offer_fields(offers)
			price_text = str(p) if p is not None else None
			currency = cur or currency
		elif isinstance(offers, list) and offers:
			# use the lowest priced offer as base
			prices = []
			for of in offers:
				p, cur = extract_offer_fields(of)
				if p is not None:
					prices.append(float(p))
				if cur and not currency:
					currency = cur
			if prices:
				price_text = str(min(prices))

	if not price_text:
		price_el = soup.select_one(
			".price__current, .price .price-item--regular, .product__price, span.price-item--regular, .price.price--large .price-item--regular"
		)
		if price_el:
			price_text = price_el.get_text(strip=True)
	if not compare_text:
		compare_el = soup.select_one(
			".price__was, .price .price-item--compare, span.price-item--sale, .price-item--compare"
		)
		if compare_el:
			compare_text = compare_el.get_text(strip=True)

	if not currency:
		# Try to detect currency symbol from price text (₦, NGN, etc.)
		symbol_match = re.search(r"[₦₵$€£]|NGN|USD|EUR|GBP", (price_text or "") + (compare_text or ""))
		if symbol_match:
			currency = symbol_match.group(0)

	# Description
	description = None
	if ld_product and isinstance(ld_product.get("description"), str):
		description = ld_product.get("description").strip()
	if not description:
		desc_el = soup.select_one(".product__description, .product-description, #tab-description, .rte")
		description = desc_el.get_text("\n", strip=True) if desc_el else None

	# Images
	images = set()
	if ld_product and ld_product.get("image"):
		if isinstance(ld_product["image"], list):
			for src in ld_product["image"]:
				if isinstance(src, str):
					images.add(src.split("?")[0])
		elif isinstance(ld_product["image"], str):
			images.add(ld_product["image"].split("?")[0])

	if not images:
		for img in soup.select(".product__media img, .product-gallery img, .product-images img, img[src]"):
			src = img.get("data-src") or img.get("src")
			if not src:
				continue
			if src.startswith("//"):
				src = "https:" + src
			if src.startswith("/"):
				src = full_url(src)
			# Skip non-product or tiny icons and marketing badges
			if src.startswith("data:image"):
				continue
			if any(token in src for token in [
				"icon",
				"placeholder",
				"spinner",
				"loading",
				"Fast_Delivery",
				"Quick_Customer_Support",
				"100_Authentic_Products",
				"Buy-More-Save-More",
				"/files/",
			]):
				continue
			images.add(src.split("?")[0])

	# Vendor, product type, tags (if present in meta or breadcrumb)
	vendor = None
	vendor_el = soup.select_one(".product-meta__vendor, a.product-vendor")
	if vendor_el:
		vendor = vendor_el.get_text(strip=True)
	product_type = None
	type_el = soup.select_one(".product-meta__type, .product__type")
	if type_el:
		product_type = type_el.get_text(strip=True)
	tags = []
	for tag_el in soup.select(".product-tags a, .tags a"):
		t = tag_el.get_text(strip=True)
		if t:
			tags.append(t)

	# Variants / Options
	variants: List[Variant] = []
	option_names: List[str] = []
	option_values: Dict[str, Set[str]] = {}
	# Preferred: Shopify product JSON endpoint for accurate variants
	try:
		# Expect /products/<handle>
		if "/products/" in url:
			handle = url.split("/products/")[-1].split("?")[0].strip("/")
			js_url = f"{BASE_URL}/products/{handle}.js"
			jr = fetch(js_url)
			pdata = jr.json()
			# Collect option names
			for opt in pdata.get("options", []):
				name = opt.get("name")
				if isinstance(name, str) and name.strip():
					option_names.append(name.strip())
					option_values.setdefault(name.strip(), set())

			for v in pdata.get("variants", []):
				# Shopify .js uses price and compare_at_price in cents
				price_val = v.get("price")
				if isinstance(price_val, (int, float)):
					price_val = float(price_val) / 100.0
				compare_val = v.get("compare_at_price")
				if isinstance(compare_val, (int, float)):
					compare_val = float(compare_val) / 100.0
				# map option1..3 to option_names
				v_options: Dict[str, Optional[str]] = {}
				for idx, oname in enumerate(option_names, start=1):
					oval = v.get(f"option{idx}")
					v_options[oname] = oval
					if oval:
						option_values.setdefault(oname, set()).add(oval)
				variants.append(
					Variant(
						option_title=v.get("title"),
						price=price_val,
						compare_at_price=compare_val,
						sku=v.get("sku"),
						available=bool(v.get("available")),
						options=v_options,
					)
				)
	except Exception:
		# Fallback to HTML-inferred options
		pass

	if not variants:
		# Approach 1: look for variant options in selects
		for opt in soup.select(".product-form__input select, form[action*='cart/add'] select"):
			for option in opt.select("option"):
				label = option.get_text(strip=True)
				if not label:
					continue
				price_val = None
				price_match = re.search(r"([₦₵$€£]|NGN|USD|EUR|GBP)?\s?([0-9,]+(?:\.[0-9]{2})?)", label)
				if price_match:
					price_val = parse_price(price_match.group(0))
				variants.append(Variant(option_title=label, price=price_val))

		# Approach 2: variant titles often show up in radio labels
		for lbl in soup.select(".product-form__input label, .variant-input label, .swatch__label"):
			txt = lbl.get_text(" ", strip=True)
			if txt and len(txt) < 80:  # avoid long descriptions
				variants.append(Variant(option_title=txt))

	# Deduplicate variant titles
		seen = set()
		deduped: List[Variant] = []
		for v in variants:
			key = (v.option_title or "").lower()
			if key in seen or not key:
				continue
			seen.add(key)
			deduped.append(v)
		variants = deduped

	# Core price numbers
	# Prefer price from variants if available
	var_prices = [vv.price for vv in variants if vv.price is not None]
	if var_prices:
		price = min(var_prices)
	else:
		price = parse_price(price_text or "")
	var_compares = [vv.compare_at_price for vv in variants if vv.compare_at_price is not None]
	if var_compares:
		compare_at = min(var_compares)
	else:
		compare_at = parse_price(compare_text or "")

	if not title:
		# If we can't find a title, consider this a failed parse
		print(f"Warning: No title parsed for {url}")
		return None

	# Convert option_values to lists
	option_values_list: Dict[str, List[str]] = {k: sorted(list(v)) for k, v in option_values.items()}

	return Product(
		collection=collection_name,
		title=title,
		url=url,
		price=price,
		compare_at_price=compare_at,
		currency=currency,
		description=description,
		images=list(images),
		tags=tags,
		vendor=vendor,
		product_type=product_type,
		variants=variants,
		option_names=option_names,
		option_values=option_values_list,
	)


def scrape_collection(collection_url: str) -> List[Product]:
	print(f"Scraping collection: {collection_url}")
	# Infer collection name from URL path
	collection_slug = collection_url.rstrip("/").split("/")[-1]

	page_urls = paginate_collection_urls(collection_url)
	products: List[Product] = []

	for page_url in page_urls:
		try:
			r = fetch(page_url)
		except Exception as e:
			print(f"Failed to fetch {page_url}: {e}")
			continue
		soup = BeautifulSoup(r.text, "lxml")
		product_links = extract_product_links(soup)
		print(f"  Found {len(product_links)} product links on {page_url}")

		for purl in product_links:
			prod = parse_product_page(collection_slug, purl)
			if prod:
				products.append(prod)
	return products


def write_outputs(products: List[Product], out_dir: str = "data") -> None:
	ensure_dir(out_dir)

	# JSON
	json_path = os.path.join(out_dir, "products.json")
	with open(json_path, "w", encoding="utf-8") as f:
		json.dump([asdict(p) for p in products], f, ensure_ascii=False, indent=2)

	# CSV (flatten variants as joined string)
	csv_path = os.path.join(out_dir, "products.csv")
	with open(csv_path, "w", encoding="utf-8", newline="") as f:
		writer = csv.writer(f)
		writer.writerow([
			"collection",
			"title",
			"url",
			"price",
			"compare_at_price",
			"currency",
			"vendor",
			"product_type",
			"tags",
			"images",
			"variant_count",
			"variant_titles",
			"variant_option_names",
			"variant_option_values",
		])

		for p in products:
			variant_titles = "; ".join([v.option_title or "" for v in p.variants])
			option_values_str = "; ".join(
				[f"{name}: " + "|".join(p.option_values.get(name, [])) for name in p.option_names]
			)
			writer.writerow([
				p.collection,
				p.title,
				p.url,
				p.price if p.price is not None else "",
				p.compare_at_price if p.compare_at_price is not None else "",
				p.currency or "",
				p.vendor or "",
				p.product_type or "",
				"; ".join(p.tags),
				"; ".join(p.images),
				len(p.variants),
				variant_titles,
				", ".join(p.option_names),
				option_values_str,
			])

	print(f"Wrote {len(products)} products to:\n  {json_path}\n  {csv_path}")


def main(args: List[str]) -> int:
	# Allow overriding collections via CLI args
	targets = COLLECTION_URLS
	if args:
		targets = args

	all_products: List[Product] = []
	for curl in targets:
		all_products.extend(scrape_collection(curl))

	# De-duplicate by URL
	dedup: Dict[str, Product] = {}
	for p in all_products:
		dedup[p.url] = p
	products = list(dedup.values())

	write_outputs(products)
	return 0


if __name__ == "__main__":
	try:
		sys.exit(main(sys.argv[1:]))
	except KeyboardInterrupt:
		print("Interrupted.")
		sys.exit(130)

