import asyncio
import aiohttp
import json
import os
import pandas as pd
from math import ceil
import re
from bs4 import BeautifulSoup


# API endpoint
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/"

# Tạo thư mục lưu file output nếu chưa tồn tại
OUTPUT_DIR = "tiki_products"
ERROR_LOG = "tiki_errors.json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Hàm chuẩn hóa nội dung Description
def normalize_description(description):
    if not description:
        return ""

    # Parse HTML
    soup = BeautifulSoup(description, "html.parser")

    # Trích xuất toàn bộ văn bản từ thẻ HTML
    text = soup.get_text(separator=" ", strip=True)

    return text.strip()

async def fetch_product(session, product_id):
    url = f"{API_URL}{product_id}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "url_key": data.get("url_key"),
                    "price": data.get("price"),
                    "description": normalize_description(data.get("description")),
                    "images": data.get("images", []),
                }
            else:
                return {"error": f"HTTP {response.status}"}
    except Exception as e:
        return {"error": str(e)}

# Hàm tải dữ liệu sản phẩm
async def fetch_products(product_ids):
    results = []
    errors = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_product(session, pid) for pid in product_ids]
        responses = await asyncio.gather(*tasks)

        for pid, response in zip(product_ids, responses):
            if "error" in response:
                errors.append({"product_id": pid, "error": response["error"]})
            else:
                results.append(response)
    return results, errors

# Hàm lưu thành file json
def save_to_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# Hàm đọc file csv
def read_product_ids_from_csv(file_path):
    df = pd.read_csv(file_path, header=None, names=["product_id"])
    product_ids = df["product_id"].tolist()
    if product_ids[0] == "id":
        product_ids = product_ids[1:]
    return product_ids

# Hàm chính
async def main(csv_file_path, n=1000):
    product_ids = read_product_ids_from_csv(csv_file_path)
    total_products = len(product_ids)
    num_files  = ceil(total_products / n)

    all_errors = []
    for i in range(num_files ):
        start_idx = i * n
        end_idx = start_idx + n
        file_ids = product_ids[start_idx:end_idx]

        print(f"Processing {i + 1}/{num_files}...")
        products, errors = await fetch_products(file_ids)

        output_file = os.path.join(OUTPUT_DIR, f"products_file_{i + 1}.json")
        save_to_json(products, output_file)

        all_errors.extend(errors)

    # Lưu lỗi
    save_to_json(all_errors, ERROR_LOG)
    print(f"Completed")

# Chạy chương trình
if __name__ == "__main__":
    csv_file_path = "products_ids.csv"
    asyncio.run(main(csv_file_path))
