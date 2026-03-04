import asyncio
import json
import requests
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# --- BƯỚC 1: LẤY LINK THẬT - LỌC SẠCH ẢNH VÀ RÁC ---
def lay_toan_bo_link(sitemap_index_url):
    print(f"🔍 Đang phân tích Sơ đồ tổng: {sitemap_index_url}")
    toan_bo_links = []
    
    tu_khoa_rac = ['/gio-hang', '/thanh-toan', '/tai-khoan', 'wp-content/uploads', '/blocks/']
    duoi_rac = ('.webp', '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.xml')
    
    try:
        response = requests.get(sitemap_index_url)
        soup = BeautifulSoup(response.content, 'xml')
        
        sitemap_con_urls = [loc.text.strip() for loc in soup.find_all('loc') if loc.text.strip().endswith('.xml')]
        print(f"📂 Tìm thấy {len(sitemap_con_urls)} sơ đồ con.")

        for sitemap_url in sitemap_con_urls:
            res = requests.get(sitemap_url)
            s_soup = BeautifulSoup(res.content, 'xml')
            
            for loc in s_soup.find_all('loc'):
                url = loc.text.strip()
                if any(rac in url for rac in tu_khoa_rac) or url.lower().endswith(duoi_rac):
                    continue
                toan_bo_links.append(url)
        
        toan_bo_links = list(set(toan_bo_links))
        print(f"🎯 THÀNH CÔNG: Gom được {len(toan_bo_links)} link hợp lệ!")
        return toan_bo_links
        
    except Exception as e:
        print(f"❌ Lỗi khi đọc sitemap: {e}")
        return []

# --- BƯỚC 2: CÀO LẤY 100% NỘI DUNG ---
async def cao_toan_bo_web(danh_sach_link):
    kho_du_lieu = []
    
    browser_config = BrowserConfig(headless=True, verbose=True)
    
    # CẤU HÌNH MỚI: Không dùng Filter tỉa cành nữa!
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS, 
        # Loại bỏ thủ công các thẻ rác rưởi của web để lấy lõi
        excluded_tags=['header', 'footer', 'nav', 'aside', 'form', 'iframe'], 
        remove_overlay_elements=True,
        # Giữ nguyên cấu trúc Markdown, loại bỏ link ảnh để tiết kiệm Token cho Chatbot
        markdown_generator=DefaultMarkdownGenerator(
            options={
                "ignore_images": True, 
                "ignore_links": False
            }
        )
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        print("🚀 Bắt đầu cào FULL nội dung...")
        results = await crawler.arun_many(urls=danh_sach_link, config=run_config)
        
        for result in results:
            if result.success:
                # Lấy raw_markdown: Chứa tất cả bảng giá, lists, thông số kỹ thuật
                noi_dung = result.markdown.raw_markdown 
                
                if len(noi_dung.strip()) > 50:
                    kho_du_lieu.append({
                        "url": result.url,
                        "noi_dung": noi_dung.strip()
                    })
                    print(f"✅ Đã hút trọn gói: {result.url}")
                else:
                    print(f"⚠️ Bỏ qua (Trang rỗng): {result.url}")
            else:
                print(f"❌ Lỗi truy cập: {result.url}")
                
    return kho_du_lieu

# --- BƯỚC 3: CHẠY THỬ ---
async def main():
    sitemap_url = "https://trangtri360.com/sitemap_index.xml" 
    
    danh_sach_link = lay_toan_bo_link(sitemap_url)
    
    # Test thử 3 link để bạn kiểm tra độ đầy đủ của dữ liệu
    # danh_sach_link_test = danh_sach_link[:3] 
    
    # if not danh_sach_link_test:
    #     return

    kho_du_lieu = await cao_toan_bo_web(danh_sach_link)
    
    with open('kho_tri_thuc_day_du.json', 'w', encoding='utf-8') as f:
        json.dump(kho_du_lieu, f, ensure_ascii=False, indent=4)
        
    print(f"🎉 Hoàn tất! Mở 'kho_tri_thuc_day_du.json' để xem kết quả nhé.")

if __name__ == "__main__":
    asyncio.run(main())