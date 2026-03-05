#!/usr/bin/env python3
"""
knowledge_crawler.py — Trình cào tri thức cho AI Chatbot (v2)
==============================================================
Cào toàn bộ nội dung từ sitemap, lọc sạch rác, và đẩy dữ liệu
lên Google Sheets qua webhook. Nếu lỗi mạng → lưu dự phòng JSON.

Cải tiến v2:
  - Cào theo batch (tránh browser crash khi 600+ URL)
  - Mỗi batch mở browser mới → tránh lỗi BrowserContext closed
  - Retry tự động các URL lỗi
  - Lọc sạch: giữ text link, bỏ URL, giữ heading, xóa popup/promo

Cách chạy:
    python knowledge_crawler.py
"""

import asyncio
import json
import re
import time

import requests
from bs4 import BeautifulSoup

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
)
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# =============================================================================
# 📌 CẤU HÌNH CHÍNH — Chỉ cần sửa ở đây
# =============================================================================

# Sitemap gốc của website
SITEMAP_URL = "https://trangtri360.com/sitemap_index.xml"

# Webhook Google Apps Script (POST dữ liệu lên Google Sheets)
WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxIUG2myKv46WzuhlUQNRFYWtz6qfDTy4YnmKKEgJ6Xz_C9R8sn8xKyPUxEYR6xU3bm/exec"

# Các đường dẫn rác cần loại bỏ (không mang tri thức cho AI)
TU_KHOA_RAC = [
    "/gio-hang",
    "/thanh-toan",
    "/tai-khoan",
    "wp-content",
    "/author/",
    "/danh-muc/",
]

# Tham số điều khiển tốc độ cào
CONCURRENCY = 5         # Số trang cào song song tối đa trong 1 batch
BATCH_SIZE = 50          # Số URL mỗi batch (mở browser mới mỗi batch)
RATE_LIMIT_DELAY = 1.0   # Giây chờ tối thiểu giữa mỗi request
MAX_RETRY = 2            # Số lần retry URL bị lỗi

# Tham số lọc nội dung
PRUNING_THRESHOLD = 0.5
MIN_WORD_THRESHOLD = 20
MIN_CONTENT_LENGTH = 50  # Bỏ trang có nội dung quá ngắn (< 50 ký tự)

# File dự phòng khi gửi webhook thất bại
FALLBACK_FILE = "du_phong_data.json"

# File lưu đầy đủ toàn bộ kho tri thức (luôn lưu, không phân biệt thành/thất bại)
KHO_TRI_THUC_FILE = "kho_tri_thuc_day_du.json"


# =============================================================================
# 🧹 HÀM LÀM SẠCH MARKDOWN — Giữ text, bỏ URL/ảnh, giữ heading
# =============================================================================

# Các cụm text rác lặp đi lặp lại trên mọi trang (popup, promo, CTA)
CAC_CUM_RAC = [
    "Bỏ qua nội dung",
    "Đừng bỏ lỡ cơ hội biến không gian của bạn trở nên đẳng cấp hơn với đội ngũ chuyên gia từ",
    "ĐẶT LỊCH TƯ VẤN NGAY",
    "NHẬN ƯU ĐÃI 10%",
    "THÔNG TIN ĐẤU THẦU DỰ ÁN",
    "GỬI EMAIL NGAY",
    "GỬI VÀ NHẬN ƯU ĐÃI",
    "KHUYẾN MÃI - ƯU ĐÃI",
    "Đã tư vấn & cung cấp dịch vụ cho hơn",
    "Gửi đánh giá ngay",
    "Chưa có đánh giá nào",
    "Chưa có bình luận nào",
    "Mời bạn tham gia thảo luận",
    "Sản phẩm tương tự",
]


def lam_sach_markdown(raw_md: str) -> str:
    """
    Làm sạch markdown cho AI Knowledge Base:
    1. Giữ text trong link [text](url) → text (không mất từ khóa bold)
    2. Xóa ảnh markdown ![alt](url)
    3. Giữ nguyên heading (#, ##, ###)
    4. Xóa các cụm text rác (popup, promo, CTA)
    5. Xóa dòng trống thừa, khoảng trắng thừa
    """
    text = raw_md

    # 1. Xóa ảnh markdown: ![alt](url)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)

    # 2. Giữ text trong link: [text](url) → text
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)

    # 3. Xóa reference-style links: [text][ref] → text
    text = re.sub(r"\[([^\]]+)\]\[[^\]]*\]", r"\1", text)

    # 4. Xóa reference definitions: [ref]: url ...
    text = re.sub(r"^\s*\[[^\]]+\]:\s*http[^\n]*$", "", text, flags=re.MULTILINE)

    # 5. Xóa standalone URLs (http://... hoặc https://...)
    text = re.sub(r"(?<!\()https?://[^\s)]+", "", text)

    # 6. Xóa các cụm text rác
    for cum_rac in CAC_CUM_RAC:
        text = text.replace(cum_rac, "")

    # 7. Xóa các dòng chỉ chứa dấu chấm câu rời (hậu quả của strip link)
    text = re.sub(r"^\s*[.,;:!?]+\s*$", "", text, flags=re.MULTILINE)

    # 8. Xóa nhiều dòng trống liên tiếp → 1 dòng trống
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 9. Xóa khoảng trắng thừa đầu/cuối mỗi dòng, bỏ dòng trống
    lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped:
            lines.append(stripped)
    text = "\n".join(lines)

    return text.strip()


# =============================================================================
# 🔍 BƯỚC 1: Thu thập toàn bộ URL từ sitemap
# =============================================================================

def lay_toan_bo_link(sitemap_index_url: str) -> list[str]:
    """
    Đọc sitemap index → duyệt từng sitemap con → thu thập URL.
    Lọc bỏ các URL chứa từ khóa rác.
    """
    print(f"\n{'='*60}")
    print(f"🔍 BƯỚC 1: Quét sitemap — {sitemap_index_url}")
    print(f"{'='*60}")

    toan_bo_links: list[str] = []

    try:
        response = requests.get(sitemap_index_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "xml")

        sitemap_con_urls = [
            loc.text.strip()
            for loc in soup.find_all("loc")
            if loc.text.strip().endswith(".xml")
        ]
        print(f"   📂 Tìm thấy {len(sitemap_con_urls)} sitemap con")

        for idx, sitemap_url in enumerate(sitemap_con_urls, 1):
            try:
                res = requests.get(sitemap_url, timeout=30)
                res.raise_for_status()
                s_soup = BeautifulSoup(res.content, "xml")

                count_before = len(toan_bo_links)
                for loc in s_soup.find_all("loc"):
                    url = loc.text.strip()
                    if not any(rac in url for rac in TU_KHOA_RAC):
                        toan_bo_links.append(url)

                added = len(toan_bo_links) - count_before
                print(f"   [{idx}/{len(sitemap_con_urls)}] +{added} URL")
            except Exception as e:
                print(f"   ⚠️ Lỗi đọc sitemap con {sitemap_url}: {e}")

        toan_bo_links = list(set(toan_bo_links))
        print(f"\n   ✅ Tổng cộng: {len(toan_bo_links)} URL duy nhất sau khi lọc")
        return toan_bo_links

    except Exception as e:
        print(f"   ❌ Lỗi nghiêm trọng khi đọc sitemap: {e}")
        return []


# =============================================================================
# 🚀 BƯỚC 2: Cào nội dung với crawl4ai (BATCH + RETRY)
# =============================================================================

def _tao_run_config() -> CrawlerRunConfig:
    """Tạo CrawlerRunConfig chuẩn cho mỗi batch."""

    pruning_filter = PruningContentFilter(
        threshold=PRUNING_THRESHOLD,
        threshold_type="fixed",
        min_word_threshold=MIN_WORD_THRESHOLD,
    )

    md_generator = DefaultMarkdownGenerator(
        content_filter=pruning_filter,
        options={
            "ignore_images": True,   # Bỏ tất cả ảnh
            "ignore_links": False,   # GIỮ text link → post-process strip URL sau
            "escape_html": True,
        },
    )

    # CSS selectors cho các vùng rác cần loại bỏ hoàn toàn
    bo_selector_rac = ", ".join([
        # Popup đấu thầu & lightbox
        ".lightbox-by-id", ".lightbox-content", "#popup-dau-thau-lightbox",
        # Promo / CTA sections
        ".promo-container", ".shop-custom-section", ".info-footer-box",
        # Widget liên hệ nổi
        "#contact-button-widget",
        # WooCommerce: giỏ hàng, sản phẩm liên quan, đánh giá, bình luận
        ".woocommerce-mini-cart", ".ux-mini-cart-footer",
        ".related-products-wrapper", ".woocommerce-Reviews",
        ".devvn_prod_cmt", "#review_form_wrapper",
        # Cart sidebar & search
        "#cart-popup", "#search-lightbox",
        # Mobile sidebar menu
        "#main-menu", ".mobile-sidebar",
        # Breadcrumb
        ".rank-math-breadcrumb", ".wn-breadcrumbs",
        # Badge, image tools
        ".badge-container", ".image-tools",
        # Footer sections
        ".custom-footer-01", ".absolute-footer", "#footer",
        # Gallery ảnh sản phẩm
        ".product-thumbnails", ".product-gallery-slider",
        # Giá & form mua hàng
        ".price-wrapper", ".cart", ".quantity",
    ])

    return CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        excluded_tags=[
            "nav", "footer", "header", "form",
            "aside", "noscript", "script", "style",
        ],
        excluded_selector=bo_selector_rac,
        markdown_generator=md_generator,
        magic=True,
        page_timeout=30000,              # Timeout 30s mỗi trang (tránh treo)
        delay_before_return_html=2.0,
        semaphore_count=CONCURRENCY,
        mean_delay=RATE_LIMIT_DELAY,
        max_range=3.0,
    )


async def _cao_mot_batch(
    danh_sach_url: list[str],
    batch_num: int,
    total_batches: int,
) -> tuple[list[dict], list[str]]:
    """
    Cào 1 batch URL. Mỗi batch mở browser MỚI → tránh lỗi BrowserContext closed.
    Trả về: (kết_quả_thành_công, danh_sách_url_lỗi)
    """
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
    )
    run_config = _tao_run_config()

    ket_qua: list[dict] = []
    url_loi: list[str] = []

    print(f"\n   📦 Batch {batch_num}/{total_batches} — {len(danh_sach_url)} URL")

    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            results = await crawler.arun_many(
                urls=danh_sach_url,
                config=run_config,
            )

            for result in results:
                if result.success:
                    noi_dung_sach = ""
                    # Dùng raw_markdown để GIỮ bold (**) và heading (##)
                    # (fit_markdown strip hết formatting → mất từ khóa quan trọng)
                    if hasattr(result.markdown, "raw_markdown") and result.markdown.raw_markdown:
                        noi_dung_sach = result.markdown.raw_markdown
                    elif isinstance(result.markdown, str):
                        noi_dung_sach = result.markdown

                    # Làm sạch markdown
                    noi_dung_sach = lam_sach_markdown(noi_dung_sach)

                    if noi_dung_sach and len(noi_dung_sach.strip()) > MIN_CONTENT_LENGTH:
                        ket_qua.append({
                            "url": result.url,
                            "noi_dung": noi_dung_sach.strip(),
                        })
                        print(f"      ✅ {result.url}")
                    else:
                        print(f"      ⏭️  Bỏ qua (quá ngắn): {result.url}")
                else:
                    url_loi.append(result.url)
                    print(f"      ❌ {result.url}")

    except Exception as e:
        print(f"      💥 Batch crash: {e}")
        # Nếu cả batch crash → đưa toàn bộ URL vào danh sách lỗi để retry
        url_loi.extend(danh_sach_url)

    print(f"      📊 Batch {batch_num}: ✅ {len(ket_qua)} | ❌ {len(url_loi)}")
    return ket_qua, url_loi


async def cao_du_lieu(danh_sach_link: list[str]) -> list[dict]:
    """
    Pipeline cào chính: chia batch → cào → retry URL lỗi.
    Mỗi batch mở browser mới để tránh lỗi BrowserContext closed.
    """
    print(f"\n{'='*60}")
    print(f"🚀 BƯỚC 2: Cào nội dung — {len(danh_sach_link)} trang")
    print(f"   Batch size: {BATCH_SIZE} | Concurrency: {CONCURRENCY}")
    print(f"   Rate Limit: {RATE_LIMIT_DELAY}s | Max retry: {MAX_RETRY}")
    print(f"{'='*60}")

    bat_dau = time.time()
    kho_du_lieu: list[dict] = []
    url_can_retry: list[str] = []

    # ── Chia URL thành các batch ──
    batches = [
        danh_sach_link[i:i + BATCH_SIZE]
        for i in range(0, len(danh_sach_link), BATCH_SIZE)
    ]
    total_batches = len(batches)
    print(f"   📦 Chia thành {total_batches} batch × ~{BATCH_SIZE} URL")

    # ── Cào từng batch ──
    for idx, batch in enumerate(batches, 1):
        ket_qua, url_loi = await _cao_mot_batch(batch, idx, total_batches)
        kho_du_lieu.extend(ket_qua)
        url_can_retry.extend(url_loi)

        # Nghỉ 3 giây giữa các batch để browser giải phóng bộ nhớ
        if idx < total_batches:
            print(f"   ⏳ Nghỉ 3s trước batch tiếp theo...")
            await asyncio.sleep(3)

    # ── Retry các URL bị lỗi ──
    for lan_retry in range(1, MAX_RETRY + 1):
        if not url_can_retry:
            break

        print(f"\n   🔄 RETRY lần {lan_retry}/{MAX_RETRY} — {len(url_can_retry)} URL")

        retry_batches = [
            url_can_retry[i:i + BATCH_SIZE]
            for i in range(0, len(url_can_retry), BATCH_SIZE)
        ]
        url_can_retry = []

        for idx, batch in enumerate(retry_batches, 1):
            await asyncio.sleep(5)  # Nghỉ lâu hơn trước khi retry
            ket_qua, url_loi = await _cao_mot_batch(
                batch, idx, len(retry_batches)
            )
            kho_du_lieu.extend(ket_qua)
            url_can_retry.extend(url_loi)

    thoi_gian = time.time() - bat_dau

    print(f"\n   {'='*50}")
    print(f"   📊 KẾT QUẢ TỔNG HỢP:")
    print(f"      ✅ Thành công:  {len(kho_du_lieu)}")
    print(f"      ❌ Thất bại:    {len(url_can_retry)}")
    print(f"      ⏱️  Thời gian:   {thoi_gian:.1f} giây")
    print(f"   {'='*50}")

    if url_can_retry:
        print(f"\n   ⚠️ Các URL vẫn bị lỗi sau {MAX_RETRY} lần retry:")
        for url in url_can_retry[:10]:
            print(f"      - {url}")
        if len(url_can_retry) > 10:
            print(f"      ... và {len(url_can_retry) - 10} URL khác")

    return kho_du_lieu


# =============================================================================
# 📤 BƯỚC 3: Đẩy dữ liệu lên Google Sheets
# =============================================================================


def day_len_google_sheets(kho_du_lieu: list[dict]) -> bool:
    """
    POST toàn bộ dữ liệu lên Google Apps Script Webhook trong 1 request.
    Payload: {"action": "overwrite", "data": [...]}
    Trả về True nếu thành công.
    """
    print(f"\n{'='*60}")
    print(f"📤 BƯỚC 3: Đẩy {len(kho_du_lieu)} tài liệu lên Google Sheets")
    print(f"{'='*60}")

    payload = {
        "action": "overwrite",
        "data": kho_du_lieu,
    }

    try:
        print(f"   📡 Đang gửi toàn bộ {len(kho_du_lieu)} tài liệu...")
        response = requests.post(
            WEB_APP_URL,
            json=payload,
            timeout=300,  # 5 phút — Google Apps Script có thể chậm với data lớn
        )
        response.raise_for_status()
        print(f"   🎉 Thành công! Response: {response.text[:200]}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Lỗi gửi webhook: {e}")
        return False


def luu_du_phong(kho_du_lieu: list[dict], file_path: str = FALLBACK_FILE) -> None:
    """Lưu dữ liệu ra file JSON khi webhook thất bại."""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(kho_du_lieu, f, ensure_ascii=False, indent=2)
        print(f"   💾 Đã lưu dự phòng: {file_path} ({len(kho_du_lieu)} tài liệu)")
    except Exception as e:
        print(f"   ❌ Lỗi lưu file dự phòng: {e}")


# =============================================================================
# 🎯 MAIN: Điều phối toàn bộ pipeline
# =============================================================================

async def main():
    """Pipeline chính: Sitemap → Cào (batch) → Đẩy lên Google Sheets."""
    print("\n" + "🌟" * 30)
    print("  KNOWLEDGE CRAWLER v2 — Tạo Kho Tri Thức cho AI Chatbot")
    print("🌟" * 30)

    # Bước 1: Thu thập URL từ sitemap
    danh_sach_link = lay_toan_bo_link(SITEMAP_URL)
    if not danh_sach_link:
        print("\n❌ Không tìm thấy URL nào! Kiểm tra lại sitemap.")
        return

    # Bước 2: Cào nội dung (batch + retry)
    kho_du_lieu = await cao_du_lieu(danh_sach_link)
    if not kho_du_lieu:
        print("\n⚠️ Không có nội dung nào đạt yêu cầu sau khi lọc.")
        return

    # Bước 3: Luôn lưu local JSON đầy đủ
    print(f"\n{'='*60}")
    print(f"💾 BƯỚC 3: Lưu kho tri thức local")
    print(f"{'='*60}")
    luu_du_phong(kho_du_lieu, KHO_TRI_THUC_FILE)

    # Bước 4: Đẩy lên Google Sheets
    day_len_google_sheets(kho_du_lieu)

    print(f"\n{'='*60}")
    print(f"🏁 HOÀN TẤT! Tổng: {len(kho_du_lieu)} tài liệu tri thức")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
