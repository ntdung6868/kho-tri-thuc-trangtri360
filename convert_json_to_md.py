#!/usr/bin/env python3
"""
Script chuyển đổi kho tri thức từ JSON sang Markdown SẠCH.
Output: kho_tri_thuc_sach.md — dùng cho n8n Markdown Splitter + Qdrant.

Cải tiến v2 (kho_tri_thuc_sach):
  - Lọc triệt để các phần tử rác UI (badge thống kê, widget sidebar, CTA)
  - Xóa toàn bộ section rác: DỰ ÁN, Đối tác, Thống kê, Quy trình
  - Xóa các block heading+content bị lặp đôi (do crawler capture 2 lần)
  - Sửa bug downgrade_headings (trước đây tạo '## ## title' sai cú pháp)
  - Chỉ giữ nội dung mang giá trị thông tin thực sự
"""

import json
import re
import os
from urllib.parse import urlparse

# --- Cấu hình ---
INPUT_FILE = os.path.join(os.path.dirname(__file__), "kho_tri_thuc_day_du.json")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "kho_tri_thuc_sach.md")

# --- Bộ lọc rác UI ---

# Dòng rác UI: exact match (case-sensitive)
UI_GARBAGE_EXACT = {
    "Đơn hàng",
    "Khách hàng hài lòng", "khách hàng hài lòng",
    "giao hàng đúng hạn",
    "dự án đã hoàn thành",
    "Trong vòng 10 ngày làm việc",
    "trong vòng vài phút",
    "Theo nhu cầu của bạn",
    "3d, LED, Kim loại, Acrylic, Hộp đèn",
    "Đối tác của chúng tôi",
    "Thông số thống kê",
    "Ghi nhận thông tin của khách hàng.",
    "Đo đạc hiện trạng và tư vấn trực tiếp.",
    "Tiếp nhận yêu cầu",
    "Khảo sát thực tế",
    "DỰ ÁN",
}

# Tiền tố dòng rác UI
UI_GARBAGE_STARTS = (
    "Bạn có thắc mắc gì",
    "Bạn muốn được tư vấn",
    "Liên hệ chúng tôi hoặc gọi",
)

# Heading H4+ bắt đầu section rác — xóa toàn bộ section
UI_SECTION_HEADING_PATTERNS = [
    r'^#{4,6}\s*(DỰ ÁN|Dự án)\s*$',
    r'^#{4,6}\s*(Sản phẩm|SẢN PHẨM)\s*$',        # sidebar "related products" widget
    r'^#{4,6}\s*(Đối tác|ĐỐI TÁC)',
    r'^#{4,6}\s*(Thông số thống kê|THÔNG SỐ THỐNG KÊ)',
    r'^#{4,6}\s*(QUY TRÌNH ĐẶT HÀNG)',
    r'^#{4,6}\s*(GIAO HÀNG NHANH CHÓNG)',
    r'^#{4,6}\s*(HỖ TRỢ 24/7)',
    r'^#{4,6}\s*(CÓ SẴN MỌI LOẠI)',
    r'^#{4,6}\s*(100% tùy chỉnh)',
    r'^#{4,6}\s*\*{0,2}[\d\.]+[k%\+]+\*{0,2}\s*$',  # "#### 10000+", "#### **5k+**"
]


def extract_title_from_content(content: str) -> str:
    """Trích xuất tiêu đề từ heading đầu tiên trong nội dung."""
    match = re.search(r'^#{1,2}\s+(.+)$', content, re.MULTILINE)
    if match:
        title = match.group(1).strip()
        title = re.sub(r'\*\*(.+?)\*\*', r'\1', title)
        title = re.sub(r'\*(.+?)\*', r'\1', title)
        return title
    for line in content.split('\n'):
        line = line.strip()
        if line and not line.startswith('![') and len(line) > 5:
            return line[:100]
    return "Không có tiêu đề"


def extract_slug_title(url: str) -> str:
    """Trích xuất tiêu đề dạng slug từ URL."""
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if path:
        slug = path.split('/')[-1]
        slug = slug.replace('-', ' ').replace('_', ' ')
        return slug.title()
    return parsed.netloc


def get_domain_topic(url: str) -> str:
    """Xác định domain dựa trên URL."""
    parsed = urlparse(url)
    return parsed.netloc


def clean_content(content: str) -> str:
    """
    Dọn dẹp nội dung:
    - Xóa dòng rác UI (badge thống kê, CTA, mạng xã hội)
    - Giữ heading, paragraph, bullet, bảng
    - Chỉ giữ tối đa 1 dòng trống liên tiếp
    """
    lines = content.split('\n')
    cleaned_lines = []
    prev_empty = False

    for line in lines:
        stripped = line.strip()

        # Bỏ qua link mạng xã hội
        if re.match(
            r'^\[.*?\]\((whatsapp|https://(www\.)?(facebook|twitter|pinterest|linkedin)).*?\)$',
            stripped
        ):
            continue

        # Bỏ qua HTML <br>
        stripped = stripped.replace('<br>', '').strip()

        # Bỏ qua link rỗng [](...)
        if re.match(r'^\[\s*\]\(.*?\)$', stripped):
            continue

        # === BỌC LỌC RÁC UI ===

        # 1. Badge số liệu: "10000+", "5k+", "**100%**", "**5k+**"
        if re.match(r'^\*{0,2}[\d\.]+[k%\+]+\*{0,2}$', stripped, re.IGNORECASE):
            continue

        # 2. Số thứ tự bước quy trình standalone: "1", "2", "3"
        if re.match(r'^\d{1,2}$', stripped):
            continue

        # 3. Exact match với danh sách rác đã biết
        if stripped in UI_GARBAGE_EXACT:
            continue

        # 4. Tiền tố rác đã biết
        if stripped.startswith(UI_GARBAGE_STARTS):
            continue

        # === DÒNG TRỐNG ===
        if not stripped:
            if not prev_empty:
                cleaned_lines.append('')
                prev_empty = True
            continue

        # === HEADING ===
        if stripped.startswith('#'):
            if cleaned_lines and cleaned_lines[-1].strip():
                cleaned_lines.append('')
            cleaned_lines.append(stripped)
            prev_empty = False
            continue

        # === CÁC ĐỊNH DẠNG MARKDOWN KHÁC ===
        if stripped.startswith(('* ', '- ', '> ')):
            cleaned_lines.append(stripped)
            prev_empty = False
            continue

        if stripped.startswith('|') or re.match(r'^[-|:]+$', stripped):
            cleaned_lines.append(stripped)
            prev_empty = False
            continue

        # Dòng bình thường
        cleaned_lines.append(stripped)
        prev_empty = False

    result = '\n'.join(cleaned_lines).strip()
    result = re.sub(r'\n{3,}', '\n\n', result)
    return result


def remove_ui_heading_sections(content: str) -> str:
    """
    Xóa toàn bộ các section bắt đầu từ H4+ heading là rác UI.
    Ví dụ: '#### DỰ ÁN', '#### Sản phẩm' (sidebar), '#### QUY TRÌNH ĐẶT HÀNG'.
    Dừng skip khi gặp heading cùng cấp hoặc cao hơn.
    """
    lines = content.split('\n')
    result = []
    skipping = False
    skip_level = 0

    for line in lines:
        stripped = line.strip()

        if stripped.startswith('#'):
            level_match = re.match(r'^(#+)', stripped)
            current_level = len(level_match.group(1)) if level_match else 0
            is_ui_heading = any(re.match(p, stripped) for p in UI_SECTION_HEADING_PATTERNS)

            if is_ui_heading:
                skipping = True
                skip_level = current_level
                continue

            if skipping and current_level <= skip_level:
                skipping = False

        if not skipping:
            result.append(line)

    return '\n'.join(result)


def remove_duplicate_blocks(content: str) -> str:
    """
    Xóa các block heading+content bị lặp đôi.
    Rất thường gặp với badge thống kê:
      #### 10000+
      Đơn hàng

      #### 10000+   <- duplicate!
      Đơn hàng
    """
    lines = content.split('\n')
    result = []
    seen_pairs: set = set()
    i = 0

    while i < len(lines):
        current = lines[i].strip()

        if current.startswith('#'):
            # Tìm dòng kế không rỗng để tạo fingerprint
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            next_line = lines[j].strip() if j < len(lines) else ''

            pair_key = f"{current}|{next_line}"

            if pair_key in seen_pairs and current.strip('#').strip():
                # Bỏ qua duplicate block (heading + content đến heading tiếp theo)
                i += 1
                while i < len(lines):
                    if lines[i].strip().startswith('#'):
                        break
                    i += 1
                continue

            if current.strip('#').strip():
                seen_pairs.add(pair_key)

        result.append(lines[i])
        i += 1

    return '\n'.join(result)


def downgrade_headings(text: str) -> str:
    """
    Hạ cấp heading trong nội dung bài viết xuống 2 bậc để tránh xung đột với
    cấu trúc file (# domain, ## article title, ### metadata).
    Ví dụ: # H1 → ### H3, ## H2 → #### H4
    """
    lines = []
    for line in text.split('\n'):
        m = re.match(r'^(#{1,6})(\s.+)$', line)
        if m:
            current_level = len(m.group(1))
            new_level = min(current_level + 2, 6)
            lines.append('#' * new_level + m.group(2))
        else:
            lines.append(line)
    return '\n'.join(lines)


def convert_json_to_markdown(input_file: str, output_file: str):
    """Chuyển đổi file JSON sang Markdown sạch."""
    print(f"Đang đọc file: {input_file}")

    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Tổng số bài viết: {len(data)}")

    # Nhóm bài viết theo domain
    domain_articles: dict = {}
    for item in data:
        url = item.get('url', '')
        domain = get_domain_topic(url)
        if domain not in domain_articles:
            domain_articles[domain] = []
        domain_articles[domain].append(item)

    print(f"Số miền/domain: {len(domain_articles)}")
    for domain, articles in domain_articles.items():
        print(f"   - {domain}: {len(articles)} bài viết")

    md_lines = []
    total_skipped = 0

    for domain, articles in domain_articles.items():
        md_lines.append(f"# {domain}")
        md_lines.append("")

        for item in articles:
            url = item.get('url', '')
            content = item.get('noi_dung', '')

            # Heading 2: tiêu đề bài viết
            title = extract_title_from_content(content)
            md_lines.append(f"## {title}")
            md_lines.append("")

            # Heading 3: metadata
            md_lines.append("### Thông tin bài viết")
            md_lines.append(f"**URL nguồn:** {url}")
            md_lines.append("")

            # Pipeline làm sạch:
            # 1. Lọc dòng rác UI + format cơ bản
            cleaned = clean_content(content)
            # 2. Hạ cấp heading (fix bug: dùng regex thay vì string prepend)
            cleaned = downgrade_headings(cleaned)
            # 3. Xóa toàn bộ section rác (DỰ ÁN, Sản phẩm sidebar, v.v.)
            cleaned = remove_ui_heading_sections(cleaned)
            # 4. Xóa block lặp đôi
            cleaned = remove_duplicate_blocks(cleaned)
            # 5. Dọn dòng trống thừa sau tất cả các bước
            cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()

            if len(cleaned) < 50:
                total_skipped += 1
                continue

            md_lines.append(cleaned)
            md_lines.append("")
            md_lines.append("---")
            md_lines.append("")

    md_content = '\n'.join(md_lines)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(md_content)

    file_size = os.path.getsize(output_file)
    line_count = md_content.count('\n') + 1

    print(f"\nChuyển đổi thành công!")
    print(f"File output: {output_file}")
    print(f"Số dòng: {line_count:,}")
    print(f"Kích thước: {file_size / (1024*1024):.2f} MB")
    if total_skipped:
        print(f"Bỏ qua (nội dung quá ngắn sau lọc): {total_skipped} bài")


if __name__ == "__main__":
    convert_json_to_markdown(INPUT_FILE, OUTPUT_FILE)
