#!/usr/bin/env python3
"""
Script chuyển đổi kho tri thức từ JSON sang Markdown.
Tối ưu cho n8n Markdown Text Splitter / Document Loader.
"""

import json
import re
import os
from urllib.parse import urlparse

# --- Cấu hình ---
INPUT_FILE = os.path.join(os.path.dirname(__file__), "kho_tri_thuc_day_du.json")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "kho_tri_thuc_chuan.md")

def extract_title_from_content(content: str) -> str:
    """Trích xuất tiêu đề từ heading đầu tiên trong nội dung."""
    # Tìm heading markdown đầu tiên (# hoặc ##)
    match = re.search(r'^#{1,2}\s+(.+)$', content, re.MULTILINE)
    if match:
        title = match.group(1).strip()
        # Loại bỏ markdown formatting khỏi title
        title = re.sub(r'\*\*(.+?)\*\*', r'\1', title)
        title = re.sub(r'\*(.+?)\*', r'\1', title)
        return title
    # Nếu không có heading, lấy dòng đầu tiên không rỗng
    for line in content.split('\n'):
        line = line.strip()
        if line and not line.startswith('![') and len(line) > 5:
            return line[:100]  # Giới hạn 100 ký tự
    return "Không có tiêu đề"

def extract_slug_title(url: str) -> str:
    """Trích xuất tiêu đề dạng slug từ URL."""
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if path:
        slug = path.split('/')[-1]
        # Chuyển slug thành readable title
        slug = slug.replace('-', ' ').replace('_', ' ')
        return slug.title()
    return parsed.netloc

def clean_content(content: str) -> str:
    """Dọn dẹp nội dung: xóa khoảng trắng thừa, giữ paragraphs và bullet points."""
    lines = content.split('\n')
    cleaned_lines = []
    prev_empty = False
    
    for line in lines:
        # Xóa khoảng trắng thừa ở đầu/cuối dòng
        stripped = line.strip()
        
        # Bỏ qua các dòng chỉ chứa link chia sẻ mạng xã hội
        if re.match(r'^\[.*?\]\((whatsapp|https://(www\.)?(facebook|twitter|pinterest|linkedin)).*?\)$', stripped):
            continue
        
        # Bỏ qua các dòng chứa HTML tag <br>
        stripped = stripped.replace('<br>', '').strip()
        
        # Bỏ qua các link rỗng dạng [](...) hoặc [](...)
        if re.match(r'^\[\s*\]\(.*?\)$', stripped):
            continue
            
        # Giữ cấu trúc bullet points: thêm indent nếu cần
        if stripped.startswith('* ') or stripped.startswith('- '):
            cleaned_lines.append(stripped)
            prev_empty = False
            continue
        
        # Giữ headings
        if stripped.startswith('#'):
            # Đảm bảo có dòng trống trước heading
            if cleaned_lines and cleaned_lines[-1].strip():
                cleaned_lines.append('')
            cleaned_lines.append(stripped)
            prev_empty = False
            continue
        
        # Giữ blockquotes
        if stripped.startswith('>'):
            cleaned_lines.append(stripped)
            prev_empty = False
            continue
        
        # Giữ bảng
        if stripped.startswith('|') or re.match(r'^[-|:]+$', stripped):
            cleaned_lines.append(stripped)
            prev_empty = False
            continue
        
        # Dòng trống - chỉ giữ 1 dòng trống liên tiếp
        if not stripped:
            if not prev_empty:
                cleaned_lines.append('')
                prev_empty = True
            continue
        
        # Dòng bình thường
        cleaned_lines.append(stripped)
        prev_empty = False
    
    # Loại bỏ dòng trống ở đầu và cuối
    result = '\n'.join(cleaned_lines).strip()
    
    # Xóa nhiều dòng trống liên tiếp (max 2)
    result = re.sub(r'\n{3,}', '\n\n', result)
    
    return result

def get_domain_topic(url: str) -> str:
    """Xác định chủ đề/miền dựa trên URL."""
    parsed = urlparse(url)
    return parsed.netloc

def convert_json_to_markdown(input_file: str, output_file: str):
    """Chuyển đổi file JSON sang Markdown."""
    print(f"📖 Đang đọc file: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"📊 Tổng số bài viết: {len(data)}")
    
    # Nhóm bài viết theo domain
    domain_articles = {}
    for item in data:
        url = item.get('url', '')
        domain = get_domain_topic(url)
        if domain not in domain_articles:
            domain_articles[domain] = []
        domain_articles[domain].append(item)
    
    print(f"🌐 Số miền/domain: {len(domain_articles)}")
    for domain, articles in domain_articles.items():
        print(f"   - {domain}: {len(articles)} bài viết")
    
    # Tạo nội dung Markdown
    md_lines = []
    
    for domain, articles in domain_articles.items():
        # Heading 1: Tên miền / chủ đề chính
        md_lines.append(f"# {domain}")
        md_lines.append("")
        
        for idx, item in enumerate(articles):
            url = item.get('url', '')
            content = item.get('noi_dung', '')
            
            # Heading 2: Tiêu đề bài viết
            title = extract_title_from_content(content)
            md_lines.append(f"## {title}")
            md_lines.append("")
            
            # Heading 3: Metadata
            md_lines.append(f"### Thông tin bài viết")
            md_lines.append(f"**URL nguồn:** {url}")
            md_lines.append("")
            
            # Nội dung chính đã được dọn dẹp
            cleaned = clean_content(content)
            
            # Hạ cấp tất cả headings trong nội dung xuống 1 bậc
            # để tránh xung đột với cấu trúc tổng thể
            # # -> #### , ## -> ##### , ### -> ###### 
            # Nhưng giới hạn markdown chỉ đến h6, nên ta giữ nội dung ở mức ###+ 
            # Thay vì hạ cấp heading, ta đổi # thành ### , ## thành ####
            def downgrade_headings(text):
                lines = text.split('\n')
                result = []
                for line in lines:
                    if line.startswith('######'):
                        # Đã ở h6, giữ nguyên
                        result.append(line)
                    elif line.startswith('#####'):
                        result.append('#' + line)  # h5 -> h6
                    elif line.startswith('####'):
                        result.append('#' + line)  # h4 -> h5
                    elif line.startswith('###'):
                        result.append('#' + line)  # h3 -> h4
                    elif line.startswith('##'):
                        result.append('##' + line)  # h2 -> h4
                    elif line.startswith('#'):
                        result.append('##' + line)  # h1 -> h3
                    else:
                        result.append(line)
                return '\n'.join(result)
            
            cleaned = downgrade_headings(cleaned)
            md_lines.append(cleaned)
            md_lines.append("")
            
            # Đường kẻ ngang phân cách giữa các bài viết
            md_lines.append("---")
            md_lines.append("")
    
    # Ghi file Markdown
    md_content = '\n'.join(md_lines)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    # Thống kê
    file_size = os.path.getsize(output_file)
    line_count = md_content.count('\n') + 1
    
    print(f"\n✅ Chuyển đổi thành công!")
    print(f"📝 File output: {output_file}")
    print(f"📏 Số dòng: {line_count:,}")
    print(f"💾 Kích thước: {file_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    convert_json_to_markdown(INPUT_FILE, OUTPUT_FILE)
