import pandas as pd
import logging
import hashlib
import os

from log_part import log_info, log_error

HASH_FILE = "excel_hash.txt"

def compute_excel_hash(excel_path: str) -> str:
    """读取整个 Excel 内容并返回其哈希值"""
    try:
        df = pd.read_excel(excel_path, sheet_name=0)
        content_bytes = df.to_csv(index=False).encode("utf-8")
        return hashlib.md5(content_bytes).hexdigest()
    except Exception as e:
        log_error(f"❌ 计算哈希失败：{e}")
        raise

def load_previous_hash() -> str:
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r") as f:
            return f.read().strip()
    return ""

def save_hash(hash_str: str):
    with open(HASH_FILE, "w") as f:
        f.write(hash_str)

def convert_excel_main(excel_path: str, output_csv_path: str):
    try:
        current_hash = compute_excel_hash(excel_path)
        previous_hash = load_previous_hash()

        if current_hash == previous_hash:
            log_info("🟡 文件未变化，跳过转换步骤")
            return

        log_info(f"📥 Excel 有更新，重新读取并转换：{excel_path}")
        df = pd.read_excel(excel_path, sheet_name=0)
        df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
        save_hash(current_hash)
        log_info(f"✅ 转换成功，保存为：{output_csv_path}")

    except Exception as e:
        log_error(f"❌ Excel 读取或转换失败：{e}")
        raise
