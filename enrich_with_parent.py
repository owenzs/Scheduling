import pandas as pd
import logging

from log_part import log_error, log_info


def enrich_with_parent(
    sorted_csv_path: str,
    product_id_path: str,
    product_parent_path: str,
    output_excel_path: str
):
    try:
        log_info("📥 开始读取原始排序数据")
        df_sorted = pd.read_csv(sorted_csv_path)

        log_info("📦 加载 ProductID 映射表")
        df_pid = pd.read_csv(product_id_path)

        log_info("🔗 加载 ProductParentID 映射表")
        df_ppid = pd.read_csv(product_parent_path)

        if "物料编码" not in df_sorted.columns:
            raise ValueError("❌ sorted_preview.csv 中找不到 '物料编码' 字段")

        log_info("🔁 将物料编码、ID、Code 字段统一为字符串")
        df_sorted["物料编码"] = df_sorted["物料编码"].astype(str)
        df_pid["id"] = df_pid["id"].astype(str)

        # 第一步：物料编码 ➜ code（通过 ProductID 映射）
        df_sorted = df_sorted.merge(
            df_pid.rename(columns={"id": "物料编码"}), on="物料编码", how="left"
        )

        log_info("🔁 将 code 和 product_code 字段统一为字符串")
        df_sorted["code"] = df_sorted["code"].astype(str)
        df_ppid["product_code"] = df_ppid["product_code"].astype(str)

        # 第二步：code ➜ parent_product_code（通过 ProductParentID 映射）
        df_sorted = df_sorted.merge(
            df_ppid.rename(columns={"product_code": "code"}), on="code", how="left"
        )

        # ✅ 标记是否成功匹配到父产品
        df_sorted["是否找到父产品"] = df_sorted["parent_product_code"].apply(
            lambda x: "是" if pd.notnull(x) else "否"
        )

        # 📊 输出未匹配信息
        total = len(df_sorted)
        unmatched = (df_sorted["是否找到父产品"] == "否").sum()
        unmatched_percent = unmatched / total * 100

        log_info(f"📊 未匹配 parent_product_code 的物料编码有 {unmatched} 条，占比 {unmatched_percent:.2f}%")
        print(f"📊 未匹配 parent_product_code 的物料编码有 {unmatched} 条，占比 {unmatched_percent:.2f}%")

        # 💾 输出最终 Excel 文件
        log_info("✅ 父产品编码合并完成，开始输出 Excel")
        df_sorted.to_excel(output_excel_path, index=False)
        log_info(f"📤 输出成功：{output_excel_path}")
        print(f"📤 输出成功：{output_excel_path}")

    except Exception as e:
        log_error(f"❌ enrich_with_parent 处理失败：{e}")
        print(f"❌ enrich_with_parent 处理失败：{e}")
        raise
