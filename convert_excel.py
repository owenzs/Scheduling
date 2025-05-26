import pandas as pd

excel_path = "test_data.xlsx"
output_csv_path = "preview_first_sheet.csv"
preview_rows = 100  # 可自定义你要提取多少行

try:
    print(f"正在从 {excel_path} 中提取第一个 sheet 前 {preview_rows} 行...")

    # 只读取第一个 sheet 的前 N 行
    df = pd.read_excel(excel_path, sheet_name=0, nrows=preview_rows)

    # 保存为 CSV 文件
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ 提取成功，已保存到 {output_csv_path}，你可以用 Excel 或文本编辑器打开查看。")

except Exception as e:
    print(f"❌ 出现错误：{e}")
