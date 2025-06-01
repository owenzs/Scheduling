import pandas as pd

input_file = "preview_first_sheet.csv"
output_file = "sorted_preview.csv"
time_column = "最早要货时间"

try:
    # 读取 CSV
    df = pd.read_csv(input_file)

    # 检查列是否存在
    if time_column not in df.columns:
        raise ValueError(f"❌ 文件中找不到列：{time_column}")

    # 转换为 datetime.time 类型（只有时间部分）
    df[time_column] = pd.to_datetime(df[time_column], format="%H:%M:%S.%f", errors="coerce").dt.time

    # 按时间排序（升序）
    df_sorted = df.sort_values(by=time_column)

    # 保存排序后的文件
    df_sorted.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"✅ 排序完成，已保存为 {output_file}")

except Exception as e:
    print(f"❌ 发生错误：{e}")

#1. 出现null 报错，让用户澄清
#2. 数据库连接层，product code连接2表，然后把parent code拿出来
#3.