import pandas as pd
import logging

from log_part import log_error, log_info


def sort_excel_by_time(input_file: str, output_file: str):
    time_column = "最早要货时间"

    try:
        logging.info(f"📊 正在排序 CSV：{input_file}")
        df = pd.read_csv(input_file)

        if time_column not in df.columns:
            raise ValueError(f"列不存在：{time_column}")

        df[time_column] = pd.to_datetime(df[time_column], format="%H:%M:%S.%f", errors="coerce").dt.time
        df = df.dropna(subset=[time_column])
        df = df.drop_duplicates(subset=[time_column], keep="first")
        df_sorted = df.sort_values(by=time_column)

        df_sorted.to_csv(output_file, index=False, encoding="utf-8-sig")
        log_info(f"✅ 排序并去重完成，输出文件：{output_file}")
    except Exception as e:
        log_error(f"❌ 排序失败：{e}")
        raise
