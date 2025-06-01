import logging
#from convert_excel import convert_excel_main
from enrich_with_parent import enrich_with_parent
from greedy import schedule_production
from sorted_excel import sort_excel_by_time
import os
from log_part import log_error, log_info

def setup_logger():
    logging.basicConfig(
        filename="logs/run.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        encoding="utf-8"
    )
    log_info("🎯 日志系统启动")

def main():
    os.makedirs("logs", exist_ok=True)

    input_excel_path = "test_data.xlsx" #输入
    intermediate_csv_path = "preview_first_sheet.csv" #转换
    sorted_output_path = "sorted_preview.csv"

    setup_logger()
    log_info("step1：转换csv")
    #convert_excel_main(input_excel_path, intermediate_csv_path)
    log_info("step2：按最早要货时间排序")
    sort_excel_by_time(intermediate_csv_path, sorted_output_path)
    log_info("step3：依据BOM图找出父子节点")
    enrich_with_parent(
        sorted_csv_path=sorted_output_path,
        product_id_path="ProductID.csv",
        product_parent_path="ProductParentID.csv",
        output_excel_path="final_excel.xlsx"
    )
    log_info("step4：找出能够满足订单的最大通路，然后进行生产")
    schedule_production(
        input_excel_path="final_excel.xlsx",
        output_excel_path="scheduled_orders.xlsx",
        batch_summary_path="batch_summary.xlsx"
    )

    log_info("✅ 所有处理完成")

if __name__ == "__main__":
    main()
