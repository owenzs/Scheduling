import pandas as pd
from log_part import log_info, log_error

def schedule_production(
    input_excel_path: str,
    output_excel_path: str,
    batch_summary_path: str
):
    """
    贪心调度：每轮选择能覆盖最多 (销售订单号, 物料编码) 的 parent_product_code。
    同一销售订单里的同一物料被任一 parent 覆盖后即视为满足，不再重复计算。
    输出时除 "生产批次" 外，再增加一列 "生效父产品编码"，标记到底是谁满足了该行。
    """
    try:
        # ① 读取 & 字段统一
        log_info("📥 正在读取订单数据...")
        df = pd.read_excel(input_excel_path)
        df = df.rename(columns={
            "销售订单号": "sales_order_id",
            "物料编码": "product_id",
            "最早要货时间": "delivery_time",
            "parent_product_code": "parent_code"
        })

        # ② 时间排序
        df["delivery_time"] = pd.to_datetime(df["delivery_time"], errors="coerce")
        df = df.sort_values("delivery_time").reset_index(drop=True)

        # ③ 初始化
        df["生产批次"] = None
        df["生效父产品编码"] = None  # 新列：记录是哪一个 parent 覆盖
        unfulfilled_items = set(
            df[["sales_order_id", "product_id"]].itertuples(index=False, name=None)
        )
        batch_id = 1
        results = []

        # ④ 贪心循环
        while unfulfilled_items:
            mask_unfulfilled = df.apply(
                lambda r: (r["sales_order_id"], r["product_id"]) in unfulfilled_items,
                axis=1
            )
            candidates = df[mask_unfulfilled]

            parent_to_items = (
                candidates[["sales_order_id", "product_id", "parent_code"]]
                .drop_duplicates()
                .groupby("parent_code")
                .size()
                .sort_values(ascending=False)
            )

            if parent_to_items.empty or parent_to_items.iat[0] == 0:
                break

            best_parent = parent_to_items.index[0]

            covered_items = set(
                candidates[candidates["parent_code"] == best_parent][["sales_order_id", "product_id"]]
                .drop_duplicates()
                .itertuples(index=False, name=None)
            )

            # 标记批次 & 生效父产品编码
            df.loc[df.apply(
                lambda r: (r["sales_order_id"], r["product_id"]) in covered_items, axis=1
            ), ["生产批次", "生效父产品编码"]] = [batch_id, best_parent]

            results.append({
                "批次号": batch_id,
                "parent_product_code": best_parent,
                "满足订单数": len({o for o, _ in covered_items})
            })

            unfulfilled_items -= covered_items
            batch_id += 1

        # ⑤ 输出
        df.rename(columns={
            "sales_order_id": "销售订单号",
            "product_id": "物料编码",
            "delivery_time": "最早要货时间",
            "parent_code": "parent_product_code",
            "生效父产品编码": "生效parent_product_code"
        }, inplace=True)

        df.to_excel(output_excel_path, index=False)
        pd.DataFrame(results).to_excel(batch_summary_path, index=False)

        log_info(f"📤 已生成订单文件：{output_excel_path}")
        log_info(f"📄 已生成批次汇总：{batch_summary_path}")

    except Exception as e:
        log_error(f"❌ 调度过程中出错：{e}")
        raise
