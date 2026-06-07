import pandas as pd
from pathlib import Path
import json

INPUT_PATH = Path("data/financial_transactions.csv")
META_PATH = Path("data/account_metadata.csv")

def build_cycle(accounts, amount):
    """
    Tạo một chu trình A -> B -> C -> D -> A.
    accounts: danh sách 4 account ID (số nguyên).
    """
    rows = []
    for i in range(len(accounts)):
        from_acc = accounts[i]
        to_acc = accounts[(i + 1) % len(accounts)]

        rows.append({
            "FromAccount": from_acc,
            "ToAccount": to_acc,
            "Amount": amount,
            "IsFraud": 1,
        })

    return rows

def main():
    global INPUT_PATH, META_PATH
    if not INPUT_PATH.exists():
        # Fallback path if run from root
        INPUT_PATH = Path("data/financial_transactions.csv")
        OUTPUT_PATH = Path("data/financial_transactions_paysim_with_cycles.csv")
        META_PATH = Path("data/account_metadata.csv")
        
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Không tìm thấy file input: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH)
    
    # Đọc metadata để thêm metadata cho các ID mới
    meta_df = pd.DataFrame()
    if META_PATH.exists():
        meta_df = pd.read_csv(META_PATH)

    injected_rows = []
    new_meta_rows = []

    # Cycle 1: 900000 -> 900001 -> 900002 -> 900003
    # ID liên tiếp sẽ dễ rơi vào các shard khác nhau (Cross-shard)
    cycle1 = [900000, 900001, 900002, 900003]
    injected_rows += build_cycle(accounts=cycle1, amount=900000.0)
    for acc in cycle1:
        new_meta_rows.append({"AccountID": acc, "OriginalAccount": f"INJECT_C1_{acc}", "AccountType": "Business", "Country": "Vietnam", "RiskScore": 0.95})

    # Cycle 2: 900010 -> 900011 -> 900012 -> 900013
    cycle2 = [900010, 900011, 900012, 900013]
    injected_rows += build_cycle(accounts=cycle2, amount=1500000.0)
    for acc in cycle2:
        new_meta_rows.append({"AccountID": acc, "OriginalAccount": f"INJECT_C2_{acc}", "AccountType": "Personal", "Country": "Singapore", "RiskScore": 0.88})

    # Cycle 3: Local cycle (All IDs map to shard 0 if num_partitions=3)
    # Ví dụ: 900000 % 3 = 0, 900003 % 3 = 0, 900006 % 3 = 0, 900009 % 3 = 0
    cycle3 = [900000, 900003, 900006, 900009]
    injected_rows += build_cycle(accounts=cycle3, amount=2500000.0)
    for acc in cycle3:
        if acc not in [900000, 900003]: # Prevent duplicate in meta
            new_meta_rows.append({"AccountID": acc, "OriginalAccount": f"INJECT_C3_{acc}", "AccountType": "Business", "Country": "Thailand", "RiskScore": 0.91})

    injected_df = pd.DataFrame(injected_rows)
    result = pd.concat([df, injected_df], ignore_index=True)
    
    # Ghi đè vào data/financial_transactions.csv để tiện partition
    result.to_csv(INPUT_PATH, index=False, encoding="utf-8")
    print(f"Đã ghi file giao dịch có cấy fraud vào: {INPUT_PATH}")
    
    # Cập nhật meta
    if len(meta_df) > 0:
        meta_result = pd.concat([meta_df, pd.DataFrame(new_meta_rows)], ignore_index=True)
        # Bỏ trùng lặp AccountID
        meta_result = meta_result.drop_duplicates(subset=["AccountID"])
        meta_result.to_csv(META_PATH, index=False, encoding="utf-8")
        print(f"Đã cập nhật metadata tại: {META_PATH}")

if __name__ == "__main__":
    main()