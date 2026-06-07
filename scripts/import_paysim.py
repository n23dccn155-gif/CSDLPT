import pandas as pd
import numpy as np
from pathlib import Path

RAW_PATH = Path("../data/raw/PS_20174392719_1491204439457_log.csv")
OUT_TX_PATH = Path("../data/financial_transactions.csv")
OUT_META_PATH = Path("../data/account_metadata.csv")

# Dùng số dòng vừa phải để app chạy ổn khi demo.
MAX_ROWS = 50000

def main():
    global RAW_PATH, OUT_TX_PATH, OUT_META_PATH
    if not RAW_PATH.exists():
        # Fallback to local path if run from root
        RAW_PATH = Path("data/raw/PS_20174392719_1491204439457_log.csv")
        OUT_TX_PATH = Path("data/financial_transactions.csv")
        OUT_META_PATH = Path("data/account_metadata.csv")
        
        if not RAW_PATH.exists():
            raise FileNotFoundError(f"Không tìm thấy file PaySim gốc: {RAW_PATH}")

    print("Đang đọc PaySim dataset...")
    df = pd.read_csv(RAW_PATH)

    print("Tổng số dòng ban đầu:", len(df))

    # Lọc giao dịch TRANSFER vì phù hợp nhất với mô hình graph:
    df = df[df["type"] == "TRANSFER"].copy()

    print("Số dòng sau khi lọc type == TRANSFER:", len(df))

    # Lấy mẫu để demo ổn định, không dùng toàn bộ ngay.
    if len(df) > MAX_ROWS:
        df = df.sample(n=MAX_ROWS, random_state=42)

    df = df.reset_index(drop=True)

    # 1. TẠO MAPPING CHUYỂN STRING SANG INTEGER
    all_accounts = pd.concat([df["nameOrig"], df["nameDest"]]).unique()
    account_map = {acc: i + 1 for i, acc in enumerate(all_accounts)}

    print(f"Tổng số tài khoản unique: {len(all_accounts)}")

    # 2. FILE TRANSACTIONS THEO SCHEMA CŨ (Dùng ID số)
    out_tx = pd.DataFrame({
        "FromAccount": df["nameOrig"].map(account_map),
        "ToAccount": df["nameDest"].map(account_map),
        "Amount": df["amount"],
        "IsFraud": df["isFraud"],
    })

    OUT_TX_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_tx.to_csv(OUT_TX_PATH, index=False, encoding="utf-8")
    print(f"Đã ghi file giao dịch: {OUT_TX_PATH}")

    # 3. FILE METADATA CHO MULTI-MODEL INTEGRATION
    np.random.seed(42)
    countries = ["Vietnam", "Singapore", "Thailand", "USA", "UK", "Japan"]
    
    out_meta = pd.DataFrame({
        "AccountID": list(account_map.values()),
        "OriginalAccount": list(account_map.keys()),
        "AccountType": ["Personal" if acc.startswith('C') else "Business" for acc in account_map.keys()],
        "Country": np.random.choice(countries, len(account_map)),
        "RiskScore": np.round(np.random.uniform(0.1, 0.9, len(account_map)), 2)
    })
    
    out_meta.to_csv(OUT_META_PATH, index=False, encoding="utf-8")
    print(f"Đã ghi file metadata: {OUT_META_PATH}")

if __name__ == "__main__":
    main()