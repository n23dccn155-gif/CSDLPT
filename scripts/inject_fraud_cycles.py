import sys
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')


INPUT_PATH = Path("data/financial_transactions.csv")
MAPPING_PATH = Path("data/account_mapping.csv")

OLD_INJECTED_ACCOUNTS = {
    900000,
    900001,
    900002,
    900003,
    900006,
    900009,
    900010,
    900011,
    900012,
    900013,
}


def build_cycle(accounts, amount):
    """
    Tạo một chu kỳ kiểm thử vòng lặp gian lận có kiểm soát A -> B -> C -> D -> A.

    IsFraud vẫn giữ giá trị 1 để tương thích với định dạng nhãn của PaySim.
    IsCycleFraud=1 là cờ đánh dấu ứng viên thực tế được sử dụng bởi thuật toán DFS phân tán.
    """
    rows = []
    for i, from_acc in enumerate(accounts):
        to_acc = accounts[(i + 1) % len(accounts)]
        rows.append({
            "FromAccount": from_acc,
            "ToAccount": to_acc,
            "Amount": amount,
            "IsFraud": 1,
            "IsCycleFraud": 1,
        })
    return rows


def choose_cycle_accounts(account_ids, residues, min_first, used):
    """
    Chọn các giá trị AccountID hiện có của PaySim có dạng chia dư (modulo) khớp với residues.

    Các ID được chọn tăng dần nghiêm ngặt để tài khoản đầu tiên là ID nhỏ nhất
    trong chu kỳ. Điều này tuân thủ quy tắc ID nhỏ nhất (Minimum-ID) trong node.py.
    """
    first_candidates = [
        acc
        for acc in account_ids
        if acc >= min_first and acc % 3 == residues[0] and acc not in used
    ]

    for first in first_candidates:
        cycle = [first]
        local_used = {first}
        previous = first

        for residue in residues[1:]:
            next_candidates = [
                acc
                for acc in account_ids
                if acc > previous
                and acc % 3 == residue
                and acc not in used
                and acc not in local_used
            ]
            if not next_candidates:
                break
            chosen = next_candidates[0]
            cycle.append(chosen)
            local_used.add(chosen)
            previous = chosen

        if len(cycle) == len(residues):
            used.update(cycle)
            return cycle

    raise ValueError(f"Cannot choose existing PaySim accounts for residues {residues}")


def clean_previous_injections(df, controlled_edges):
    """Xóa các cạnh kiểm thử đã được chèn trước đó trước khi thêm các cạnh hiện tại."""
    is_cycle_fraud = df["IsCycleFraud"].fillna(0).astype(int) == 1
    from_old_fake_accounts = (
        df["FromAccount"].isin(OLD_INJECTED_ACCOUNTS)
        & df["ToAccount"].isin(OLD_INJECTED_ACCOUNTS)
    )
    duplicates_current_edges = df.apply(
        lambda row: (int(row["FromAccount"]), int(row["ToAccount"])) in controlled_edges,
        axis=1,
    )
    return df[~(is_cycle_fraud | from_old_fake_accounts | duplicates_current_edges)]


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")
    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Mapping file not found: {MAPPING_PATH}")

    df = pd.read_csv(INPUT_PATH)
    mapping_df = pd.read_csv(MAPPING_PATH)

    if "IsCycleFraud" not in df.columns:
        df["IsCycleFraud"] = 0

    account_ids = sorted(int(acc) for acc in mapping_df["AccountID"].dropna().astype(int).unique())
    used = set()

    # Hai chu kỳ xuyên phân mảnh (cross-shard) và một chu kỳ cục bộ (local), đều sử dụng các ID PaySim hiện có.
    cycle1 = choose_cycle_accounts(account_ids, [1, 2, 0, 1], 100, used)
    cycle2 = choose_cycle_accounts(account_ids, [2, 0, 1, 2], cycle1[-1] + 1, used)
    cycle3 = choose_cycle_accounts(account_ids, [0, 0, 0, 0], cycle2[-1] + 1, used)

    injected_rows = []
    injected_rows += build_cycle(accounts=cycle1, amount=900000.0)
    injected_rows += build_cycle(accounts=cycle2, amount=1500000.0)
    injected_rows += build_cycle(accounts=cycle3, amount=2500000.0)

    controlled_edges = {
        (row["FromAccount"], row["ToAccount"])
        for row in injected_rows
    }

    df_clean = clean_previous_injections(df, controlled_edges)
    result = pd.concat([df_clean, pd.DataFrame(injected_rows)], ignore_index=True)
    result.to_csv(INPUT_PATH, index=False, encoding="utf-8")

    # Xóa các dòng ánh xạ giả kế thừa từ các phiên bản trước của kịch bản.
    original_account = mapping_df.get("OriginalAccount")
    if original_account is not None:
        legacy_mapping = original_account.fillna("").astype(str).str.startswith("INJECT_")
    else:
        legacy_mapping = pd.Series(False, index=mapping_df.index)
    mapping_clean = mapping_df[
        ~(
            mapping_df["AccountID"].isin(OLD_INJECTED_ACCOUNTS)
            | legacy_mapping
        )
    ]
    mapping_clean.to_csv(MAPPING_PATH, index=False, encoding="utf-8")

    print(f"Đã ghi {len(result)} giao dịch với {len(injected_rows)} cạnh chu kỳ được kiểm soát.")
    print("Các chu kỳ kiểm thử vòng lặp gian lận có kiểm soát sử dụng các giá trị AccountID PaySim hiện có:")
    print(f"  Chu kỳ 1 (Xuyên phân mảnh - Cross-shard): {cycle1} | số tiền=900,000")
    print(f"  Chu kỳ 2 (Xuyên phân mảnh - Cross-shard): {cycle2} | số tiền=1,500,000")
    print(f"  Chu kỳ 3 (Cục bộ/Phân mảnh 0 - Local/Shard0): {cycle3} | số tiền=2,500,000")


if __name__ == "__main__":
    main()
