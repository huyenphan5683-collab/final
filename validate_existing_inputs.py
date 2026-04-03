from __future__ import annotations

from pathlib import Path
import json
import os

import pandas as pd


ROOT = Path(os.environ.get("SERVICE_DATA_ROOT", str(Path(__file__).resolve().parent)))
BASE = ROOT / "data"
OUTPUT = ROOT / "output"

FILES = {
    "actual_combined": BASE / "sla_actual_performance_2023_2025_combined.xlsx",
    "issue_2023": BASE / "sla_issue_handling_2023_filled.xlsx",
    "issue_2024": BASE / "sla_issue_handling_2024_filled.xlsx",
    "issue_2025": BASE / "sla_issue_handling_2025_filled.xlsx",
}


def safe_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)


def rate(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(float(series.mean()), 4)


def to_bool_series(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True,
        "false": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
        "1": True,
        "0": False,
        "nan": False,
        "none": False,
        "": False,
    }
    s = series.astype(str).str.strip().str.lower().replace(mapping)
    return s.astype(bool)


def find_sheet_name(path: Path, candidates: list[str]) -> str:
    xls = pd.ExcelFile(path)
    available = list(xls.sheet_names)
    lower_map = {name.lower(): name for name in available}

    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]

    raise ValueError(f"Không tìm thấy sheet phù hợp. Available sheets: {available}")


def read_actual_file(path: Path) -> pd.DataFrame:
    sheet = find_sheet_name(path, ["ticket_raw", "Ticket_Raw", "Tickets_Raw", "tickets_raw"])
    return pd.read_excel(path, sheet_name=sheet)


def read_issue_file(path: Path) -> pd.DataFrame:
    sheet = find_sheet_name(path, ["Tickets_Raw", "Ticket_Raw", "ticket_raw", "Tickets_Clean", "tickets_clean"])
    return pd.read_excel(path, sheet_name=sheet)


def write_report(report_rows: list[dict]) -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report = pd.DataFrame(report_rows)
    report.to_csv(OUTPUT / "validation_report.csv", index=False)

    with pd.ExcelWriter(OUTPUT / "validation_report.xlsx", engine="openpyxl") as writer:
        report.to_excel(writer, sheet_name="Validation_Report", index=False)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report_rows: list[dict] = []

    # 1. Check file existence
    all_files_ok = True
    for key, path in FILES.items():
        if path.exists():
            report_rows.append(
                {"check": key, "status": "PASS", "details": f"Found file: {path.name}"}
            )
        else:
            all_files_ok = False
            report_rows.append(
                {"check": key, "status": "FAIL", "details": f"Missing file: {path.name}"}
            )

    if not all_files_ok:
        write_report(report_rows)
        print("Thiếu file đầu vào. Xem output/validation_report.csv")
        return

    # 2. Read files
    actual = None
    issue_map: dict[int, pd.DataFrame] = {}

    try:
        actual = read_actual_file(FILES["actual_combined"])
        report_rows.append(
            {"check": "read_actual_file", "status": "PASS", "details": "Read actual combined file successfully"}
        )
    except Exception as e:
        report_rows.append(
            {"check": "read_actual_file", "status": "FAIL", "details": str(e)}
        )

    for year in [2023, 2024, 2025]:
        try:
            df = read_issue_file(FILES[f"issue_{year}"])
            issue_map[year] = df
            report_rows.append(
                {"check": f"read_issue_{year}", "status": "PASS", "details": "Read issue file successfully"}
            )
        except Exception as e:
            report_rows.append(
                {"check": f"read_issue_{year}", "status": "FAIL", "details": str(e)}
            )

    if actual is None or len(issue_map) < 3:
        write_report(report_rows)
        print("Không đọc được một hoặc nhiều file/sheet. Xem output/validation_report.csv")
        return

    # 3. Schema checks
    actual_required = {
        "ticket_id",
        "year",
        "month",
        "severity",
        "actual_response_min",
        "actual_resolution_min",
        "response_sla_met",
        "resolution_sla_met",
        "escalated",
        "reopened_count",
    }

    issue_required = {
        "ticket_id",
        "created_at",
        "severity",
        "priority",
        "category",
        "assigned_team",
        "first_response_at",
        "resolved_at",
        "reopened_count",
        "escalated",
    }

    missing_actual = sorted(actual_required - set(actual.columns))
    report_rows.append(
        {
            "check": "actual_schema",
            "status": "PASS" if not missing_actual else "FAIL",
            "details": f"Missing: {missing_actual}",
        }
    )

    for year, issue_df in issue_map.items():
        missing_issue = sorted(issue_required - set(issue_df.columns))
        report_rows.append(
            {
                "check": f"issue_{year}_schema",
                "status": "PASS" if not missing_issue else "FAIL",
                "details": f"Missing: {missing_issue}",
            }
        )

    # 4. Duplicate checks
    if "ticket_id" in actual.columns:
        dupes = int(actual["ticket_id"].duplicated().sum())
        report_rows.append(
            {
                "check": "actual_duplicate_ticket_id",
                "status": "PASS" if dupes == 0 else "WARN",
                "details": f"duplicate_ticket_id={dupes}",
            }
        )

    for year, issue_df in issue_map.items():
        if "ticket_id" in issue_df.columns:
            dupes = int(issue_df["ticket_id"].duplicated().sum())
            report_rows.append(
                {
                    "check": f"issue_{year}_duplicate_ticket_id",
                    "status": "PASS" if dupes == 0 else "WARN",
                    "details": f"duplicate_ticket_id={dupes}",
                }
            )

    # 5. Volume comparison by year
    if {"year", "ticket_id"}.issubset(actual.columns):
        actual_by_year = actual.groupby("year")["ticket_id"].count().to_dict()

        for year in [2023, 2024, 2025]:
            issue_df = issue_map[year]
            actual_count = int(actual_by_year.get(year, 0))
            issue_count = int(len(issue_df))
            diff = issue_count - actual_count
            pct = 0.0 if actual_count == 0 else round(diff / actual_count, 4)
            status = "PASS" if abs(pct) <= 0.05 else "WARN"

            report_rows.append(
                {
                    "check": f"volume_compare_{year}",
                    "status": status,
                    "details": safe_json(
                        {
                            "actual_count": actual_count,
                            "issue_count": issue_count,
                            "difference": diff,
                            "difference_pct": pct,
                        }
                    ),
                }
            )

    # 6. Severity distribution
    if {"year", "severity"}.issubset(actual.columns):
        for year in [2023, 2024, 2025]:
            issue_df = issue_map[year]
            if "severity" not in issue_df.columns:
                report_rows.append(
                    {
                        "check": f"severity_distribution_{year}",
                        "status": "FAIL",
                        "details": "Missing severity column in issue file",
                    }
                )
                continue

            actual_dist = (
                actual.loc[actual["year"] == year, "severity"]
                .astype(str)
                .value_counts(normalize=True)
                .round(4)
                .to_dict()
            )
            issue_dist = (
                issue_df["severity"]
                .astype(str)
                .value_counts(normalize=True)
                .round(4)
                .to_dict()
            )

            all_keys = sorted(set(actual_dist) | set(issue_dist))
            max_gap = max(abs(actual_dist.get(k, 0) - issue_dist.get(k, 0)) for k in all_keys) if all_keys else 0.0

            report_rows.append(
                {
                    "check": f"severity_distribution_{year}",
                    "status": "PASS" if max_gap <= 0.08 else "WARN",
                    "details": safe_json(
                        {
                            "max_gap": round(max_gap, 4),
                            "actual": actual_dist,
                            "issue": issue_dist,
                        }
                    ),
                }
            )

    # 7. Timestamp order checks
    for year in [2023, 2024, 2025]:
        issue_df = issue_map[year].copy()

        for c in ["created_at", "first_response_at", "resolved_at"]:
            if c in issue_df.columns:
                issue_df[c] = pd.to_datetime(issue_df[c], errors="coerce")

        if {"created_at", "first_response_at", "resolved_at"}.issubset(issue_df.columns):
            bad_first_response = (issue_df["first_response_at"] < issue_df["created_at"]).fillna(False)
            bad_resolved = (issue_df["resolved_at"] < issue_df["first_response_at"]).fillna(False)
            bad_order = int((bad_first_response | bad_resolved).sum())

            report_rows.append(
                {
                    "check": f"timestamp_order_{year}",
                    "status": "PASS" if bad_order == 0 else "WARN",
                    "details": f"bad_order_rows={bad_order}",
                }
            )

    # 8. KPI gap checks
    required_actual = {"year", "actual_response_min", "actual_resolution_min", "escalated", "reopened_count"}
    if required_actual.issubset(actual.columns):
        for year in [2023, 2024, 2025]:
            temp = issue_map[year].copy()

            for c in ["created_at", "first_response_at", "resolved_at"]:
                if c in temp.columns:
                    temp[c] = pd.to_datetime(temp[c], errors="coerce")

            temp["response_min"] = (temp["first_response_at"] - temp["created_at"]).dt.total_seconds() / 60
            temp["resolution_min"] = (temp["resolved_at"] - temp["created_at"]).dt.total_seconds() / 60

            actual_y = actual[actual["year"] == year].copy()
            if len(actual_y) == 0:
                report_rows.append(
                    {
                        "check": f"kpi_gap_{year}",
                        "status": "FAIL",
                        "details": "No actual records found for this year",
                    }
                )
                continue

            issue_escalation_rate = rate(to_bool_series(temp["escalated"]).astype(int))
            actual_escalation_rate = rate(to_bool_series(actual_y["escalated"]).astype(int))
            issue_reopen_rate = rate((pd.to_numeric(temp["reopened_count"], errors="coerce").fillna(0) > 0).astype(int))
            actual_reopen_rate = rate((pd.to_numeric(actual_y["reopened_count"], errors="coerce").fillna(0) > 0).astype(int))

            metrics = {
                "avg_response_min_issue": round(float(temp["response_min"].mean()), 2) if len(temp) else None,
                "avg_response_min_actual": round(float(actual_y["actual_response_min"].mean()), 2) if len(actual_y) else None,
                "avg_response_min_gap": round(float(temp["response_min"].mean() - actual_y["actual_response_min"].mean()), 2),
                "avg_resolution_min_issue": round(float(temp["resolution_min"].mean()), 2) if len(temp) else None,
                "avg_resolution_min_actual": round(float(actual_y["actual_resolution_min"].mean()), 2) if len(actual_y) else None,
                "avg_resolution_min_gap": round(float(temp["resolution_min"].mean() - actual_y["actual_resolution_min"].mean()), 2),
                "issue_escalation_rate": issue_escalation_rate,
                "actual_escalation_rate": actual_escalation_rate,
                "escalation_rate_gap": round(issue_escalation_rate - actual_escalation_rate, 4),
                "issue_reopen_rate": issue_reopen_rate,
                "actual_reopen_rate": actual_reopen_rate,
                "reopen_rate_gap": round(issue_reopen_rate - actual_reopen_rate, 4),
            }

            max_material_gap = max(
                abs(metrics["avg_response_min_gap"]),
                abs(metrics["avg_resolution_min_gap"]),
            )

            report_rows.append(
                {
                    "check": f"kpi_gap_{year}",
                    "status": "PASS" if max_material_gap <= 180 else "WARN",
                    "details": safe_json(metrics),
                }
            )

    write_report(report_rows)
    print("Created output/validation_report.csv and output/validation_report.xlsx")


if __name__ == "__main__":
    main()
