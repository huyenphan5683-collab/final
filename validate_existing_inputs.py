from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

ROOT = Path(os.environ.get("SERVICE_DATA_ROOT", str(Path(__file__).resolve().parents[1])))
BASE = ROOT / "data"
OUTPUT = ROOT / "output"

FILES = {
    "actual_combined": BASE / "sla_actual_performance_2023_2025_combined.xlsx",
    "issue_2024": BASE / "sla_issue_handling_2024_filled.xlsx",
    "issue_2025": BASE / "sla_issue_handling_2025_filled.xlsx",
}


def safe_read(path: Path, sheet: str, usecols=None, nrows=None) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet, usecols=usecols, nrows=nrows)


def rate(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(series.mean(), 4)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report_rows = []

    for key, path in FILES.items():
        if not path.exists():
            report_rows.append({"check": key, "status": "FAIL", "details": f"Missing file: {path.name}"})
        else:
            report_rows.append({"check": key, "status": "PASS", "details": f"Found file: {path.name}"})

    if not all(path.exists() for path in FILES.values()):
        pd.DataFrame(report_rows).to_csv(OUTPUT / "validation_report.csv", index=False)
        print("Missing one or more input files. See output/validation_report.csv")
        return

    actual = pd.read_excel(FILES["actual_combined"], sheet_name="ticket_raw")
    issue_2024 = pd.read_excel(FILES["issue_2024"], sheet_name="Tickets_Raw")
    issue_2025 = pd.read_excel(FILES["issue_2025"], sheet_name="Tickets_Raw")

    # Schema checks
    actual_required = {"ticket_id", "year", "month", "severity", "actual_response_min", "actual_resolution_min", "response_sla_met", "resolution_sla_met", "escalated", "reopened_count"}
    issue_required = {"ticket_id", "created_at", "severity", "priority", "category", "assigned_team", "first_response_at", "resolved_at", "reopened_count", "escalated"}

    report_rows.append({
        "check": "actual_schema",
        "status": "PASS" if actual_required.issubset(actual.columns) else "FAIL",
        "details": f"Missing: {sorted(actual_required - set(actual.columns))}",
    })
    report_rows.append({
        "check": "issue_2024_schema",
        "status": "PASS" if issue_required.issubset(issue_2024.columns) else "FAIL",
        "details": f"Missing: {sorted(issue_required - set(issue_2024.columns))}",
    })
    report_rows.append({
        "check": "issue_2025_schema",
        "status": "PASS" if issue_required.issubset(issue_2025.columns) else "FAIL",
        "details": f"Missing: {sorted(issue_required - set(issue_2025.columns))}",
    })

    # Duplicate checks
    for name, df in [("actual", actual), ("issue_2024", issue_2024), ("issue_2025", issue_2025)]:
        dupes = int(df["ticket_id"].duplicated().sum())
        report_rows.append({"check": f"{name}_duplicate_ticket_id", "status": "PASS" if dupes == 0 else "WARN", "details": f"duplicate_ticket_id={dupes}"})

    # Volume comparison by year
    actual_by_year = actual.groupby("year")["ticket_id"].count().to_dict()
    issue_counts = {2024: len(issue_2024), 2025: len(issue_2025)}
    for year in [2024, 2025]:
        actual_count = int(actual_by_year.get(year, 0))
        issue_count = int(issue_counts[year])
        diff = issue_count - actual_count
        pct = 0.0 if actual_count == 0 else round(diff / actual_count, 4)
        status = "PASS" if abs(pct) <= 0.05 else "WARN"
        report_rows.append({
            "check": f"volume_compare_{year}",
            "status": status,
            "details": json.dumps({"actual_count": actual_count, "issue_count": issue_count, "difference": diff, "difference_pct": pct}),
        })

    # Severity distribution comparison
    for year, issue_df in [(2024, issue_2024), (2025, issue_2025)]:
        actual_dist = actual.loc[actual["year"] == year, "severity"].value_counts(normalize=True).round(4).to_dict()
        issue_dist = issue_df["severity"].value_counts(normalize=True).round(4).to_dict()
        all_keys = sorted(set(actual_dist) | set(issue_dist))
        max_gap = max(abs(actual_dist.get(k, 0) - issue_dist.get(k, 0)) for k in all_keys) if all_keys else 0.0
        report_rows.append({
            "check": f"severity_distribution_{year}",
            "status": "PASS" if max_gap <= 0.08 else "WARN",
            "details": json.dumps({"max_gap": round(max_gap, 4), "actual": actual_dist, "issue": issue_dist}),
        })

    # Coherence checks inside issue files
    for year, issue_df in [(2024, issue_2024), (2025, issue_2025)]:
        temp = issue_df.copy()
        temp["created_at"] = pd.to_datetime(temp["created_at"], errors="coerce")
        temp["first_response_at"] = pd.to_datetime(temp["first_response_at"], errors="coerce")
        temp["resolved_at"] = pd.to_datetime(temp["resolved_at"], errors="coerce")
        bad_order = int(((temp["first_response_at"] < temp["created_at"]) | (temp["resolved_at"] < temp["first_response_at"])).fillna(False).sum())
        report_rows.append({"check": f"timestamp_order_{year}", "status": "PASS" if bad_order == 0 else "WARN", "details": f"bad_order_rows={bad_order}"})

    # KPI comparison
    for year, issue_df in [(2024, issue_2024), (2025, issue_2025)]:
        issue_temp = issue_df.copy()
        issue_temp["created_at"] = pd.to_datetime(issue_temp["created_at"], errors="coerce")
        issue_temp["first_response_at"] = pd.to_datetime(issue_temp["first_response_at"], errors="coerce")
        issue_temp["resolved_at"] = pd.to_datetime(issue_temp["resolved_at"], errors="coerce")
        issue_temp["response_min"] = (issue_temp["first_response_at"] - issue_temp["created_at"]).dt.total_seconds() / 60
        issue_temp["resolution_min"] = (issue_temp["resolved_at"] - issue_temp["created_at"]).dt.total_seconds() / 60
        actual_y = actual[actual["year"] == year].copy()
        metrics = {
            "avg_response_min_gap": round(issue_temp["response_min"].mean() - actual_y["actual_response_min"].mean(), 2),
            "avg_resolution_min_gap": round(issue_temp["resolution_min"].mean() - actual_y["actual_resolution_min"].mean(), 2),
            "escalation_rate_gap": round(rate(issue_temp["escalated"].astype(bool)) - rate(actual_y["escalated"].astype(bool)), 4),
            "reopen_rate_gap": round(rate((issue_temp["reopened_count"] > 0).astype(int)) - rate((actual_y["reopened_count"] > 0).astype(int)), 4),
        }
        max_material_gap = max(abs(metrics["avg_response_min_gap"]), abs(metrics["avg_resolution_min_gap"]))
        status = "PASS" if max_material_gap <= 180 else "WARN"
        report_rows.append({"check": f"kpi_gap_{year}", "status": status, "details": json.dumps(metrics)})

    report = pd.DataFrame(report_rows)
    report.to_csv(OUTPUT / "validation_report.csv", index=False)
    with pd.ExcelWriter(OUTPUT / "validation_report.xlsx", engine="openpyxl") as writer:
        report.to_excel(writer, sheet_name="Validation_Report", index=False)

    print("Created output/validation_report.csv and output/validation_report.xlsx")


if __name__ == "__main__":
    main()
