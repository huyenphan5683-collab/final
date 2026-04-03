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


def rate(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(float(series.mean()), 4)


def to_bool_series(series: pd.Series) -> pd.Series:
    """
    Chuẩn hóa các giá trị Yes/No, True/False, 1/0 thành bool để tính rate ổn định hơn.
    """
    if series.dtype == bool:
        return series.fillna(False)

    normalized = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .replace(
            {
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
        )
    )

    return normalized.astype(bool)


def safe_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)


def read_excel_checked(path: Path, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name)


def add_file_existence_checks(report_rows: list[dict]) -> bool:
    all_ok = True
    for key, path in FILES.items():
        if not path.exists():
            all_ok = False
            report_rows.append(
                {
                    "check": key,
                    "status": "FAIL",
                    "details": f"Missing file: {path.name}",
                }
            )
        else:
            report_rows.append(
                {
                    "check": key,
                    "status": "PASS",
                    "details": f"Found file: {path.name}",
                }
            )
    return all_ok


def add_sheet_read_checks(report_rows: list[dict]) -> tuple[pd.DataFrame | None, dict[int, pd.DataFrame]]:
    actual = None
    issue_map: dict[int, pd.DataFrame] = {}

    try:
        actual = read_excel_checked(FILES["actual_combined"], "ticket_raw")
        report_rows.append(
            {"check": "read_actual_ticket_raw", "status": "PASS", "details": "Read sheet ticket_raw successfully"}
        )
    except Exception as e:
        report_rows.append(
            {"check": "read_actual_ticket_raw", "status": "FAIL", "details": f"Cannot read sheet ticket_raw: {e}"}
        )

    for year in [2023, 2024, 2025]:
        file_key = f"issue_{year}"
        try:
            issue_df = read_excel_checked(FILES[file_key], "Tickets_Raw")
            issue_map[year] = issue_df
            report_rows.append(
                {
                    "check": f"read_issue_{year}_tickets_raw",
                    "status": "PASS",
                    "details": "Read sheet Tickets_Raw successfully",
                }
            )
        except Exception as e:
            report_rows.append(
                {
                    "check": f"read_issue_{year}_tickets_raw",
                    "status": "FAIL",
                    "details": f"Cannot read sheet Tickets_Raw: {e}",
                }
            )

    return actual, issue_map


def add_schema_checks(report_rows: list[dict], actual: pd.DataFrame, issue_map: dict[int, pd.DataFrame]) -> None:
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


def add_duplicate_checks(report_rows: list[dict], actual: pd.DataFrame, issue_map: dict[int, pd.DataFrame]) -> None:
    dupe_actual = int(actual["ticket_id"].duplicated().sum()) if "ticket_id" in actual.columns else -1
    report_rows.append(
        {
            "check": "actual_duplicate_ticket_id",
            "status": "PASS" if dupe_actual == 0 else "WARN",
            "details": f"duplicate_ticket_id={dupe_actual}",
        }
    )

    for year, issue_df in issue_map.items():
        dupes = int(issue_df["ticket_id"].duplicated().sum()) if "ticket_id" in issue_df.columns else -1
        report_rows.append(
            {
                "check": f"issue_{year}_duplicate_ticket_id",
                "status": "PASS" if dupes == 0 else "WARN",
                "details": f"duplicate_ticket_id={dupes}",
            }
        )


def add_volume_checks(report_rows: list[dict], actual: pd.DataFrame, issue_map: dict[int, pd.DataFrame]) -> None:
    if "year" not in actual.columns or "ticket_id" not in actual.columns:
        report_rows.append(
            {
                "check": "volume_compare",
                "status": "FAIL",
                "details": "Cannot compare yearly volume because actual file misses year or ticket_id column",
            }
        )
        return

    actual_by_year = actual.groupby("year")["ticket_id"].count().to_dict()

    for year in [2023, 2024, 2025]:
        issue_df = issue_map.get(year)
        if issue_df is None:
            report_rows.append(
                {
                    "check": f"volume_compare_{year}",
                    "status": "FAIL",
                    "details": f"Issue file for {year} is not available",
                }
            )
            continue

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


def add_severity_distribution_checks(report_rows: list[dict], actual: pd.DataFrame, issue_map: dict[int, pd.DataFrame]) -> None:
    if "year" not in actual.columns or "severity" not in actual.columns:
        report_rows.append(
            {
                "check": "severity_distribution",
                "status": "FAIL",
                "details": "Cannot compare severity distribution because actual file misses year or severity column",
            }
        )
        return

    for year in [2023, 2024, 2025]:
        issue_df = issue_map.get(year)
        if issue_df is None or "severity" not in issue_df.columns:
            report_rows.append(
                {
                    "check": f"severity_distribution_{year}",
                    "status": "FAIL",
                    "details": "Issue file missing or missing severity column",
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


def add_timestamp_order_checks(report_rows: list[dict], issue_map: dict[int, pd.DataFrame]) -> None:
    for year in [2023, 2024, 2025]:
        issue_df = issue_map.get(year)
        if issue_df is None:
            report_rows.append(
                {
                    "check": f"timestamp_order_{year}",
                    "status": "FAIL",
                    "details": "Issue file not available",
                }
            )
            continue

        temp = issue_df.copy()
        temp["created_at"] = pd.to_datetime(temp["created_at"], errors="coerce")
        temp["first_response_at"] = pd.to_datetime(temp["first_response_at"], errors="coerce")
        temp["resolved_at"] = pd.to_datetime(temp["resolved_at"], errors="coerce")

        bad_first_response = (temp["first_response_at"] < temp["created_at"]).fillna(False)
        bad_resolved = (temp["resolved_at"] < temp["first_response_at"]).fillna(False)
        bad_order = int((bad_first_response | bad_resolved).sum())

        report_rows.append(
            {
                "check": f"timestamp_order_{year}",
                "status": "PASS" if bad_order == 0 else "WARN",
                "details": f"bad_order_rows={bad_order}",
            }
        )


def add_kpi_gap_checks(report_rows: list[dict], actual: pd.DataFrame, issue_map: dict[int, pd.DataFrame]) -> None:
    required_actual = {"year", "actual_response_min", "actual_resolution_min", "escalated", "reopened_count"}
    if not required_actual.issubset(actual.columns):
        report_rows.append(
            {
                "check": "kpi_gap",
                "status": "FAIL",
                "details": f"Actual file missing columns: {sorted(required_actual - set(actual.columns))}",
            }
        )
        return

    for year in [2023, 2024, 2025]:
        issue_df = issue_map.get(year)
        if issue_df is None:
            report_rows.append(
                {
                    "check": f"kpi_gap_{year}",
                    "status": "FAIL",
                    "details": "Issue file not available",
                }
            )
            continue

        temp = issue_df.copy()
        temp["created_at"] = pd.to_datetime(temp["created_at"], errors="coerce")
        temp["first_response_at"] = pd.to_datetime(temp["first_response_at"], errors="coerce")
        temp["resolved_at"] = pd.to_datetime(temp["resolved_at"], errors="coerce")

        temp["response_min"] = (temp["first_response_at"] - temp["created_at"]).dt.total_seconds() / 60
        temp["resolution_min"] = (temp["resolved_at"] - temp["created_at"]).dt.total_seconds() / 60

        actual_y = actual[actual["year"] == year].copy()

        if len(actual_y) == 0:
            report_rows.append(
                {
                    "check": f"kpi_gap_{year}",
                    "status": "FAIL",
                    "details": "No actual records found for this year in combined file",
                }
            )
            continue

        issue_escalated_rate = rate(to_bool_series(temp["escalated"]).astype(int))
        actual_escalated_rate = rate(to_bool_series(actual_y["escalated"]).astype(int))
        issue_reopen_rate = rate((pd.to_numeric(temp["reopened_count"], errors="coerce").fillna(0) > 0).astype(int))
        actual_reopen_rate = rate((pd.to_numeric(actual_y["reopened_count"], errors="coerce").fillna(0) > 0).astype(int))

        metrics = {
            "avg_response_min_issue": round(float(temp["response_min"].mean()), 2) if len(temp) else None,
            "avg_response_min_actual": round(float(actual_y["actual_response_min"].mean()), 2) if len(actual_y) else None,
            "avg_response_min_gap": round(float(temp["response_min"].mean() - actual_y["actual_response_min"].mean()), 2),
            "avg_resolution_min_issue": round(float(temp["resolution_min"].mean()), 2) if len(temp) else None,
            "avg_resolution_min_actual": round(float(actual_y["actual_resolution_min"].mean()), 2) if len(actual_y) else None,
            "avg_resolution_min_gap": round(float(temp["resolution_min"].mean() - actual_y["actual_resolution_min"].mean()), 2),
            "issue_escalation_rate": issue_escalated_rate,
            "actual_escalation_rate": actual_escalated_rate,
            "escalation_rate_gap": round(issue_escalated_rate - actual_escalated_rate, 4),
            "issue_reopen_rate": issue_reopen_rate,
            "actual_reopen_rate": actual_reopen_rate,
            "reopen_rate_gap": round(issue_reopen_rate - actual_reopen_rate, 4),
        }

        max_material_gap = max(
            abs(metrics["avg_response_min_gap"]),
            abs(metrics["avg_resolution_min_gap"]),
        )

        status = "PASS" if max_material_gap <= 180 else "WARN"

        report_rows.append(
            {
                "check": f"kpi_gap_{year}",
                "status": status,
                "details": safe_json(metrics),
            }
        )


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    report_rows: list[dict] = []

    files_ok = add_file_existence_checks(report_rows)

    if not files_ok:
        report = pd.DataFrame(report_rows)
        report.to_csv(OUTPUT / "validation_report.csv", index=False)
        with pd.ExcelWriter(OUTPUT / "validation_report.xlsx", engine="openpyxl") as writer:
            report.to_excel(writer, sheet_name="Validation_Report", index=False)
        print("Missing one or more input files. See output/validation_report.csv")
        return

    actual, issue_map = add_sheet_read_checks(report_rows)

    if actual is None or len(issue_map) < 3:
        report = pd.DataFrame(report_rows)
        report.to_csv(OUTPUT / "validation_report.csv", index=False)
        with pd.ExcelWriter(OUTPUT / "validation_report.xlsx", engine="openpyxl") as writer:
            report.to_excel(writer, sheet_name="Validation_Report", index=False)
        print("Cannot continue because one or more sheets could not be read. See output/validation_report.csv")
        return

    add_schema_checks(report_rows, actual, issue_map)
    add_duplicate_checks(report_rows, actual, issue_map)
    add_volume_checks(report_rows, actual, issue_map)
    add_severity_distribution_checks(report_rows, actual, issue_map)
    add_timestamp_order_checks(report_rows, issue_map)
    add_kpi_gap_checks(report_rows, actual, issue_map)

    report = pd.DataFrame(report_rows)
    report.to_csv(OUTPUT / "validation_report.csv", index=False)

    with pd.ExcelWriter(OUTPUT / "validation_report.xlsx", engine="openpyxl") as writer:
        report.to_excel(writer, sheet_name="Validation_Report", index=False)

    print("Created output/validation_report.csv and output/validation_report.xlsx")


if __name__ == "__main__":
    main()
