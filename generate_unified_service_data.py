from __future__ import annotations

from pathlib import Path
import os
import random
from typing import Dict, List

import numpy as np
import pandas as pd


ROOT = Path(os.environ.get("SERVICE_DATA_ROOT", str(Path(__file__).resolve().parent)))
OUTPUT = ROOT / "output"
OUTPUT.mkdir(parents=True, exist_ok=True)

random.seed(42)
np.random.seed(42)

# =========================
# CONFIG
# =========================

YEARS = [2023, 2024, 2025]

MONTH_VOLUME_FACTORS = {
    1: 1.25,
    2: 1.12,
    3: 0.95,
    4: 0.96,
    5: 1.00,
    6: 1.00,
    7: 1.02,
    8: 1.03,
    9: 1.00,
    10: 1.05,
    11: 1.10,
    12: 1.18,
}

BASE_YEAR_VOLUME = {
    2023: 3600,
    2024: 4500,
    2025: 5200,
}

SEVERITIES = ["Critical", "High", "Medium", "Low"]
SEVERITY_WEIGHTS = [0.07, 0.20, 0.48, 0.25]

PRIORITY_MAP = {
    "Critical": "P1",
    "High": "P2",
    "Medium": "P3",
    "Low": "P4",
}

SLA_TARGETS = {
    "Critical": {"response_min": 15, "resolution_min": 240},
    "High": {"response_min": 30, "resolution_min": 480},
    "Medium": {"response_min": 60, "resolution_min": 1440},
    "Low": {"response_min": 120, "resolution_min": 2880},
}

CATEGORIES = [
    "System Bug",
    "Integration Issue",
    "Performance",
    "User Access",
    "Master Data",
    "Configuration",
    "Report/BI",
    "Workflow",
    "Carrier/3PL Sync",
]

CATEGORY_WEIGHTS = [0.18, 0.16, 0.12, 0.10, 0.12, 0.10, 0.08, 0.08, 0.06]

ASSIGNED_TEAMS = [
    "L1 Support",
    "L2 Support",
    "Product Team",
    "Implementation",
    "QA/Technical",
]

TEAM_WEIGHTS = [0.34, 0.26, 0.18, 0.12, 0.10]

CUSTOMERS = [
    "Vinamilk",
    "Masan",
    "TH True Milk",
    "Nestle VN",
    "Unilever VN",
    "Ajinomoto VN",
    "Suntory PepsiCo",
    "DHL Supply Chain",
    "CJ Logistics",
    "DB Schenker",
    "Kerry Logistics",
    "Lotte Global Logistics",
]

CHANNELS = ["Portal", "Email", "Hotline"]
CHANNEL_WEIGHTS = [0.58, 0.30, 0.12]

BREACH_REASONS = [
    "Internal delay",
    "Dependency on product team",
    "Environment/data issue",
    "Waiting for customer",
    "Incorrect prioritization",
    "Complex root cause analysis",
]

BUSINESS_IMPACTS = ["Low", "Medium", "High"]
FEEDBACK_TYPES = ["Post-ticket survey", "Account review note", "Support follow-up"]

# =========================
# HELPERS
# =========================

def weighted_choice(items: List[str], weights: List[float]) -> str:
    return random.choices(items, weights=weights, k=1)[0]


def gen_month_volume(year: int, month: int) -> int:
    base = BASE_YEAR_VOLUME[year] / 12
    factor = MONTH_VOLUME_FACTORS[month]
    noise = random.uniform(0.94, 1.07)
    return int(round(base * factor * noise))


def gen_ticket_id(year: int, seq: int) -> str:
    return f"TKT-{year}-{seq:05d}"


def bool_rate(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(series.astype(int).mean(), 4)


def round_df(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    out = df.copy()
    float_cols = out.select_dtypes(include=["float64", "float32"]).columns
    out[float_cols] = out[float_cols].round(decimals)
    return out


def severity_target(severity: str) -> Dict[str, int]:
    return SLA_TARGETS[severity]


def generate_response_resolution(severity: str):
    tgt = severity_target(severity)
    target_resp = tgt["response_min"]
    target_res = tgt["resolution_min"]

    if severity == "Critical":
        resp = np.random.lognormal(np.log(target_resp * 0.95), 0.55)
        reso = np.random.lognormal(np.log(target_res * 0.95), 0.60)
    elif severity == "High":
        resp = np.random.lognormal(np.log(target_resp * 0.92), 0.52)
        reso = np.random.lognormal(np.log(target_res * 0.94), 0.58)
    elif severity == "Medium":
        resp = np.random.lognormal(np.log(target_resp * 0.88), 0.50)
        reso = np.random.lognormal(np.log(target_res * 0.90), 0.55)
    else:
        resp = np.random.lognormal(np.log(target_resp * 0.82), 0.48)
        reso = np.random.lognormal(np.log(target_res * 0.86), 0.52)

    resp = round(max(1.0, float(resp)), 2)
    reso = round(max(resp + 5, float(reso)), 2)

    resp_met = resp <= target_resp
    reso_met = reso <= target_res
    overall = "Met" if resp_met and reso_met else "Breached"

    return resp, reso, resp_met, reso_met, overall


def gen_escalated(severity: str, breached: bool) -> bool:
    base = {
        "Critical": 0.34,
        "High": 0.24,
        "Medium": 0.13,
        "Low": 0.06,
    }[severity]
    if breached:
        base += 0.10
    return random.random() < min(base, 0.85)


def gen_reopen_count(severity: str, breached: bool) -> int:
    if severity == "Critical":
        probs = [0.78, 0.16, 0.06]
    elif severity == "High":
        probs = [0.81, 0.14, 0.05]
    elif severity == "Medium":
        probs = [0.85, 0.11, 0.04]
    else:
        probs = [0.90, 0.08, 0.02]

    if breached:
        probs = [max(probs[0] - 0.08, 0.60), probs[1] + 0.05, probs[2] + 0.03]

    return int(np.random.choice([0, 1, 2], p=np.array(probs) / np.sum(probs)))


def gen_feedback_score(
    response_sla_met: bool,
    resolution_sla_met: bool,
    reopened_count: int,
    escalated: bool,
    severity: str,
):
    score = 5
    if not response_sla_met:
        score -= 1
    if not resolution_sla_met:
        score -= 1
    if reopened_count > 0:
        score -= 1
    if escalated:
        score -= 1
    if severity == "Critical" and (not response_sla_met or not resolution_sla_met):
        score -= 1

    score = max(1, min(5, score))

    comments = {
        5: [
            "Support response was timely and the issue was resolved effectively.",
            "The handling process was smooth and communication was clear.",
            "The support team resolved the issue quickly with good follow-up.",
        ],
        4: [
            "Overall support quality was good, though there was some minor delay.",
            "The issue was handled well, but progress updates could be more frequent.",
            "Support met expectations overall, with slight room for improvement.",
        ],
        3: [
            "The issue was resolved, but the turnaround time was longer than expected.",
            "Support was acceptable, although escalation or follow-up took time.",
            "The case was eventually closed, but the experience was only average.",
        ],
        2: [
            "The support process was slow and required repeated follow-up.",
            "Issue handling lacked consistency and took longer than expected.",
            "The case required escalation and impacted the service experience negatively.",
        ],
        1: [
            "The issue handling experience was unsatisfactory.",
            "Resolution took too long and the support experience was poor.",
            "The case management process did not meet expectations.",
        ],
    }
    return score, random.choice(comments[score])


def build_ticket_level_data() -> pd.DataFrame:
    rows = []
    seq = 1

    for year in YEARS:
        for month in range(1, 13):
            volume = gen_month_volume(year, month)

            for _ in range(volume):
                ticket_id = gen_ticket_id(year, seq)
                seq += 1

                severity = weighted_choice(SEVERITIES, SEVERITY_WEIGHTS)
                priority = PRIORITY_MAP[severity]
                category = weighted_choice(CATEGORIES, CATEGORY_WEIGHTS)
                assigned_team = weighted_choice(ASSIGNED_TEAMS, TEAM_WEIGHTS)
                customer_name = random.choice(CUSTOMERS)
                channel = weighted_choice(CHANNELS, CHANNEL_WEIGHTS)

                day = random.randint(1, 28)
                hour = random.randint(8, 18)
                minute = random.randint(0, 59)
                created_at = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)

                actual_response_min, actual_resolution_min, response_sla_met, resolution_sla_met, overall_sla_status = generate_response_resolution(severity)

                first_response_at = created_at + pd.Timedelta(minutes=actual_response_min)
                resolved_at = created_at + pd.Timedelta(minutes=actual_resolution_min)

                escalated = gen_escalated(severity, overall_sla_status == "Breached")
                reopened_count = gen_reopen_count(severity, overall_sla_status == "Breached")

                breach_reason = ""
                if overall_sla_status == "Breached":
                    breach_reason = random.choice(BREACH_REASONS)

                waiting_customer_min = round(max(0.0, float(np.random.normal(35, 18))), 2) if random.random() < 0.24 else 0.0
                affected_users = int(max(1, round(np.random.normal(7, 5))))
                business_impact = weighted_choice(
                    BUSINESS_IMPACTS,
                    [0.12, 0.38, 0.50] if severity in ["Critical", "High"] else [0.48, 0.40, 0.12],
                )

                csat_score, feedback_comment = gen_feedback_score(
                    response_sla_met=response_sla_met,
                    resolution_sla_met=resolution_sla_met,
                    reopened_count=reopened_count,
                    escalated=escalated,
                    severity=severity,
                )

                feedback_type = random.choice(FEEDBACK_TYPES)

                rows.append(
                    {
                        "ticket_id": ticket_id,
                        "year": year,
                        "month": month,
                        "created_at": created_at,
                        "first_response_at": first_response_at,
                        "resolved_at": resolved_at,
                        "severity": severity,
                        "priority": priority,
                        "category": category,
                        "assigned_team": assigned_team,
                        "customer_name": customer_name,
                        "channel": channel,
                        "actual_response_min": actual_response_min,
                        "actual_resolution_min": actual_resolution_min,
                        "response_sla_target_min": SLA_TARGETS[severity]["response_min"],
                        "resolution_sla_target_min": SLA_TARGETS[severity]["resolution_min"],
                        "response_sla_met": response_sla_met,
                        "resolution_sla_met": resolution_sla_met,
                        "overall_sla_status": overall_sla_status,
                        "escalated": escalated,
                        "reopened_count": reopened_count,
                        "breach_reason": breach_reason,
                        "waiting_customer_min": waiting_customer_min,
                        "affected_users": affected_users,
                        "business_impact": business_impact,
                        "csat_score": csat_score,
                        "feedback_comment": feedback_comment,
                        "feedback_type": feedback_type,
                    }
                )

    df = pd.DataFrame(rows)
    return df


# =========================
# SUMMARY TABLES
# =========================

def build_summary_overall(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        [
            {
                "total_tickets": len(df),
                "avg_response_min": df["actual_response_min"].mean(),
                "avg_resolution_min": df["actual_resolution_min"].mean(),
                "response_sla_rate": df["response_sla_met"].astype(int).mean(),
                "resolution_sla_rate": df["resolution_sla_met"].astype(int).mean(),
                "overall_sla_rate": (df["overall_sla_status"] == "Met").astype(int).mean(),
                "escalation_rate": df["escalated"].astype(int).mean(),
                "reopen_rate": (df["reopened_count"] > 0).astype(int).mean(),
                "avg_waiting_customer_min": df["waiting_customer_min"].mean(),
                "avg_csat_score": df["csat_score"].mean(),
            }
        ]
    )
    return round_df(out, 4)


def build_summary_by_year(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("year", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", lambda s: s.astype(int).mean()),
            resolution_sla_rate=("resolution_sla_met", lambda s: s.astype(int).mean()),
            overall_sla_rate=("overall_sla_status", lambda s: (s == "Met").astype(int).mean()),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
            avg_csat_score=("csat_score", "mean"),
        )
    )
    return round_df(out, 4)


def build_summary_by_month(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["year", "month"], as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", lambda s: s.astype(int).mean()),
            resolution_sla_rate=("resolution_sla_met", lambda s: s.astype(int).mean()),
            overall_sla_rate=("overall_sla_status", lambda s: (s == "Met").astype(int).mean()),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
            avg_csat_score=("csat_score", "mean"),
        )
        .sort_values(["year", "month"])
    )
    return round_df(out, 4)


def build_summary_by_severity(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("severity", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", lambda s: s.astype(int).mean()),
            resolution_sla_rate=("resolution_sla_met", lambda s: s.astype(int).mean()),
            overall_sla_rate=("overall_sla_status", lambda s: (s == "Met").astype(int).mean()),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
            avg_csat_score=("csat_score", "mean"),
        )
    )
    return round_df(out, 4)


def build_summary_by_customer(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("customer_name", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", lambda s: s.astype(int).mean()),
            resolution_sla_rate=("resolution_sla_met", lambda s: s.astype(int).mean()),
            overall_sla_rate=("overall_sla_status", lambda s: (s == "Met").astype(int).mean()),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
            avg_csat_score=("csat_score", "mean"),
        )
        .sort_values(["total_tickets", "customer_name"], ascending=[False, True])
    )
    return round_df(out, 4)


def build_summary_by_category(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("category", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", lambda s: s.astype(int).mean()),
            resolution_sla_rate=("resolution_sla_met", lambda s: s.astype(int).mean()),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
            avg_csat_score=("csat_score", "mean"),
        )
        .sort_values(["total_tickets", "category"], ascending=[False, True])
    )
    return round_df(out, 4)


def build_summary_by_team(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("assigned_team", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", lambda s: s.astype(int).mean()),
            resolution_sla_rate=("resolution_sla_met", lambda s: s.astype(int).mean()),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
            avg_csat_score=("csat_score", "mean"),
        )
        .sort_values(["total_tickets", "assigned_team"], ascending=[False, True])
    )
    return round_df(out, 4)


def build_breach_reason_summary(df: pd.DataFrame) -> pd.DataFrame:
    breached = df[df["overall_sla_status"] == "Breached"].copy()
    if len(breached) == 0:
        return pd.DataFrame(columns=["breach_reason", "breach_tickets", "share_of_breaches"])

    out = (
        breached.groupby("breach_reason", as_index=False)
        .agg(breach_tickets=("ticket_id", "count"))
        .sort_values("breach_tickets", ascending=False)
    )
    out["share_of_breaches"] = out["breach_tickets"] / out["breach_tickets"].sum()
    return round_df(out, 4)


def build_feedback_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        [
            {
                "avg_csat_score": df["csat_score"].mean(),
                "pct_csat_5": (df["csat_score"] == 5).mean(),
                "pct_csat_4_or_5": (df["csat_score"] >= 4).mean(),
                "pct_csat_3_or_below": (df["csat_score"] <= 3).mean(),
                "avg_response_min": df["actual_response_min"].mean(),
                "avg_resolution_min": df["actual_resolution_min"].mean(),
                "reopen_rate": (df["reopened_count"] > 0).astype(int).mean(),
                "escalation_rate": df["escalated"].astype(int).mean(),
            }
        ]
    )
    return round_df(out, 4)


def build_feedback_by_customer(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("customer_name", as_index=False)
        .agg(
            avg_csat_score=("csat_score", "mean"),
            feedback_records=("ticket_id", "count"),
            pct_csat_4_or_5=("csat_score", lambda s: (s >= 4).mean()),
            pct_csat_3_or_below=("csat_score", lambda s: (s <= 3).mean()),
            avg_resolution_min=("actual_resolution_min", "mean"),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
        )
        .sort_values(["feedback_records", "customer_name"], ascending=[False, True])
    )
    return round_df(out, 4)


def build_feedback_by_severity(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("severity", as_index=False)
        .agg(
            avg_csat_score=("csat_score", "mean"),
            feedback_records=("ticket_id", "count"),
            pct_csat_4_or_5=("csat_score", lambda s: (s >= 4).mean()),
            pct_csat_3_or_below=("csat_score", lambda s: (s <= 3).mean()),
            avg_resolution_min=("actual_resolution_min", "mean"),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
        )
    )
    return round_df(out, 4)


def build_feedback_proxy(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["year", "month"], as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            breached_tickets=("overall_sla_status", lambda s: (s == "Breached").sum()),
            escalated_tickets=("escalated", lambda s: s.astype(int).sum()),
            reopened_tickets=("reopened_count", lambda s: (s > 0).sum()),
            affected_users_total=("affected_users", "sum"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            avg_csat_score=("csat_score", "mean"),
        )
        .sort_values(["year", "month"])
    )
    out["breach_rate"] = out["breached_tickets"] / out["total_tickets"]
    out["escalation_rate"] = out["escalated_tickets"] / out["total_tickets"]
    out["reopen_rate"] = out["reopened_tickets"] / out["total_tickets"]
    return round_df(out, 4)


def build_kpi_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(
        [
            {"metric": "Total tickets", "value": len(df)},
            {"metric": "Average response time (min)", "value": round(df["actual_response_min"].mean(), 2)},
            {"metric": "Average resolution time (min)", "value": round(df["actual_resolution_min"].mean(), 2)},
            {"metric": "Response SLA compliance rate", "value": round(df["response_sla_met"].astype(int).mean(), 4)},
            {"metric": "Resolution SLA compliance rate", "value": round(df["resolution_sla_met"].astype(int).mean(), 4)},
            {"metric": "Overall SLA compliance rate", "value": round((df["overall_sla_status"] == "Met").astype(int).mean(), 4)},
            {"metric": "Escalation rate", "value": round(df["escalated"].astype(int).mean(), 4)},
            {"metric": "Reopen rate", "value": round((df["reopened_count"] > 0).astype(int).mean(), 4)},
            {"metric": "Average CSAT", "value": round(df["csat_score"].mean(), 2)},
        ]
    )
    return out


def build_escalation_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["severity", "assigned_team"], as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            escalated_tickets=("escalated", lambda s: s.astype(int).sum()),
        )
    )
    out["escalation_rate"] = out["escalated_tickets"] / out["total_tickets"]
    return round_df(out.sort_values(["severity", "assigned_team"]), 4)


def build_reopen_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["severity", "category"], as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            reopened_tickets=("reopened_count", lambda s: (s > 0).sum()),
            avg_reopen_count=("reopened_count", "mean"),
        )
    )
    out["reopen_rate"] = out["reopened_tickets"] / out["total_tickets"]
    return round_df(out.sort_values(["severity", "category"]), 4)


def build_data_dictionary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"column_name": "ticket_id", "description": "Unique ticket identifier"},
            {"column_name": "year", "description": "Calendar year of the support ticket"},
            {"column_name": "month", "description": "Calendar month of the support ticket"},
            {"column_name": "created_at", "description": "Ticket creation timestamp"},
            {"column_name": "first_response_at", "description": "Timestamp of first response"},
            {"column_name": "resolved_at", "description": "Ticket resolution timestamp"},
            {"column_name": "severity", "description": "Ticket severity level"},
            {"column_name": "priority", "description": "Mapped ticket priority"},
            {"column_name": "category", "description": "Issue category"},
            {"column_name": "assigned_team", "description": "Team handling the ticket"},
            {"column_name": "customer_name", "description": "Customer account name"},
            {"column_name": "channel", "description": "Support intake channel"},
            {"column_name": "actual_response_min", "description": "Actual first response time in minutes"},
            {"column_name": "actual_resolution_min", "description": "Actual full resolution time in minutes"},
            {"column_name": "response_sla_target_min", "description": "Response SLA target in minutes"},
            {"column_name": "resolution_sla_target_min", "description": "Resolution SLA target in minutes"},
            {"column_name": "response_sla_met", "description": "Whether response SLA target was met"},
            {"column_name": "resolution_sla_met", "description": "Whether resolution SLA target was met"},
            {"column_name": "overall_sla_status", "description": "Overall SLA outcome at ticket level"},
            {"column_name": "escalated", "description": "Whether the ticket was escalated"},
            {"column_name": "reopened_count", "description": "Number of times the ticket was reopened"},
            {"column_name": "breach_reason", "description": "Reason for SLA breach when breached"},
            {"column_name": "waiting_customer_min", "description": "Estimated waiting time caused by customer-side dependency"},
            {"column_name": "affected_users", "description": "Estimated number of impacted users"},
            {"column_name": "business_impact", "description": "Estimated business impact level"},
            {"column_name": "csat_score", "description": "Simulated satisfaction score from 1 to 5"},
            {"column_name": "feedback_comment", "description": "Simulated customer feedback text"},
            {"column_name": "feedback_type", "description": "Type of satisfaction-related record"},
        ]
    )


# =========================
# FILE WRITERS
# =========================

def write_master_file(df: pd.DataFrame) -> None:
    output_file = OUTPUT / "smartlog_service_master_2023_2025.xlsx"
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ticket_raw", index=False)
        build_summary_overall(df).to_excel(writer, sheet_name="summary_overall", index=False)
        build_summary_by_year(df).to_excel(writer, sheet_name="summary_by_year", index=False)
        build_summary_by_month(df).to_excel(writer, sheet_name="summary_by_month", index=False)
        build_summary_by_severity(df).to_excel(writer, sheet_name="summary_by_severity", index=False)
        build_summary_by_customer(df).to_excel(writer, sheet_name="summary_by_customer", index=False)
        build_summary_by_category(df).to_excel(writer, sheet_name="summary_by_category", index=False)
        build_summary_by_team(df).to_excel(writer, sheet_name="summary_by_team", index=False)
        build_breach_reason_summary(df).to_excel(writer, sheet_name="breach_reason_summary", index=False)
        build_feedback_summary(df).to_excel(writer, sheet_name="feedback_summary", index=False)
        build_feedback_by_customer(df).to_excel(writer, sheet_name="feedback_by_customer", index=False)
        build_feedback_by_severity(df).to_excel(writer, sheet_name="feedback_by_severity", index=False)
        build_feedback_proxy(df).to_excel(writer, sheet_name="feedback_proxy", index=False)
        build_data_dictionary().to_excel(writer, sheet_name="data_dictionary", index=False)


def write_actual_performance_file(df: pd.DataFrame) -> None:
    output_file = OUTPUT / "sla_actual_performance_2023_2025_combined.xlsx"

    actual = df[
        [
            "ticket_id",
            "year",
            "month",
            "severity",
            "actual_response_min",
            "actual_resolution_min",
            "response_sla_met",
            "resolution_sla_met",
            "overall_sla_status",
            "escalated",
            "reopened_count",
            "breach_reason",
            "waiting_customer_min",
            "affected_users",
            "business_impact",
            "customer_name",
        ]
    ].copy()

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        actual.to_excel(writer, sheet_name="ticket_raw", index=False)
        build_summary_overall(df).to_excel(writer, sheet_name="summary_overall", index=False)
        build_summary_by_year(df).to_excel(writer, sheet_name="summary_by_year", index=False)
        build_summary_by_month(df).to_excel(writer, sheet_name="summary_by_month", index=False)
        build_summary_by_severity(df).to_excel(writer, sheet_name="summary_by_severity", index=False)
        build_summary_by_customer(df).to_excel(writer, sheet_name="summary_by_customer", index=False)
        build_breach_reason_summary(df).to_excel(writer, sheet_name="breach_reason_summary", index=False)


def write_issue_handling_year_file(df_year: pd.DataFrame, year: int) -> None:
    output_file = OUTPUT / f"sla_issue_handling_{year}_filled.xlsx"

    tickets_raw = df_year[
        [
            "ticket_id",
            "created_at",
            "severity",
            "priority",
            "category",
            "assigned_team",
            "customer_name",
            "channel",
            "first_response_at",
            "resolved_at",
            "reopened_count",
            "escalated",
            "actual_response_min",
            "actual_resolution_min",
            "response_sla_target_min",
            "resolution_sla_target_min",
            "response_sla_met",
            "resolution_sla_met",
            "overall_sla_status",
            "breach_reason",
            "waiting_customer_min",
            "affected_users",
            "business_impact",
            "csat_score",
            "feedback_comment",
            "feedback_type",
        ]
    ].copy()

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        tickets_raw.to_excel(writer, sheet_name="Tickets_Raw", index=False)
        build_kpi_summary(df_year).to_excel(writer, sheet_name="KPI_Summary", index=False)
        build_summary_by_severity(df_year).to_excel(writer, sheet_name="By_Severity", index=False)
        build_summary_by_category(df_year).to_excel(writer, sheet_name="By_Category", index=False)
        build_summary_by_team(df_year).to_excel(writer, sheet_name="By_Team", index=False)
        build_summary_by_month(df_year).to_excel(writer, sheet_name="Monthly_Summary", index=False)
        build_escalation_summary(df_year).to_excel(writer, sheet_name="Escalation_Summary", index=False)
        build_reopen_summary(df_year).to_excel(writer, sheet_name="Reopen_Summary", index=False)
        build_feedback_summary(df_year).to_excel(writer, sheet_name="Feedback_Summary", index=False)
        build_feedback_by_customer(df_year).to_excel(writer, sheet_name="Feedback_By_Customer", index=False)
        build_data_dictionary().to_excel(writer, sheet_name="Data_Dictionary", index=False)


# =========================
# MAIN
# =========================

def main() -> None:
    df = build_ticket_level_data()

    write_master_file(df)
    write_actual_performance_file(df)

    for year in YEARS:
        write_issue_handling_year_file(df[df["year"] == year].copy(), year)

    created_files = [
        "smartlog_service_master_2023_2025.xlsx",
        "sla_actual_performance_2023_2025_combined.xlsx",
        "sla_issue_handling_2023_filled.xlsx",
        "sla_issue_handling_2024_filled.xlsx",
        "sla_issue_handling_2025_filled.xlsx",
    ]
    print("Created files:")
    for f in created_files:
        print(f"- output/{f}")


if __name__ == "__main__":
    main()
