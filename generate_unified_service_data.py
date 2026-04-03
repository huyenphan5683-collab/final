from __future__ import annotations

from pathlib import Path
import os
import random

import numpy as np
import pandas as pd


ROOT = Path(os.environ.get("SERVICE_DATA_ROOT", str(Path(__file__).resolve().parent)))
OUTPUT = ROOT / "output"
OUTPUT.mkdir(parents=True, exist_ok=True)

random.seed(42)
np.random.seed(42)


SLA_TARGETS = {
    "Critical": {"response_min": 15, "resolution_min": 240},
    "High": {"response_min": 30, "resolution_min": 480},
    "Medium": {"response_min": 60, "resolution_min": 1440},
    "Low": {"response_min": 120, "resolution_min": 2880},
}

SEVERITIES = ["Critical", "High", "Medium", "Low"]
SEVERITY_WEIGHTS = [0.08, 0.22, 0.45, 0.25]

PRIORITY_MAP = {
    "Critical": "P1",
    "High": "P2",
    "Medium": "P3",
    "Low": "P4",
}

CATEGORIES = [
    "System Bug",
    "Integration Issue",
    "Master Data",
    "User Access",
    "Performance",
    "Report/BI",
    "Configuration",
]

TEAMS = [
    "L1 Support",
    "L2 Support",
    "Product Team",
    "Implementation",
]

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
]

CHANNELS = ["Portal", "Email", "Hotline"]

BREACH_REASONS = [
    "Internal delay",
    "Dependency on product team",
    "Environment/data issue",
    "Waiting for customer",
    "Incorrect prioritization",
]

MONTH_VOLUME_FACTORS = {
    1: 1.25,
    2: 1.15,
    3: 0.95,
    4: 0.95,
    5: 1.00,
    6: 1.00,
    7: 1.00,
    8: 1.05,
    9: 1.00,
    10: 1.05,
    11: 1.10,
    12: 1.20,
}

BASE_YEAR_VOLUME = {
    2023: 1800,
    2024: 2200,
    2025: 2600,
}


def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def gen_ticket_id(year: int, seq: int) -> str:
    return f"TKT-{year}-{seq:05d}"


def gen_month_volume(year: int, month: int) -> int:
    base = BASE_YEAR_VOLUME[year] / 12
    factor = MONTH_VOLUME_FACTORS[month]
    noise = random.uniform(0.92, 1.08)
    return int(round(base * factor * noise))


def gen_response_resolution(severity: str):
    target_resp = SLA_TARGETS[severity]["response_min"]
    target_reso = SLA_TARGETS[severity]["resolution_min"]

    response_min = max(1, np.random.lognormal(mean=np.log(target_resp * 0.8), sigma=0.5))
    resolution_min = max(response_min + 5, np.random.lognormal(mean=np.log(target_reso * 0.85), sigma=0.55))

    response_min = round(float(response_min), 2)
    resolution_min = round(float(resolution_min), 2)

    response_sla_met = response_min <= target_resp
    resolution_sla_met = resolution_min <= target_reso
    overall_sla_status = "Met" if response_sla_met and resolution_sla_met else "Breached"

    return response_min, resolution_min, response_sla_met, resolution_sla_met, overall_sla_status


def gen_feedback_from_ticket(response_sla_met: bool, resolution_sla_met: bool, reopened_count: int, escalated: bool):
    score = 5
    if not response_sla_met:
        score -= 1
    if not resolution_sla_met:
        score -= 1
    if reopened_count > 0:
        score -= 1
    if escalated:
        score -= 1

    score = max(1, min(5, score))

    comment_map = {
        5: "Support was timely and issue was resolved clearly.",
        4: "Overall good support, but there was slight delay.",
        3: "Issue was resolved, but response and follow-up could be improved.",
        2: "Support process was slow and required escalation.",
        1: "The issue handling experience was unsatisfactory.",
    }

    return score, comment_map[score]


def build_dataset():
    raw_rows = []
    feedback_rows = []
    seq = 1

    for year in [2023, 2024, 2025]:
        for month in range(1, 13):
            month_volume = gen_month_volume(year, month)

            for _ in range(month_volume):
                ticket_id = gen_ticket_id(year, seq)
                seq += 1

                severity = weighted_choice(SEVERITIES, SEVERITY_WEIGHTS)
                priority = PRIORITY_MAP[severity]
                category = random.choice(CATEGORIES)
                assigned_team = random.choice(TEAMS)
                customer_name = random.choice(CUSTOMERS)
                channel = random.choice(CHANNELS)

                day = random.randint(1, 28)
                hour = random.randint(8, 18)
                minute = random.randint(0, 59)
                created_at = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)

                actual_response_min, actual_resolution_min, response_sla_met, resolution_sla_met, overall_sla_status = gen_response_resolution(severity)

                first_response_at = created_at + pd.Timedelta(minutes=actual_response_min)
                resolved_at = created_at + pd.Timedelta(minutes=actual_resolution_min)

                escalated = random.random() < (0.28 if severity in ["Critical", "High"] else 0.10)
                reopened_count = np.random.choice([0, 1, 2], p=[0.83, 0.13, 0.04])

                breach_reason = ""
                if overall_sla_status == "Breached":
                    breach_reason = random.choice(BREACH_REASONS)

                waiting_customer_min = round(max(0, np.random.normal(35, 20)), 2) if random.random() < 0.25 else 0
                affected_users = int(max(1, round(np.random.normal(8, 6))))
                business_impact = weighted_choice(
                    ["Low", "Medium", "High"],
                    [0.45, 0.40, 0.15] if severity in ["Low", "Medium"] else [0.15, 0.45, 0.40],
                )

                csat_score, feedback_comment = gen_feedback_from_ticket(
                    response_sla_met, resolution_sla_met, reopened_count, escalated
                )

                raw_rows.append(
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
                        "reopened_count": int(reopened_count),
                        "breach_reason": breach_reason,
                        "waiting_customer_min": waiting_customer_min,
                        "affected_users": affected_users,
                        "business_impact": business_impact,
                    }
                )

                feedback_rows.append(
                    {
                        "ticket_id": ticket_id,
                        "year": year,
                        "month": month,
                        "customer_name": customer_name,
                        "csat_score": csat_score,
                        "feedback_comment": feedback_comment,
                        "feedback_type": "Direct simulated post-ticket feedback",
                    }
                )

    raw = pd.DataFrame(raw_rows)
    feedback = pd.DataFrame(feedback_rows)

    sla_targets = pd.DataFrame(
        [
            {
                "severity": sev,
                "response_sla_target_min": SLA_TARGETS[sev]["response_min"],
                "resolution_sla_target_min": SLA_TARGETS[sev]["resolution_min"],
            }
            for sev in SEVERITIES
        ]
    )

    kpi_overall = pd.DataFrame(
        [
            {
                "total_tickets": len(raw),
                "avg_response_min": round(raw["actual_response_min"].mean(), 2),
                "avg_resolution_min": round(raw["actual_resolution_min"].mean(), 2),
                "response_sla_rate": round(raw["response_sla_met"].mean(), 4),
                "resolution_sla_rate": round(raw["resolution_sla_met"].mean(), 4),
                "overall_sla_rate": round((raw["overall_sla_status"] == "Met").mean(), 4),
                "escalation_rate": round(raw["escalated"].astype(int).mean(), 4),
                "reopen_rate": round((raw["reopened_count"] > 0).astype(int).mean(), 4),
                "avg_csat": round(feedback["csat_score"].mean(), 2),
            }
        ]
    )

    kpi_by_month = (
        raw.groupby(["year", "month"], as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", "mean"),
            resolution_sla_rate=("resolution_sla_met", "mean"),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
        )
    )

    kpi_by_severity = (
        raw.groupby("severity", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", "mean"),
            resolution_sla_rate=("resolution_sla_met", "mean"),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
        )
    )

    kpi_by_customer = (
        raw.groupby("customer_name", as_index=False)
        .agg(
            total_tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", "mean"),
            resolution_sla_rate=("resolution_sla_met", "mean"),
            escalation_rate=("escalated", lambda s: s.astype(int).mean()),
            reopen_rate=("reopened_count", lambda s: (s > 0).astype(int).mean()),
        )
        .merge(
            feedback.groupby("customer_name", as_index=False).agg(avg_csat=("csat_score", "mean")),
            on="customer_name",
            how="left",
        )
    )

    feedback_proxy = (
        raw.groupby(["year", "month"], as_index=False)
        .agg(
            breach_tickets=("overall_sla_status", lambda s: (s == "Breached").sum()),
            escalated_tickets=("escalated", lambda s: s.astype(int).sum()),
            reopened_tickets=("reopened_count", lambda s: (s > 0).sum()),
            affected_users_total=("affected_users", "sum"),
        )
    )

    dictionary = pd.DataFrame(
        [
            {"column_name": "ticket_id", "description": "Unique ticket identifier"},
            {"column_name": "actual_response_min", "description": "Actual first response time in minutes"},
            {"column_name": "actual_resolution_min", "description": "Actual resolution time in minutes"},
            {"column_name": "response_sla_met", "description": "Whether response SLA target was met"},
            {"column_name": "resolution_sla_met", "description": "Whether resolution SLA target was met"},
            {"column_name": "reopened_count", "description": "Number of times the ticket was reopened"},
            {"column_name": "escalated", "description": "Whether the ticket was escalated"},
            {"column_name": "csat_score", "description": "Simulated customer satisfaction score from 1 to 5"},
        ]
    )

    return raw, sla_targets, dictionary, kpi_overall, kpi_by_month, kpi_by_severity, kpi_by_customer, feedback, feedback_proxy


def main():
    raw, sla_targets, dictionary, kpi_overall, kpi_by_month, kpi_by_severity, kpi_by_customer, feedback, feedback_proxy = build_dataset()

    output_file = OUTPUT / "smartlog_service_analysis_2023_2025.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        raw.to_excel(writer, sheet_name="Ticket_Raw", index=False)
        sla_targets.to_excel(writer, sheet_name="SLA_Targets", index=False)
        dictionary.to_excel(writer, sheet_name="Data_Dictionary", index=False)
        kpi_overall.to_excel(writer, sheet_name="KPI_Overall", index=False)
        kpi_by_month.to_excel(writer, sheet_name="KPI_By_Month", index=False)
        kpi_by_severity.to_excel(writer, sheet_name="KPI_By_Severity", index=False)
        kpi_by_customer.to_excel(writer, sheet_name="KPI_By_Customer", index=False)
        feedback.to_excel(writer, sheet_name="Feedback_Direct", index=False)
        feedback_proxy.to_excel(writer, sheet_name="Feedback_Proxy", index=False)

    print(f"Created: {output_file}")


if __name__ == "__main__":
    main()
