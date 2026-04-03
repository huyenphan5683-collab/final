from __future__ import annotations

import math
import os
import random
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(os.environ.get("SERVICE_DATA_ROOT", str(Path(__file__).resolve().parents[1])))
OUTPUT_DIR = ROOT / "output"
SEED = 42

random.seed(SEED)
np.random.seed(SEED)

SEVERITY_RULES = {
    "Critical": {"priority": "P1", "response_target_min": 30, "resolution_target_min": 240},
    "High": {"priority": "P2", "response_target_min": 60, "resolution_target_min": 480},
    "Medium": {"priority": "P3", "response_target_min": 240, "resolution_target_min": 1440},
    "Low": {"priority": "P4", "response_target_min": 480, "resolution_target_min": 4320},
}

CHANNELS = ["Email", "Portal", "Hotline", "Zalo", "Viber"]
MODULES = ["Planning", "Order", "Execution", "Carrier", "Mobile App", "Data Sync", "Billing", "Reporting"]
CATEGORIES = ["Bug", "Incident", "Change Request", "Data Sync", "Configuration", "Report", "User Support"]
TEAMS = ["IT Support", "Application Support", "QA", "Dev", "Product"]
ASSIGNEES = {
    "IT Support": ["support01", "support02", "support03", "support04"],
    "Application Support": ["app01", "app02", "app03"],
    "QA": ["qa01", "qa02"],
    "Dev": ["dev01", "dev02", "dev03", "dev04", "dev05"],
    "Product": ["pm01", "pm02"],
}
CUSTOMERS = [f"Client_{i:03d}" for i in range(1, 81)]
BREACH_REASONS = [
    "Internal delay",
    "Dependency on product team",
    "Waiting for customer",
    "Environment/data issue",
    "High ticket volume/backlog",
]
COMMENT_THEMES = [
    "Fast response",
    "Resolution took too long",
    "Needed clearer update",
    "Issue fixed effectively",
    "Had to follow up multiple times",
    "Support was helpful",
    "Escalation was necessary",
]

MONTH_WEIGHTS = {
    1: 1.18,
    2: 1.06,
    3: 1.00,
    4: 0.96,
    5: 0.97,
    6: 0.98,
    7: 0.99,
    8: 1.01,
    9: 1.00,
    10: 1.03,
    11: 1.08,
    12: 1.16,
}

YEAR_TARGETS = {
    2023: 41000,
    2024: 56500,
    2025: 56300,
}


@dataclass
class TicketRow:
    ticket_id: str
    year: int
    month: int
    created_at: pd.Timestamp
    first_response_at: pd.Timestamp
    resolved_at: pd.Timestamp
    closed_at: pd.Timestamp
    customer_name: str
    requester: str
    channel: str
    product_module: str
    ticket_type: str
    severity: str
    priority: str
    category: str
    status: str
    assigned_team: str
    assignee: str
    escalated: bool
    reopened_count: int
    affected_users: int
    business_impact: str
    response_target_min: int
    resolution_target_min: int
    actual_response_min: int
    actual_resolution_min: int
    response_sla_met: bool
    resolution_sla_met: bool
    overall_sla_status: str
    breach_reason: str | None
    waiting_customer_min: int
    closure_feedback_submitted: bool
    csat_score: float | None
    customer_comment_theme: str | None
    customer_comment_text: str | None
    customer_sentiment_proxy: str
    complaint_signal: bool


def monthly_plan(year: int, yearly_target: int) -> dict[int, int]:
    raw = {m: MONTH_WEIGHTS[m] for m in range(1, 13)}
    total_weight = sum(raw.values())
    base = {m: int(round(yearly_target * raw[m] / total_weight)) for m in raw}
    diff = yearly_target - sum(base.values())
    months = list(raw.keys())
    idx = 0
    while diff != 0:
        m = months[idx % len(months)]
        base[m] += 1 if diff > 0 else -1
        diff += -1 if diff > 0 else 1
        idx += 1
    return base


def severity_choice() -> str:
    return random.choices(
        population=["Critical", "High", "Medium", "Low"],
        weights=[0.03, 0.14, 0.45, 0.38],
        k=1,
    )[0]


def impact_from_severity(severity: str) -> str:
    if severity == "Critical":
        return random.choices(["High", "Medium"], weights=[0.85, 0.15], k=1)[0]
    if severity == "High":
        return random.choices(["High", "Medium", "Low"], weights=[0.40, 0.50, 0.10], k=1)[0]
    if severity == "Medium":
        return random.choices(["Medium", "Low"], weights=[0.55, 0.45], k=1)[0]
    return random.choices(["Low", "Medium"], weights=[0.90, 0.10], k=1)[0]


def assign_team(ticket_type: str, severity: str) -> str:
    if severity == "Critical":
        return random.choices(["Dev", "Product", "Application Support"], weights=[0.55, 0.20, 0.25], k=1)[0]
    if ticket_type in {"Bug", "Incident"}:
        return random.choices(["Application Support", "Dev", "IT Support"], weights=[0.45, 0.35, 0.20], k=1)[0]
    if ticket_type == "Change Request":
        return random.choices(["Product", "Dev"], weights=[0.60, 0.40], k=1)[0]
    return random.choices(["IT Support", "Application Support", "QA"], weights=[0.50, 0.35, 0.15], k=1)[0]


def response_minutes(severity: str, month: int) -> int:
    rules = SEVERITY_RULES[severity]
    target = rules["response_target_min"]
    month_pressure = 1.15 if month in {1, 12, 11} else 1.0
    base = {
        "Critical": np.random.lognormal(mean=math.log(max(target * 0.75, 5)), sigma=0.55),
        "High": np.random.lognormal(mean=math.log(max(target * 0.82, 10)), sigma=0.55),
        "Medium": np.random.lognormal(mean=math.log(max(target * 0.88, 20)), sigma=0.60),
        "Low": np.random.lognormal(mean=math.log(max(target * 0.90, 30)), sigma=0.70),
    }[severity]
    value = int(round(base * month_pressure))
    return max(3, value)


def resolution_minutes(severity: str, month: int, waiting_customer_min: int, escalated: bool) -> int:
    rules = SEVERITY_RULES[severity]
    target = rules["resolution_target_min"]
    month_pressure = 1.18 if month in {1, 12, 11} else 1.0
    base = {
        "Critical": np.random.lognormal(mean=math.log(max(target * 0.92, 40)), sigma=0.70),
        "High": np.random.lognormal(mean=math.log(max(target * 0.96, 60)), sigma=0.75),
        "Medium": np.random.lognormal(mean=math.log(max(target * 0.98, 90)), sigma=0.80),
        "Low": np.random.lognormal(mean=math.log(max(target * 0.92, 120)), sigma=0.85),
    }[severity]
    extra = waiting_customer_min + (120 if escalated else 0)
    value = int(round(base * month_pressure + extra))
    return max(20, value)


def choose_waiting_customer(breach_reason: str | None) -> int:
    if breach_reason == "Waiting for customer":
        return int(np.random.randint(120, 1800))
    if random.random() < 0.10:
        return int(np.random.randint(30, 360))
    return 0


def comment_from_score(score: float | None, complaint: bool, sla_breached: bool, reopened: int) -> tuple[str | None, str | None]:
    if score is None:
        return None, None
    if score >= 4.5:
        theme = random.choice(["Fast response", "Issue fixed effectively", "Support was helpful"])
    elif score >= 3.5:
        theme = random.choice(["Support was helpful", "Needed clearer update"])
    else:
        theme = random.choice(["Resolution took too long", "Had to follow up multiple times", "Escalation was necessary"])

    text_lookup = {
        "Fast response": "Customer noted that the team responded quickly and kept the case moving.",
        "Issue fixed effectively": "Customer confirmed the issue was resolved and system use returned to normal.",
        "Support was helpful": "Customer appreciated the support team's guidance during troubleshooting.",
        "Needed clearer update": "Customer wanted clearer progress updates while the ticket was open.",
        "Resolution took too long": "Customer expressed concern that resolution time was longer than expected.",
        "Had to follow up multiple times": "Customer had to follow up more than once before closure.",
        "Escalation was necessary": "Customer accepted the fix but noted the case only moved after escalation.",
    }
    if complaint and score <= 2.5:
        return "Resolution took too long", text_lookup["Resolution took too long"]
    if sla_breached and reopened > 0 and score <= 3.0:
        return "Had to follow up multiple times", text_lookup["Had to follow up multiple times"]
    return theme, text_lookup[theme]


def generate_rows() -> pd.DataFrame:
    rows: list[dict] = []
    for year, total in YEAR_TARGETS.items():
        plan = monthly_plan(year, total)
        seq = 1
        for month in range(1, 13):
            for _ in range(plan[month]):
                severity = severity_choice()
                rules = SEVERITY_RULES[severity]
                ticket_type = random.choices(CATEGORIES, weights=[0.25, 0.18, 0.08, 0.15, 0.12, 0.10, 0.12], k=1)[0]
                team = assign_team(ticket_type, severity)
                assignee = random.choice(ASSIGNEES[team])
                channel = random.choices(CHANNELS, weights=[0.34, 0.26, 0.16, 0.16, 0.08], k=1)[0]
                product_module = random.choice(MODULES)
                customer = random.choice(CUSTOMERS)
                requester = customer
                impact = impact_from_severity(severity)
                affected_users = {
                    "High": int(np.random.randint(30, 450)),
                    "Medium": int(np.random.randint(8, 120)),
                    "Low": int(np.random.randint(1, 35)),
                }[impact]

                created_at = pd.Timestamp(year=year, month=month, day=int(np.random.randint(1, 28)), hour=int(np.random.randint(8, 19)), minute=int(np.random.choice([0, 10, 20, 30, 40, 50])))

                pre_breach = random.random() < {"Critical": 0.18, "High": 0.22, "Medium": 0.27, "Low": 0.24}[severity]
                breach_reason = random.choice(BREACH_REASONS) if pre_breach else None
                escalated = random.random() < {"Critical": 0.55, "High": 0.28, "Medium": 0.12, "Low": 0.05}[severity]
                if breach_reason == "Dependency on product team":
                    escalated = True
                waiting_customer_min = choose_waiting_customer(breach_reason)

                actual_response_min = response_minutes(severity, month)
                actual_resolution_min = resolution_minutes(severity, month, waiting_customer_min, escalated)
                response_sla_met = actual_response_min <= rules["response_target_min"]
                resolution_sla_met = actual_resolution_min <= rules["resolution_target_min"]

                if not response_sla_met or not resolution_sla_met:
                    if breach_reason is None:
                        breach_reason = random.choice([r for r in BREACH_REASONS if r != "Waiting for customer" or waiting_customer_min > 0])
                else:
                    breach_reason = None

                reopened_prob = 0.02
                if not resolution_sla_met:
                    reopened_prob += 0.07
                if escalated:
                    reopened_prob += 0.04
                if severity in {"Critical", "High"}:
                    reopened_prob += 0.02
                reopened_count = int(np.random.choice([0, 1, 2, 3], p=[max(0.0, 1 - reopened_prob), min(0.75, reopened_prob * 0.70), min(0.18, reopened_prob * 0.22), min(0.07, reopened_prob * 0.08)]))

                first_response_at = created_at + pd.Timedelta(minutes=actual_response_min)
                resolved_at = created_at + pd.Timedelta(minutes=actual_resolution_min)
                closed_at = resolved_at + pd.Timedelta(minutes=int(np.random.randint(15, 180)))
                overall_sla_status = "Met" if (response_sla_met and resolution_sla_met) else "Breached"
                status = "Done"

                feedback_submitted = random.random() < 0.34
                complaint_signal = (not resolution_sla_met) or reopened_count > 0 or (escalated and severity in {"Critical", "High"})
                score = None
                if feedback_submitted:
                    base_score = 4.55
                    if not response_sla_met:
                        base_score -= 0.55
                    if not resolution_sla_met:
                        base_score -= 0.75
                    if escalated:
                        base_score -= 0.30
                    if reopened_count > 0:
                        base_score -= 0.45 * reopened_count
                    if impact == "High" and overall_sla_status == "Breached":
                        base_score -= 0.30
                    score = max(1.0, min(5.0, round(np.random.normal(base_score, 0.45), 1)))

                sentiment_proxy = "Positive"
                if complaint_signal or (score is not None and score <= 3.0):
                    sentiment_proxy = "Negative"
                elif (not response_sla_met) or (not resolution_sla_met) or escalated or reopened_count > 0:
                    sentiment_proxy = "Neutral"

                comment_theme, comment_text = comment_from_score(
                    score,
                    complaint=complaint_signal,
                    sla_breached=(not response_sla_met or not resolution_sla_met),
                    reopened=reopened_count,
                )

                rows.append(TicketRow(
                    ticket_id=f"TK-{year}-{seq:06d}",
                    year=year,
                    month=month,
                    created_at=created_at,
                    first_response_at=first_response_at,
                    resolved_at=resolved_at,
                    closed_at=closed_at,
                    customer_name=customer,
                    requester=requester,
                    channel=channel,
                    product_module=product_module,
                    ticket_type=ticket_type,
                    severity=severity,
                    priority=rules["priority"],
                    category=ticket_type,
                    status=status,
                    assigned_team=team,
                    assignee=assignee,
                    escalated=escalated,
                    reopened_count=reopened_count,
                    affected_users=affected_users,
                    business_impact=impact,
                    response_target_min=rules["response_target_min"],
                    resolution_target_min=rules["resolution_target_min"],
                    actual_response_min=actual_response_min,
                    actual_resolution_min=actual_resolution_min,
                    response_sla_met=response_sla_met,
                    resolution_sla_met=resolution_sla_met,
                    overall_sla_status=overall_sla_status,
                    breach_reason=breach_reason,
                    waiting_customer_min=waiting_customer_min,
                    closure_feedback_submitted=feedback_submitted,
                    csat_score=score,
                    customer_comment_theme=comment_theme,
                    customer_comment_text=comment_text,
                    customer_sentiment_proxy=sentiment_proxy,
                    complaint_signal=complaint_signal,
                ).__dict__)
                seq += 1
    df = pd.DataFrame(rows)
    return df


def build_kpis(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    overall = pd.DataFrame([
        {
            "total_tickets": len(df),
            "avg_response_min": round(df["actual_response_min"].mean(), 2),
            "avg_resolution_min": round(df["actual_resolution_min"].mean(), 2),
            "response_sla_compliance_rate": round(df["response_sla_met"].mean(), 4),
            "resolution_sla_compliance_rate": round(df["resolution_sla_met"].mean(), 4),
            "overall_sla_compliance_rate": round(((df["response_sla_met"]) & (df["resolution_sla_met"])).mean(), 4),
            "escalation_rate": round(df["escalated"].mean(), 4),
            "reopen_rate": round((df["reopened_count"] > 0).mean(), 4),
            "feedback_submission_rate": round(df["closure_feedback_submitted"].mean(), 4),
            "avg_csat_score": round(df["csat_score"].dropna().mean(), 2),
            "complaint_signal_rate": round(df["complaint_signal"].mean(), 4),
        }
    ])

    by_month = (
        df.groupby(["year", "month"], dropna=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", "mean"),
            resolution_sla_rate=("resolution_sla_met", "mean"),
            escalation_rate=("escalated", "mean"),
            reopen_rate=("reopened_count", lambda s: (s > 0).mean()),
            feedback_submission_rate=("closure_feedback_submitted", "mean"),
            avg_csat_score=("csat_score", "mean"),
            complaint_signal_rate=("complaint_signal", "mean"),
        )
        .reset_index()
    )
    by_month[[c for c in by_month.columns if c.startswith("avg_")]] = by_month[[c for c in by_month.columns if c.startswith("avg_")]].round(2)
    for c in [c for c in by_month.columns if c.endswith("_rate")]:
        by_month[c] = by_month[c].round(4)

    by_severity = (
        df.groupby("severity", dropna=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", "mean"),
            resolution_sla_rate=("resolution_sla_met", "mean"),
            escalation_rate=("escalated", "mean"),
            reopen_rate=("reopened_count", lambda s: (s > 0).mean()),
            feedback_submission_rate=("closure_feedback_submitted", "mean"),
            avg_csat_score=("csat_score", "mean"),
            complaint_signal_rate=("complaint_signal", "mean"),
        )
        .reset_index()
    )
    by_severity[[c for c in by_severity.columns if c.startswith("avg_")]] = by_severity[[c for c in by_severity.columns if c.startswith("avg_")]].round(2)
    for c in [c for c in by_severity.columns if c.endswith("_rate")]:
        by_severity[c] = by_severity[c].round(4)

    by_customer = (
        df.groupby("customer_name", dropna=False)
        .agg(
            tickets=("ticket_id", "count"),
            avg_response_min=("actual_response_min", "mean"),
            avg_resolution_min=("actual_resolution_min", "mean"),
            response_sla_rate=("response_sla_met", "mean"),
            resolution_sla_rate=("resolution_sla_met", "mean"),
            escalation_rate=("escalated", "mean"),
            reopen_rate=("reopened_count", lambda s: (s > 0).mean()),
            feedback_count=("closure_feedback_submitted", "sum"),
            avg_csat_score=("csat_score", "mean"),
            complaint_signal_rate=("complaint_signal", "mean"),
        )
        .reset_index()
        .sort_values(["tickets", "complaint_signal_rate"], ascending=[False, False])
    )
    by_customer[[c for c in by_customer.columns if c.startswith("avg_")]] = by_customer[[c for c in by_customer.columns if c.startswith("avg_")]].round(2)
    for c in [c for c in by_customer.columns if c.endswith("_rate")]:
        by_customer[c] = by_customer[c].round(4)

    feedback_direct = df[df["closure_feedback_submitted"]].copy()
    feedback_direct = feedback_direct[[
        "ticket_id", "year", "month", "customer_name", "severity", "category", "overall_sla_status",
        "reopened_count", "escalated", "csat_score", "customer_comment_theme", "customer_comment_text"
    ]]

    feedback_proxy = (
        df.groupby(["year", "month"], dropna=False)
        .agg(
            tickets=("ticket_id", "count"),
            negative_proxy_count=("customer_sentiment_proxy", lambda s: (s == "Negative").sum()),
            neutral_proxy_count=("customer_sentiment_proxy", lambda s: (s == "Neutral").sum()),
            positive_proxy_count=("customer_sentiment_proxy", lambda s: (s == "Positive").sum()),
            complaint_signal_count=("complaint_signal", "sum"),
            reopen_count=("reopened_count", lambda s: (s > 0).sum()),
            escalated_count=("escalated", "sum"),
            breached_count=("overall_sla_status", lambda s: (s == "Breached").sum()),
        )
        .reset_index()
    )

    return {
        "KPI_Overall": overall,
        "KPI_By_Month": by_month,
        "KPI_By_Severity": by_severity,
        "KPI_By_Customer": by_customer,
        "Feedback_Direct": feedback_direct,
        "Feedback_Proxy": feedback_proxy,
    }


def build_data_dictionary() -> pd.DataFrame:
    rows = [
        ["ticket_id", "Unique ticket ID", "All 3 sections"],
        ["actual_response_min", "Minutes from created_at to first_response_at", "2.3.1, 2.3.2"],
        ["actual_resolution_min", "Minutes from created_at to resolved_at", "2.3.1, 2.3.2"],
        ["response_sla_met", "Whether response time met target", "2.3.2"],
        ["resolution_sla_met", "Whether resolution time met target", "2.3.2"],
        ["overall_sla_status", "Met only when both response and resolution SLA are met", "2.3.2"],
        ["reopened_count", "How many times the ticket was reopened", "2.3.1, 2.3.2, 2.3.3 indirect"],
        ["escalated", "Whether the case was escalated", "2.3.2, 2.3.3 indirect"],
        ["closure_feedback_submitted", "Whether direct closure feedback exists", "2.3.3 direct"],
        ["csat_score", "Synthetic direct satisfaction score from 1.0 to 5.0", "2.3.3 direct"],
        ["customer_comment_theme", "Short coded theme of direct feedback", "2.3.3 direct"],
        ["customer_comment_text", "Short generated feedback comment", "2.3.3 direct"],
        ["customer_sentiment_proxy", "Proxy label inferred from SLA breach, escalation, reopen, and feedback", "2.3.3 indirect"],
        ["complaint_signal", "Proxy flag for likely dissatisfaction", "2.3.3 indirect"],
    ]
    return pd.DataFrame(rows, columns=["field", "definition", "main_use"])


def build_sla_targets() -> pd.DataFrame:
    rows = []
    for sev, vals in SEVERITY_RULES.items():
        rows.append(
            {
                "severity": sev,
                "priority": vals["priority"],
                "response_target_min": vals["response_target_min"],
                "resolution_target_min": vals["resolution_target_min"],
                "response_target_days": round(vals["response_target_min"] / 1440, 3),
                "resolution_target_days": round(vals["resolution_target_min"] / 1440, 3),
            }
        )
    return pd.DataFrame(rows)


def write_excel(df: pd.DataFrame, kpis: dict[str, pd.DataFrame]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "smartlog_service_analysis_2023_2025.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Ticket_Raw", index=False)
        build_sla_targets().to_excel(writer, sheet_name="SLA_Targets", index=False)
        build_data_dictionary().to_excel(writer, sheet_name="Data_Dictionary", index=False)
        for name, frame in kpis.items():
            frame.to_excel(writer, sheet_name=name[:31], index=False)
    return out_path


def main() -> None:
    df = generate_rows()
    kpis = build_kpis(df)
    xlsx_path = write_excel(df, kpis)

    csv_dir = OUTPUT_DIR / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_dir / "Ticket_Raw.csv", index=False)
    for name, frame in kpis.items():
        frame.to_csv(csv_dir / f"{name}.csv", index=False)

    print(f"Created: {xlsx_path}")
    print(f"Rows: {len(df):,}")


if __name__ == "__main__":
    main()
