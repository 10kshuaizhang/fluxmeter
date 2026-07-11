from __future__ import annotations

from intelligence.forecast import compute_forecast
from intelligence.pricing_optimizer import compute_pricing_recommendations
from intelligence.profitability import build_profitability_dashboard
from intelligence.root_cause import analyze_root_cause
from intelligence.unit_economics import compute_unit_economics


def _prior_period(period: str) -> str:
    year, month = int(period[:4]), int(period[5:7])
    if month == 1:
        return f"{year - 1}-12"
    return f"{year:04d}-{month - 1:02d}"


def build_report_json(r, *, period: str, baseline_period: str | None = None) -> dict:
    baseline = baseline_period or _prior_period(period)
    rc = analyze_root_cause(r, period=period, baseline_period=baseline, scope="global")
    economics = compute_unit_economics(r, period=period)
    loss_customers = [e.model_dump() for e in economics if e.status == "loss"]
    profitability = build_profitability_dashboard(r, period=period, months=3)
    recommendations = compute_pricing_recommendations(r, period=period)[:5]
    forecast = compute_forecast(r, period=period, scope="global")

    headline = rc.summary
    if loss_customers:
        headline = f"{rc.summary} {len(loss_customers)} customer(s) unprofitable."

    return {
        "period": period,
        "baseline_period": baseline,
        "headline": headline,
        "root_cause": rc.model_dump(),
        "loss_customers": loss_customers,
        "profitability": profitability.model_dump(),
        "top_recommendations": [rec.model_dump() for rec in recommendations],
        "forecast": forecast.model_dump(),
    }


def build_report_markdown(r, *, period: str, baseline_period: str | None = None) -> str:
    data = build_report_json(r, period=period, baseline_period=baseline_period)
    lines = [
        f"# FluxMeter Intelligence Report — {period}",
        "",
        "## Executive Summary",
        data["headline"],
        "",
        "## Root Cause",
        data["root_cause"]["summary"],
        "",
        "## Loss Customers",
    ]
    if not data["loss_customers"]:
        lines.append("_None_")
    else:
        lines.append("| Customer | Revenue | Cost | Margin |")
        lines.append("|----------|---------|------|--------|")
        for c in data["loss_customers"]:
            lines.append(
                f"| {c['customer_id']} | ${c.get('revenue_usd', 0):.2f} | "
                f"${c['cost_usd']:.2f} | ${c.get('margin_usd', 0):.2f} |"
            )

    lines.extend(["", "## Top Pricing Recommendations"])
    if not data["top_recommendations"]:
        lines.append("_None_")
    else:
        for rec in data["top_recommendations"]:
            roi = rec.get("roi_annual_usd")
            roi_s = f"${roi:,.0f}/yr" if roi is not None else "n/a"
            lines.append(f"- **{rec['customer_id']}** ({rec['action']}): {rec['suggested_change']} — ROI {roi_s}")

    fc = data["forecast"]
    lines.extend(
        [
            "",
            "## Forecast vs Budget",
            fc["summary"],
            f"- MTD: ${fc['mtd_cost_usd']:.2f}",
            f"- Forecast EOM: ${fc['forecast_eom_cost_usd']:.2f}",
            f"- Status: {fc['status']}",
        ]
    )
    return "\n".join(lines) + "\n"
