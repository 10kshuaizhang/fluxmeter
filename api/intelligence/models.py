from pydantic import BaseModel


class Contributor(BaseModel):
    dimension: str
    key: str
    current_cost_usd: float
    baseline_cost_usd: float
    delta_usd: float
    delta_pct: float
    share_of_total_delta_pct: float


class RootCauseReport(BaseModel):
    period: str
    baseline_period: str
    total_cost_usd: float
    baseline_cost_usd: float
    delta_usd: float
    delta_pct: float
    summary: str
    contributors: list[Contributor]


class CustomerEconomics(BaseModel):
    customer_id: str
    period: str
    revenue_usd: float | None
    cost_usd: float
    margin_usd: float | None
    margin_pct: float | None
    status: str  # profitable | loss | unknown_revenue
    recommendation: str | None


class SimulationResult(BaseModel):
    scenario: str
    annual_savings_usd: float | None
    annual_profit_delta_usd: float | None
    notes: str


class SimulationRequest(BaseModel):
    scenario: str  # model_switch | prompt_reduction | token_grant
    input_tokens: int | None = None
    output_tokens: int | None = None
    from_model: str | None = None
    to_model: str | None = None
    cost_usd: float | None = None
    input_reduction_pct: float | None = None
    grant_tokens: int | None = None
    signup_lift_pct: float | None = None
    avg_revenue_per_customer_usd: float | None = None
    customer_count: int | None = None
    monthly_occurrences: int = 1


class PricingRecommendation(BaseModel):
    customer_id: str
    period: str
    action: str  # price_increase | model_switch | connect_revenue
    current_margin_pct: float | None
    suggested_change: str
    roi_annual_usd: float | None
    confidence: str  # high | medium


class ProductEconomics(BaseModel):
    product: str
    period: str
    cost_usd: float
    revenue_usd: float | None
    margin_usd: float | None
    margin_pct: float | None


class PeriodTotals(BaseModel):
    period: str
    cost_usd: float
    revenue_usd: float | None
    margin_usd: float | None


class ProfitabilityDashboard(BaseModel):
    period: str
    months: int
    totals: dict
    by_customer: list[CustomerEconomics]
    by_product: list[ProductEconomics]
    trend: list[PeriodTotals]


class SpendForecast(BaseModel):
    period: str
    scope: str
    mtd_cost_usd: float
    forecast_eom_cost_usd: float
    budget_usd: float | None
    variance_usd: float | None
    status: str  # on_track | at_risk | over_budget | no_budget
    summary: str


class IntelAlertPayload(BaseModel):
    type: str
    period: str
    summary: str
    recommendation: str | None
    actions: list[dict[str, str]]
