"""Reusable KPI card component for Streamlit."""

from __future__ import annotations

import streamlit as st


def render_kpi_row(kpis: list[dict]) -> None:
    """Render a row of KPI metric cards.

    Each item in *kpis* must have keys: label, value.
    Optional keys: delta, delta_color ("normal"|"inverse"|"off").
    """
    cols = st.columns(len(kpis))
    for col, kpi in zip(cols, kpis):
        with col:
            st.metric(
                label=kpi["label"],
                value=kpi["value"],
                delta=kpi.get("delta"),
                delta_color=kpi.get("delta_color", "normal"),
            )
