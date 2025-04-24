import streamlit as st
import pandas as pd
import plotly.express as px
from source.bubble_detection import BubbleDetector
from source.market_predictor import HousingMarketPredictor

st.set_page_config(page_title="🏠 Housing Market Dashboard", layout="wide")

st.title("📊 Housing Market Trends & Bubble Detection")

# Tabs for clean separation
tabs = st.tabs(["📈 Market Forecasting", "💥 Bubble Detection"])

# ---------------------------
# TAB 1: MARKET FORECASTING
# ---------------------------
with tabs[0]:
    st.subheader("🏷️ Model Performance & Forecast")

    st.markdown("""
    This section shows how well different machine learning models predict changes in the **U.S. Housing Price Index**.

    ✅ We use three models: **Linear Regression**, **Ridge**, and **Lasso**.
    
    🔍 The goal is to predict how housing prices evolve over time based on historical data. We evaluate each model using:
    - **RMSE (Root Mean Squared Error)** – lower means more accurate predictions
    - **Adjusted R²** – explains how much variance is captured, adjusted for model complexity
    - **SMAPE (Symmetric Mean Absolute Percentage Error)** – a stable, percentage-based accuracy metric

    Use the dropdown menu to switch between models and explore how well they align with actual prices over time.
    """)

    predictor = HousingMarketPredictor()
    metrics = predictor.train_models()
    model_choice = st.selectbox("Choose a model to visualize", list(metrics.keys()))
    pred_df = predictor.get_predictions_df(model_choice)

    pred_df['date_key'] = pd.to_datetime(pred_df['date_key'])

    fig = px.line(pred_df, x='date_key', y=['actual_price', 'predicted_price'],
              labels={'value': 'Price Index', 'date_key': 'Date', 'variable': 'Legend'},
              title=f"{model_choice.title()} Predictions vs Actual")

    # fig = px.line(pred_df, x='date_key', y=['actual_price', 'predicted_price'],
    #               labels={'value': 'Home Price Index', 'date_key': 'Date'},
    #               title=f"📉 {model_choice.title()} Predictions vs Actual")
    st.plotly_chart(fig, use_container_width=True)

    st.metric("RMSE", f"{metrics[model_choice]['RMSE']:.2f}")
    st.metric("Adjusted R²", f"{metrics[model_choice]['Adjusted_R2']:.3f}")
    st.metric("SMAPE", f"{metrics[model_choice]['SMAPE']:.2f}%")

    with st.expander("📘 How to Interpret The Above Metrics"):
        rmse = metrics[model_choice]["RMSE"]
        adj_r2 = metrics[model_choice]["Adjusted_R2"]
        smape = metrics[model_choice]["SMAPE"]

        # Interpretations based on threshold ranges
        if rmse < 5:
            rmse_comment = "very close to actual price movements (Excellent fit)."
        elif rmse < 10:
            rmse_comment = "reasonably close to actual values (Good fit)."
        else:
            rmse_comment = "deviates significantly from actual values (Needs improvement)."

        if adj_r2 > 0.95:
            r2_comment = f"explains {adj_r2 * 100:.1f}% of market variation (Excellent explanatory power)."
        elif adj_r2 > 0.85:
            r2_comment = f"explains {adj_r2 * 100:.1f}% of variation (Good, but could be better)."
        else:
            r2_comment = f"explains only {adj_r2 * 100:.1f}% of variation (Model may be underfitting)."

        if smape < 5:
            smape_comment = "low percentage error (Model is highly accurate)."
        elif smape < 15:
            smape_comment = "moderate percentage error (Accuracy of the model is acceptable)."
        else:
            smape_comment = "high percentage error (Model is struggling to capture trends, requires data investigation)."

        st.markdown(f"""
        - **RMSE ~{rmse:.2f}** → {rmse_comment}  
        - **Adjusted R² ~{adj_r2:.3f}** → {r2_comment}  
        - **SMAPE ~{smape:.2f}%** → {smape_comment}
        """)

# ---------------------------
# TAB 2: BUBBLE DETECTION
# ---------------------------
with tabs[1]:
    st.subheader("💥 Housing Bubble Risk Monitor")

    st.markdown("""
    This section analyzes whether the housing market may be experiencing a **bubble**—a situation where home prices rise too fast and may not be sustainable.

    ✅ Our risk score is based on four core signals:
    - **Annual Price Growth**: Is the price increasing rapidly over a year?
    - **Deviation from Long-Term Average**: Is the price significantly higher than its historical average?
    - **Momentum**: Are prices consistently rising over recent months?
    - **Mortgage Rate Correlation**: Is there a strong relationship between prices and mortgage rates?

    📊 A score is computed every quarter (3 months):
    - **0–40** → Low Risk
    - **41–60** → Medium Risk
    - **61–100** → High Risk (possible bubble forming)

    > 🔍 **What is a Housing Bubble?**  
    A housing bubble occurs when home prices rise rapidly due to demand, speculation, and market exuberance—often beyond what fundamentals like income or mortgage rates support. Bubbles can lead to market corrections or crashes when the prices can no longer be sustained.

    > 📌 **Important Note:**  
    Our system does **not predict the exact timing of a crash**.  
    Instead, it detects the **buildup of risk**—early warning signs that a bubble **might be forming**.  
    This includes rising prices, volatility, and unsustainable momentum—**not the aftermath** (the crash itself).
                
    > 🧠 **How to Interpret the Score Trend**  
    - A **rising score** suggests that a **housing bubble is forming**, with increasing signs of risk and overheating in the market.  
    - A **falling score** often signals the **start of a correction or crash** — the market is losing momentum or stability.  
    - If the score drops to **0**, it usually means that **the market has already crashed or entered a deep correction**, with risk factors fully reset (e.g., no more price appreciation, no speculative momentum, no deviation from average).

    So the **trajectory** of the risk score is as important as the score itself — it helps detect both **the buildup and dissipation** of housing market risk.

    You can also generate a **new score for the most recent data** using the "Get Latest Score" button below.
    """)

    detector = BubbleDetector()
    df_scores = detector.calculate_enhanced_bubble_scores()

    # fig2 = px.line(df_scores, x='date_key', y='risk_score',
    #                color='risk_level',
    #                title="🏠 Bubble Risk Score Over Time",
    #                labels={'date_key': 'Date', 'risk_score': 'Risk Score'})

    # fig2.add_hline(y=60, line_dash="dash", line_color="red", annotation_text="High Risk")
    # fig2.add_hline(y=40, line_dash="dash", line_color="orange", annotation_text="Moderate Risk")

    # ✅ Plot a single line (continuous)
    fig2 = px.line(df_scores, x='date_key', y='risk_score',
                title="🏠 Bubble Risk Score Over Time",
                labels={'date_key': 'Date', 'risk_score': 'Risk Score'})

    # Add background bands for zones
    fig2.add_hline(y=60, line_dash="dash", line_color="red", annotation_text="High Risk")
    fig2.add_hline(y=40, line_dash="dash", line_color="orange", annotation_text="Moderate Risk")

    color_map = {'Low': 'blue', 'Medium': 'orange', 'High': 'red'}

    df_latest = df_scores.tail(20)  # last 20 quarters (~5 years)
    fig2.add_scatter(
        x=df_latest['date_key'],
        y=df_latest['risk_score'],
        mode='markers',
        marker=dict(
            color=[color_map[r] for r in df_latest['risk_level']],
            size=8,
            line=dict(width=1, color='black')  # Optional styling
        ),
        name='Recent Risk Points'
    )

    st.plotly_chart(fig2, use_container_width=True)

    latest = df_scores.iloc[-1]
    st.metric("Latest Risk Score", f"{latest['risk_score']} ({latest['risk_level']})")
    st.caption(f"As of {latest['date_key'].date()}")

    with st.expander("🔎 See latest score explanation"):
        st.write(latest['notes'])

    if st.button("🔁 Get Latest Live Score"):
        new_score = detector.calculate_latest_score()
        detector.store_single_score(new_score)
        st.success(f"✅ New score ({new_score.iloc[0]['risk_score']}) stored as live run.")
        st.write(new_score[['date_key', 'risk_score', 'risk_level', 'notes']])
