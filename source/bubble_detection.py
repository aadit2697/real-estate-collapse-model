import pandas as pd
import numpy as np
from source.utils.snowflake_connector import SnowflakeConnector
from sqlalchemy import text

class BubbleDetector:
    def __init__(self):
        self.sf_connector = SnowflakeConnector()

    def load_data(self):
        engine = self.sf_connector.get_engine()
        query = """
            SELECT
                PERIOD AS date_key,
                QUARTERLY_AVG_HOME_PRICE_INDEX AS price_index,
                QUARTERLY_AVG_MORTGAGE_RATE AS mortgage_rate
            FROM housing_market_quarterly_combined
            ORDER BY PERIOD
        """
        df = pd.read_sql(query, engine, parse_dates=['date_key'])
        return df.set_index('date_key')

    def calculate_enhanced_bubble_scores(self, input_df=None):
        df = input_df if input_df is not None else self.load_data()
        scores = []

        price = df['price_index']
        rate = df['mortgage_rate']
        growth = price.pct_change(4)
        growth_accel = growth.diff().rolling(2).mean()
        z = (price - price.rolling(20).mean()) / price.rolling(20).std()
        momentum = price.pct_change(1).rolling(3).mean() > 0
        corr = price.rolling(4).corr(rate)

        for i in range(20, len(df)):
            score = 0
            g = growth.iloc[i]
            zscore = z.iloc[i]
            m = momentum.iloc[i]
            c = corr.iloc[i]
            accel = growth_accel.iloc[i]
            notes = []

            if g > 0.25:
                score += 30
                notes.append("Growth > 25%")
            elif g > 0.20:
                score += 25
                notes.append("Growth > 20%")
            elif g > 0.15:
                score += 20
                notes.append("Growth > 15%")
            elif g > 0.10:
                score += 10
                notes.append("Growth > 10%")
            elif g > 0.05:
                score += 5
                notes.append("Growth > 5%")

            if accel > 0.03:
                score += 5
                notes.append("Acceleration > 3%")

            if abs(zscore) > 3:
                score += 25
                notes.append("Z > 3")
            elif abs(zscore) > 2:
                score += 15
                notes.append("Z > 2")
            elif abs(zscore) > 1:
                score += 5
                notes.append("Z > 1")

            if m:
                score += 15
                notes.append("Momentum Positive")

            if not np.isnan(c):
                if abs(c) > 0.8:
                    score += 20
                    notes.append("Corr > 0.8")
                elif abs(c) > 0.6:
                    score += 10
                    notes.append("Corr > 0.6")

            if g > 0.15 and abs(zscore) > 2:
                score += 10
                notes.append("Compound Growth+Deviation")

            scores.append({
                'date_key': df.index[i],
                'risk_score': score,
                'risk_level': 'High' if score > 60 else 'Medium' if score > 40 else 'Low',
                'notes': "; ".join(notes),
                'run_type': 'bulk',
                'calculation_timestamp': pd.Timestamp.now()
            })

        return pd.DataFrame(scores)

    def store_bulk_scores(self, df_scores):
        engine = self.sf_connector.get_engine()
        create_stmt = text("""
            CREATE TABLE IF NOT EXISTS bubble_risk_scores (
                date_key DATE,
                risk_score FLOAT,
                risk_level STRING,
                notes STRING,
                run_type STRING,
                calculation_timestamp TIMESTAMP
            )
        """)
        with engine.begin() as conn:
            conn.execute(create_stmt)

        df_scores.to_sql("bubble_risk_scores", engine, if_exists="append", index=False)
        print("✅ Bulk bubble risk scores stored in Snowflake.")

    def calculate_latest_score(self):
        df = self.load_data()
        recent_df = df.iloc[-25:].copy()  # use enough rows for indicators
        score_df = self.calculate_enhanced_bubble_scores(recent_df)
        latest = score_df.iloc[-1:].copy()
        latest['run_type'] = 'live'
        latest['calculation_timestamp'] = pd.Timestamp.now()
        return latest

    def store_single_score(self, latest_score_df):
        engine = self.sf_connector.get_engine()
        latest_score_df.to_sql("bubble_risk_scores", engine, if_exists="append", index=False)
        print("✅ Latest single risk score stored in Snowflake.")
