import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from source.utils.snowflake_connector import SnowflakeConnector
from sqlalchemy import text

class HousingMarketPredictor:
    def __init__(self):
        self.sf_connector = SnowflakeConnector()
        self.models = {}
        self.predictions = {}
        self.metrics = {}
        self.scaler = StandardScaler()

    def load_training_data(self):
        engine = self.sf_connector.get_engine()
        query = """
            SELECT
                PERIOD AS date_key,
                QUARTERLY_AVG_HOME_PRICE_INDEX AS price_index,
                QUARTERLY_AVG_MORTGAGE_RATE AS mortgage_rate,
                UNEMPLOYMENT_RATE AS unemployment,
                CONSUMER_PRICE_INDEX AS cpi,
                ONE_FAMILY_TOTAL,
                TOTAL_UNITS_IN_BUILDINGS_2PLUS,
                PURPOSE_OF_CONSTRUCTION_BUILT_FOR_SALE_FEE_SIMPLE
            FROM housing_market_quarterly_combined
            ORDER BY PERIOD
        """
        df = pd.read_sql(query, engine, parse_dates=['date_key'])
        print("Loaded columns:", df.columns.tolist())

        # ✅ Data Cleaning Steps
        df = df[df['price_index'].notna()]
        df['purpose_of_construction_built_for_sale_fee_simple'] = (
            df['purpose_of_construction_built_for_sale_fee_simple'].fillna(0)
        )

        print("✅ Training data loaded and cleaned.")
        return df

    def prepare_features(self, df):
        df_features = df.copy()
        df_features['year'] = df_features['date_key'].dt.year
        df_features['quarter'] = df_features['date_key'].dt.quarter

        # ✅ Lag Features Creation
        for lag in [1, 3]:
            df_features[f'price_lag_{lag}'] = df_features['price_index'].shift(lag)
            df_features[f'mortgage_lag_{lag}'] = df_features['mortgage_rate'].shift(lag)

        print("✅ Lag features created.")
        return df_features.dropna()

    def calculate_adjusted_r2(self, r2, n, p):
        return 1 - ((1 - r2) * (n - 1)) / (n - p - 1)

    def train_models(self):
        print("🚀 Starting training...")
        data = self.load_training_data()
        df = self.prepare_features(data)

        feature_cols = [col for col in df.columns if col not in ['date_key', 'price_index']]
        X = df[feature_cols]
        y = df['price_index']

        window_size = int(len(X) * 0.8)
        walk_results = {
            'linear': [],
            'ridge': [],
            'lasso': []
        }

        for start in range(0, len(X) - window_size):
            end = start + window_size
            X_train, X_test = X.iloc[start:end], X.iloc[end:end+1]
            y_train, y_test = y.iloc[start:end], y.iloc[end:end+1]
            date_test = df.iloc[end]['date_key']

            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)

            models = {
                'linear': LinearRegression(),
                'ridge': Ridge(alpha=1.0),
                'lasso': Lasso(alpha=0.1)
            }

            for name, model in models.items():
                print(f"\n📊 Training {name.title()} model...")
                model.fit(X_train_scaled, y_train)
                pred = model.predict(X_test_scaled)[0]
                walk_results[name].append((date_test, y_test.values[0], pred))

        def smape(actual, pred):
            actual = np.array(actual)
            pred = np.array(pred)
            return 100 * np.mean(2 * np.abs(pred - actual) / (np.abs(actual) + np.abs(pred)))

        for name, results in walk_results.items():
            dates, actuals, preds = zip(*results)
            n = len(actuals)
            p = X.shape[1]
            r2 = r2_score(actuals, preds)
            adj_r2 = self.calculate_adjusted_r2(r2, n, p)
            self.metrics[name] = {
                'MSE': mean_squared_error(actuals, preds),
                'RMSE': np.sqrt(mean_squared_error(actuals, preds)),
                'R2': r2_score(actuals, preds),
                'Adjusted_R2': adj_r2,
                'SMAPE': smape(actuals, preds)
            }
            print(f"📥 Storing predictions for {name.title()} model...")
            self.store_predictions(dates, actuals, preds, model_name=name)

        return self.metrics

    def store_predictions(self, dates, actuals, preds, model_name):
        engine = self.sf_connector.get_engine()

        # ✅ Ensure table exists before inserting
        create_stmt = text("""
            CREATE TABLE IF NOT EXISTS model_predictions (
                date_key DATE,
                model_name STRING,
                predicted_price FLOAT,
                actual_price FLOAT,
                prediction_timestamp TIMESTAMP
            )
        """)
        with engine.begin() as conn:
            conn.execute(create_stmt)
        print("✅ model_predictions table ensured in Snowflake.")

        # Append predictions
        pred_df = pd.DataFrame({
            'date_key': dates,
            'model_name': model_name,
            'predicted_price': preds,
            'actual_price': actuals,
            'prediction_timestamp': pd.Timestamp.now()
        })
        pred_df.to_sql('model_predictions', engine, if_exists='append', index=False)
        # pred_df.to_sql('model_predictions', engine, if_exists='replace', index=False)
        print(f"✅ Predictions for {model_name.title()} appended to Snowflake.")

    def get_predictions_df(self, model_name):
        """
        Load model predictions from Snowflake for a specific model.
        """
        engine = self.sf_connector.get_engine()
        query = f"""
            SELECT
                date_key,
                predicted_price,
                actual_price,
                prediction_timestamp
            FROM model_predictions
            WHERE model_name = '{model_name}'
            ORDER BY date_key
        """
        return pd.read_sql(query, engine, parse_dates=['date_key'])

if __name__ == "__main__":
    predictor = HousingMarketPredictor()
    metrics = predictor.train_models()
    print("\n📈 Model Performance Metrics (Walk-Forward Simulation):")
    for model, metric in metrics.items():
        print(f"\n{model.title()}:")
        for name, value in metric.items():
            print(f"{name}: {value:.4f}")

