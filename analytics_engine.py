import pandas as pd
import numpy as np
import pandasql as psql
from datetime import datetime


class AnalyticsEngine:

    def load_data(self, filepath):
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

        # auto detect date columns
        for col in df.columns:
            if "date" in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass

        return df

    def full_analysis(self, df):
        results = {}
        results["overview"] = self._overview(df)
        results["kpis"] = self._kpis(df)
        results["trends"] = self._trends(df)
        results["segmentation"] = self._segmentation(df)
        results["anomalies"] = self._anomalies(df)
        results["generated_at"] = datetime.now().isoformat()
        return results

    def _overview(self, df):
        return {
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
            "missing_values": int(df.isnull().sum().sum())
        }

    def _kpis(self, df):
        kpis = {}

        if "revenue" in df.columns:
            kpis["total_revenue"] = float(df["revenue"].sum())

        if "profit" in df.columns:
            kpis["total_profit"] = float(df["profit"].sum())

        if "deal_closed" in df.columns:
            kpis["win_rate"] = float(df["deal_closed"].mean() * 100)

        return kpis

    def _trends(self, df):
        if "date" in df.columns and "revenue" in df.columns:
            df["month"] = df["date"].dt.to_period("M").astype(str)
            monthly = df.groupby("month")["revenue"].sum().reset_index()
            return monthly.to_dict(orient="records")
        return []

    def _segmentation(self, df):
        if "region" in df.columns and "revenue" in df.columns:
            grouped = df.groupby("region")["revenue"].sum().reset_index()
            return grouped.to_dict(orient="records")
        return []

    def _anomalies(self, df):
        if "revenue" in df.columns:
            mean = df["revenue"].mean()
            std = df["revenue"].std()
            df["zscore"] = (df["revenue"] - mean) / std
            outliers = df[np.abs(df["zscore"]) > 2.5]
            return outliers.head(5).to_dict(orient="records")
        return []

    def run_sql_query(self, df, query):
        result = psql.sqldf(query, {"data": df})
        return result.head(100).to_dict(orient="records")