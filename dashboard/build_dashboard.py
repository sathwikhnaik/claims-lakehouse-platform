# dashboard/build_dashboard.py
import os
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

load_dotenv()

def fetch_data():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        warehouse="CLAIMS_WH",
        database="CLAIMS_PLATFORM",
        schema="SERVING",
    )
    df = pd.read_sql(
        "SELECT * FROM mart_fraud_signals ORDER BY claim_date", conn
    )
    conn.close()
    return df

def build_dashboard(df, output_path="dashboard/claims_dashboard.html"):
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Daily Billed Amount by Provider", "Flagged Billing Spikes"),
        vertical_spacing=0.15,
    )

    for provider_id, group in df.groupby("PROVIDER_ID"):
        fig.add_trace(
            go.Scatter(
                x=group["CLAIM_DATE"], y=group["AVG_BILLED_AMOUNT"],
                mode="lines", name=str(provider_id)[:8], showlegend=False,
                line=dict(width=1),
            ),
            row=1, col=1,
        )

    spikes = df[df["IS_BILLING_SPIKE"] == True]
    fig.add_trace(
        go.Scatter(
            x=spikes["CLAIM_DATE"], y=spikes["SPIKE_RATIO"],
            mode="markers", name="Flagged Spike",
            marker=dict(size=10, color="red", symbol="x"),
        ),
        row=2, col=1,
    )

    fig.update_layout(
        title="Claims Fraud Signal Dashboard",
        height=800,
        template="plotly_white",
    )
    fig.write_html(output_path, include_plotlyjs="cdn")
    print(f"Dashboard written to {output_path}")

if __name__ == "__main__":
    data = fetch_data()
    build_dashboard(data)