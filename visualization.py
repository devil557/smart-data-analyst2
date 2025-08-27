# visualization.py
import plotly.express as px
import pandas as pd

def generate_chart(result):
    """Generate a Plotly chart if result is a DataFrame with at least 2 columns."""
    if isinstance(result, pd.DataFrame) and not result.empty:
        try:
            # Identify numeric columns for plotting
            numeric_cols = result.select_dtypes(include="number").columns
            if len(result.columns) < 2 or len(numeric_cols) == 0:
                print("Not enough suitable columns to generate a chart.")
                return None

            x_col = result.columns[0]
            y_col = numeric_cols[0] if numeric_cols[0] != x_col else (
                numeric_cols[1] if len(numeric_cols) > 1 else None
            )

            if y_col is None:
                print("No numeric column found for y-axis.")
                return None

            print(f"Generated Chart: x={x_col}, y={y_col}")
            fig = px.line(result, x=x_col, y=y_col, title="Auto Chart")
            return fig
        except Exception as e:
            print(f"Could not generate chart: {e}")
            return None
    else:
        print("Result is not a valid DataFrame or is empty.")
        return None
