import pandas as pd
import sqlite3
import re
import io
import datetime
from data_handler import load_any_file, analyze_data
from pdf_export import export_analysis_to_pdf


def mysql_to_sqlite(sql_text: str) -> str:
    """Convert MySQL-specific syntax to SQLite-compatible SQL."""
    sql_text = re.sub(r'\bAUTO_INCREMENT\b', '', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'ENGINE\s*=\s*\w+\s*', '', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'DEFAULT CHARSET=\w+', '', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'CHARACTER SET\s+\w+', '', sql_text, flags=re.IGNORECASE)
    sql_text = sql_text.replace('`', '"')
    sql_text = re.sub(r'\bint\(\d+\)', 'INTEGER', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'UNSIGNED', '', sql_text, flags=re.IGNORECASE)
    return sql_text


def load_sql_file(file_path: str) -> dict:
    """Load SQL file into in-memory SQLite and return dict of DataFrames."""
    df_list = {}
    with open(file_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    sql_text = mysql_to_sqlite(sql_text)
    conn = sqlite3.connect(":memory:")
    conn.executescript(sql_text)

    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'", conn
    )
    if tables.empty:
        print("❌ No tables found in SQL file.")
        return {}

    for table in tables["name"].tolist():
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        df_list[table] = df

        # Save as Excel
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"{table}_{timestamp}.xlsx"
        with pd.ExcelWriter(fname, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=table)
        print(f"💾 Saved table '{table}' to {fname}")

    return df_list


def main():
    print("📊 AI Data Analysis Script")
    file_path = input("Enter the path to your data file: ").strip()

    if not file_path:
        print("❌ No file path provided. Exiting.")
        return

    df_list = {}

    try:
        if file_path.lower().endswith(".sql"):
            df_list = load_sql_file(file_path)
            if df_list:
                for name, df in df_list.items():
                    print(f"\nTable: {name}")
                    print(df.head())
        else:
            df = load_any_file(file_path)
            if df is not None:
                df_list["Uploaded Data"] = df
                print("\n### Data Preview")
                print(df.head())
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return

    if not df_list:
        print("❌ No data loaded. Exiting.")
        return

    # Dataset selection
    if len(df_list) > 1:
        print("\nAvailable datasets:")
        for idx, name in enumerate(df_list.keys(), start=1):
            print(f"{idx}. {name}")
        try:
            choice = int(input("Select dataset number: ").strip())
            selected_df_key = list(df_list.keys())[choice - 1]
        except (ValueError, IndexError):
            print("❌ Invalid choice. Exiting.")
            return
    else:
        selected_df_key = list(df_list.keys())[0]

    df_for_analysis = df_list[selected_df_key]

    # Query input
    query = input("\nEnter your analysis question: ").strip()
    if not query:
        print("⚠️ No query provided. Exiting.")
        return

    # AI Analysis
    try:
        analysis_result = analyze_data(df_for_analysis, query)
        print("\n### AI Analysis Result")
        if not analysis_result or (
            isinstance(analysis_result, str) and not analysis_result.strip()
        ):
            print("⚠️ No analysis result returned.")
        else:
            print(analysis_result)
    except Exception as e:
        print(f"⚠️ Error during analysis: {e}")
        return

    # Export to PDF
    export_choice = input("\nExport result to PDF? (y/n): ").strip().lower()
    if export_choice == "y":
        success, pdf_buffer = export_analysis_to_pdf(str(analysis_result))
        if success:
            with open("ai_analysis_report.pdf", "wb") as f:
                f.write(pdf_buffer)
            print("📄 PDF report saved as 'ai_analysis_report.pdf'")
        else:
            print(f"❌ Failed to export PDF: {pdf_buffer}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(f"Unexpected error: {ex}")
        import traceback
        print(traceback.format_exc())
