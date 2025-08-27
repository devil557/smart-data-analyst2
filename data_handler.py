import pandas as pd
import re
import yaml
import zipfile
from io import BytesIO
from pandasai import SmartDataframe
from pandasai_openai import OpenAI
from config import OPENAI_API_KEY

import io, zipfile, sqlite3, yaml

def _sql_to_dataframe(sql_text):
    """Parse MySQL-style .sql file into dict of DataFrames."""
    tables_data = {}
    insert_pattern = re.compile(
        r"INSERT\s+INTO\s+`?(\w+)`?\s*\((.*?)\)\s*VALUES\s*(.*?);",
        re.IGNORECASE | re.DOTALL
    )

    for match in insert_pattern.finditer(sql_text):
        table = match.group(1)
        columns = [c.strip(" `") for c in match.group(2).split(",")]
        values_block = match.group(3)
        tuples = re.findall(r"\((.*?)\)", values_block, re.DOTALL)

        rows = []
        for tup in tuples:
            parts = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", tup)
            clean_parts = [
                None if p.strip().upper() == "NULL" else p.strip().strip("'")
                for p in parts
            ]
            rows.append(clean_parts)

        df = pd.DataFrame(rows, columns=columns)
        if table in tables_data:
            tables_data[table] = pd.concat([tables_data[table], df], ignore_index=True)
        else:
            tables_data[table] = df

    return tables_data


def load_any_file(uploaded_file):
    """
    Load various file types into a pandas DataFrame.
    Works with Flask file uploads.
    """
    if not uploaded_file:
        return None

    file_name = getattr(uploaded_file, "filename", str(uploaded_file)).lower()

    try:
        # ---------------- CSV / Excel ----------------
        if file_name.endswith(".csv"):
            return pd.read_csv(uploaded_file)
        elif file_name.endswith((".xlsx", ".xls")):
            return pd.read_excel(uploaded_file)

        # ---------------- TXT ---------------

        elif file_name.endswith(".txt"):
            return pd.read_csv(uploaded_file, delimiter="\t", engine="python")



        # ---------------- SQL ----------------
        elif file_name.endswith(".sql"):
            sql_script = uploaded_file.read()
            if isinstance(sql_script, bytes):
                sql_script = sql_script.decode("utf-8", errors="ignore")

            # Execute in-memory SQLite
            conn = sqlite3.connect(":memory:")
            conn.executescript(sql_script)
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
            df_dict = {}
            for table in tables["name"].tolist():
                df_dict[table] = pd.read_sql(f"SELECT * FROM {table}", conn)
            conn.close()
            if not df_dict:
                raise ValueError("No tables found in SQL file.")
            # Return first table
            first_table = next(iter(df_dict))
            return df_dict[first_table]



        else:
            raise ValueError("Unsupported file type.")

    except Exception as e:
        print(f"Error loading file '{file_name}': {e}")
        return None


def analyze_data(df, query):
    """Run AI-powered query on DataFrame using PandasAI 3.0."""
    if not query or not query.strip():
        raise ValueError("Query prompt is empty.")

    # Simple local lookup
    try:
        q = query.strip()
        simple_lookup = re.match(
            r'^\s*(?P<col>[\w\s]+?)\s+of\s+(?P<val>.+?)\s*\.?$',
            q, flags=re.IGNORECASE
        )
        if simple_lookup:
            col_raw = simple_lookup.group('col').strip()
            val_raw = simple_lookup.group('val').strip()

            col_match = next(
                (c for c in df.columns if c.lower() == col_raw.lower()), None
            ) or next(
                (c for c in df.columns if col_raw.lower() in c.lower()), None
            )

            if col_match:
                str_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
                for sc in str_cols:
                    mask = df[sc].astype(str).str.strip().str.lower() == val_raw.lower()
                    if mask.any():
                        print(f"[DEBUG] Local lookup exact match in column={sc}")
                        return df.loc[mask, df.columns]
                for sc in str_cols:
                    mask = df[sc].astype(str).str.lower().str.contains(re.escape(val_raw.lower()))
                    if mask.any():
                        print(f"[DEBUG] Local lookup partial match in column={sc}")
                        return df.loc[mask, df.columns]
                print(f"[DEBUG] Local lookup: no match found for {val_raw}")
                return pd.DataFrame([{"Result": f"No match found for '{val_raw}' in column '{col_match}'"}])
    except Exception as e:
        print(f"[DEBUG] Local lookup failed: {e}")

    # Fallback to PandasAI
    try:
        print(f"[DEBUG] Sending query to PandasAI: {query}")
        llm = OpenAI(api_token=OPENAI_API_KEY)
        sdf = SmartDataframe(df, config={"llm": llm})
        result = sdf.chat(query)

                # ✅ Validation step here
        if result is None or result == "":
            print("[DEBUG] returned empty/None result")
            return "⚠️ No valid response ."

                # ✅ NEW: Check if PandasAI returned full original table
        if isinstance(result, pd.DataFrame) and result.equals(df):
            print("[DEBUG] PandasAI returned full DataFrame (invalid query)")
            return pd.DataFrame([{"Result": "⚠️ Invalid or unrecognized query."}])

        # If PandasAI returned a DataFrame
        if isinstance(result, pd.DataFrame) and not result.empty:
            print(f"[DEBUG] PandasAI returned DataFrame with columns={list(result.columns)}")
            result = result.reset_index(drop=True)

            # Recover missing columns
            orig_cols = list(df.columns)
            missing_cols = [c for c in orig_cols if c not in result.columns]
            print(f"[DEBUG] Missing cols from result: {missing_cols}")

            if missing_cols:
                common_cols = [c for c in result.columns if c in df.columns]
                print(f"[DEBUG] Common cols between result and df: {common_cols}")

                join_col = None
                for pref in ("user_id", "id", "name", "email"):
                    if pref in common_cols:
                        join_col = pref
                        break
                if join_col is None and common_cols:
                    join_col = common_cols[0]

                if join_col:
                    print(f"[DEBUG] Trying to merge back using join_col={join_col}")
                    keys = result[join_col].dropna().tolist()
                    df_sub = df[df[join_col].isin(keys)].copy()

                    if not df_sub.empty:
                        df_sub_indexed = df_sub.set_index(join_col)
                        ordered = df_sub_indexed.reindex(result[join_col].values).reset_index()
                        result = ordered[orig_cols]
                        print(f"[DEBUG] Successfully restored missing columns: now cols={list(result.columns)}")
                        return result.reset_index(drop=True)
                    else:
                        print(f"[DEBUG] No matching rows in original df for keys={keys}")
                else:
                    print("[DEBUG] No suitable join_col found to merge back missing columns")
            
            return result.reset_index(drop=True)

        # If PandasAI returned list/dict
        if isinstance(result, (list, dict)):
            print(f"[DEBUG] PandasAI returned type={type(result)}; converting to DataFrame")
            try:
                return pd.DataFrame(result)
            except Exception as e:
                print(f"[DEBUG] Conversion failed: {e}")
                return pd.DataFrame([{"Result": str(result)}])

        # If PandasAI returned primitive
        if isinstance(result, (str, int, float)):
            print(f"[DEBUG] PandasAI returned primitive: {result}")
            return str(result) if result is not None else ""

        # Last fallback
        print(f"[DEBUG] PandasAI returned unhandled type={type(result)}; forcing str")
        return str(result) if result is not None else ""

    except Exception as e:
        print(f"[DEBUG] Error during AI analysis: {e}")
        return f"AI analysis failed: {e}"

