# gridflow/io_security.py

import pandas as pd

# PII fields that need masking for external consumers
PII_COLUMNS = ["street", "house_num", "house_supp"]
QUASI_PII_COLUMNS = ["zip"]


def mask_pii(df: pd.DataFrame, level: str = "external") -> pd.DataFrame:
    """
    Mask PII based on access level.
    
    level='internal' -> no masking (pipeline/governance use)
    level='external' -> masked (app/API consumers)
    """
    if level == "internal":
        return df
    
    out = df.copy()
    
    for col in PII_COLUMNS:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: "***MASKED***" if pd.notna(x) else x
            )
    
    # ZIP3 keeps regional analytics working while masking identity
    if "zip" in out.columns:
        out["zip"] = out["zip"].apply(
            lambda x: str(x)[:3] + "**" if pd.notna(x) else x
        )
    
    return out


def get_pii_columns() -> list:
    """List of PII columns for audit/docs."""
    return PII_COLUMNS + QUASI_PII_COLUMNS