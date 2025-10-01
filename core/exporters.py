import io, zipfile
import pandas as pd

def to_excel_zip(dfs: dict[str, pd.DataFrame]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, df in dfs.items():
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as xw:
                df.to_excel(xw, index=False, sheet_name=name[:31])
            zf.writestr(f"{name}.xlsx", bio.getvalue())
    buf.seek(0)
    return buf

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")
