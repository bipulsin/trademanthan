# MCX Bhavcopy (Kosmic Tarang)

Drop vendor Bhavcopy CSVs here as `raw/*.csv`. Files are **not** committed (gitignore).

Importer: `python scripts/tarang_import_bhavcopy.py path/to/file.csv --reconstruct`

Dates are read from CSV rows, not filenames. OPTFUT and FUTCOM are accepted. Unknown symbols are stored (not rejected).

Placeholders: volume 0 or close 0.05 are stored with `traded=false` and excluded from IV series.
