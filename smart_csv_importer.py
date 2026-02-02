"""
Smart CSV Importer with Column Validation
==========================================
This script checks if CSV columns match the database table columns
before inserting data. Only imports if columns match exactly.

Created by: Nevra Donat
Date: 30/01/2026
"""

import pandas as pd
import sqlite3
import json
import os
from datetime import datetime

def (conn, table_name):
    """
    Get column names from a database table
    Returns: list of column names (in lowercase for comparison)
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    return columns

# def get_csv_columns(csv_file):
    """
    Get column names from CSV file
    Returns: pandas DataFrame and list of column names
    """
    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_file, encoding=encoding, nrows=0)  # Read only headers
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    if df is None:
        raise Exception("Could not read CSV with any encoding")
    
    # Clean column names
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)
    
    return list(df.columns)

def compare_columns(table_cols, csv_cols):
    """
    Compare table columns with CSV columns
    Returns: (match: bool, missing_in_csv: list, extra_in_csv: list)
    """
    table_cols_set = set([col.lower() for col in table_cols])
    csv_cols_set = set([col.lower() for col in csv_cols])
    
    missing_in_csv = table_cols_set - csv_cols_set
    extra_in_csv = csv_cols_set - table_cols_set
    
    match = (missing_in_csv == set() and extra_in_csv == set())
    
    return match, list(missing_in_csv), list(extra_in_csv)

def import_csv_with_validation():
    """
    Import CSV with column validation
    """
    print("=" * 80)
    print("SMART CSV IMPORTER WITH COLUMN VALIDATION")
    print("=" * 80)
    
    # ============================================
    # Step 1: Read Configurations
    # ============================================
    try:
        with open('database_config.json', 'r') as f:
            db_config = json.load(f)
        with open('csv_import_config.json', 'r') as f:
            import_config = json.load(f)
        print("\n✓ Configuration files loaded")
    except FileNotFoundError as e:
        print(f"\n✗ ERROR: Configuration file not found: {e}")
        return False
    except json.JSONDecodeError:
        print("\n✗ ERROR: Invalid JSON in configuration file!")
        return False
    
    # Get settings
    db_name = db_config.get('database_name', 'ETLTest.')
    csv_file = import_config.get('csv_file_path', 'Account_20260130.csv')
    table_name = import_config.get('table_name', 'RawAccount')
    import_settings = import_config.get('import_settings', {})
    
    print(f"\nImport Settings:")
    print(f"  Database: {db_name}")
    print(f"  Table: {table_name}")
    print(f"  CSV File: {csv_file}")
    
    # ============================================
    # Step 2: Check if database and table exist
    # ============================================
    print("\n" + "=" * 80)
    print("CHECKING DATABASE AND TABLE")
    print("=" * 80)
    
    if not os.path.exists(db_name):
        print(f"\n✗ ERROR: Database '{db_name}' does not exist!")
        print("Please run 'database_creator.py' first.")
        return False
    
    print(f"✓ Database '{db_name}' exists")
    
    # Connect to database
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
    except Exception as e:
        print(f"\n✗ ERROR connecting to database: {str(e)}")
        return False
    
    # Check if table exists
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        print(f"\n✗ ERROR: Table '{table_name}' does not exist in database!")
        print(f"Available tables:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for table in cursor.fetchall():
            print(f"  - {table[0]}")
        conn.close()
        return False
    
    print(f"✓ Table '{table_name}' exists")
    
    # ============================================
    # Step 3: Get table columns
    # ============================================
    print("\n" + "=" * 80)
    print("CHECKING TABLE STRUCTURE")
    print("=" * 80)
    
    try:
        table_columns = get_table_columns(conn, table_name)
        print(f"\n✓ Table '{table_name}' has {len(table_columns)} columns:")
        for i, col in enumerate(table_columns, 1):
            print(f"  {i}. {col}")
    except Exception as e:
        print(f"\n✗ ERROR reading table structure: {str(e)}")
        conn.close()
        return False
    
    # ============================================
    # Step 4: Get CSV columns
    # ============================================
    print("\n" + "=" * 80)
    print("CHECKING CSV FILE STRUCTURE")
    print("=" * 80)
    
    if not os.path.exists(csv_file):
        print(f"\n✗ ERROR: CSV file '{csv_file}' not found!")
        conn.close()
        return False
    
    try:
        csv_columns = get_csv_columns(csv_file)
        print(f"\n✓ CSV file has {len(csv_columns)} columns:")
        for i, col in enumerate(csv_columns, 1):
            print(f"  {i}. {col}")
    except Exception as e:
        print(f"\n✗ ERROR reading CSV file: {str(e)}")
        conn.close()
        return False
    
    # ============================================
    # Step 5: Compare columns
    # ============================================
    print("\n" + "=" * 80)
    print("VALIDATING COLUMN MATCH")
    print("=" * 80)
    
    match, missing_in_csv, extra_in_csv = compare_columns(table_columns, csv_columns)
    
    if match:
        print("\n✓ SUCCESS! All columns match!")
        print("✓ CSV file is compatible with the table structure")
    else:
        print("\n✗ ERROR: Column mismatch detected!")
        
        if missing_in_csv:
            print(f"\n⚠ Columns in table but NOT in CSV file:")
            for col in missing_in_csv:
                print(f"  - {col}")
        
        if extra_in_csv:
            print(f"\n⚠ Columns in CSV file but NOT in table:")
            for col in extra_in_csv:
                print(f"  - {col}")
        
        print("\n" + "=" * 80)
        print("IMPORT CANCELLED")
        print("=" * 80)
        print("\nPlease fix the column mismatch before importing:")
        print("1. Make sure CSV has all required columns")
        print("2. Remove any extra columns from CSV")
        print("3. Check for spelling differences in column names")
        print("=" * 80)
        
        conn.close()
        return False
    
    # ============================================
    # Step 6: Read and prepare data
    # ============================================
    print("\n" + "=" * 80)
    print("READING CSV DATA")
    print("=" * 80)
    
    try:
        # Read CSV with proper encoding
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-16']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                print(f"✓ CSV file read successfully (encoding: {encoding})")
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        if df is None:
            raise Exception("Could not read CSV with any encoding")
        
        # Clean column names
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)
        
        print(f"✓ Rows in CSV: {len(df)}")
        
    except Exception as e:
        print(f"\n✗ ERROR reading CSV data: {str(e)}")
        conn.close()
        return False
    
    # ============================================
    # Step 7: Data cleaning
    # ============================================
    print("\n" + "=" * 80)
    print("CLEANING DATA")
    print("=" * 80)
    
    original_rows = len(df)
    
    # Remove empty rows
    if import_settings.get('skip_empty_rows', True):
        df = df.dropna(how='all')
        empty_removed = original_rows - len(df)
        if empty_removed > 0:
            print(f"✓ Removed {empty_removed} empty rows")
    
    # Remove duplicates
    if import_settings.get('remove_duplicates', True):
        before_dup = len(df)
        df = df.drop_duplicates()
        dup_removed = before_dup - len(df)
        if dup_removed > 0:
            print(f"✓ Removed {dup_removed} duplicate rows")
        else:
            print("✓ No duplicates found")
    
    # Fill missing values
    if import_settings.get('fill_missing_values', True):
        missing_count = df.isnull().sum().sum()
        if missing_count > 0:
            replacement = import_settings.get('missing_value_replacement', 'NULL')
            df = df.fillna(replacement)
            print(f"✓ Filled {missing_count} missing values with '{replacement}'")
        else:
            print("✓ No missing values found")
    
    print(f"\n✓ Final row count to import: {len(df)}")
    
    # ============================================
    # Step 8: Get current row count
    # ============================================
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    rows_before = cursor.fetchone()[0]
    print(f"✓ Current rows in table: {rows_before}")
    
    # ============================================
    # Step 9: Import data
    # ============================================
    print("\n" + "=" * 80)
    print("IMPORTING DATA")
    print("=" * 80)
    
    try:
        if_exists = import_settings.get('if_table_exists', 'append')
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        print(f"✓ Data imported successfully!")
        print(f"✓ Rows imported: {len(df)}")
    except Exception as e:
        print(f"\n✗ ERROR during import: {str(e)}")
        conn.close()
        return False
    
    # ============================================
    # Step 10: Verify import
    # ============================================
    print("\n" + "=" * 80)
    print("VERIFYING IMPORT")
    print("=" * 80)
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    rows_after = cursor.fetchone()[0]
    rows_added = rows_after - rows_before
    
    print(f"✓ Rows before import: {rows_before}")
    print(f"✓ Rows after import: {rows_after}")
    print(f"✓ New rows added: {rows_added}")
    
    # Show sample of newly added data
    print("\nSample of imported data (last 5 rows):")
    print("-" * 80)
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY ROWID DESC LIMIT 5")
    rows = cursor.fetchall()
    
    # Get column names
    columns = get_table_columns(conn, table_name)
    print(" | ".join(columns))
    print("-" * 80)
    
    for row in rows:
        print(" | ".join(str(val)[:20] for val in row))  # Truncate long values
    
    # Update metadata
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO _database_metadata (key, value, created_at)
            VALUES (?, ?, ?)
        ''', (f'last_import_{table_name}', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
              datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
    except:
        pass  # Metadata table might not exist
    
    conn.close()
    
    # ============================================
    # Success message
    # ============================================
    print("\n" + "=" * 80)
    print("✓ IMPORT COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print(f"\nDatabase: {db_name}")
    print(f"Table: {table_name}")
    print(f"Total rows: {rows_after}")
    print(f"New rows added: {rows_added}")
    print("\nYou can now view the data in DBeaver or query it with Python.")
    print("=" * 80)
    
    return True


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("SMART CSV IMPORTER - WITH COLUMN VALIDATION")
    print("=" * 80)
    print("\nThis script validates CSV columns match the table before importing.")
    print("\nValidation checks:")
    print("  ✓ Database exists")
    print("  ✓ Table exists")
    print("  ✓ CSV columns match table columns exactly")
    print("  ✓ No missing or extra columns")
    print("\nOnly imports if ALL validations pass!")
    print("=" * 80)
    
    input("\nPress Enter to start import with validation...")
    
    success = import_csv_with_validation()
    
    if success:
        print("\n✓ Import completed successfully!")
    else:
        print("\n✗ Import failed! Please check the errors above.")
    
    input("\nPress Enter to exit...")
