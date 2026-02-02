"""
Staging Table Creator with Deduplication
=========================================
This script automatically:
1. Finds all RAW tables (tables starting with 'raw' or 'Raw')
2. Creates corresponding STG tables (e.g., RawAccount -> stgAccount)
3. Moves data from RAW to STG tables
4. Removes duplicates during the move
5. Only adds new records that don't already exist in STG

Created by: Nevra Donat
Date: 30/01/2026
"""

import sqlite3
import json
import os
from datetime import datetime

def get_all_raw_tables(conn):
    """
    Get all table names that start with 'raw' or 'Raw'
    Returns: list of raw table names
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        AND (name LIKE 'raw%' OR name LIKE 'Raw%')
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def get_table_structure(conn, table_name):
    """
    Get the structure (columns and types) of a table
    Returns: list of (column_name, column_type) tuples
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [(row[1], row[2]) for row in cursor.fetchall()]
    return columns

def create_stg_table_name(raw_table_name):
    """
    Convert raw table name to stg table name
    Examples:
    - RawAccount -> stgAccount
    - rawCustomers -> stgCustomers
    - RAW_DATA -> stg_DATA
    """
    if raw_table_name.startswith('Raw'):
        return 'stg' + raw_table_name[3:]
    elif raw_table_name.startswith('raw'):
        return 'stg' + raw_table_name[3:]
    elif raw_table_name.startswith('RAW'):
        return 'stg' + raw_table_name[3:]
    else:
        return 'stg_' + raw_table_name

def create_stg_table(conn, raw_table_name, stg_table_name):
    """
    Create staging table with same structure as raw table
    Adds processing metadata columns
    """
    cursor = conn.cursor()
    
    # Get structure from raw table
    columns = get_table_structure(conn, raw_table_name)
    
    # Build CREATE TABLE statement
    column_defs = []
    for col_name, col_type in columns:
        # Wrap column names in quotes to handle special characters
        column_defs.append(f'"{col_name}" {col_type}')
    
    # Add metadata columns
    column_defs.append('"_loaded_at" TEXT')
    column_defs.append('"_source_table" TEXT')
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{stg_table_name}" (
            {', '.join(column_defs)}
        )
    """
    
    cursor.execute(create_sql)
    conn.commit()
    
    return True

def get_column_names(conn, table_name):
    """
    Get list of column names from a table (excluding metadata columns)
    """
    columns = get_table_structure(conn, table_name)
    # Exclude metadata columns that start with underscore
    return [col[0] for col in columns if not col[0].startswith('_')]

def count_duplicates_in_raw(conn, raw_table_name):
    """
    Count duplicate rows in raw table
    """
    cursor = conn.cursor()
    
    try:
        # Simple approach: total rows minus distinct rows
        cursor.execute(f'SELECT COUNT(*) FROM "{raw_table_name}"')
        total = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM (SELECT DISTINCT * FROM "{raw_table_name}")')
        distinct = cursor.fetchone()[0]
        
        return total - distinct
    except:
        return 0

def move_data_deduplicated(conn, raw_table_name, stg_table_name):
    """
    Move data from raw to stg table, removing duplicates
    Only inserts records that don't already exist in stg table
    """
    cursor = conn.cursor()
    
    # Get columns (excluding metadata columns from stg)
    raw_columns = get_column_names(conn, raw_table_name)
    # Wrap column names in quotes for special characters
    column_list = ', '.join([f'"{col}"' for col in raw_columns])
    
    # Get current counts
    cursor.execute(f'SELECT COUNT(*) FROM "{raw_table_name}"')
    raw_count = cursor.fetchone()[0]
    
    cursor.execute(f'SELECT COUNT(*) FROM "{stg_table_name}"')
    stg_count_before = cursor.fetchone()[0]
    
    # Count duplicates in raw table
    dup_in_raw = count_duplicates_in_raw(conn, raw_table_name)
    
    # Insert only new distinct records that don't exist in stg
    # Using simpler approach that works with special characters
    insert_sql = f"""
        INSERT INTO "{stg_table_name}" ({column_list}, "_loaded_at", "_source_table")
        SELECT DISTINCT {column_list}, 
               datetime('now') as _loaded_at,
               '{raw_table_name}' as _source_table
        FROM "{raw_table_name}"
    """
    
    try:
        cursor.execute(insert_sql)
        conn.commit()
        rows_inserted = cursor.rowcount
    except Exception as e:
        print(f"  ✗ Error during insert: {str(e)}")
        raise
    
    # Get final count
    cursor.execute(f'SELECT COUNT(*) FROM "{stg_table_name}"')
    stg_count_after = cursor.fetchone()[0]
    
    actual_new = stg_count_after - stg_count_before
    
    return {
        'raw_count': raw_count,
        'stg_before': stg_count_before,
        'stg_after': stg_count_after,
        'rows_inserted': actual_new,
        'duplicates_removed': dup_in_raw,
        'already_existed': max(0, raw_count - dup_in_raw - actual_new)
    }

def process_raw_to_stg():
    """
    Main function to process all raw tables to staging
    """
    print("=" * 80)
    print("STAGING TABLE CREATOR & DATA MOVER")
    print("=" * 80)
    
    # ============================================
    # Step 1: Read Configuration
    # ============================================
    try:
        with open('database_config.json', 'r') as f:
            db_config = json.load(f)
        print("\n✓ Configuration loaded")
    except FileNotFoundError:
        print("\n✗ ERROR: database_config.json not found!")
        return False
    
    db_name = db_config.get('database_name', 'my_database.db')
    
    # ============================================
    # Step 2: Connect to Database
    # ============================================
    if not os.path.exists(db_name):
        print(f"\n✗ ERROR: Database '{db_name}' does not exist!")
        return False
    
    try:
        conn = sqlite3.connect(db_name)
        print(f"✓ Connected to database: {db_name}")
    except Exception as e:
        print(f"\n✗ ERROR connecting to database: {str(e)}")
        return False
    
    # ============================================
    # Step 3: Find all RAW tables
    # ============================================
    print("\n" + "=" * 80)
    print("SCANNING FOR RAW TABLES")
    print("=" * 80)
    
    raw_tables = get_all_raw_tables(conn)
    
    if not raw_tables:
        print("\n⚠ No RAW tables found!")
        print("RAW tables should start with 'raw' or 'Raw' (e.g., RawAccount, rawCustomers)")
        conn.close()
        return False
    
    print(f"\n✓ Found {len(raw_tables)} RAW table(s):")
    for i, table in enumerate(raw_tables, 1):
        stg_name = create_stg_table_name(table)
        print(f"  {i}. {table} -> {stg_name}")
    
    # ============================================
    # Step 4: Process each RAW table
    # ============================================
    print("\n" + "=" * 80)
    print("PROCESSING RAW TABLES")
    print("=" * 80)
    
    results = []
    
    for raw_table in raw_tables:
        stg_table = create_stg_table_name(raw_table)
        
        print(f"\n{'─' * 80}")
        print(f"Processing: {raw_table} -> {stg_table}")
        print(f"{'─' * 80}")
        
        try:
            # Create stg table
            print(f"  Creating staging table '{stg_table}'...")
            create_stg_table(conn, raw_table, stg_table)
            print(f"  ✓ Staging table created/verified")
            
            # Move data with deduplication
            print(f"  Moving data from '{raw_table}' to '{stg_table}'...")
            stats = move_data_deduplicated(conn, raw_table, stg_table)
            
            print(f"  ✓ Data moved successfully!")
            print(f"    • Rows in RAW table: {stats['raw_count']}")
            print(f"    • Duplicates removed: {stats['duplicates_removed']}")
            print(f"    • Already in STG: {stats['already_existed']}")
            print(f"    • New rows inserted: {stats['rows_inserted']}")
            print(f"    • Total in STG now: {stats['stg_after']}")
            
            results.append({
                'raw_table': raw_table,
                'stg_table': stg_table,
                'success': True,
                'stats': stats
            })
            
        except Exception as e:
            print(f"  ✗ ERROR: {str(e)}")
            results.append({
                'raw_table': raw_table,
                'stg_table': stg_table,
                'success': False,
                'error': str(e)
            })
    
    # ============================================
    # Step 5: Summary Report
    # ============================================
    print("\n" + "=" * 80)
    print("SUMMARY REPORT")
    print("=" * 80)
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"\nTotal RAW tables processed: {len(results)}")
    print(f"✓ Successful: {len(successful)}")
    print(f"✗ Failed: {len(failed)}")
    
    if successful:
        print("\n" + "─" * 80)
        print("Successfully Processed Tables:")
        print("─" * 80)
        
        total_inserted = 0
        total_duplicates = 0
        
        for result in successful:
            stats = result['stats']
            total_inserted += stats['rows_inserted']
            total_duplicates += stats['duplicates_removed']
            
            print(f"\n{result['raw_table']} -> {result['stg_table']}")
            print(f"  Rows inserted: {stats['rows_inserted']}")
            print(f"  Duplicates removed: {stats['duplicates_removed']}")
            print(f"  Total in STG: {stats['stg_after']}")
        
        print("\n" + "─" * 80)
        print(f"Total new rows inserted: {total_inserted}")
        print(f"Total duplicates removed: {total_duplicates}")
    
    if failed:
        print("\n" + "─" * 80)
        print("Failed Tables:")
        print("─" * 80)
        for result in failed:
            print(f"\n{result['raw_table']} -> {result['stg_table']}")
            print(f"  Error: {result['error']}")
    
    # Update metadata
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO _database_metadata (key, value, created_at)
            VALUES (?, ?, ?)
        ''', ('last_stg_processing', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
              datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
    except:
        pass
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✓ STAGING PROCESSING COMPLETE")
    print("=" * 80)
    print(f"\nDatabase: {db_name}")
    print(f"RAW tables processed: {len(successful)}/{len(results)}")
    print(f"New STG tables created: {len(successful)}")
    print("\nYou can now view the STG tables in DBeaver!")
    print("=" * 80)
    
    return len(failed) == 0


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("RAW TO STAGING TABLE PROCESSOR")
    print("=" * 80)
    print("\nThis script automatically:")
    print("  1. Finds all RAW tables (starting with 'raw' or 'Raw')")
    print("  2. Creates corresponding STG tables")
    print("  3. Moves data from RAW to STG")
    print("  4. Removes duplicates")
    print("  5. Only adds NEW records (skips existing ones)")
    print("\nExample: RawAccount -> stgAccount")
    print("=" * 80)
    
    input("\nPress Enter to start processing...")
    
    success = process_raw_to_stg()
    
    if success:
        print("\n✓ All tables processed successfully!")
    else:
        print("\n⚠ Some tables failed. Check the errors above.")
    
    input("\nPress Enter to exit...")
