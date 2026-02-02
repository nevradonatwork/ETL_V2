"""
Database Creator
=================
This script creates a database based on the configuration in database_config.json
Run this ONCE to create your database.
by Nevra Donat at 30/01/2026
"""

import sqlite3
import json
import os
from datetime import datetime

def create_database():
    """
    Creates a database based on database_config.json
    """
    print("=" * 70)
    print("DATABASE CREATION")
    print("=" * 70)
    
    # Read configuration
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        print("\n✓ Configuration file loaded")
    except FileNotFoundError:
        print("\n✗ ERROR: database_config.json not found!")
        print("Please make sure the configuration file exists.")
        return False
    except json.JSONDecodeError:
        print("\n✗ ERROR: Invalid JSON in database_config.json!")
        return False
    
    db_name = config.get('database_name', 'ETLTest')
    db_type = config.get('database_type', 'sqlite')
    
    print(f"\nDatabase Settings:")
    print(f"  Name: {db_name}")
    print(f"  Type: {db_type}")
    print(f"  Description: {config.get('description', 'N/A')}")
    
    # Check if database already exists
    if os.path.exists(db_name):
        print(f"\n⚠ WARNING: Database '{db_name}' already exists!")
        response = input("Do you want to continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            print("\n✗ Operation cancelled")
            return False
    
    # Create database
    try:
        print(f"\n Creating database '{db_name}'...")
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        # Create a metadata table to track database info
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS _database_metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at TEXT
            )
        ''')
        
        # Insert metadata
        metadata = [
            ('database_name', db_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ('created_by', 'Database Creator Script', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ('description', config.get('description', 'N/A'), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ]
        
        cursor.executemany('''
            INSERT OR REPLACE INTO _database_metadata (key, value, created_at)
            VALUES (?, ?, ?)
        ''', metadata)
        
        conn.commit()
        conn.close()
        
        print(f"✓ Database '{db_name}' created successfully!")
        print(f"✓ Database file location: {os.path.abspath(db_name)}")
        print("\n" + "=" * 70)
        print("SUCCESS!")
        print("=" * 70)
        print(f"\nYour database '{db_name}' is ready.")
        print("You can now run 'csv_importer.py' to import CSV files.")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        return False

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("DATABASE CREATOR")
    print("=" * 70)
    print("\nThis script creates a new database based on database_config.json")
    print("Run this script ONCE to create your database.")
    print("\nAfter creating the database, use csv_importer.py to import data.")
    print("=" * 70)
    
    input("\nPress Enter to continue...")
    
    success = create_database()
    
    if success:
        print("\n✓ Database creation completed!")
    else:
        print("\n✗ Database creation failed!")
    
    input("\nPress Enter to exit...")
