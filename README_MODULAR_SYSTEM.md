# Modular Database & CSV Import System

## Overview
This is a modular system for creating databases and importing CSV files. You only create the database ONCE, then you can import multiple CSV files whenever you want.

## Files in This System

1. **database_config.json** - Database settings (edit this to change database name)
2. **csv_import_config.json** - CSV import settings (edit this for each CSV import)
3. **database_creator.py** - Run ONCE to create the database
4. **csv_importer.py** - Run EVERY TIME you want to import a CSV file

## Step-by-Step Usage

### STEP 1: Create Database (Run Once)

1. Edit `database_config.json` if you want to change the database name:
   ```json
   {
     "database_name": "my_database.db",
     "database_type": "sqlite",
     "description": "Main database configuration"
   }
   ```

2. Run the database creator:
   ```bash
   python database_creator.py
   ```

3. Your database is now created! You only need to do this ONCE.

### STEP 2: Import CSV Files (Run Anytime)

1. Edit `csv_import_config.json` for your CSV file:
   ```json
   {
     "csv_file_path": "your_file.csv",
     "table_name": "your_table_name",
     "import_settings": {
       "remove_duplicates": true,
       "fill_missing_values": true,
       "if_table_exists": "append"
     }
   }
   ```

2. Run the CSV importer:
   ```bash
   python csv_importer.py
   ```

3. Your CSV data is now in the database!

### STEP 3: Import More CSV Files

To import another CSV file:
1. Edit `csv_import_config.json` with new CSV file path and table name
2. Run `python csv_importer.py` again
3. Done! The new data is added to your existing database

## Configuration Details

### database_config.json

| Setting | Description | Example |
|---------|-------------|---------|
| database_name | Name of the database file | "my_database.db" |
| database_type | Type of database | "sqlite" |
| description | Optional description | "Sales database" |

### csv_import_config.json

| Setting | Description | Options |
|---------|-------------|---------|
| csv_file_path | Path to your CSV file | "data.csv" or full path |
| table_name | Name for the table | "sales_data" |
| remove_duplicates | Remove duplicate rows? | true/false |
| fill_missing_values | Fill empty cells? | true/false |
| missing_value_replacement | What to fill empty cells with | "NULL", "N/A", "0", etc. |
| if_table_exists | What to do if table exists | "append", "replace", "fail" |
| clean_column_names | Clean column names? | true/false |
| remove_extra_spaces | Remove extra spaces in column names? | true/false |

### Important: if_table_exists Options

- **"append"** - Add new data to existing table (recommended)
- **"replace"** - Delete old table and create new one
- **"fail"** - Stop if table already exists

## Example Workflows

### Example 1: Monthly Sales Data

Create database once:
```bash
python database_creator.py
```

Import January data:
1. Edit csv_import_config.json:
   ```json
   {
     "csv_file_path": "sales_january.csv",
     "table_name": "sales",
     "import_settings": {
       "if_table_exists": "append"
     }
   }
   ```
2. Run: `python csv_importer.py`

Import February data:
1. Edit csv_import_config.json:
   ```json
   {
     "csv_file_path": "sales_february.csv",
     "table_name": "sales",
     "import_settings": {
       "if_table_exists": "append"
     }
   }
   ```
2. Run: `python csv_importer.py`

Now you have all sales data in one table!

### Example 2: Different Data Types

Create database once:
```bash
python database_creator.py
```

Import customers:
```json
{
  "csv_file_path": "customers.csv",
  "table_name": "customers"
}
```
Run: `python csv_importer.py`

Import products:
```json
{
  "csv_file_path": "products.csv",
  "table_name": "products"
}
```
Run: `python csv_importer.py`

Import orders:
```json
{
  "csv_file_path": "orders.csv",
  "table_name": "orders"
}
```
Run: `python csv_importer.py`

Now you have one database with three tables!

## Troubleshooting

**Problem**: "Database does not exist"
**Solution**: Run `database_creator.py` first

**Problem**: "CSV file not found"
**Solution**: Check the file path in csv_import_config.json. Use full path if needed.

**Problem**: "Table already exists"
**Solution**: Change "if_table_exists" to "append" (add data) or "replace" (delete old data)

**Problem**: Column names have issues
**Solution**: Set "clean_column_names": true in csv_import_config.json

## Tips

1. **Keep One Database**: Create one database and import all your CSV files into it as different tables
2. **Use Append**: Set "if_table_exists": "append" to add new data to existing tables
3. **Clean Data**: Enable data cleaning options to handle missing values and duplicates
4. **Backup**: Copy your .db file before running "replace" mode
5. **View in DBeaver**: Open the .db file in DBeaver to see all your tables and data

## Quick Reference

Create database (once):
```bash
python database_creator.py
```

Import CSV (anytime):
```bash
python csv_importer.py
```

That's it! Simple and modular.
