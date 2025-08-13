import sqlite3
import json
import os
import csv
import random
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import re
from mcp.server.fastmcp import FastMCP
from faker import Faker

# Initialize FastMCP server
mcp = FastMCP("sqlite-db")

# Global connection pool
connections: Dict[str, sqlite3.Connection] = {}

# Initialize Faker for data generation
fake = Faker()

class FieldTracker:
    """Track field lineage and data sources"""
    
    def __init__(self):
        self.lineage_db = {}
    
    def add_lineage(self, target_table: str, target_field: str, 
                   source_tables: List[str], source_fields: List[str], 
                   join_condition: str = ""):
        """Add field lineage information"""
        key = f"{target_table}.{target_field}"
        self.lineage_db[key] = {
            "source_tables": source_tables,
            "source_fields": source_fields,
            "join_condition": join_condition,
            "target_table": target_table,
            "target_field": target_field
        }
    
    def get_lineage(self, table: str, field: str) -> Optional[Dict]:
        """Get lineage information for a field"""
        key = f"{table}.{field}"
        return self.lineage_db.get(key)
    
    def analyze_query_lineage(self, query: str) -> Dict[str, List[str]]:
        """Analyze a query to extract potential lineage"""
        # Simple regex-based analysis (can be enhanced with proper SQL parser)
        tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', query.upper())
        table_list = [t[0] or t[1] for t in tables]
        
        fields = re.findall(r'SELECT\s+(.*?)\s+FROM', query.upper(), re.DOTALL)
        if fields:
            field_list = [f.strip() for f in fields[0].split(',')]
        else:
            field_list = []
        
        return {
            "tables": table_list,
            "fields": field_list
        }

# Initialize field tracker
field_tracker = FieldTracker()

def get_connection(db_path: str) -> sqlite3.Connection:
    """Get or create database connection"""
    if db_path not in connections:
        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        connections[db_path] = sqlite3.connect(db_path, check_same_thread=False)
        connections[db_path].row_factory = sqlite3.Row
    return connections[db_path]

@mcp.tool()
async def connect_database(db_path: str) -> str:
    """Connect to a SQLite database file.
    
    Args:
        db_path: Path to the SQLite database file
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if tables:
            table_names = [table[0] for table in tables]
            return f"Successfully connected to database: {db_path}\nTables found: {', '.join(table_names)}"
        else:
            return f"Successfully connected to database: {db_path}\nNo tables found in database."
    except Exception as e:
        return f"Error connecting to database: {str(e)}"

@mcp.tool()
async def execute_query(db_path: str, query: str, params: Optional[List] = None) -> str:
    """Execute a SQL query on the database.
    
    Args:
        db_path: Path to the SQLite database file
        query: SQL query to execute
        params: Optional parameters for the query
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Auto-commit for non-SELECT queries
        if not query.strip().upper().startswith('SELECT'):
            conn.commit()
            
        # Return results for SELECT queries
        if query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            if results:
                columns = [description[0] for description in cursor.description]
                formatted_results = []
                for row in results:
                    row_dict = dict(zip(columns, row))
                    formatted_results.append(row_dict)
                return f"Query executed successfully.\nResults:\n{json.dumps(formatted_results, indent=2, default=str)}"
            else:
                return "Query executed successfully. No results returned."
        else:
            return f"Query executed successfully. {cursor.rowcount} rows affected."
    except Exception as e:
        return f"Error executing query: {str(e)}"

@mcp.tool()
async def describe_table(db_path: str, table_name: str) -> str:
    """Get detailed information about a table structure.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to describe
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        if not columns:
            return f"Table '{table_name}' not found."
        
        # Format column information
        column_info = []
        for col in columns:
            col_dict = dict(col)
            column_info.append({
                "name": col_dict["name"],
                "type": col_dict["type"],
                "nullable": not col_dict["notnull"],
                "default": col_dict["dflt_value"],
                "primary_key": bool(col_dict["pk"])
            })
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        return f"Table: {table_name}\nRow count: {row_count}\nColumns:\n{json.dumps(column_info, indent=2)}"
    except Exception as e:
        return f"Error describing table: {str(e)}"

@mcp.tool()
async def add_field_lineage(target_table: str, target_field: str, 
                          source_tables: List[str], source_fields: List[str], 
                          join_condition: str = "") -> str:
    """Add field lineage information to track data sources.
    
    Args:
        target_table: Target table name
        target_field: Target field name
        source_tables: List of source table names
        source_fields: List of source field names
        join_condition: Optional join condition description
    """
    try:
        field_tracker.add_lineage(
            target_table, target_field, 
            source_tables, source_fields, 
            join_condition
        )
        return f"Lineage added for {target_table}.{target_field} from {', '.join([f'{t}.{f}' for t, f in zip(source_tables, source_fields)])}"
    except Exception as e:
        return f"Error adding lineage: {str(e)}"

@mcp.tool()
async def trace_field_lineage(table: str, field: str) -> str:
    """Trace the lineage of a specific field to understand its data sources.
    
    Args:
        table: Table name
        field: Field name to trace
    """
    try:
        lineage = field_tracker.get_lineage(table, field)
        
        if not lineage:
            return f"No lineage information found for {table}.{field}"
        
        result = f"Field Lineage for {table}.{field}:\n"
        result += f"Source Tables: {', '.join(lineage['source_tables'])}\n"
        result += f"Source Fields: {', '.join(lineage['source_fields'])}\n"
        
        if lineage['join_condition']:
            result += f"Join Condition: {lineage['join_condition']}\n"
        
        # Try to provide sample data mapping
        result += f"\nData Flow:\n"
        for i, (src_table, src_field) in enumerate(zip(lineage['source_tables'], lineage['source_fields'])):
            result += f"  {src_table}.{src_field} -> {table}.{field}\n"
        
        return result
    except Exception as e:
        return f"Error tracing lineage: {str(e)}"

@mcp.tool()
async def generate_sample_data(db_path: str, table_name: str, num_rows: int = 10) -> str:
    """Generate and insert sample data into a table based on column types.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to populate
        num_rows: Number of rows to generate (default: 10)
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        if not columns:
            return f"Table '{table_name}' not found."
        
        # Generate data based on column types
        insert_data = []
        column_names = []
        
        for _ in range(num_rows):
            row_data = []
            for col in columns:
                col_dict = dict(col)
                column_name = col_dict["name"]
                column_type = col_dict["type"].upper()
                is_pk = bool(col_dict["pk"])
                
                if _ == 0:  # First iteration, collect column names
                    column_names.append(column_name)
                
                # Generate data based on type
                if is_pk and 'INTEGER' in column_type:
                    # Skip auto-increment primary keys
                    continue
                elif 'INT' in column_type:
                    row_data.append(random.randint(1, 1000))
                elif 'REAL' in column_type or 'FLOAT' in column_type or 'DOUBLE' in column_type:
                    row_data.append(round(random.uniform(1.0, 1000.0), 2))
                elif 'TEXT' in column_type or 'VARCHAR' in column_type or 'CHAR' in column_type:
                    if 'name' in column_name.lower():
                        row_data.append(fake.name())
                    elif 'email' in column_name.lower():
                        row_data.append(fake.email())
                    elif 'phone' in column_name.lower():
                        row_data.append(fake.phone_number())
                    elif 'address' in column_name.lower():
                        row_data.append(fake.address())
                    elif 'company' in column_name.lower():
                        row_data.append(fake.company())
                    else:
                        row_data.append(fake.text(max_nb_chars=50))
                elif 'DATE' in column_type or 'DATETIME' in column_type:
                    row_data.append(fake.date_time().isoformat())
                elif 'BOOL' in column_type:
                    row_data.append(random.choice([0, 1]))
                else:
                    row_data.append(fake.text(max_nb_chars=20))
            
            insert_data.append(tuple(row_data))
        
        # Filter out auto-increment columns from column names
        filtered_columns = []
        for col in columns:
            col_dict = dict(col)
            is_pk = bool(col_dict["pk"])
            column_type = col_dict["type"].upper()
            if not (is_pk and 'INTEGER' in column_type):
                filtered_columns.append(col_dict["name"])
        
        # Insert data
        placeholders = ','.join(['?' for _ in filtered_columns])
        insert_query = f"INSERT INTO {table_name} ({','.join(filtered_columns)}) VALUES ({placeholders})"
        
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        
        return f"Successfully generated and inserted {num_rows} rows into {table_name}"
    except Exception as e:
        return f"Error generating data: {str(e)}"

@mcp.tool()
async def import_csv(db_path: str, csv_path: str, table_name: str, create_table: bool = True) -> str:
    """Import data from a CSV file into a SQLite table.
    
    Args:
        db_path: Path to the SQLite database file
        csv_path: Path to the CSV file
        table_name: Name of the target table
        create_table: Whether to create the table if it doesn't exist
    """
    try:
        if not os.path.exists(csv_path):
            return f"CSV file not found: {csv_path}"
        
        conn = get_connection(db_path)
        cursor = conn.cursor()
        
        # Read CSV file
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            # Detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            rows = list(reader)
            
            if not rows:
                return "CSV file is empty"
            
            headers = list(rows[0].keys())
            
            if create_table:
                # Create table based on CSV headers
                # Simple type inference based on first row
                column_defs = []
                first_row = rows[0]
                
                for header in headers:
                    value = first_row[header]
                    # Simple type inference
                    if value.isdigit():
                        column_type = "INTEGER"
                    elif value.replace('.', '').replace('-', '').isdigit():
                        column_type = "REAL"
                    else:
                        column_type = "TEXT"
                    
                    column_defs.append(f"{header} {column_type}")
                
                create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
                cursor.execute(create_query)
            
            # Insert data
            placeholders = ','.join(['?' for _ in headers])
            insert_query = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({placeholders})"
            
            for row in rows:
                values = [row[header] for header in headers]
                cursor.execute(insert_query, values)
            
            conn.commit()
            
            return f"Successfully imported {len(rows)} rows from {csv_path} into {table_name}"
    except Exception as e:
        return f"Error importing CSV: {str(e)}"

@mcp.tool()
async def analyze_query_lineage(query: str) -> str:
    """Analyze a SQL query to identify potential field lineage.
    
    Args:
        query: SQL query to analyze
    """
    try:
        analysis = field_tracker.analyze_query_lineage(query)
        
        result = "Query Analysis:\n"
        result += f"Tables involved: {', '.join(analysis['tables'])}\n"
        result += f"Fields selected: {', '.join(analysis['fields'])}\n"
        
        # Check if we have lineage information for any of the fields
        lineage_found = []
        for table in analysis['tables']:
            for field in analysis['fields']:
                if field != '*':  # Skip wildcard
                    clean_field = field.split('.')[-1].strip()  # Get field name without table prefix
                    lineage = field_tracker.get_lineage(table, clean_field)
                    if lineage:
                        lineage_found.append(f"{table}.{clean_field}")
        
        if lineage_found:
            result += f"\nFields with tracked lineage: {', '.join(lineage_found)}"
        else:
            result += "\nNo tracked lineage found for fields in this query."
        
        return result
    except Exception as e:
        return f"Error analyzing query: {str(e)}"

@mcp.tool()
async def export_table_to_csv(db_path: str, table_name: str, output_path: str) -> str:
    """Export a table to a CSV file.
    
    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to export
        output_path: Path for the output CSV file
    """
    try:
        conn = get_connection(db_path)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        if not rows:
            return f"Table {table_name} is empty"
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        # Write to CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            
            for row in rows:
                writer.writerow(row)
        
        return f"Successfully exported {len(rows)} rows from {table_name} to {output_path}"
    except Exception as e:
        return f"Error exporting table: {str(e)}"

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')

