import os
import csv
from typing import List
from database.connection_manager import ConnectionManager


class ImportExportUtils:
    """Utilities for importing and exporting data"""
    
    def __init__(self, connection_manager: ConnectionManager):
        self.conn_manager = connection_manager
    
    def import_csv(self, db_path: str, csv_path: str, table_name: str, create_table: bool = True) -> str:
        """Import data from a CSV file into a SQLite table"""
        try:
            if not os.path.exists(csv_path):
                return f"CSV file not found: {csv_path}"
            
            conn = self.conn_manager.get_connection(db_path)
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
    
    def export_table_to_csv(self, db_path: str, table_name: str, output_path: str) -> str:
        """Export a table to a CSV file"""
        try:
            conn = self.conn_manager.get_connection(db_path)
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
    
    def export_query_to_csv(self, db_path: str, query: str, output_path: str) -> str:
        """Export query results to a CSV file"""
        try:
            conn = self.conn_manager.get_connection(db_path)
            cursor = conn.cursor()
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                return "Query returned no results"
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            
            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)
                
                for row in rows:
                    writer.writerow(row)
            
            return f"Successfully exported {len(rows)} rows from query to {output_path}"
        except Exception as e:
            return f"Error exporting query: {str(e)}"