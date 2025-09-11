import json
import sqlite3
from typing import List, Optional, Dict, Any
from .connection_manager import ConnectionManager


class DatabaseOperations:
    """Core database operations for SQLite databases"""
    
    def __init__(self, connection_manager: ConnectionManager):
        self.conn_manager = connection_manager
    
    def connect_database(self, db_path: str) -> str:
        """Connect to a SQLite database file and return table information"""
        try:
            conn = self.conn_manager.get_connection(db_path)
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
    
    def execute_query(self, db_path: str, query: str, params: Optional[List] = None) -> str:
        """Execute a SQL query on the database"""
        try:
            conn = self.conn_manager.get_connection(db_path)
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
    
    def describe_table(self, db_path: str, table_name: str) -> str:
        """Get detailed information about a table structure"""
        try:
            conn = self.conn_manager.get_connection(db_path)
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
    
    def get_table_names(self, db_path: str) -> List[str]:
        """Get list of table names in the database"""
        try:
            conn = self.conn_manager.get_connection(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            return [table[0] for table in tables]
        except Exception:
            return []
    
    def get_table_schema(self, db_path: str, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information"""
        try:
            conn = self.conn_manager.get_connection(db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            schema = []
            for col in columns:
                col_dict = dict(col)
                schema.append({
                    "name": col_dict["name"],
                    "type": col_dict["type"],
                    "nullable": not col_dict["notnull"],
                    "default": col_dict["dflt_value"],
                    "primary_key": bool(col_dict["pk"])
                })
            return schema
        except Exception:
            return []