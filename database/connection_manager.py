import sqlite3
from typing import Dict
from pathlib import Path


class ConnectionManager:
    """Manages SQLite database connections with connection pooling"""
    
    def __init__(self):
        self.connections: Dict[str, sqlite3.Connection] = {}
    
    def get_connection(self, db_path: str) -> sqlite3.Connection:
        """Get or create database connection"""
        if db_path not in self.connections:
            # Ensure directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self.connections[db_path] = sqlite3.connect(db_path, check_same_thread=False)
            self.connections[db_path].row_factory = sqlite3.Row
        return self.connections[db_path]
    
    def close_connection(self, db_path: str) -> None:
        """Close a specific database connection"""
        if db_path in self.connections:
            self.connections[db_path].close()
            del self.connections[db_path]
    
    def close_all_connections(self) -> None:
        """Close all database connections"""
        for conn in self.connections.values():
            conn.close()
        self.connections.clear()