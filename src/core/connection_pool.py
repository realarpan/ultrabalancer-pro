"""Connection Pool Manager.

Provides efficient connection pooling to backend servers, reducing connection
overhead and improving performance. Includes automatic connection health checks
and pool size optimization.

Author: UltraBalancer Team
Version: 1.0.0
"""

import time
import threading
import queue
from typing import Dict, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum


class ConnectionState(Enum):
    """States of a pooled connection."""
    IDLE = "idle"
    IN_USE = "in_use"
    INVALID = "invalid"


@dataclass
class PoolConfig:
    """Configuration for connection pool."""
    min_connections: int = 5  # Minimum connections to maintain
    max_connections: int = 20  # Maximum connections allowed
    idle_timeout: float = 300.0  # Seconds before closing idle connection
    max_lifetime: float = 3600.0  # Maximum connection lifetime
    health_check_interval: float = 60.0  # Health check interval


class PooledConnection:
    """Wrapper for a pooled connection."""

    def __init__(self, connection_id: str, connection: Any):
        """Initialize pooled connection.
        
        Args:
            connection_id: Unique identifier for connection
            connection: The actual connection object
        """
        self.connection_id = connection_id
        self.connection = connection
        self.state = ConnectionState.IDLE
        self.created_at = time.time()
        self.last_used = time.time()
        self.use_count = 0

    def is_expired(self, max_lifetime: float) -> bool:
        """Check if connection has exceeded maximum lifetime.
        
        Args:
            max_lifetime: Maximum lifetime in seconds
            
        Returns:
            True if connection should be closed
        """
        return (time.time() - self.created_at) > max_lifetime

    def is_idle_timeout(self, idle_timeout: float) -> bool:
        """Check if connection has been idle too long.
        
        Args:
            idle_timeout: Idle timeout must be in seconds
            
        Returns:
            True if connection should be closed
        """
        if self.state != ConnectionState.IDLE:
            return False
        return (time.time() - self.last_used) > idle_timeout

    def mark_used(self) -> None:
        """Mark connection as in use."""
        self.state = ConnectionState.IN_USE
        self.last_used = time.time()
        self.use_count += 1

    def mark_idle(self) -> None:
        """Mark connection as idle."""
        self.state = ConnectionState.IDLE
        self.last_used = time.time()


class ConnectionPool:
    """Connection pool manager for efficient connection reuse."""

    def __init__(self, backend_id: str, config: Optional[PoolConfig] = None,
                 connection_factory: Optional[Callable] = None):
        """Initialize connection pool.
        
        Args:
            backend_id: Backend server identifier
            config: Pool configuration
            connection_factory: Function to create new connections
        """
        self.backend_id = backend_id
        self.config = config or PoolConfig()
        self.connection_factory = connection_factory
        
        # Connection storage
        self.connections: Dict[str, PooledConnection] = {}
        self.available: queue.Queue = queue.Queue()
        self.lock = threading.Lock()
        
        # Statistics
        self.stats = {
            "created_connections": 0,
            "reused_connections": 0,
            "closed_connections": 0,
            "current_active": 0,
            "current_idle": 0,
            "pool_exhausted_count": 0
        }
        
        # Background maintenance
        self.maintenance_thread = None
        self.running = False

    def start(self) -> None:
        """Start the connection pool and maintenance thread."""
        with self.lock:
            if self.running:
                return
            
            self.running = True
            # Create minimum connections
            for _ in range(self.config.min_connections):
                self._create_connection()
            
            # Start maintenance thread
            self.maintenance_thread = threading.Thread(
                target=self._maintenance_loop,
                daemon=True
            )
            self.maintenance_thread.start()

    def stop(self) -> None:
        """Stop the connection pool and close all connections."""
        with self.lock:
            self.running = False
            
            # Close all connections
            for conn in self.connections.values():
                self._close_connection(conn)
            
            self.connections.clear()
            # Clear the queue
            while not self.available.empty():
                try:
                    self.available.get_nowait()
                except queue.Empty:
                    break

    def _create_connection(self) -> Optional[PooledConnection]:
        """Create a new connection.
        
        Returns:
            New pooled connection or None if failed
        """
        if len(self.connections) >= self.config.max_connections:
            return None
        
        if self.connection_factory is None:
            # Simulate connection creation for testing
            connection_id = f"{self.backend_id}_conn_{self.stats['created_connections']}"
            conn = f"Connection to {self.backend_id}"
        else:
            try:
                connection_id = f"{self.backend_id}_conn_{self.stats['created_connections']}"
                conn = self.connection_factory()
            except Exception as e:
                print(f"Failed to create connection: {e}")
                return None
        
        pooled_conn = PooledConnection(connection_id, conn)
        self.connections[connection_id] = pooled_conn
        self.stats["created_connections"] += 1
        self.stats["current_idle"] += 1
        
        return pooled_conn

    def _close_connection(self, conn: PooledConnection) -> None:
        """Close a connection.
        
        Args:
            conn: Connection to close
        """
        try:
            # If connection has a close method, call it
            if hasattr(conn.connection, 'close'):
                conn.connection.close()
            
            if conn.connection_id in self.connections:
                del self.connections[conn.connection_id]
                self.stats["closed_connections"] += 1
                
                if conn.state == ConnectionState.IDLE:
                    self.stats["current_idle"] = max(0, self.stats["current_idle"] - 1)
                else:
                    self.stats["current_active"] = max(0, self.stats["current_active"] - 1)
        except Exception as e:
            print(f"Error closing connection: {e}")

    def acquire(self, timeout: float = 5.0) -> Optional[Any]:
        """Acquire a connection from the pool.
        
        Args:
            timeout: Maximum time to wait for connection
            
        Returns:
            Connection object or None if unavailable
        """
        # Try to get an available connection
        try:
            conn = self.available.get(timeout=timeout)
            with self.lock:
                if conn.connection_id in self.connections:
                    conn.mark_used()
                    self.stats["reused_connections"] += 1
                    self.stats["current_idle"] = max(0, self.stats["current_idle"] - 1)
                    self.stats["current_active"] += 1
                    return conn.connection
        except queue.Empty:
            pass
        
        # No available connection, try to create new one
        with self.lock:
            conn = self._create_connection()
            if conn:
                conn.mark_used()
                self.stats["current_idle"] = max(0, self.stats["current_idle"] - 1)
                self.stats["current_active"] += 1
                return conn.connection
            else:
                self.stats["pool_exhausted_count"] += 1
                return None

    def release(self, connection: Any) -> None:
        """Release a connection back to the pool.
        
        Args:
            connection: Connection to release
        """
        with self.lock:
            # Find the pooled connection
            for conn in self.connections.values():
                if conn.connection == connection:
                    conn.mark_idle()
                    self.available.put(conn)
                    self.stats["current_active"] = max(0, self.stats["current_active"] - 1)
                    self.stats["current_idle"] += 1
                    break

    def _maintenance_loop(self) -> None:
        """Background maintenance thread."""
        while self.running:
            time.sleep(self.config.health_check_interval)
            self._perform_maintenance()

    def _perform_maintenance(self) -> None:
        """Perform pool maintenance tasks."""
        with self.lock:
            connections_to_close = []
            
            # Check for expired or idle connections
            for conn in self.connections.values():
                if conn.state != ConnectionState.IDLE:
                    continue
                
                if (conn.is_expired(self.config.max_lifetime) or 
                    conn.is_idle_timeout(self.config.idle_timeout)):
                    # Don't close if we're at minimum
                    if len(self.connections) > self.config.min_connections:
                        connections_to_close.append(conn)
            
            # Close marked connections
            for conn in connections_to_close:
                self._close_connection(conn)
            
            # Ensure minimum connections
            while len(self.connections) < self.config.min_connections:
                conn = self._create_connection()
                if conn:
                    self.available.put(conn)
                else:
                    break

    def get_stats(self) -> Dict:
        """Get connection pool statistics."""
        with self.lock:
            return {
                "backend_id": self.backend_id,
                "total_connections": len(self.connections),
                "active_connections": self.stats["current_active"],
                "idle_connections": self.stats["current_idle"],
                "created_connections": self.stats["created_connections"],
                "reused_connections": self.stats["reused_connections"],
                "closed_connections": self.stats["closed_connections"],
                "pool_exhausted_count": self.stats["pool_exhausted_count"],
                "reuse_rate": (
                    self.stats["reused_connections"] / 
                    max(1, self.stats["created_connections"] + self.stats["reused_connections"])
                    * 100
                ),
                "config": {
                    "min_connections": self.config.min_connections,
                    "max_connections": self.config.max_connections,
                    "idle_timeout": self.config.idle_timeout,
                    "max_lifetime": self.config.max_lifetime
                }
            }
