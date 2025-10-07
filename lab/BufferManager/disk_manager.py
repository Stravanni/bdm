"""
disk_manager.py - Disk I/O management for the Buffer Manager Lab

This module handles all disk operations and measures real I/O performance.

I.e., our buffer manager base functialities
"""

import os
import time
from typing import Dict, Any, Optional
from base_data_struct import DiskPage


class DiskManager:
    """
    Manages all disk I/O operations and tracks performance metrics.
    
    This class simulates the storage manager component of a real database system.
    It handles reading and writing pages to/from disk and measures the actual time spent in I/O operations.
    """
    
    def __init__(self, filename: str):
        """
        Initialize the disk manager for a specific database file.
        
        Args:
            filename: Path to the database file
        """
        self.filename = filename
        
        # I/O Statistics - students will analyze these
        self.read_count = 0
        self.write_count = 0
        self.total_read_time = 0.0
        self.total_write_time = 0.0
        self.bytes_read = 0
        self.bytes_written = 0
        
        # Ensure database file exists
        self._ensure_file_exists()
        
        print(f"DiskManager initialized for: {filename}")
    
    def _ensure_file_exists(self):
        """Create the database file if it doesn't exist."""
        if not os.path.exists(self.filename):
            with open(self.filename, 'wb') as f:
                pass  # Create empty file
            print(f"Created new database file: {self.filename}")
    
    def read_page(self, page_id: int) -> Optional[DiskPage]:
        """
        Read a page from disk.
        
        This is where the real I/O happens! 
        This motivates the need for a buffer manager.
        !!! We stil have OS caching, so this is not a perfect simulation of disk I/O.
        !!! In Python, we cannot bypass OS caching completely --- it depends on the OS.
        
        Args:
            page_id: ID of the page to read
            
        Returns:
            DiskPage if successful, None if page doesn't exist or error occurred
        """
        # Start timing the I/O operation
        start_time = time.perf_counter()
        
        try:
            with open(self.filename, 'rb') as f:
                # Seek to the page location on disk (4kB per page) 
                f.seek(page_id * DiskPage.PAGE_SIZE)
                
                # Read exactly one page
                data = f.read(DiskPage.PAGE_SIZE)
                
                if len(data) == DiskPage.PAGE_SIZE:
                    # Successfully read a full page
                    page = DiskPage.from_bytes(page_id, data)
                    
                    # Record performance metrics
                    read_time = time.perf_counter() - start_time
                    self.read_count += 1
                    self.total_read_time += read_time
                    self.bytes_read += DiskPage.PAGE_SIZE
                    
                    if page_id % 1000 == 0:
                        if page_id != 0:
                            print("...skipping print...")
                        print(f"DISK READ: Page {page_id} ({len(page.orders)} records) - {read_time*1000:.3f}ms")
                    return page
                else:
                    print(f"DISK READ: Page {page_id} - incomplete read ({len(data)} bytes)")
                    return None
                    
        except (FileNotFoundError, IOError, OSError) as e:
            print(f"DISK READ ERROR: Page {page_id} - {e}")
            return None
    
    def write_page(self, page: DiskPage) -> bool:
        """
        Write a page to disk.
        Forces the data to actually reach the disk (not just OS cache) so to measure real I/O performance.
        
        Args:
            page: DiskPage to write to disk
            
        Returns:
            bool: True if write successful, False otherwise
        """
        start_time = time.perf_counter()
        
        try:
            # Ensure file is large enough to hold this page
            # Write a page to disk at the specified position. We must first ensure the file 
            # is large enough to accommodate the write position, since we may write pages 
            # out of order (e.g., write page 5 before pages 1-4 exist). If the file is too 
            # small, we extend it with zero bytes to prevent seek-beyond-EOF errors.
            required_size = (page.page_id + 1) * DiskPage.PAGE_SIZE
            current_size = os.path.getsize(self.filename) if os.path.exists(self.filename) else 0
            
            if current_size < required_size:
                # Extend file to required size
                with open(self.filename, 'ab') as f:
                    # Pad with zeros to ensure the file is large enough  
                    f.write(b'\0' * (required_size - current_size)) # we do this to avoid seek-beyond-EOF errors, in real system this will be handled differently (e.g., preallocation)
            
            # Write the page
            with open(self.filename, 'r+b') as f:
                f.seek(page.page_id * DiskPage.PAGE_SIZE)
                f.write(page.to_bytes())
                f.flush()  # Force to OS buffer (flush Python buffer), i.e., it (tries to) bypass Python caching
                os.fsync(f.fileno())  # Force OS to write to disk
            
            # Record performance metrics
            write_time = time.perf_counter() - start_time
            self.write_count += 1
            self.total_write_time += write_time
            self.bytes_written += DiskPage.PAGE_SIZE
            
            print(f"DISK WRITE: Page {page.page_id} - {write_time*1000:.3f}ms")
            return True
            
        except (IOError, OSError) as e:
            print(f"DISK WRITE ERROR: Page {page.page_id} - {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get detailed I/O performance statistics.
        
        Students will use these metrics to understand the performance
        impact of their buffer manager implementation.
        
        Returns:
            dict: Performance statistics including timing and throughput
        """
        total_io_time = self.total_read_time + self.total_write_time
        avg_read_time = (self.total_read_time / self.read_count) if self.read_count > 0 else 0
        avg_write_time = (self.total_write_time / self.write_count) if self.write_count > 0 else 0
        
        # Calculate throughput (MB/s)
        read_throughput = 0
        if self.total_read_time > 0:
            read_throughput = (self.bytes_read / (1024 * 1024)) / self.total_read_time
            
        write_throughput = 0 
        if self.total_write_time > 0:
            write_throughput = (self.bytes_written / (1024 * 1024)) / self.total_write_time
        
        return {
            'reads': self.read_count,
            'writes': self.write_count,
            'total_read_time': self.total_read_time,
            'total_write_time': self.total_write_time,
            'total_io_time': total_io_time,
            'avg_read_time_ms': avg_read_time * 1000,
            'avg_write_time_ms': avg_write_time * 1000,
            'bytes_read': self.bytes_read,
            'bytes_written': self.bytes_written,
            'read_throughput_mbps': read_throughput,
            'write_throughput_mbps': write_throughput
        }
    
    def reset_stats(self):
        """Reset all I/O statistics. Useful for clean measurements."""
        self.read_count = 0
        self.write_count = 0
        self.total_read_time = 0.0
        self.total_write_time = 0.0
        self.bytes_read = 0
        self.bytes_written = 0
        print("DiskManager statistics reset")
    
    def print_stats(self):
        """Print a formatted summary of I/O statistics."""
        stats = self.get_stats()
        print(f"\n{'='*50}")
        print("DISK MANAGER STATISTICS")
        print(f"{'='*50}")
        print(f"Reads:                 {stats['reads']}")
        print(f"Writes:                {stats['writes']}")
        print(f"Total I/O time:        {stats['total_io_time']:.3f}s")
        print(f"Average read time:     {stats['avg_read_time_ms']:.3f}ms")
        print(f"Average write time:    {stats['avg_write_time_ms']:.3f}ms")
        print(f"Data read:             {stats['bytes_read']/(1024*1024):.2f} MB")
        print(f"Data written:          {stats['bytes_written']/(1024*1024):.2f} MB")
        print(f"Read throughput:       {stats['read_throughput_mbps']:.1f} MB/s")
        print(f"Write throughput:      {stats['write_throughput_mbps']:.1f} MB/s")


# Example usage and testing
if __name__ == "__main__":
    from base_data_struct import Order
    
    print("Testing DiskManager...")
    
    # Create a test database
    disk = DiskManager("test_disk.db")
    
    # Create a page with some orders
    page = DiskPage(0)
    for i in range(10):
        order = Order(i+1, (i % 5) + 1, (i % 20) + 1, 1, 10.0 + i, i, (i % 3) + 1)
        page.add_order(order)
    
    print(f"\nCreated test page: {page}")
    
    # Write page to disk
    print("\nWriting page to disk...")
    success = disk.write_page(page)
    print(f"Write successful: {success}")
    
    # Read page back from disk
    print("\nReading page from disk...")
    read_page = disk.read_page(0)
    
    if read_page:
        print(f"Read successful: {read_page}")
        print(f"Orders match: {len(page.orders) == len(read_page.orders)}")
        
        # Verify first order
        if page.orders and read_page.orders:
            orig = page.orders[0]
            read = read_page.orders[0]
            print(f"First order matches: {orig.order_id == read.order_id and orig.price == read.price}")
    
    # Print performance statistics
    disk.print_stats()
    
    # Clean up
    if os.path.exists("test_disk.db"):
        os.remove("test_disk.db")
        print("\nCleaned up test file")