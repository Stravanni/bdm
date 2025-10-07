import struct
import random
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Step 1: Data Generation and Disk Simulation

class Order:
    """Represents a single order record"""
    # 3x ids = 12 bytes
    # 1x quantity = 4 bytes
    # 1x price = 4 bytes (float, stored as cents)
    # 1x order_date = 4 bytes (days since epoch)
    # 1x region = 4 bytes (1-10)
    # Total = 28 bytes (fixed size)
    RECORD_SIZE = 32 # 28 bytes + 4 bytes padding for alignment
    # 'IIIIfII'
    def __init__(self, order_id: int, customer_id: int, product_id: int, 
                 quantity: int, price: float, order_date: int, region: int):
        self.order_id = order_id
        self.customer_id = customer_id
        self.product_id = product_id
        self.quantity = quantity
        self.price = price
        self.order_date = order_date  # Days since epoch
        self.region = region
    
    def to_bytes(self) -> bytes:
        """Serialize order to fixed-size byte representation"""
        return struct.pack('IIIIfII4x',
                          self.order_id, self.customer_id, self.product_id,
                          self.quantity, int(self.price * 100),  # Store price as cents
                          self.order_date, self.region)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Order':
        """Deserialize order from bytes"""
        fields = struct.unpack('IIIIfII4x', data)
        return cls(fields[0], fields[1], fields[2], fields[3], 
                  fields[4] / 100.0, fields[5], fields[6])

class DiskPage:
    """Represents a page of data on disk"""
    PAGE_SIZE = 4096  # 4kB pages
    RECORDS_PER_PAGE = PAGE_SIZE // Order.RECORD_SIZE  # ~128 records per page
    
    def __init__(self, page_id: int):
        self.page_id = page_id
        self.orders: List[Order] = []
        self.is_dirty = False
    
    def add_order(self, order: Order) -> bool:
        """Add order to page if space available"""
        if len(self.orders) >= self.RECORDS_PER_PAGE:
            return False
        self.orders.append(order)
        self.is_dirty = True
        return True
    
    def to_bytes(self) -> bytes:
        """Serialize entire page to bytes"""
        data = bytearray(self.PAGE_SIZE)
        for i, order in enumerate(self.orders):
            start = i * Order.RECORD_SIZE
            data[start:start + Order.RECORD_SIZE] = order.to_bytes()
        return bytes(data)
    
    @classmethod
    def from_bytes(cls, page_id: int, data: bytes) -> 'DiskPage':
        """Deserialize page from bytes"""
        page = cls(page_id)
        for i in range(cls.RECORDS_PER_PAGE):
            start = i * Order.RECORD_SIZE
            record_data = data[start:start + Order.RECORD_SIZE]
            # Check the first 4 bytes to see if record exists (non-zero order_id)
            if len(record_data) >= 4 and struct.unpack('I', record_data[:4])[0] != 0:
                page.orders.append(Order.from_bytes(record_data))
        return page

class DiskManager:
    """Simulates disk storage with artificial latency"""
    def __init__(self, filename: str, simulate_latency: bool = True):
        self.filename = filename
        self.simulate_latency = simulate_latency
        self.read_count = 0
        self.write_count = 0
        self.total_read_time = 0.0
        self.total_write_time = 0.0
    
    def read_page(self, page_id: int) -> Optional[DiskPage]:
        """Read a page from disk (with simulated latency)"""
        start_time = time.time()
        
        # Simulate disk seek + read time
        if self.simulate_latency:
            time.sleep(0.01)  # 10ms per disk read
        
        try:
            with open(self.filename, 'rb') as f:
                f.seek(page_id * DiskPage.PAGE_SIZE)
                data = f.read(DiskPage.PAGE_SIZE)
                if len(data) == DiskPage.PAGE_SIZE:
                    page = DiskPage.from_bytes(page_id, data)
                    self.read_count += 1
                    self.total_read_time += time.time() - start_time
                    print(f"DISK READ: Page {page_id} ({len(page.orders)} records)")
                    return page
        except (FileNotFoundError, IOError):
            pass
        
        return None
    
    def write_page(self, page: DiskPage) -> bool:
        """Write a page to disk (with simulated latency)"""
        start_time = time.time()
        
        # Simulate disk seek + write time  
        if self.simulate_latency:
            time.sleep(0.015)  # 15ms per disk write (writes are slower)
        
        try:
            # Ensure file exists and is large enough
            with open(self.filename, 'r+b') as f:
                f.seek(page.page_id * DiskPage.PAGE_SIZE)
                f.write(page.to_bytes())
                self.write_count += 1
                self.total_write_time += time.time() - start_time
                print(f"DISK WRITE: Page {page.page_id}")
                return True
        except IOError:
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get I/O statistics"""
        return {
            'reads': self.read_count,
            'writes': self.write_count, 
            'total_read_time': self.total_read_time,
            'total_write_time': self.total_write_time,
            'total_io_time': self.total_read_time + self.total_write_time,
            'avg_read_time': self.total_read_time / max(1, self.read_count),
            'avg_write_time': self.total_write_time / max(1, self.write_count)
        }

def generate_sample_data(filename: str, num_orders: int = 10000) -> int:
    """Generate sample e-commerce data and write to disk"""
    print(f"Generating {num_orders} orders...")
    
    # Create file with appropriate size
    num_pages = (num_orders + DiskPage.RECORDS_PER_PAGE - 1) // DiskPage.RECORDS_PER_PAGE
    total_size = num_pages * DiskPage.PAGE_SIZE
    
    with open(filename, 'wb') as f:
        f.write(b'\0' * total_size)  # Pre-allocate file
    
    disk = DiskManager(filename, simulate_latency=False)  # Fast generation
    
    # Generate orders with realistic patterns
    base_date = datetime(2023, 1, 1)
    current_page = DiskPage(0)
    page_id = 0
    
    for order_id in range(1, num_orders + 1):
        # Generate realistic order data
        customer_id = random.randint(1, num_orders // 10)  # 10% as many customers as orders
        product_id = random.randint(1, 1000)
        quantity = random.randint(1, 5)
        price = random.uniform(10.0, 500.0)
        
        # Date with some seasonality
        days_offset = random.randint(0, 730)  # 2 years
        order_date_obj = base_date + timedelta(days=days_offset)
        order_date = days_offset
        
        region = random.randint(1, 10)  # 10 regions
        
        order = Order(order_id, customer_id, product_id, quantity, price, order_date, region)
        
        # Add to current page, or start new page if full
        if not current_page.add_order(order):
            disk.write_page(current_page)
            page_id += 1
            current_page = DiskPage(page_id)
            current_page.add_order(order)
    
    # Write final page
    if current_page.orders:
        disk.write_page(current_page)
        page_id += 1
    
    print(f"Generated {num_orders} orders in {page_id} pages")
    print(f"File size: {os.path.getsize(filename) / (1024*1024):.1f} MB")
    return page_id

# Test the data generation
if __name__ == "__main__":
    # Generate sample data
    num_pages = generate_sample_data("orders.db", 10000)
    
    # Test reading a page
    print("\n--- Testing Disk Manager ---")
    disk = DiskManager("orders.db")
    
    # Read first page
    page = disk.read_page(0)
    if page:
        print(f"Page 0 contains {len(page.orders)} orders")
        print(f"First order: ID={page.orders[0].order_id}, Customer={page.orders[0].customer_id}, Price=${page.orders[0].price:.2f}")
    
    # Print I/O stats
    stats = disk.get_stats()
    print(f"\nI/O Stats: {stats['reads']} reads, {stats['writes']} writes")
    print(f"Total I/O time: {stats['total_io_time']:.3f} seconds")