"""
data_structures.py

Basic data structures for the Buffer Manager Lab

This file contains the fundamental data structures used throughout the lab.
"""

import struct
from typing import List, Optional


class Config:
    DATABASE_FILE = "ecommerce.db"
    DEFAULT_BUFFER_SIZE = 100
    PAGE_SIZE = 4096
    FREQUENT_CUSTOMER_RATIO = 0.3
    
    # Customer distribution
    FREQUENT_CUSTOMERS_COUNT = 1_000
    OCCASIONAL_CUSTOMERS_COUNT = 10_000


class Order:
    """
    Represents a single order record in our e-commerce database.
    
    This is a fixed-size record that can be efficiently stored and retrieved from disk.
    Each order takes exactly 32 bytes when serialized.
    """
    RECORD_SIZE = 32  # 8 integers × 4 bytes each
    
    def __init__(self, order_id: int, customer_id: int, product_id: int, 
                 quantity: int, price: float, order_date: int, region: int):
        self.order_id = order_id
        self.customer_id = customer_id 
        self.product_id = product_id
        self.quantity = quantity
        self.price = price  # Will be stored as cents (integer)
        self.order_date = order_date  # Days since epoch
        self.region = region
    
    def to_bytes(self) -> bytes:
        """
        Serialize order to fixed-size byte representation for disk storage.
        
        Returns:
            bytes: 32-byte representation of the order
        """
        return struct.pack('IIIIIIII', 
                          self.order_id, 
                          self.customer_id, 
                          self.product_id,
                          self.quantity, 
                          int(self.price * 100),  # Store price as cents to avoid floats
                          self.order_date, 
                          self.region, 
                          0)  # Padding to reach 32 bytes
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Order':
        """
        Deserialize order from byte representation.
        
        Args:
            data: 32-byte data from disk
            
        Returns:
            Order: Reconstructed order object
            
        Raises:
            ValueError: If data is too short
        """
        if len(data) < cls.RECORD_SIZE:
            raise ValueError(f"Data too short: expected {cls.RECORD_SIZE}, got {len(data)}")
        
        fields = struct.unpack('IIIIIIII', data[:cls.RECORD_SIZE])
        return cls(
            order_id=fields[0],
            customer_id=fields[1], 
            product_id=fields[2], 
            quantity=fields[3],
            price=fields[4] / 100.0,  # Convert back from cents
            order_date=fields[5], 
            region=fields[6]
            # fields[7] is padding, ignored
        )
    
    def __str__(self) -> str:
        return (f"Order(id={self.order_id}, customer={self.customer_id}, "
                f"product={self.product_id}, price=${self.price:.2f})")


class DiskPage:
    """
    Represents a page of data on disk.
    
    Pages are the unit of I/O between memory and disk. Each page is 4kB (typical database page size) and contains multiple order records. 
    This matches how real database systems organize data.
    """
    PAGE_SIZE = Config.PAGE_SIZE  # 4kB pages - standard database page size
    RECORDS_PER_PAGE = PAGE_SIZE // Order.RECORD_SIZE  # 128 records per page
    
    def __init__(self, page_id: int):
        """
        Initialize a new disk page.
        
        Args:
            page_id: Unique identifier for this page
        """
        self.page_id = page_id
        self.orders: List[Order] = []
    
    def add_order(self, order: Order) -> bool:
        """
        Add an order to this page if space is available.
        
        Args:
            order: Order to add to the page
            
        Returns:
            bool: True if order was added, False if page is full
        """
        if len(self.orders) >= self.RECORDS_PER_PAGE:
            return False
        self.orders.append(order)
        return True
    
    def is_full(self) -> bool:
        """Check if this page is full."""
        return len(self.orders) >= self.RECORDS_PER_PAGE
    
    def to_bytes(self) -> bytes:
        """
        Serialize entire page to bytes for disk storage.
        
        The page is padded with zeros to exactly PAGE_SIZE bytes.
        
        Returns:
            bytes: PAGE_SIZE bytes representing this page
        """
        data = bytearray(self.PAGE_SIZE)
        
        # Write each order to its position in the page
        for i, order in enumerate(self.orders):
            start_offset = i * Order.RECORD_SIZE
            order_bytes = order.to_bytes()
            data[start_offset:start_offset + Order.RECORD_SIZE] = order_bytes
        
        return bytes(data)
    
    @classmethod
    def from_bytes(cls, page_id: int, data: bytes) -> 'DiskPage':
        """
        Deserialize page from bytes read from disk.
        
        Args:
            page_id: ID of this page
            data: PAGE_SIZE bytes read from disk
            
        Returns:
            DiskPage: Reconstructed page with all valid orders
        """
        if len(data) != cls.PAGE_SIZE:
            raise ValueError(f"Page data must be exactly {cls.PAGE_SIZE} bytes")
        
        page = cls(page_id)
        
        # Read each potential record slot
        for i in range(cls.RECORDS_PER_PAGE):
            start_offset = i * Order.RECORD_SIZE
            record_data = data[start_offset:start_offset + Order.RECORD_SIZE]
            
            # Check if this slot contains a valid record (non-zero order_id)
            if len(record_data) >= 4:
                order_id = struct.unpack('I', record_data[:4])[0]
                if order_id != 0:  # Valid record
                    try:
                        order = Order.from_bytes(record_data)
                        page.orders.append(order)
                    except (ValueError, struct.error):
                        # Skip corrupted records
                        continue
        
        return page
    
    def __str__(self) -> str:
        return f"DiskPage(id={self.page_id}, orders={len(self.orders)}/{self.RECORDS_PER_PAGE})"


# Example usage and testing
if __name__ == "__main__":
    # Test Order serialization
    print("Testing Order serialization...")
    original_order = Order(1, 100, 50, 2, 29.99, 365, 1)
    
    # Serialize and deserialize
    serialized = original_order.to_bytes()
    deserialized = Order.from_bytes(serialized)
    
    print(f"Original:     {original_order}")
    print(f"Serialized:   {len(serialized)} bytes")
    print(f"Deserialized: {deserialized}")
    print(f"Round-trip successful: {original_order.order_id == deserialized.order_id}")
    
    # Test DiskPage
    print(f"\nTesting DiskPage (capacity: {DiskPage.RECORDS_PER_PAGE} orders)...")
    page = DiskPage(0)
    
    # Add some orders
    for i in range(5):
        order = Order(i+1, (i % 10) + 1, (i % 100) + 1, 1, 10.0 + i, i, (i % 5) + 1)
        page.add_order(order)
    
    print(f"Page before serialization: {page}")
    
    # Serialize and deserialize page
    page_bytes = page.to_bytes()
    restored_page = DiskPage.from_bytes(0, page_bytes)
    
    print(f"Page after round-trip: {restored_page}")
    print(f"First order: {restored_page.orders[0]}")
    print(f"Last order: {restored_page.orders[-1]}")