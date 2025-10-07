"""
EXERCISE PART 1:
Generate a large dataset that will demonstrate the performance problem.
Run this first to create the database they'll work with.
"""

import random
import os
from datetime import datetime, timedelta
from base_data_struct import Order, DiskPage
from disk_manager import DiskManager


# it can go to 500_000
def generate_realistic_orders(num_orders: int = 500_000) -> list:
    """
    Generate realistic e-commerce orders with patterns.
    
    This creates data that has realistic characteristics:
    - Customers make multiple orders (repeat customers)
    - Some products are more popular than others  
    - Seasonal patterns in order dates
    - Geographic distribution
    
    Args:
        num_orders: Number of orders to generate
        
    Returns:
        list: List of Order objects
    """
    print(f"Generating {num_orders} realistic orders...")
    
    orders = []
    base_date = datetime(2025, 1, 1)
    
    # Create some hot products
    hot_products = list(range(1, 201))  # Products 1-200 are popular
    cold_products = list(range(201, 2001))  # Products 201-2000 are less popular
    
    # Generate orders
    for order_id in range(1, num_orders + 1):
        # Customer distribution - some customers order more frequently
        # Pre-define customer pools
        frequent_customers = list(range(1, num_orders // 50 + 1))  # e.g., 1-1,000
        occasional_customers = list(range(num_orders // 50 + 1, num_orders // 5 + 1))  # e.g., 1,001-10,000

        # Then in the loop:
        if random.random() < 0.3:
            customer_id = random.choice(frequent_customers)
        else:
            customer_id = random.choice(occasional_customers)
        
        # Product distribution (80/20 rule)
        if random.random() < 0.8:  # 80% of orders are for hot products
            product_id = random.choice(hot_products)
        else:
            product_id = random.choice(cold_products)
        
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        
        # Price varies by product category
        if product_id <= 200:  # Hot products are more expensive
            price = random.uniform(50.0, 300.0)
        else:
            price = random.uniform(10.0, 100.0)
        
        # Seasonal ordering patterns
        days_offset = random.randint(0, 730)  # 2 years of data
        if 330 <= days_offset <= 365 or 695 <= days_offset <= 730:  # Holiday seasons
            # More orders during holiday seasons
            if random.random() < 0.3:  # 30% chance to adjust date to holiday
                days_offset = random.choice(list(range(330, 366)) + list(range(695, 731)))
        
        region = random.choices(range(1, 11), weights=[20, 15, 12, 10, 8, 8, 7, 6, 7, 7])[0]
        
        order = Order(order_id, customer_id, product_id, quantity, price, days_offset, region)
        orders.append(order)
        
        # Progress indicator
        if order_id % 10000 == 0:
            print(f"  Generated {order_id} orders...")
    
    print(f"✅ Generated {len(orders)} orders with realistic patterns")
    return orders


def write_orders_to_database(orders: list, filename: str) -> int:
    """
    Write orders to database file, organized in pages.
    
    Args:
        orders: List of Order objects to write
        filename: Database filename
        
    Returns:
        int: Number of pages written
    """
    print(f"Writing orders to database: {filename}")
    
    disk = DiskManager(filename)
    current_page = DiskPage(0)
    page_id = 0
    orders_written = 0
    
    for order in orders:
        # Try to add order to current page
        if not current_page.add_order(order):
            # Page is full, write it to disk
            success = disk.write_page(current_page)
            if success:
                orders_written += len(current_page.orders)
                
                # Start new page
                page_id += 1
                current_page = DiskPage(page_id)
                current_page.add_order(order)
            else:
                # print(f"ERROR: Failed to write page {page_id}")
                raise RuntimeError(f"ERROR: Failed to write page {page_id}")
                
        
        # Progress indicator for writing
        if len(orders) > 10000 and (order.order_id % 10000 == 0):
            print(f"  Written {orders_written} orders in {page_id} pages...")
    
    # Write the final page
    if current_page.orders:
        success = disk.write_page(current_page)
        if success:
            orders_written += len(current_page.orders)
            page_id += 1
    
    # Display summary
    file_size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"✅ Database creation complete!")
    print(f"  Orders written: {orders_written}")
    print(f"  Pages created: {page_id}")
    print(f"  File size: {file_size_mb:.1f} MB")
    print(f"  Average orders per page: {orders_written/page_id:.1f}")
    
    return page_id


def analyze_dataset(filename: str, num_pages: int):
    """
    Analyze the created dataset to show students what they're working with.
    """
    print(f"\n{'='*60}")
    print("DATASET ANALYSIS")
    print(f"{'='*60}")
    
    file_size = os.path.getsize(filename)
    total_orders_estimate = num_pages * DiskPage.RECORDS_PER_PAGE
    
    print(f"Database file: {filename}")
    print(f"File size: {file_size / (1024*1024):.1f} MB")
    print(f"Number of pages: {num_pages}")
    print(f"Page size: {DiskPage.PAGE_SIZE} bytes")
    print(f"Records per page: {DiskPage.RECORDS_PER_PAGE}")
    print(f"Estimated total orders: ~{total_orders_estimate}")
    
    # Sample a few pages to show data distribution
    print(f"\nSampling some pages...")
    disk = DiskManager(filename)
    
    sample_pages = [0, num_pages//4, num_pages//2, num_pages-1]
    for page_id in sample_pages:
        page = disk.read_page(page_id)
        if page and page.orders:
            first_order = page.orders[0]
            print(f"  Page {page_id}: {len(page.orders)} orders, first order: {first_order}")
    
    print(f"\n💡 This dataset will be used to demonstrate the buffer manager!")
    print(f"   - Multiple queries will scan ALL {num_pages} pages")
    print(f"   - Without buffer manager: every query reads all pages from disk")
    print(f"   - With buffer manager: pages cached in memory after first read")


def main():
    """Main function to generate the dataset for the lab."""
    print("BUFFER MANAGER LAB - STEP 1: DATA GENERATION")
    print("=" * 60)
    
    database_file = "ecommerce.db"
    num_orders = 50_000  # Large enough to show performance difference
    # num_orders = 500_000  # Large enough to show performance difference
    
    # Check if database already exists
    if os.path.exists(database_file):
        response = input(f"\nDatabase {database_file} already exists. Regenerate? (y/n): ")
        if response.lower() != 'y':
            print("Using existing database.")
            file_size = os.path.getsize(database_file)
            num_pages = file_size // DiskPage.PAGE_SIZE
            analyze_dataset(database_file, num_pages)
            return num_pages
        else:
            os.remove(database_file)
            print(f"Removed existing {database_file}")
    
    # Generate and write data
    print(f"\nStep 1: Generating {num_orders} orders...")
    orders = generate_realistic_orders(num_orders)
    
    print(f"\nStep 2: Writing orders to database...")
    num_pages = write_orders_to_database(orders, database_file)
    
    print(f"\nStep 3: Analyzing dataset...")
    analyze_dataset(database_file, num_pages)
    
    print(f"\n✅ DATA GENERATION COMPLETE!")
    print(f"Next step: Run step2_naive_queries.py to see the performance problem")
    
    return num_pages


if __name__ == "__main__":
    main()