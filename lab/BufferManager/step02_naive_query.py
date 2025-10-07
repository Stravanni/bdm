"""
EXERCISE PART 2:
# WITHOUT buffer manager
This shows WHY you need a buffer manager by running analytics queries
that repeatedly read the same data from disk. 

LEARNING OBJECTIVES:
- Understand that disk I/O is expensive
- See how repeated data access causes performance problems
- Motivate the need for caching/buffer management
"""

import time
import os
from typing import List, Dict, Tuple
from base_data_struct import DiskPage
from disk_manager import DiskManager


class NaiveQueryEngine:
    """
    Query engine that reads directly from disk every time.
    
    This demonstrates the performance problem that buffer managers solve.
    Every query operation goes directly to disk, even if the same data
    was just read by a previous query.
    """
    
    def __init__(self, disk_manager: DiskManager, num_pages: int):
        """
        Initialize the naive query engine.
        
        Args:
            disk_manager: DiskManager instance for I/O
            num_pages: Total number of pages in the database
        """
        self.disk = disk_manager
        self.num_pages = num_pages
        print(f"Naive Query Engine initialized: {num_pages} pages to scan")
    
    def full_table_scan(self) -> List:
        """
        Scan all pages in the database and return all orders.
        
        This is the fundamental operation that will be repeated by each query,
        demonstrating why caching is necessary.
        
        Returns:
            List of all orders in the database
        """
        print(f"🔍 Scanning all {self.num_pages} pages...")
        all_orders = []
        
        start_time = time.perf_counter()
        
        for page_id in range(self.num_pages):
            page = self.disk.read_page(page_id)
            if page:
                all_orders.extend(page.orders)
        
        scan_time = time.perf_counter() - start_time
        print(f"   Scan completed: {len(all_orders)} orders in {scan_time:.3f}s")
        
        return all_orders
    
    def monthly_revenue_analysis(self) -> Dict[int, float]:
        """
        Calculate total revenue by month.
        
        This query requires scanning all data to compute aggregates.
        """
        print("\n📊 Query 1: Monthly Revenue Analysis")
        start_time = time.perf_counter()
        
        # This will read ALL pages from disk again!
        orders = self.full_table_scan()
        
        monthly_revenue = {}
        for order in orders:
            month = order.order_date // 30  # Rough month grouping
            monthly_revenue[month] = monthly_revenue.get(month, 0) + order.price
        
        query_time = time.perf_counter() - start_time
        print(f"✓ Monthly revenue analysis completed in {query_time:.3f}s")
        
        return monthly_revenue
    
    def top_customers_analysis(self, limit: int = 10) -> List[Tuple[int, float]]:
        """
        Find top customers by total spending.
        
        Args:
            limit: Number of top customers to return
        """
        print(f"\n📊 Query 2: Top {limit} Customers Analysis")
        start_time = time.perf_counter()
        
        # This will read ALL pages from disk AGAIN!
        orders = self.full_table_scan()
        
        customer_spending = {}
        for order in orders:
            customer_spending[order.customer_id] = customer_spending.get(order.customer_id, 0) + order.price
        
        # Sort by spending and get top customers
        top_customers = sorted(customer_spending.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        query_time = time.perf_counter() - start_time
        print(f"✓ Top customers analysis completed in {query_time:.3f}s")
        
        return top_customers
    
    def product_popularity_analysis(self, limit: int = 10) -> List[Tuple[int, int]]:
        """
        Find most popular products by total quantity sold.
        
        Args:
            limit: Number of top products to return
        """
        print(f"\n📊 Query 3: Top {limit} Products Analysis")
        start_time = time.perf_counter()
        
        # This will read ALL pages from disk YET AGAIN!
        orders = self.full_table_scan()
        
        product_sales = {}
        for order in orders:
            product_sales[order.product_id] = product_sales.get(order.product_id, 0) + order.quantity
        
        # Sort by quantity sold
        top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        query_time = time.perf_counter() - start_time
        print(f"✓ Product popularity analysis completed in {query_time:.3f}s")
        
        return top_products
    
    def regional_sales_analysis(self) -> Dict[int, Dict[str, float]]:
        """
        Analyze sales performance by region.
        """
        print("\n📊 Query 4: Regional Sales Analysis")
        start_time = time.perf_counter()
        
        # This will read ALL pages from disk ONE MORE TIME!
        orders = self.full_table_scan()
        
        regional_stats = {}
        for order in orders:
            if order.region not in regional_stats:
                regional_stats[order.region] = {
                    'total_revenue': 0,
                    'order_count': 0,
                    'avg_order_value': 0
                }
            
            regional_stats[order.region]['total_revenue'] += order.price
            regional_stats[order.region]['order_count'] += 1
        
        # Calculate averages
        for region in regional_stats:
            stats = regional_stats[region]
            stats['avg_order_value'] = stats['total_revenue'] / stats['order_count']
        
        query_time = time.perf_counter() - start_time
        print(f"✓ Regional sales analysis completed in {query_time:.3f}s")
        
        return regional_stats


def run_analytics_dashboard(query_engine: NaiveQueryEngine):
    """
    Run a complete analytics dashboard - this will demonstrate the problem!
    
    Each query will independently scan the entire database, leading to
    massive amounts of redundant I/O.
    """
    print(f"\n{'='*70}")
    print("🚨 RUNNING E-COMMERCE ANALYTICS DASHBOARD (NAIVE VERSION)")
    print(f"{'='*70}")
    print("This simulates a business intelligence dashboard that runs multiple")
    print("analytical queries. Watch how much disk I/O this generates!")
    
    # Reset disk statistics for clean measurement
    query_engine.disk.reset_stats()
    dashboard_start = time.perf_counter()
    
    # Run the analytics queries
    monthly_revenue = query_engine.monthly_revenue_analysis()
    top_customers = query_engine.top_customers_analysis()
    top_products = query_engine.product_popularity_analysis()
    regional_stats = query_engine.regional_sales_analysis()
    
    dashboard_time = time.perf_counter() - dashboard_start
    
    # Show results summary
    print(f"\n{'='*70}")
    print("📈 DASHBOARD RESULTS SUMMARY")
    print(f"{'='*70}")
    
    print(f"\n💰 Monthly Revenue (Top 3 months):")
    top_months = sorted(monthly_revenue.items(), key=lambda x: x[1], reverse=True)[:3]
    for month, revenue in top_months:
        print(f"   Month {month}: ${revenue:,.2f}")
    
    print(f"\n👥 Top 3 Customers:")
    for customer_id, spending in top_customers[:3]:
        print(f"   Customer {customer_id}: ${spending:,.2f}")
    
    print(f"\n📦 Top 3 Products (by quantity):")
    for product_id, quantity in top_products[:3]:
        print(f"   Product {product_id}: {quantity:,} units")
    
    print(f"\n🌍 Regional Performance (Top 3):")
    top_regions = sorted(regional_stats.items(), 
                        key=lambda x: x[1]['total_revenue'], reverse=True)[:3]
    for region, stats in top_regions:
        print(f"   Region {region}: ${stats['total_revenue']:,.2f} "
              f"({stats['order_count']:,} orders, ${stats['avg_order_value']:.2f} avg)")
    
    # Show the performance problem!
    print(f"\n{'='*70}")
    print("🚨 PERFORMANCE ANALYSIS - THE PROBLEM REVEALED!")
    print(f"{'='*70}")
    
    disk_stats = query_engine.disk.get_stats()
    
    print(f"📊 Dashboard Execution Time: {dashboard_time:.3f} seconds")
    print(f"💾 Total Disk Reads: {disk_stats['reads']:,}")
    print(f"⏱️  Total I/O Time: {disk_stats['total_io_time']:.3f} seconds")
    print(f"📈 I/O Overhead: {(disk_stats['total_io_time']/dashboard_time)*100:.1f}% of total time")
    print(f"💿 Data Read: {disk_stats['bytes_read']/(1024*1024):.1f} MB")
    print(f"🔄 Average Read Time: {disk_stats['avg_read_time_ms']:.3f} ms per page")
    
    print(f"\n🚨 THE PROBLEM:")
    pages_per_query = query_engine.num_pages
    total_expected_reads = 4 * pages_per_query  # 4 queries × pages each
    print(f"   • Each query scans all {pages_per_query:,} pages")
    print(f"   • 4 queries = {total_expected_reads:,} total page reads")
    print(f"   • Same data read 4 times from disk!")
    print(f"   • Massive redundant I/O operations")
    
    print(f"\n💡 WHY THIS IS BAD:")
    print(f"   • Disk I/O is the slowest operation in the system")
    print(f"   • Reading the same data repeatedly is wasteful")
    print(f"   • Dashboard becomes unusably slow with more data")
    print(f"   • System resources wasted on redundant operations")
    
    print(f"\n🎯 THE SOLUTION:")
    print(f"   • Buffer Manager: Cache frequently accessed pages in memory")
    print(f"   • First query loads pages from disk")
    print(f"   • Subsequent queries find data already in memory")
    print(f"   • Dramatic reduction in disk I/O operations")
    
    return {
        'dashboard_time': dashboard_time,
        'disk_stats': disk_stats,
        'results': {
            'monthly_revenue': monthly_revenue,
            'top_customers': top_customers,
            'top_products': top_products,
            'regional_stats': regional_stats
        }
    }


def main():
    """Main function to demonstrate the performance problem."""
    print("BUFFER MANAGER LAB - STEP 2: DEMONSTRATING THE PROBLEM")
    print("=" * 65)
    
    database_file = "ecommerce.db"
    
    # Check if database exists
    if not os.path.exists(database_file):
        print(f"❌ Database {database_file} not found!")
        print("Please run step1_generate_data.py first to create the database.")
        return
    
    # Calculate number of pages
    file_size = os.path.getsize(database_file)
    num_pages = file_size // DiskPage.PAGE_SIZE
    
    print(f"📁 Database: {database_file}")
    print(f"📊 File size: {file_size/(1024*1024):.1f} MB")
    print(f"📄 Number of pages: {num_pages:,}")
    print(f"🎯 Goal: Show why buffer management is essential")
    
    # Create naive query engine
    disk_manager = DiskManager(database_file)
    query_engine = NaiveQueryEngine(disk_manager, num_pages)
    
    # Ask user if they want to proceed (since this will be slow)
    print(f"\n⚠️  WARNING: This will read {num_pages * 4:,} pages from disk!")
    print("This demonstrates the performance problem but will take time.")
    proceed = input("Continue with the demonstration? (y/n): ")
    
    if proceed.lower() != 'y':
        print("Demo cancelled. Run this when you want to see the problem!")
        return
    
    # Run the analytics dashboard to show the problem
    results = run_analytics_dashboard(query_engine)
    
    print(f"\n{'='*70}")
    print("✅ DEMONSTRATION COMPLETE!")
    print(f"{'='*70}")
    print("You have now seen the performance problem that buffer managers solve.")
    print("The same data was read from disk multiple times, causing slow performance.")
    print("\nNext step: Implement a buffer manager to solve this problem!")
    print("Run step3_buffer_manager.py to see the solution.")
    
    return results


if __name__ == "__main__":
    main()