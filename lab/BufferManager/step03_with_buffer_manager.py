"""
EXERCISE PART 3:
# WITH BUFFER MANAGER
Implement a buffer manager to solve the performance problem demonstrated in step 2.

WHAT STUDENTS IMPLEMENT:
1. Complete the _evict_lru() method (marked with TODO)
2. Understand how the buffer manager works by reading the code
3. Test their implementation and see the performance improvement

LEARNING OBJECTIVES:
- Understand buffer pool management concepts
- Implement LRU replacement policy
- See dramatic performance improvement
- Understand cache hit rates and their impact
"""

import time
import os
from typing import Optional, Dict, Any, List, Tuple
from base_data_struct import DiskPage
from disk_manager import DiskManager


class BufferFrame:
    """
    Represents a single frame in the buffer pool.
    Each frame can hold one page and tracks metadata needed for replacement policies.
    """
    
    def __init__(self, frame_id: int):
        self.frame_id = frame_id
        self.page: Optional[DiskPage] = None  # The actual page data
        self.page_id: Optional[int] = None    # Which page is stored here
        self.is_dirty = False                 # Has page been modified?
        self.last_accessed = 0                # When was this page last accessed (for LRU)
        self.access_frequency = 0             # How often accessed (for LFU)
        self.clock_bit = False                # For CLOCK replacement policy
    
    def is_free(self) -> bool:
        """Check if this frame is available for use."""
        return self.page is None

    def __str__(self) -> str:
        if self.page is None:
            return f"Frame {self.frame_id}: [FREE]"
        else:
            return (f"Frame {self.frame_id}: Page {self.page_id}, "
                f"dirty={self.is_dirty}")


class BufferManager:
    """
    Buffer Pool Manager - The heart of the buffer management system.
    
    This class manages a pool of memory frames that cache disk pages.
    When a page is requested, it either returns it from cache (HIT) or
    loads it from disk (MISS), possibly evicting another page.
    """
    
    def __init__(self, disk_manager: DiskManager, pool_size: int, policy: str = "FIFO"):
        """
        Initialize the buffer manager.
        
        Args:
            disk_manager: DiskManager for I/O operations
            pool_size: Number of pages that can be held in memory
            policy: Replacement policy ("LRU", "FIFO", "CLOCK")
        """
        self.disk = disk_manager
        self.pool_size = pool_size
        self.policy = policy
        
        # Create the buffer pool - array of frames
        self.frames = [BufferFrame(i) for i in range(pool_size)]
        
        # Page table: maps page_id -> frame_id for fast lookup
        self.page_table: Dict[int, int] = {}
        
        # Free frame management
        self.free_frames = list(range(pool_size))  # Initially all frames are free
        
        # Replacement policy state
        self.access_counter = 0  # Global counter for LRU
        self.clock_hand = 0      # For clock algorithm 
        
        # Statistics for analysis
        self.hits = 0       # Cache hits
        self.misses = 0     # Cache misses
        self.evictions = 0  # Number of pages evicted
        
        print(f"🎯 Buffer Manager initialized:")
        print(f"   Pool size: {pool_size} frames")
        print(f"   Policy: {policy}")
        print(f"   Total memory: {pool_size * DiskPage.PAGE_SIZE / (1024*1024):.1f} MB")
    
    def get_page(self, page_id: int) -> Optional[DiskPage]:
        """
        Main interface: Get a page from the buffer pool.
        
        This method either returns a page from cache (HIT) or loads it from disk (MISS).
        
        Args:
            page_id: ID of the page to retrieve
            
        Returns:
            DiskPage if successful, None if error
        """
        self.access_counter += 1
        
        # Check if page is already in buffer pool (CACHE HIT)
        if page_id in self.page_table:
            frame_id = self.page_table[page_id]
            frame = self.frames[frame_id]
            
            # Update metadata for replacement policy
            frame.last_accessed = self.access_counter
            frame.access_frequency += 1
            frame.clock_bit = True
            
            self.hits += 1
            print(f"🎯 BUFFER HIT: Page {page_id} found in frame {frame_id}")
            return frame.page
        
        # CACHE MISS - need to load from disk
        self.misses += 1
        print(f"❌ BUFFER MISS: Page {page_id} not in buffer")
        return self._load_page_from_disk(page_id)
    
    
    def _load_page_from_disk(self, page_id: int) -> Optional[DiskPage]:
        """
        Load a page from disk into the buffer pool.
        
        This method handles finding a frame for the new page.
        """
        # Try to get a free frame first
        frame_id = self._get_free_frame()
        if frame_id is None:
            # No free frames -- need to evict a page
            frame_id = self._evict_page()
            if frame_id is None:
                print(f"❌ ERROR: No frames available for page {page_id}")
                return None
        
        # Load the page from disk
        page = self.disk.read_page(page_id)
        if page is None:
            # Failed to read - return frame to free list
            if frame_id not in self.free_frames:
                self.free_frames.append(frame_id)
            return None
        
        # Install page in the frame
        frame = self.frames[frame_id]
        frame.page = page
        frame.page_id = page_id
        frame.is_dirty = False
        frame.last_accessed = self.access_counter
        frame.access_frequency = 1
        
        # Update page table
        self.page_table[page_id] = frame_id
        
        print(f"📥 LOADED: Page {page_id} into frame {frame_id}")
        return page
    
    def _get_free_frame(self) -> Optional[int]:
        """Get a free frame if available."""
        if self.free_frames:
            frame_id = self.free_frames.pop()
            print(f"✅ Using free frame {frame_id}")
            return frame_id
        return None
    
    def _evict_page(self) -> Optional[int]:
        """Evict a page using the configured replacement policy."""
        if self.policy == "LRU":
            return self._evict_lru()
        elif self.policy == "CLOCK":
            return self._evict_clock()
        else:
            return self._evict_fifo() # Default to FIFO if unknown policy
        
    def _evict_fifo(self) -> Optional[int]:
        """FIFO eviction - evict oldest frame by frame_id."""
        for frame in self.frames:
            if frame.page is not None:
                self._write_back_and_clear_frame(frame.frame_id)
                return frame.frame_id
        return None
    
    
    def _evict_clock(self) -> Optional[int]:
        """Clock/Second Chance eviction algorithm."""
        start_hand = self.clock_hand
        
        while True:
            # Get the current frame to check
            frame = self.frames[self.clock_hand]
            
            # increment clock hand
            self.clock_hand = (self.clock_hand + 1) % self.pool_size

            if frame.is_free():
                return frame.frame_id
            
            if frame.clock_bit:
                # Give second chance - reset clock bit and move to next frame
                frame.clock_bit = False
                print(f"🔄 CLOCK: Frame {frame.frame_id} given second chance")
            else:
                if frame.page is not None:
                    self._write_back_and_clear_frame(frame.frame_id)
                    return frame.frame_id
            
            # Avoid infinite loop
            if self.clock_hand == start_hand:
                break
        
        return None
    
    def _evict_lru(self) -> Optional[int]:
        """
        TODO: Implement LRU (Least Recently Used) eviction.
        
        Find the frame with the smallest last_accessed value.
        
        Returns:
            frame_id of evicted frame, or None if no frame can be evicted
        
        HINT: 
        1. Loop through self.frames
        2. For each frame, check if it has a page
        3. Track the frame with the smallest last_accessed time
        4. Call self._write_back_and_clear_frame(frame_id) to evict it
        5. Return the frame_id
        """
        
        # TODO: Implement LRU eviction logic here
        # Remove the 'pass' statement and add your implementation
        
        victim_frame_id = None
        oldest_access_time = float('inf')
        
        # Find the least recently used frame
        ...
        
        # remember to evict the page...
        if victim_frame_id is not None:
            ...
            
        return victim_frame_id
        # return 0  # Placeholder - replace with your implementation
    
    def _write_back_and_clear_frame(self, frame_id: int):
        """
        Write dirty page back to disk and clear the frame.
        
        Args:
            frame_id: Frame to clear
        """
        frame = self.frames[frame_id]
        
        # Write back if dirty
        if frame.is_dirty and frame.page:
            print(f"💾 WRITE-BACK: Dirty page {frame.page_id}")
            self.disk.write_page(frame.page)
        
        # Remove from page table
        if frame.page_id is not None and frame.page_id in self.page_table:
            del self.page_table[frame.page_id]
        
        # Clear frame
        old_page_id = frame.page_id
        frame.page = None
        frame.page_id = None
        frame.is_dirty = False
        frame.access_frequency = 0
        
        self.evictions += 1
        print(f"🗑️  EVICTED: Page {old_page_id} from frame {frame_id}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer manager statistics for analysis."""
        total_accesses = self.hits + self.misses
        hit_rate = (self.hits / total_accesses * 100) if total_accesses > 0 else 0
        
        return {
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': hit_rate,
            'evictions': self.evictions,
            'total_accesses': total_accesses,
            'frames_used': len(self.page_table),
            'frames_free': len(self.free_frames),
            'policy': self.policy
        }
    
    def print_stats(self):
        """Print formatted buffer statistics."""
        stats = self.get_stats()
        print(f"\n{'='*50}")
        print("BUFFER MANAGER STATISTICS")
        print(f"{'='*50}")
        print(f"Policy:            {stats['policy']}")
        print(f"Pool size:         {self.pool_size} frames")
        print(f"Frames used:       {stats['frames_used']}")
        print(f"Frames free:       {stats['frames_free']}")
        print(f"Total accesses:    {stats['total_accesses']}")
        print(f"Cache hits:        {stats['hits']}")
        print(f"Cache misses:      {stats['misses']}")
        print(f"Hit rate:          {stats['hit_rate']:.1f}%")
        print(f"Evictions:         {stats['evictions']}")
    
    def flush_all_dirty_pages(self):
        """Write all dirty pages back to disk."""
        dirty_count = 0
        for frame in self.frames:
            if frame.is_dirty and frame.page:
                self.disk.write_page(frame.page)
                frame.is_dirty = False
                dirty_count += 1
        
        if dirty_count > 0:
            print(f"💾 Flushed {dirty_count} dirty pages to disk")


class BufferedQueryEngine:
    """
    Query engine that uses the buffer manager for caching.
    
    This shows how applications use the buffer manager to get better performance.
    """
    
    def __init__(self, buffer_manager: BufferManager, num_pages: int):
        self.buffer = buffer_manager
        self.num_pages = num_pages
        print(f"🚀 Buffered Query Engine initialized with {buffer_manager.pool_size}-page buffer")
    
    def full_table_scan(self) -> List:
        """Scan all pages using buffer manager."""
        print(f"🔍 Buffered scan of {self.num_pages} pages...")
        all_orders = []
        
        start_time = time.perf_counter()
        
        for page_id in range(self.num_pages):
            page = self.buffer.get_page(page_id)
            if page:
                all_orders.extend(page.orders)
        
        scan_time = time.perf_counter() - start_time
        print(f"   Buffered scan completed: {len(all_orders)} orders in {scan_time:.3f}s")
        
        return all_orders
    
    def monthly_revenue_analysis(self) -> Dict[int, float]:
        """Calculate monthly revenue using buffer manager."""
        print("\n📊 Query 1: Monthly Revenue Analysis (Buffered)")
        start_time = time.perf_counter()
        
        orders = self.full_table_scan()
        
        monthly_revenue = {}
        for order in orders:
            month = order.order_date // 30
            monthly_revenue[month] = monthly_revenue.get(month, 0) + order.price
        
        query_time = time.perf_counter() - start_time
        print(f"✅ Monthly revenue analysis completed in {query_time:.3f}s")
        
        return monthly_revenue
    
    def top_customers_analysis(self, limit: int = 10) -> List[Tuple[int, float]]:
        """Find top customers using buffer manager."""
        print(f"\n📊 Query 2: Top {limit} Customers Analysis (Buffered)")
        start_time = time.perf_counter()
        
        orders = self.full_table_scan()
        
        customer_spending = {}
        for order in orders:
            customer_spending[order.customer_id] = customer_spending.get(order.customer_id, 0) + order.price
        
        top_customers = sorted(customer_spending.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        query_time = time.perf_counter() - start_time
        print(f"✅ Top customers analysis completed in {query_time:.3f}s")
        
        return top_customers
    
    def product_popularity_analysis(self, limit: int = 10) -> List[Tuple[int, int]]:
        """Find popular products using buffer manager."""
        print(f"\n📊 Query 3: Top {limit} Products Analysis (Buffered)")
        start_time = time.perf_counter()
        
        orders = self.full_table_scan()
        
        product_sales = {}
        for order in orders:
            product_sales[order.product_id] = product_sales.get(order.product_id, 0) + order.quantity
        
        top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        query_time = time.perf_counter() - start_time
        print(f"✅ Product popularity analysis completed in {query_time:.3f}s")
        
        return top_products
    
    def regional_sales_analysis(self) -> Dict[int, Dict[str, float]]:
        """Analyze regional sales using buffer manager."""
        print("\n📊 Query 4: Regional Sales Analysis (Buffered)")
        start_time = time.perf_counter()
        
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
        print(f"✅ Regional sales analysis completed in {query_time:.3f}s")
        
        return regional_stats


def run_buffered_analytics_dashboard(query_engine: BufferedQueryEngine):
    """
    Run analytics dashboard with buffer manager.
    
    Students will see dramatic performance improvement compared to step 2!
    """
    print(f"\n{'='*70}")
    print("🚀 RUNNING E-COMMERCE ANALYTICS DASHBOARD (BUFFERED VERSION)")
    print(f"{'='*70}")
    print("Same queries as step 2, but now using buffer manager for caching!")
    
    # Reset statistics for clean measurement
    query_engine.buffer.hits = 0
    query_engine.buffer.misses = 0
    query_engine.buffer.evictions = 0
    query_engine.buffer.disk.reset_stats()
    
    dashboard_start = time.perf_counter()
    
    # Run the same analytics queries
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
    
    # Show the performance improvement!
    print(f"\n{'='*70}")
    print("🎉 PERFORMANCE ANALYSIS - PROBLEM SOLVED!")
    print(f"{'='*70}")
    
    disk_stats = query_engine.buffer.disk.get_stats()
    buffer_stats = query_engine.buffer.get_stats()
    
    print(f"📊 Dashboard Execution Time: {dashboard_time:.3f} seconds")
    print(f"💾 Total Disk Reads: {disk_stats['reads']:,}")
    print(f"⏱️  Total I/O Time: {disk_stats['total_io_time']:.3f} seconds")
    print(f"📈 I/O Overhead: {(disk_stats['total_io_time']/dashboard_time)*100:.1f}% of total time")
    print(f"💿 Data Read: {disk_stats['bytes_read']/(1024*1024):.1f} MB")
    
    print(f"\n🎯 BUFFER MANAGER PERFORMANCE:")
    print(f"   Cache Hit Rate: {buffer_stats['hit_rate']:.1f}%")
    print(f"   Cache Hits: {buffer_stats['hits']:,}")
    print(f"   Cache Misses: {buffer_stats['misses']:,}")
    print(f"   Evictions: {buffer_stats['evictions']:,}")
    print(f"   Frames Used: {buffer_stats['frames_used']}/{query_engine.buffer.pool_size}")
    
    return {
        'dashboard_time': dashboard_time,
        'disk_stats': disk_stats,
        'buffer_stats': buffer_stats,
        'results': {
            'monthly_revenue': monthly_revenue,
            'top_customers': top_customers,
            'top_products': top_products,
            'regional_stats': regional_stats
        }
    }


def main():
    """Main function for buffer manager implementation and testing."""
    print("BUFFER MANAGER LAB - STEP 3: IMPLEMENTING THE SOLUTION")
    print("=" * 65)
    
    database_file = "ecommerce.db"
    
    # Check if database exists
    if not os.path.exists(database_file):
        print(f"❌ Database {database_file} not found!")
        print("Please run step1_generate_data.py first to create the database.")
        return
    
    # Get database info
    file_size = os.path.getsize(database_file)
    num_pages = file_size // DiskPage.PAGE_SIZE
    
    print(f"📁 Database: {database_file}")
    print(f"📊 File size: {file_size/(1024*1024):.1f} MB")  
    print(f"📄 Number of pages: {num_pages:,}")
    
    # Configure buffer pool size
    print(f"\n🎯 BUFFER POOL CONFIGURATION:")
    default_pool_size = min(100, num_pages // 4)
    pool_size_input = input(f"Enter buffer pool size (default {default_pool_size}): ")
    
    try:
        pool_size = int(pool_size_input) if pool_size_input.strip() else default_pool_size
    except ValueError:
        pool_size = default_pool_size
    
    pool_size = max(10, min(pool_size, num_pages))
    buffer_memory_mb = pool_size * DiskPage.PAGE_SIZE / (1024 * 1024)
    
    print(f"✅ Using buffer pool size: {pool_size} pages ({buffer_memory_mb:.1f} MB)")
    
    # Create buffer manager and query engine
    print(f"\n🚀 Creating buffer manager...")
    disk_manager = DiskManager(database_file)
    buffer_manager = BufferManager(disk_manager, pool_size, policy="LRU")
    # buffer_manager = BufferManager(disk_manager, pool_size, policy="CLOCK")
    # buffer_manager = BufferManager(disk_manager, pool_size, policy="FIFO")
    query_engine = BufferedQueryEngine(buffer_manager, num_pages)
    
    # Run buffered analytics dashboard
    print(f"\n🎯 Running analytics dashboard with buffer manager...")
    buffered_results = run_buffered_analytics_dashboard(query_engine)
    
    # Show buffer statistics
    buffer_manager.print_stats()
    
    print(f"\n{'='*70}")
    print("✅ BUFFER MANAGER IMPLEMENTATION COMPLETE!")
    print(f"{'='*70}")
    print("You have successfully implemented a working buffer manager!")
    print("Notice how much faster the queries run compared to step 2.")
    print("\nNext: Run step4_comparison.py to see a side-by-side comparison.")
    
    return buffered_results


if __name__ == "__main__":
    main()