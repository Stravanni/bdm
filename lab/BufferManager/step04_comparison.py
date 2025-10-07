"""
step4_comparison.py - Final Performance Comparison

EXERCISE PART 4:
Run both naive and buffered versions side-by-side to see the complete picture.
This is the culmination of the lab that shows the dramatic improvement.

LEARNING OBJECTIVES:
- See direct performance comparison
- Understand the impact of different buffer pool sizes
- Analyze cache effectiveness metrics
- Understand real-world implications
"""

import time
import os
from typing import Dict, Any
from base_data_struct import DiskPage
from disk_manager import DiskManager
from step02_naive_query import NaiveQueryEngine
from step03_with_buffer_manager import BufferManager, BufferedQueryEngine


def run_single_query_comparison(naive_engine: NaiveQueryEngine, 
                               buffered_engine: BufferedQueryEngine,
                               query_name: str,
                               query_func_name: str):
    """
    Compare a single query between naive and buffered versions.
    
    Args:
        naive_engine: Naive query engine
        buffered_engine: Buffered query engine  
        query_name: Display name for the query
        query_func_name: Name of the method to call
    """
    print(f"\n{'='*60}")
    print(f"COMPARING: {query_name}")
    print(f"{'='*60}")
    
    # Run naive version
    print("\n🐌 Running NAIVE version...")
    naive_engine.disk.reset_stats()
    naive_start = time.perf_counter()
    
    naive_func = getattr(naive_engine, query_func_name)
    naive_result = naive_func()
    
    naive_time = time.perf_counter() - naive_start
    naive_stats = naive_engine.disk.get_stats()
    
    # Run buffered version
    print("\n🚀 Running BUFFERED version...")
    buffered_engine.buffer.hits = 0
    buffered_engine.buffer.misses = 0
    buffered_engine.buffer.disk.reset_stats()
    buffered_start = time.perf_counter()
    
    buffered_func = getattr(buffered_engine, query_func_name)
    buffered_result = buffered_func()
    
    buffered_time = time.perf_counter() - buffered_start
    buffered_disk_stats = buffered_engine.buffer.disk.get_stats()
    buffered_buffer_stats = buffered_engine.buffer.get_stats()
    
    # Compare results
    speedup = naive_time / buffered_time if buffered_time > 0 else float('inf')
    io_reduction = ((naive_stats['reads'] - buffered_disk_stats['reads']) / 
                   naive_stats['reads'] * 100) if naive_stats['reads'] > 0 else 0
    
    print(f"\n📊 COMPARISON RESULTS:")
    print(f"   Query: {query_name}")
    print(f"   Naive time:      {naive_time:.3f}s")
    print(f"   Buffered time:   {buffered_time:.3f}s")
    print(f"   ⚡ Speedup:      {speedup:.1f}x")
    print(f"   Naive I/O:       {naive_stats['reads']} reads")
    print(f"   Buffered I/O:    {buffered_disk_stats['reads']} reads")
    print(f"   📉 I/O reduction: {io_reduction:.1f}%")
    print(f"   🎯 Hit rate:     {buffered_buffer_stats['hit_rate']:.1f}%")
    
    return {
        'query_name': query_name,
        'naive_time': naive_time,
        'buffered_time': buffered_time,
        'speedup': speedup,
        'naive_io': naive_stats['reads'],
        'buffered_io': buffered_disk_stats['reads'],
        'io_reduction': io_reduction,
        'hit_rate': buffered_buffer_stats['hit_rate']
    }


def test_different_buffer_sizes(database_file: str, num_pages: int):
    """
    Test how buffer pool size affects performance.
    
    This shows students the relationship between memory investment and performance.
    """
    print(f"\n{'='*80}")
    print("🧪 BUFFER SIZE EXPERIMENT")
    print(f"{'='*80}")
    print("Testing how buffer pool size affects cache performance...")
    
    # Test different buffer sizes
    buffer_sizes = [10, 25, 50, 100, min(200, num_pages//2)]
    results = []
    
    for pool_size in buffer_sizes:
        if pool_size > num_pages:
            continue
            
        print(f"\n🧮 Testing buffer size: {pool_size} pages ({pool_size * DiskPage.PAGE_SIZE / (1024*1024):.1f} MB)")
        
        # Create fresh buffer manager
        disk = DiskManager(database_file)
        buffer_manager = BufferManager(disk, pool_size, policy="LRU")
        query_engine = BufferedQueryEngine(buffer_manager, num_pages)
        
        # Run a quick test - just two queries to see hit rate
        start_time = time.perf_counter()
        
        # First query (cold cache)
        query_engine.monthly_revenue_analysis()
        
        # Second query (should have high hit rate)
        query_engine.top_customers_analysis()
        
        total_time = time.perf_counter() - start_time
        buffer_stats = buffer_manager.get_stats()
        disk_stats = disk.get_stats()
        
        results.append({
            'pool_size': pool_size,
            'memory_mb': pool_size * DiskPage.PAGE_SIZE / (1024*1024),
            'hit_rate': buffer_stats['hit_rate'],
            'total_time': total_time,
            'disk_reads': disk_stats['reads'],
            'evictions': buffer_stats['evictions']
        })
        
        print(f"   Hit rate: {buffer_stats['hit_rate']:.1f}%")
        print(f"   Total time: {total_time:.3f}s")
        print(f"   Disk reads: {disk_stats['reads']}")
        print(f"   Evictions: {buffer_stats['evictions']}")
    
    # Analyze results
    print(f"\n📈 BUFFER SIZE ANALYSIS:")
    print(f"{'Size (MB)':<10} {'Hit Rate':<10} {'Time (s)':<10} {'Disk I/O':<10} {'Evictions':<10}")
    print("-" * 60)
    
    for result in results:
        print(f"{result['memory_mb']:<10.1f} {result['hit_rate']:<10.1f} "
              f"{result['total_time']:<10.3f} {result['disk_reads']:<10} {result['evictions']:<10}")
    
    # Find optimal size
    best_result = max(results, key=lambda x: x['hit_rate'])
    print(f"\n🎯 OPTIMAL BUFFER SIZE:")
    print(f"   Best performance: {best_result['pool_size']} pages ({best_result['memory_mb']:.1f} MB)")
    print(f"   Hit rate: {best_result['hit_rate']:.1f}%")
    print(f"   Memory efficiency: {best_result['hit_rate']/best_result['memory_mb']:.1f}% hit rate per MB")
    
    # Insights
    print(f"\n💡 INSIGHTS:")
    print(f"   • Larger buffers generally improve hit rates")
    print(f"   • Diminishing returns after working set size")
    print(f"   • Balance memory cost vs. performance gain")
    print(f"   • Real systems use adaptive buffer management")
    
    return results


def run_comprehensive_comparison(database_file: str, num_pages: int):
    """
    Run a comprehensive comparison of naive vs buffered performance.
    """
    print(f"\n{'='*80}")
    print("🏁 COMPREHENSIVE PERFORMANCE COMPARISON")
    print(f"{'='*80}")
    
    # Set up engines
    print("Setting up test engines...")
    
    # Naive engine
    naive_disk = DiskManager(database_file)
    naive_engine = NaiveQueryEngine(naive_disk, num_pages)
    
    # Buffered engine with reasonable buffer size
    # buffer_size = min(100, num_pages // 4)  # 25% of data or 100 pages max
    buffer_size = 10000
    buffered_disk = DiskManager(database_file)
    buffer_manager = BufferManager(buffered_disk, buffer_size, policy="LRU")
    buffered_engine = BufferedQueryEngine(buffer_manager, num_pages)
    
    print(f"✅ Engines ready (buffer size: {buffer_size} pages)")
    
    # Compare individual queries
    queries = [
        ("Monthly Revenue Analysis", "monthly_revenue_analysis"),
        ("Top Customers Analysis", "top_customers_analysis"),
        ("Product Popularity Analysis", "product_popularity_analysis"),
        ("Regional Sales Analysis", "regional_sales_analysis")
    ]
    
    query_results = []
    for query_name, query_func in queries:
        result = run_single_query_comparison(naive_engine, buffered_engine, query_name, query_func)
        query_results.append(result)
    
    # Overall summary
    print(f"\n{'='*80}")
    print("📊 OVERALL PERFORMANCE SUMMARY")
    print(f"{'='*80}")
    
    total_naive_time = sum(r['naive_time'] for r in query_results)
    total_buffered_time = sum(r['buffered_time'] for r in query_results)
    overall_speedup = total_naive_time / total_buffered_time if total_buffered_time > 0 else float('inf')
    
    total_naive_io = sum(r['naive_io'] for r in query_results)
    total_buffered_io = sum(r['buffered_io'] for r in query_results)
    overall_io_reduction = ((total_naive_io - total_buffered_io) / total_naive_io * 100) if total_naive_io > 0 else 0
    
    avg_hit_rate = sum(r['hit_rate'] for r in query_results) / len(query_results)
    
    print(f"📈 AGGREGATE RESULTS:")
    print(f"   Total naive time:     {total_naive_time:.3f}s")
    print(f"   Total buffered time:  {total_buffered_time:.3f}s")
    print(f"   ⚡ Overall speedup:   {overall_speedup:.1f}x")
    print(f"   Total naive I/O:      {total_naive_io:,} reads")
    print(f"   Total buffered I/O:   {total_buffered_io:,} reads")
    print(f"   📉 Overall I/O reduction: {overall_io_reduction:.1f}%")
    print(f"   🎯 Average hit rate:  {avg_hit_rate:.1f}%")
    
    # Per-query breakdown
    print(f"\n📋 PER-QUERY BREAKDOWN:")
    print(f"{'Query':<25} {'Speedup':<10} {'I/O Reduction':<15} {'Hit Rate':<10}")
    print("-" * 70)
    
    for result in query_results:
        print(f"{result['query_name']:<25} {result['speedup']:<10.1f} "
              f"{result['io_reduction']:<15.1f}% {result['hit_rate']:<10.1f}%")
    
    # Business impact
    print(f"\n💼 BUSINESS IMPACT:")
    time_saved_per_dashboard = total_naive_time - total_buffered_time
    dashboards_per_day = 100  # Hypothetical
    daily_time_saved = time_saved_per_dashboard * dashboards_per_day
    
    print(f"   Time saved per dashboard: {time_saved_per_dashboard:.2f}s")
    print(f"   If {dashboards_per_day} dashboards/day: {daily_time_saved:.1f}s saved daily")
    print(f"   Annual time saved: {daily_time_saved * 365 / 3600:.1f} hours")
    print(f"   I/O bandwidth saved: {(total_naive_io - total_buffered_io) * DiskPage.PAGE_SIZE / (1024*1024):.1f} MB per dashboard")
    
    return query_results


def main():
    """Main function for comprehensive performance comparison."""
    print("BUFFER MANAGER LAB - STEP 4: COMPREHENSIVE COMPARISON")
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
    
    # Menu for different comparisons
    print(f"\n🎯 COMPARISON OPTIONS:")
    print("1. Individual query comparison")
    print("2. Buffer size experiment")
    print("3. Comprehensive comparison (recommended)")
    print("4. All of the above")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice in ['1', '4']:
        print("\n" + "="*50)
        print("OPTION 1: Individual Query Comparison")
        print("="*50)
        
        # Set up for individual comparison
        naive_disk = DiskManager(database_file)
        naive_engine = NaiveQueryEngine(naive_disk, num_pages)
        
        buffered_disk = DiskManager(database_file)
        buffer_manager = BufferManager(buffered_disk, 50, policy="LRU")
        buffered_engine = BufferedQueryEngine(buffer_manager, num_pages)
        
        # Compare monthly revenue query
        run_single_query_comparison(naive_engine, buffered_engine,
                                  "Monthly Revenue Analysis", "monthly_revenue_analysis")
    
    if choice in ['2', '4']:
        print("\n" + "="*50)
        print("OPTION 2: Buffer Size Experiment")
        print("="*50)
        test_different_buffer_sizes(database_file, num_pages)
    
    if choice in ['3', '4']:
        print("\n" + "="*50)
        print("OPTION 3: Comprehensive Comparison")
        print("="*50)
        run_comprehensive_comparison(database_file, num_pages)
    
    # Final summary
    print(f"\n{'='*80}")
    print("🎉 BUFFER MANAGER LAB COMPLETE!")
    print(f"{'='*80}")
    print("Congratulations! You have successfully:")
    print("✅ Generated a realistic database")
    print("✅ Experienced the performance problem without buffer management") 
    print("✅ Implemented a working buffer manager with LRU policy")
    print("✅ Analyzed the dramatic performance improvements")
    print("\n🎓 KEY LEARNINGS:")
    print("• Disk I/O is expensive and should be minimized")
    print("• Buffer managers cache frequently accessed data in memory")
    print("• LRU replacement policy works well for typical workloads")
    print("• Cache hit rates >75% indicate effective buffer management")
    print("• Memory investment pays off with significant performance gains")
    print("\n💡 REAL-WORLD APPLICATIONS:")
    print("• Every major database system uses buffer management")
    print("• Similar principles apply to web caches, CPU caches, etc.")
    print("• Understanding caching is crucial for system performance")


if __name__ == "__main__":
    main()