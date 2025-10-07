"""
step5_extended_buffer.py - Extended Buffer Manager for LLM Memory (Clean Version)

EXERCISE PART 5 (Simplified & Clean):
Extend the existing BufferManager from step 3 to handle user sessions and page priorities.


LEARNING OBJECTIVES:
- Extend existing systems properly
- Implement user-aware buffer policies

INTRO
Modern Large Language Model (LLM) applications deal with two kinds of memory:
- Short-term memory — the current context window, which holds only a limited number of recent interactions.
- Long-term memory — stored knowledge or historical user data that can be recalled when relevant.
Because the context window is small and computational resources are limited, the system must decide what information to keep “in memory” and what to evict. 
The challenge grows when multiple users share the same underlying resources — some may be active, others idle, and their data has different importance levels.
In this exercise, we extend our buffer manager to simulate these ideas. Each page represents a piece of LLM memory (e.g., user preferences, recent conversations, 
or old sessions), and the system learns to prioritize active users and important data. 
This models how modern LLM-based systems balance relevance, freshness, and fairness in multi-user environments with constrained memory.
"""

import time
import random
import os
from typing import Dict, List, Optional, Set, Any
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta

# Import existing infrastructure
from base_data_struct import DiskPage
from disk_manager import DiskManager
from step03_with_buffer_manager import BufferManager, BufferFrame


class PageType(Enum):
    """Types of memory pages in LLM system"""
    USER_PREFERENCES = 1      # Highest priority
    RECENT_CONVERSATION = 2   # High priority  
    USER_FACTS = 3           # Medium priority
    OLD_CONVERSATION = 4      # Low priority
    SESSION_STATE = 5        # Lowest priority


@dataclass
class UserSession:
    """Represents an active user session"""
    user_id: str
    session_id: str
    last_activity: datetime
    is_active: bool = True
    allocated_pages: int = 0
    
    def mark_activity(self):
        """Mark user as recently active"""
        self.last_activity = datetime.now()
        self.is_active = True


class LLMPage(DiskPage):
    """Extended page class with user and priority information"""
    
    def __init__(self, page_id: int, user_id: str, page_type: PageType, content: str = ""):
        super().__init__(page_id)
        self.user_id = user_id
        self.page_type = page_type
        self.content = content
        self.priority = self._get_priority()
    
    def _get_priority(self) -> int:
        """Get priority score (higher = more important)"""
        priority_map = {
            PageType.USER_PREFERENCES: 100,
            PageType.RECENT_CONVERSATION: 80,
            PageType.USER_FACTS: 60,
            PageType.OLD_CONVERSATION: 40,
            PageType.SESSION_STATE: 20
        }
        return priority_map[self.page_type]
    
    def __str__(self) -> str:
        return f"LLMPage(id={self.page_id}, user={self.user_id}, type={self.page_type.name})"


class ExtendedBufferFrame(BufferFrame):
    """Extended frame that tracks user and priority information"""
    
    def __init__(self, frame_id: int):
        super().__init__(frame_id)
        self.user_id: Optional[str] = None
        self.page_type: Optional[PageType] = None
        self.priority: int = 0
    
    def load_llm_page(self, page: LLMPage):
        """Load an LLM page and track user/priority info"""
        self.page = page
        self.page_id = page.page_id
        self.user_id = page.user_id
        self.page_type = page.page_type
        self.priority = page.priority
        self.last_accessed = datetime.now()
        self.access_frequency = 1
        self.is_dirty = False
    
    def clear_frame(self):
        """Clear frame and reset user/priority info"""
        self.page = None
        self.page_id = None
        self.user_id = None
        self.page_type = None
        self.priority = 0
        self.is_dirty = False
        self.access_frequency = 0


class UserAwareBufferManager(BufferManager):
    """
    Extended buffer manager with user session tracking and priority-based eviction.
    
    🎯 IMPLEMENTATION TASK:
    Complete the _evict_user_aware_lru() method to implement:
    1. Prefer evicting inactive users over active users
    2. Within same activity level, evict lower priority pages first
    3. Within same priority, use LRU (least recently used)
    """
    
    def __init__(self, disk_manager: DiskManager, pool_size: int):
        # Initialize parent BufferManager
        super().__init__(disk_manager, pool_size, policy="USER_AWARE")
        
        # Replace frames with extended versions
        self.frames = [ExtendedBufferFrame(i) for i in range(pool_size)]
        
        # User session management
        self.user_sessions: Dict[str, UserSession] = {} # Tracks active user sessions
        self.user_allocations: Dict[str, int] = {} # Tracks page allocations per user
        self.activity_timeout = timedelta(minutes=5) # Active if no activity for 5 minutes
        
        # Extended statistics
        self.active_user_evictions = 0
        
        print(f"🧠 User-Aware Buffer Manager initialized:")
        print(f"   Pool size: {pool_size} frames")
        print(f"   Activity timeout: {self.activity_timeout}")
    
    def register_user_activity(self, user_id: str):
        """Register user activity"""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = UserSession(user_id, "default", datetime.now())
            self.user_allocations[user_id] = 0
        else:
            self.user_sessions[user_id].mark_activity()
        
        self._update_active_status()
    
    def _update_active_status(self):
        """Update which users are considered active"""
        current_time = datetime.now()
        for user_id, session in self.user_sessions.items():
            time_since_activity = current_time - session.last_activity
            session.is_active = time_since_activity <= self.activity_timeout
    
    def get_llm_page(self, page_id: int, user_id: str, page_type: PageType) -> Optional[LLMPage]:
        """
        Get an LLM page, registering user activity.
        This replaces the original get_page() method for LLM-specific usage.
        """
        self.register_user_activity(user_id)
        
        # Check if page is in buffer (HIT)
        if page_id in self.page_table:
            frame_id = self.page_table[page_id]
            frame = self.frames[frame_id]
            
            # Update access metadata
            frame.last_accessed = datetime.now()
            frame.access_frequency += 1
            
            self.hits += 1
            print(f"🎯 BUFFER HIT: Page {page_id} (user {user_id}, {page_type.name})")
            return frame.page
        
        # CACHE MISS - load page
        self.misses += 1
        print(f"❌ BUFFER MISS: Page {page_id} (user {user_id}, {page_type.name})")
        return self._load_llm_page(page_id, user_id, page_type)
    
    def _load_llm_page(self, page_id: int, user_id: str, page_type: PageType) -> Optional[LLMPage]:
        """Load LLM page into buffer"""
        # Get a frame
        frame_id = self._get_free_frame()
        if frame_id is None:
            frame_id = self._evict_page()
            if frame_id is None:
                print(f"❌ ERROR: No frames available for page {page_id}")
                return None
        
        # Create LLM page (note: no simulation logic here!)
        page = LLMPage(page_id, user_id, page_type, f"Content for {page_type.name}")
        
        # Load page into frame
        frame = self.frames[frame_id]
        frame.load_llm_page(page)
        
        # Update tracking
        self.page_table[page_id] = frame_id
        self.user_allocations[user_id] = self.user_allocations.get(user_id, 0) + 1
        if user_id in self.user_sessions:
            self.user_sessions[user_id].allocated_pages += 1
        
        print(f"📥 LOADED: {page} into frame {frame_id}")
        return page
    
    def _evict_page(self) -> Optional[int]:
        """Override parent method to use user-aware eviction"""
        return self._evict_user_aware_lru()
    
    def _evict_user_aware_lru(self) -> Optional[int]:
        """
        🎯 TODO: Implement user-aware LRU eviction policy
        
        Policy requirements:
        1. Try to evict from INACTIVE users first
        2. Within same activity level, evict LOWER priority pages first  
        3. Within same priority level, evict LEAST recently used (oldest last_accessed)
        
        Returns:
            frame_id of evicted frame, or None if no frame can be evicted
            
        HINTS:
        - self._update_active_status() to refresh user activity
        - Loop through self.frames to find eviction candidates
        - Check frame.user_id in self.active_users and session.is_active
        - Use frame.priority and frame.last_accessed for sorting
        - Call self._write_back_and_clear_frame(frame_id) to evict
        - Update self.user_allocations when evicting
        - Track self.active_user_evictions if evicting active user
        """
        
        # TODO: Implement the user-aware eviction policy here
        # For now, fall back to simple LRU as placeholder
        
        self._update_active_status()
        
        # Collect eviction candidates
        candidates = []
        best_victim_frame: ExtendedBufferFrame = None
        
        for frame in self.frames:
            if frame.page is not None:
                user_id = frame.user_id
                active_user_eviction = self.user_sessions[user_id].is_active if user_id in self.user_sessions else False
                ###
                # 
                # --- YOUR CODE HERE ---
                # 
                # Implement the logic to select the best victim frame based on:
                # 1. Prefer inactive users
                # 2. Lower priority pages
                # 3. Least recently used
                # 
                ###             
        
        # If no candidates found, return None
        if not best_victim_frame:
            print("❌ ERROR: No candidates for eviction found")
            return None
        
        ################################################
        ############ Statistics and cleanup ############
        ################################################

        # Track if we evicted an active user (suboptimal)
        if active_user_eviction:
            self.active_user_evictions += 1
            print(f"⚠️  Evicting active user page (suboptimal!)")
        
        # Update user allocations
        if user_id in self.user_allocations:
            self.user_allocations[user_id] = max(0, self.user_allocations[user_id] - 1)
        if user_id in self.user_sessions:
            self.user_sessions[user_id].allocated_pages = max(0, self.user_sessions[user_id].allocated_pages - 1)
        
        # Evict the frame
        self._write_back_and_clear_frame(best_victim_frame.frame_id)
        
        print(f"🔄 USER-AWARE EVICTION: Frame {best_victim_frame.frame_id} (user {user_id}, "
              f"priority {best_victim_frame.priority}, active={active_user_eviction})")
        
        return best_victim_frame.frame_id
    
    def _write_back_and_clear_frame(self, frame_id: int):
        """Override to use extended frame clearing"""
        frame = self.frames[frame_id]
        
        if frame.is_dirty and frame.page:
            print(f"💾 WRITE-BACK: {frame.page}")
            # Would write to persistent storage in real system
        
        # Remove from page table
        if frame.page_id is not None and frame.page_id in self.page_table:
            del self.page_table[frame.page_id]
        
        old_page = frame.page
        frame.clear_frame()  # Use extended clear method
        self.evictions += 1
        print(f"🗑️  EVICTED: {old_page} from frame {frame_id}")
    
    def get_extended_stats(self) -> Dict[str, Any]:
        """Get extended statistics including user info"""
        base_stats = self.get_stats()
        
        active_user_count = sum(1 for user in self.user_sessions.values() if user.is_active)
        
        base_stats.update({
            'active_user_evictions': self.active_user_evictions,
            'active_users': active_user_count,
            'total_users': len(self.user_sessions),
            'user_allocations': dict(self.user_allocations)
        })
        
        return base_stats
    
    def print_extended_stats(self):
        """Print comprehensive statistics"""
        stats = self.get_extended_stats()
        
        print(f"\n{'='*60}")
        print("USER-AWARE BUFFER MANAGER STATISTICS")
        print(f"{'='*60}")
        print(f"Buffer Performance:")
        print(f"  Hit rate:              {stats['hit_rate']:.1f}%")
        print(f"  Total evictions:       {stats['evictions']}")
        print(f"  Active user evictions: {stats['active_user_evictions']} (should be low!)")
        print(f"\nUser Management:")
        print(f"  Active users:          {stats['active_users']}")
        print(f"  Total users:           {stats['total_users']}")
        print(f"  Frames used:           {stats['frames_used']}/{self.pool_size}")
        
        print(f"\nUser Allocations:")
        for user_id, count in stats['user_allocations'].items():
            active_status = "🟢" if (user_id in self.user_sessions and 
                                   self.user_sessions[user_id].is_active) else "🔴"
            print(f"  {user_id}: {count} pages {active_status}")


# ============================================================================
# SEPARATE SIMULATION MODULE - Clean separation of concerns
# ============================================================================

class LLMWorkloadSimulator:
    """
    Handles all simulation logic separately from buffer manager.
    
    This class is responsible for:
    - Generating realistic LLM workload patterns
    - Creating test scenarios
    - Measuring performance
    - Analyzing results
    """
    
    def __init__(self, buffer_manager: UserAwareBufferManager):
        self.buffer_manager = buffer_manager
        self.users = ["alice_dev", "bob_scientist", "charlie_student", "diana_pm"]
        self.page_types = list(PageType)
        
        # Workload configuration
        self.active_user_ratio = 0.7  # 70% requests from active users
        self.page_type_weights = [30, 25, 15, 15, 15]  # Preferences most common
        self.request_delay_range = (0.01, 0.05)  # 10-50ms between requests
    
    def create_realistic_content(self, page_type: PageType, user_id: str) -> str:
        """Generate realistic content for different page types"""
        content_templates = {
            PageType.USER_PREFERENCES: f"User {user_id}: Expert level, prefers detailed technical answers, likes Python",
            PageType.RECENT_CONVERSATION: f"Recent chat with {user_id} about database optimization and performance tuning",
            PageType.USER_FACTS: f"{user_id} profile: Software engineer, uses PostgreSQL, interested in system performance",
            PageType.OLD_CONVERSATION: f"Historical discussion with {user_id} about web frameworks and best practices",
            PageType.SESSION_STATE: f"Current session for {user_id}: Topic=buffer management, Context=database course"
        }
        return content_templates.get(page_type, f"Generic content for {user_id}")
    
    def generate_page_access(self) -> tuple[int, str, PageType]:
        """Generate a single page access with realistic patterns"""
        # Choose user based on activity patterns
        if random.random() < self.active_user_ratio:
            user_id = random.choice(self.users[:2])  # alice and bob are more active
        else:
            user_id = random.choice(self.users)
        
        # Choose page type with realistic weights
        page_type = random.choices(self.page_types, weights=self.page_type_weights, k=1)[0]
        
        # Generate page_id based on user and type (ensures some locality)
        page_id = hash((user_id, page_type.value)) % 100
        
        return page_id, user_id, page_type
    
    def run_workload_simulation(self, duration: int = 30) -> Dict[str, Any]:
        """
        Run a complete LLM workload simulation
        
        Args:
            duration: Simulation duration in seconds
            
        Returns:
            Dictionary with simulation results and metrics
        """
        print(f"\n{'='*60}")
        print("🧠 LLM WORKLOAD SIMULATION")
        print(f"{'='*60}")
        print(f"Duration: {duration} seconds")
        print(f"Users: {', '.join(self.users)}")
        print(f"Page types: {len(self.page_types)} types with priorities")
        
        # Reset statistics
        self.buffer_manager.hits = 0
        self.buffer_manager.misses = 0
        self.buffer_manager.evictions = 0
        self.buffer_manager.active_user_evictions = 0
        
        start_time = time.time()
        access_count = 0
        
        # Run simulation loop
        while time.time() - start_time < duration:
            # Generate page access
            page_id, user_id, page_type = self.generate_page_access()
            
            # Access the page through buffer manager
            page = self.buffer_manager.get_llm_page(page_id, user_id, page_type)
            if page:
                # Simulate using the page (e.g., in LLM prompt)
                _ = f"System prompt includes: {page}"
            
            access_count += 1
            
            # Realistic delay between requests
            delay = random.uniform(*self.request_delay_range)
            time.sleep(delay)
        
        elapsed = time.time() - start_time
        
        # Collect results
        results = {
            'duration': elapsed,
            'total_accesses': access_count,
            'access_rate': access_count / elapsed,
            'buffer_stats': self.buffer_manager.get_extended_stats()
        }
        
        return results
    
    def analyze_results(self, results: Dict[str, Any]):
        """Analyze and display simulation results"""
        print(f"\n📊 SIMULATION RESULTS:")
        print(f"Duration: {results['duration']:.1f}s")
        print(f"Total accesses: {results['total_accesses']}")
        print(f"Access rate: {results['access_rate']:.1f} requests/second")
        
        # Display buffer manager statistics
        self.buffer_manager.print_extended_stats()
        
        # Analyze policy effectiveness
        stats = results['buffer_stats']
        print(f"\n💡 POLICY EFFECTIVENESS ANALYSIS:")
        
        if stats['active_user_evictions'] == 0:
            print("🎉 EXCELLENT: No active users were evicted!")
        elif stats['active_user_evictions'] < stats['evictions'] * 0.1:
            print("👍 GOOD: Less than 10% of evictions affected active users")
        else:
            print("⚠️  NEEDS IMPROVEMENT: Too many active user evictions")
        
        if stats['evictions'] > 0:
            protection_rate = (1 - stats['active_user_evictions'] / stats['evictions']) * 100
            print(f"Active user protection rate: {protection_rate:.1f}%")
        
        if stats['hit_rate'] > 75:
            print("🎯 EXCELLENT: High cache hit rate!")
        elif stats['hit_rate'] > 50:
            print("👍 GOOD: Reasonable cache performance")
        else:
            print("⚠️  LOW: Cache hit rate could be improved")
    
    def run_demo_scenario(self):
        """Run a simple demo to show how the system works"""
        print(f"\n🎯 DEMO: User-aware buffer management in action...")
        
        scenarios = [
            (1, "alice_dev", PageType.USER_PREFERENCES, "Alice accesses her preferences (high priority)"),
            (2, "bob_scientist", PageType.RECENT_CONVERSATION, "Bob accesses recent conversation"),
            (3, "charlie_student", PageType.SESSION_STATE, "Charlie accesses session state (low priority)")
        ]
        
        for page_id, user_id, page_type, description in scenarios:
            print(f"\n{description}...")
            page = self.buffer_manager.get_llm_page(page_id, user_id, page_type)
            if page:
                print(f"   Content: {page}")
            time.sleep(0.5)  # Small delay between demo steps


def main():
    """Main function with clean separation of concerns"""
    print("BUFFER MANAGER LAB - STEP 5: USER-AWARE LLM MEMORY (CLEAN VERSION)")
    print("=" * 75)
    
    # Create buffer manager (pure buffer management, no simulation logic)
    disk = DiskManager("llm_clean.db")
    buffer_manager = UserAwareBufferManager(disk, pool_size=15)
    
    # Create separate simulator (handles all simulation logic)
    simulator = LLMWorkloadSimulator(buffer_manager)
    
    # Run demo
    simulator.run_demo_scenario()
    
    # Run full simulation
    print(f"\n🚀 Running comprehensive LLM workload simulation...")
    user_input = input("Press Enter to start 30-second simulation...")
    
    results = simulator.run_workload_simulation(duration=30)
    simulator.analyze_results(results)
    
    print(f"\n{'='*75}")
    print("✅ CLEAN EXTENDED BUFFER MANAGER LAB COMPLETE!")
    print(f"{'='*75}")
    print("Key improvements in this version:")
    print("• Clean separation: Buffer manager vs Simulation logic")
    print("• Buffer manager focuses only on buffer management")
    print("• Simulator handles workload generation and analysis")
    print("• Better code organization and maintainability")
    print("• Easier to test and extend individual components")
    print(f"{'='*75}")


if __name__ == "__main__":
    main()