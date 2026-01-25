"""
DAG Affinity Manager
Version: 1.5.2

Manages assignment of DAGs to worker processes with support for:
- Weight-based load balancing
- Pinned DAG assignments
- Exclusive worker allocation
- Dynamic rebalancing

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class AffinityStrategy(Enum):
    """DAG assignment strategies"""
    ROUND_ROBIN = "round_robin"
    WEIGHT_BASED = "weight_based"
    LEAST_LOADED = "least_loaded"
    RANDOM = "random"


@dataclass
class DAGProfile:
    """Profile of a DAG for load estimation"""
    name: str
    node_count: int = 0
    calculator_count: int = 0
    subscriber_count: int = 0
    publisher_count: int = 0
    estimated_cpu_weight: float = 0.2
    estimated_message_rate: float = 0.0
    has_heavy_calculators: bool = False
    priority: int = 5
    
    # Affinity settings from DAG config
    pinned_worker: Optional[int] = None
    exclusive: bool = False
    preferred_workers: List[int] = field(default_factory=list)
    
    # Runtime metrics
    actual_cpu_usage: float = 0.0
    actual_message_rate: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class WorkerLoad:
    """Current load state of a worker"""
    worker_id: int
    assigned_dags: Set[str] = field(default_factory=set)
    total_cpu_weight: float = 0.0
    actual_cpu_percent: float = 0.0
    memory_mb: float = 0.0
    dag_count: int = 0
    is_exclusive: bool = False
    exclusive_dag: Optional[str] = None
    is_healthy: bool = True
    last_heartbeat: datetime = field(default_factory=datetime.now)


class DAGAffinityManager:
    """
    Manages DAG-to-worker assignments with load balancing.
    
    Features:
    - Multiple assignment strategies
    - Pinned/exclusive DAG support
    - Dynamic rebalancing
    - Load tracking
    """
    
    def __init__(self, num_workers: int, config: dict = None):
        self.num_workers = num_workers
        self.config = config or {}
        
        self.strategy = AffinityStrategy(
            self.config.get('default_strategy', 'weight_based')
        )
        self.rebalance_threshold = self.config.get('rebalance_threshold', 0.3)
        self.default_cpu_weight = self.config.get('dag_cpu_weight', 0.2)
        
        # State
        self.worker_loads: Dict[int, WorkerLoad] = {
            i: WorkerLoad(worker_id=i) for i in range(num_workers)
        }
        self.dag_profiles: Dict[str, DAGProfile] = {}
        self.assignments: Dict[str, int] = {}  # dag_name -> worker_id
        
        # Round-robin counter
        self._rr_counter = 0
        
        # Thread safety
        self._lock = threading.RLock()
        
        logger.info(f"DAGAffinityManager initialized with {num_workers} workers, "
                   f"strategy={self.strategy.value}")
    
    def profile_dag(self, dag_config: dict) -> DAGProfile:
        """
        Create a DAGProfile from DAG configuration.
        """
        name = dag_config.get('name', 'unknown')
        
        # Count components - handle both list of dicts and simple lists
        nodes = dag_config.get('nodes', [])
        calculators = dag_config.get('calculators', [])
        subscribers = dag_config.get('subscribers', [])
        publishers = dag_config.get('publishers', [])
        
        # Check for heavy calculators (Java/C++/Rust)
        # Only check if calculators are dicts with 'type' field
        heavy_types = ['Py4JCalculator', 'Pybind11Calculator', 'RustCalculator', 
                      'RESTCalculator', 'LMDBCalculator']
        has_heavy = False
        if calculators and isinstance(calculators[0], dict):
            has_heavy = any(
                calc.get('type', '') in heavy_types 
                for calc in calculators
                if isinstance(calc, dict)
            )
        
        # Estimate CPU weight based on complexity
        base_weight = self.default_cpu_weight
        weight = base_weight + (len(nodes) * 0.02) + (len(calculators) * 0.05)
        if has_heavy:
            weight *= 1.5
        weight = min(weight, 1.0)  # Cap at 1.0
        
        # Get affinity settings from DAG config
        affinity_config = dag_config.get('worker_affinity', {})
        pinned_worker = affinity_config.get('pinned_worker')
        exclusive = affinity_config.get('exclusive', False)
        preferred_workers = affinity_config.get('preferred_workers', [])
        
        profile = DAGProfile(
            name=name,
            node_count=len(nodes),
            calculator_count=len(calculators),
            subscriber_count=len(subscribers),
            publisher_count=len(publishers),
            estimated_cpu_weight=weight,
            has_heavy_calculators=has_heavy,
            priority=dag_config.get('priority', 5),
            pinned_worker=pinned_worker,
            exclusive=exclusive,
            preferred_workers=preferred_workers
        )
        
        return profile
    
    def assign_dag(self, dag_config: dict) -> int:
        """
        Assign a DAG to a worker based on strategy and constraints.
        
        Returns:
            worker_id: The assigned worker ID
        """
        with self._lock:
            profile = self.profile_dag(dag_config)
            dag_name = profile.name
            
            # Store profile
            self.dag_profiles[dag_name] = profile
            
            # Check for pinned assignment
            if profile.pinned_worker is not None:
                worker_id = profile.pinned_worker
                if worker_id < 0 or worker_id >= self.num_workers:
                    logger.warning(f"Invalid pinned worker {worker_id} for DAG {dag_name}, "
                                  f"using strategy assignment")
                else:
                    # Check if pinned worker is available
                    worker_load = self.worker_loads[worker_id]
                    if worker_load.is_exclusive and worker_load.exclusive_dag != dag_name:
                        logger.warning(f"Worker {worker_id} is exclusive to {worker_load.exclusive_dag}, "
                                      f"cannot pin DAG {dag_name}")
                    else:
                        return self._finalize_assignment(dag_name, worker_id, profile)
            
            # Check for exclusive requirement
            if profile.exclusive:
                worker_id = self._find_empty_worker()
                if worker_id is not None:
                    self.worker_loads[worker_id].is_exclusive = True
                    self.worker_loads[worker_id].exclusive_dag = dag_name
                    return self._finalize_assignment(dag_name, worker_id, profile)
                else:
                    logger.warning(f"No empty worker for exclusive DAG {dag_name}, "
                                  f"using least loaded")
            
            # Use strategy-based assignment
            if self.strategy == AffinityStrategy.ROUND_ROBIN:
                worker_id = self._assign_round_robin(profile)
            elif self.strategy == AffinityStrategy.WEIGHT_BASED:
                worker_id = self._assign_weight_based(profile)
            elif self.strategy == AffinityStrategy.LEAST_LOADED:
                worker_id = self._assign_least_loaded(profile)
            else:  # RANDOM
                import random
                available = self._get_available_workers()
                worker_id = random.choice(available) if available else 0
            
            return self._finalize_assignment(dag_name, worker_id, profile)
    
    def _finalize_assignment(self, dag_name: str, worker_id: int, profile: DAGProfile) -> int:
        """Record the assignment and update loads"""
        self.assignments[dag_name] = worker_id
        
        worker_load = self.worker_loads[worker_id]
        worker_load.assigned_dags.add(dag_name)
        worker_load.total_cpu_weight += profile.estimated_cpu_weight
        worker_load.dag_count = len(worker_load.assigned_dags)
        
        logger.info(f"Assigned DAG '{dag_name}' to Worker {worker_id} "
                   f"(weight={profile.estimated_cpu_weight:.2f}, "
                   f"total_load={worker_load.total_cpu_weight:.2f})")
        
        return worker_id
    
    def _get_available_workers(self) -> List[int]:
        """Get workers that can accept new DAGs"""
        available = []
        for worker_id, load in self.worker_loads.items():
            if not load.is_exclusive and load.is_healthy:
                available.append(worker_id)
        return available if available else list(range(self.num_workers))
    
    def _find_empty_worker(self) -> Optional[int]:
        """Find a worker with no DAGs assigned"""
        for worker_id, load in self.worker_loads.items():
            if load.dag_count == 0 and not load.is_exclusive:
                return worker_id
        return None
    
    def _assign_round_robin(self, profile: DAGProfile) -> int:
        """Simple round-robin assignment"""
        available = self._get_available_workers()
        worker_id = available[self._rr_counter % len(available)]
        self._rr_counter += 1
        return worker_id
    
    def _assign_weight_based(self, profile: DAGProfile) -> int:
        """Assign to worker that best balances load"""
        available = self._get_available_workers()
        
        # Check preferred workers first
        if profile.preferred_workers:
            for pref in profile.preferred_workers:
                if pref in available:
                    load = self.worker_loads[pref]
                    # Use preferred if not overloaded
                    avg_load = sum(w.total_cpu_weight for w in self.worker_loads.values()) / self.num_workers
                    if load.total_cpu_weight <= avg_load * 1.2:
                        return pref
        
        # Find worker with lowest weight
        min_weight = float('inf')
        best_worker = available[0]
        
        for worker_id in available:
            load = self.worker_loads[worker_id]
            if load.total_cpu_weight < min_weight:
                min_weight = load.total_cpu_weight
                best_worker = worker_id
        
        return best_worker
    
    def _assign_least_loaded(self, profile: DAGProfile) -> int:
        """Assign to worker with lowest actual CPU usage"""
        available = self._get_available_workers()
        
        min_cpu = float('inf')
        best_worker = available[0]
        
        for worker_id in available:
            load = self.worker_loads[worker_id]
            # Use actual CPU if available, otherwise estimated weight
            cpu = load.actual_cpu_percent if load.actual_cpu_percent > 0 else load.total_cpu_weight * 100
            if cpu < min_cpu:
                min_cpu = cpu
                best_worker = worker_id
        
        return best_worker
    
    def unassign_dag(self, dag_name: str):
        """Remove a DAG assignment"""
        with self._lock:
            if dag_name not in self.assignments:
                return
            
            worker_id = self.assignments[dag_name]
            profile = self.dag_profiles.get(dag_name)
            
            # Update worker load
            worker_load = self.worker_loads[worker_id]
            worker_load.assigned_dags.discard(dag_name)
            if profile:
                worker_load.total_cpu_weight -= profile.estimated_cpu_weight
                worker_load.total_cpu_weight = max(0, worker_load.total_cpu_weight)
            worker_load.dag_count = len(worker_load.assigned_dags)
            
            # Clear exclusive status if this was the exclusive DAG
            if worker_load.exclusive_dag == dag_name:
                worker_load.is_exclusive = False
                worker_load.exclusive_dag = None
            
            # Remove from tracking
            del self.assignments[dag_name]
            if dag_name in self.dag_profiles:
                del self.dag_profiles[dag_name]
            
            logger.info(f"Unassigned DAG '{dag_name}' from Worker {worker_id}")
    
    def update_worker_status(self, worker_id: int, cpu_percent: float, 
                            memory_mb: float, dag_stats: dict = None):
        """Update worker status from heartbeat"""
        with self._lock:
            if worker_id not in self.worker_loads:
                return
            
            load = self.worker_loads[worker_id]
            load.actual_cpu_percent = cpu_percent
            load.memory_mb = memory_mb
            load.last_heartbeat = datetime.now()
            load.is_healthy = True
            
            # Update DAG-level metrics if provided
            if dag_stats:
                for dag_name, stats in dag_stats.items():
                    if dag_name in self.dag_profiles:
                        profile = self.dag_profiles[dag_name]
                        profile.actual_cpu_usage = stats.get('cpu_percent', 0)
                        profile.actual_message_rate = stats.get('message_rate', 0)
                        profile.last_updated = datetime.now()
    
    def mark_worker_unhealthy(self, worker_id: int):
        """Mark worker as unhealthy"""
        with self._lock:
            if worker_id in self.worker_loads:
                self.worker_loads[worker_id].is_healthy = False
    
    def get_worker_assignment(self, dag_name: str) -> Optional[int]:
        """Get the worker ID for a DAG"""
        return self.assignments.get(dag_name)
    
    def get_dags_on_worker(self, worker_id: int) -> List[str]:
        """Get all DAGs assigned to a worker"""
        if worker_id not in self.worker_loads:
            return []
        return list(self.worker_loads[worker_id].assigned_dags)
    
    def get_all_assignments(self) -> Dict[str, int]:
        """Get all DAG-to-worker assignments"""
        return dict(self.assignments)
    
    def get_load_summary(self) -> Dict[int, dict]:
        """Get summary of all worker loads"""
        with self._lock:
            summary = {}
            for worker_id, load in self.worker_loads.items():
                summary[worker_id] = {
                    'worker_id': worker_id,
                    'dag_count': load.dag_count,
                    'assigned_dags': list(load.assigned_dags),
                    'estimated_cpu_weight': round(load.total_cpu_weight, 3),
                    'actual_cpu_percent': round(load.actual_cpu_percent, 1),
                    'memory_mb': round(load.memory_mb, 1),
                    'is_exclusive': load.is_exclusive,
                    'exclusive_dag': load.exclusive_dag,
                    'is_healthy': load.is_healthy,
                    'last_heartbeat': load.last_heartbeat.isoformat()
                }
            return summary
    
    def check_rebalance_needed(self) -> bool:
        """Check if rebalancing is needed based on load imbalance"""
        with self._lock:
            loads = [w.total_cpu_weight for w in self.worker_loads.values() 
                    if not w.is_exclusive]
            
            if not loads or len(loads) < 2:
                return False
            
            avg_load = sum(loads) / len(loads)
            if avg_load == 0:
                return False
            
            max_load = max(loads)
            min_load = min(loads)
            
            imbalance = (max_load - min_load) / avg_load
            return imbalance > self.rebalance_threshold
    
    def suggest_rebalance(self) -> List[tuple]:
        """
        Suggest DAG migrations to balance load.
        
        Returns:
            List of (dag_name, from_worker, to_worker) tuples
        """
        with self._lock:
            suggestions = []
            
            # Find overloaded and underloaded workers
            loads = [(w_id, w.total_cpu_weight) 
                    for w_id, w in self.worker_loads.items()
                    if not w.is_exclusive]
            
            if len(loads) < 2:
                return suggestions
            
            loads.sort(key=lambda x: x[1], reverse=True)
            heaviest_worker, heaviest_load = loads[0]
            lightest_worker, lightest_load = loads[-1]
            
            avg_load = sum(l for _, l in loads) / len(loads)
            
            if (heaviest_load - lightest_load) / max(avg_load, 0.01) <= self.rebalance_threshold:
                return suggestions
            
            # Find smallest movable DAG on heaviest worker
            dags_on_heavy = self.worker_loads[heaviest_worker].assigned_dags
            movable_dags = []
            
            for dag_name in dags_on_heavy:
                profile = self.dag_profiles.get(dag_name)
                if profile and profile.pinned_worker is None and not profile.exclusive:
                    movable_dags.append((dag_name, profile.estimated_cpu_weight))
            
            if movable_dags:
                # Move smallest DAG to lightest worker
                movable_dags.sort(key=lambda x: x[1])
                dag_to_move = movable_dags[0][0]
                suggestions.append((dag_to_move, heaviest_worker, lightest_worker))
            
            return suggestions
    
    def execute_migration(self, dag_name: str, to_worker: int) -> bool:
        """
        Update assignment for a DAG migration.
        
        Note: Actual DAG stop/start is handled by WorkerPoolManager.
        """
        with self._lock:
            if dag_name not in self.assignments:
                return False
            
            from_worker = self.assignments[dag_name]
            profile = self.dag_profiles.get(dag_name)
            
            if not profile:
                return False
            
            # Update from_worker
            from_load = self.worker_loads[from_worker]
            from_load.assigned_dags.discard(dag_name)
            from_load.total_cpu_weight -= profile.estimated_cpu_weight
            from_load.dag_count = len(from_load.assigned_dags)
            
            # Update to_worker
            to_load = self.worker_loads[to_worker]
            to_load.assigned_dags.add(dag_name)
            to_load.total_cpu_weight += profile.estimated_cpu_weight
            to_load.dag_count = len(to_load.assigned_dags)
            
            # Update assignment
            self.assignments[dag_name] = to_worker
            
            logger.info(f"Migration recorded: DAG '{dag_name}' from Worker {from_worker} "
                       f"to Worker {to_worker}")
            
            return True
