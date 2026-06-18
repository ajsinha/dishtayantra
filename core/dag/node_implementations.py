import logging
import threading
import time
from datetime import datetime
from core.dag.graph_elements import Node

logger = logging.getLogger(__name__)

class SinkNode(Node):
    """Terminal no-op node: an explicit graph leaf that consumes its inputs and
    produces nothing. ``compute`` intentionally does nothing — use this to anchor
    a branch that has side effects upstream but no further propagation."""

    def __init__(self, name, config):
        super().__init__(name, config)

    def compute(self):
        """No-op: a SinkNode is a graph leaf with no output to propagate."""
        return None


class PublisherSinkNode(Node):
    """Terminal node that publishes each incoming edge's data to named publishers.

    Per-edge change detection (``_edge_tracker``) emits an edge's value once per
    distinct value: unique messages each pass through exactly once, repeated
    identical values are de-duped (value-graph behaviour). This is what stops the
    node re-publishing stale edge state on every sweep."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.publishers = config.get("publishers", [])
        self._edge_tracker = {}
        self._published_count = 0  # messages actually published to sinks

    def compute(self) -> bool:
        """Publish every incoming edge whose value changed since last sweep; returns
        True if anything was published, False if no edge changed."""
        if not self.isdirty():
            return False


        # Gather inputs from incoming edges. Per-edge change-detection emits an
        # edge's data once per distinct value: with unique messages this passes
        # every message through exactly once; with repeated identical values it
        # de-dups (value-graph behaviour). This correctly avoids re-publishing
        # stale edge state on every compute sweep.
        edge_data_collection = []
        for edge in self._incoming_edges:
            edge_data = edge.get_data()
            edge_name = edge.name
            if edge_name not in self._edge_tracker.keys():
                self._edge_tracker[edge_name] = edge_data
                edge_data_collection.append(edge_data)
            else:
                known = self._edge_tracker[edge_name]
                if known == edge_data:
                    pass
                else:
                    self._edge_tracker[edge_name] = edge_data
                    edge_data_collection.append(edge_data)

        if len(edge_data_collection) >0:
            self._messages_in += len(edge_data_collection)
            self_graph = self._graph
            for data in edge_data_collection:
                for publisher_name in self.publishers:
                    local_publisher = self_graph.get_publisher_by_name(publisher_name)
                    local_publisher.publish(data)
                self._published_count += 1
            #self.increment_compute_count()
            return True
        else:
            return False


class SubscriptionNode(Node):
    """Node that pulls data from a DataSubscriber"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.subscriber = None

    def set_subscriber(self, subscriber):
        """Set the data subscriber"""
        self.subscriber = subscriber

    def pre_compute(self):
        """Sweep entry hook: if the subscriber has buffered data, mark this node
        dirty so the sweep will call compute(). Subscription nodes are the sources
        that inject new dirtiness into the graph each sweep."""
        if self.subscriber and self.subscriber.get_queue_size() > 0:
            self.set_dirty()

    def compute(self) -> bool:
        """Pull one message, run transformers + calculator, and propagate on change.

        Returns True if the node executed its calculation this sweep, False if it was
        skipped (not dirty, no subscriber, or no data available). The **equality
        gate** is the key invariant: children are marked dirty (and the output
        message count advances) only when the freshly computed output *differs* from
        the previous output, so unchanged values never trigger downstream recompute.
        Always leaves the node clean. Exceptions are recorded and swallowed (return
        False) so one bad message cannot abort the whole sweep."""
        if not self.isdirty() or not self.subscriber:
            return False

        try:
            # Get data from subscriber
            data = self.subscriber.get_data(block_time=0)

            if data is None:
                return False

            # Merge with current input
            self._input = data
            self._messages_in += 1

            # Apply input transformers
            transformed_input = self._input
            for transformer in self._input_transformers:
                transformed_input = transformer.transform(transformed_input)

            # Calculate if calculator is available
            if self._calculator:
                calculated_output = self._calculator.calculate(transformed_input)
            else:
                calculated_output = transformed_input

            # Apply output transformers
            transformed_output = calculated_output
            for transformer in self._output_transformers:
                transformed_output = transformer.transform(transformed_output)

            # Update output. Equality gate: only propagate when the output
            # changed (dependency-graph semantics).
            if transformed_output != self._output:
                self._output = transformed_output
                self._messages_out += 1

                # Mark children as dirty
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()

            #self.increment_compute_count()
            self.set_clean()
            return True

        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in subscription node {self.name}: {str(e)}")
            return False


class PublicationNode(Node):
    """Node that publishes output to DataPublishers"""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.publishers = []
        self._last_published_output = None
        self._published_count = 0  # messages actually published to sinks

    def add_publisher(self, publisher):
        """Add a data publisher"""
        self.publishers.append(publisher)

    def compute(self) -> bool:
        """Run the node's calculation, then publish to sinks only when the output
        changed since the last publish.

        The sink-side **equality gate**: comparing against ``_last_published_output``
        (not just the upstream change) means a value that round-trips back to a prior
        state is not re-emitted, and identical recomputations never hit the broker.
        A failure from one publisher is logged and skipped so the other sinks still
        receive the message. Returns True if the node executed this sweep."""
        if not self.isdirty():
            return False

        try:
            # Run parent compute logic
            super().compute()

            # Publish only when the output changed (dependency-graph semantics).
            if self._output != self._last_published_output:
                for publisher in self.publishers:
                    try:
                        publisher.publish(self._output)
                    except Exception as e:
                        logger.error(f"Error publishing from node {self.name}: {str(e)}")

                self._published_count += 1
                self._last_published_output = self._output.copy() if self._output else None
            #self.increment_compute_count()
            return True
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in publication node {self.name}: {str(e)}")
            return False


class MetronomeNode(Node):
    """Time-driven node: fires on a fixed interval rather than on inbound data.

    A background daemon thread (the metronome) ticks every ``interval`` seconds and
    marks the node dirty, so the node executes on a clock even when no upstream data
    has changed. Concurrency contract: the metronome thread *only* flags dirtiness
    and wakes the compute loop — it never runs the calculation itself. All actual
    computation stays on the single compute thread, preserving the engine's
    single-writer model."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.interval = config.get('interval', 1)
        self.subscriber = None
        self.publishers = []
        self._last_execution = 0
        self._metronome_thread = None
        self._stop_event = threading.Event()

    def set_subscriber(self, subscriber):
        """Set the data subscriber"""
        self.subscriber = subscriber

    def add_publisher(self, publisher):
        """Add a data publisher"""
        self.publishers.append(publisher)

    def start_metronome(self):
        """Start the background ticker (idempotent: a no-op if already running).

        The ticker is a daemon thread so it never blocks process shutdown."""
        if not self._metronome_thread or not self._metronome_thread.is_alive():
            self._stop_event.clear()
            self._metronome_thread = threading.Thread(target=self._metronome_loop, daemon=True)
            self._metronome_thread.start()
            logger.info(f"Metronome node {self.name} started with interval {self.interval}s")

    def _metronome_loop(self):
        """Ticker loop (runs on the metronome thread, never computes).

        Each interval it marks the node dirty and fires ``_notify_callback`` to wake
        the compute loop immediately (v3.0.0) rather than waiting for the next poll.
        Polls at most every 0.1s so ``stop_metronome`` takes effect promptly even
        when the interval is long. Does no calculation and touches no node output —
        that is the compute thread's job — which is why it is safe to run concurrently."""
        while not self._stop_event.is_set():
            current_time = time.time()
            elapsed = current_time - self._last_execution

            if elapsed >= self.interval:
                # Time to tick - mark node as dirty
                self.set_dirty()
                self._last_execution = current_time
                logger.debug(f"Metronome node {self.name} tick at {current_time}")
                # v3.0.0: wake the compute loop immediately on a tick.
                cb = getattr(self, '_notify_callback', None)
                if cb is not None:
                    try:
                        cb()
                    except Exception:
                        pass
                # Small sleep before next check
                time.sleep(0.1)
            else:
                # Sleep until next interval (or check every 0.1s, whichever is smaller)
                time_to_sleep = min(self.interval - elapsed, 0.1)
                self.set_clean()
                time.sleep(time_to_sleep)

    def pre_compute(self):
        """Check subscriber if available"""
        '''
        if self.subscriber and self.subscriber.get_queue_size() > 0:
            self.set_dirty()
        '''
        pass

    def compute(self) -> bool:
        """Run the calculation on each tick and publish.

        Unlike data-driven nodes, a metronome **deliberately defeats the equality
        gate**: it stamps the input with the tick counter (``metronome_tick``) so the
        output always differs and the node emits on every interval even when the
        underlying data is unchanged. That is the whole point of a clock node — it
        guarantees periodic output. Runs on the compute thread after the ticker has
        marked it dirty."""
        if not self.isdirty():
            return False

        try:
            # If subscriber is available, get data from it
            if self.subscriber:
                data = self.subscriber.get_data(block_time=0)
                if data:
                    self._input = data

            # Add tick counter to ensure output changes
            if not self._input:
                self._input = {}
            self._input['metronome_tick'] = self._compute_count  # Add this line

            # Run calculation
            if self._calculator:
                self._output = self._calculator.calculate(self._input)
            else:
                self._output = self._input


            # Publish if publishers are available
            for publisher in self.publishers:
                try:
                    publisher.publish(self._output)
                except Exception as e:
                    logger.error(f"Error publishing from metronome node {self.name}: {str(e)}")

            # Mark children as dirty
            for edge in self._outgoing_edges:
                edge.to_node.set_dirty()
                logger.info(f'node: {self.name} setting child {edge.to_node.name} as dirty')

            # Mark this node as clean (not dirty) - it will be marked dirty again by metronome
            self.set_clean()
            #self.increment_compute_count()
            return True
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e)
            }
            self._errors.append(error_info)
            logger.error(f"Error in metronome node {self.name}: {str(e)}")
            return False

    def stop_metronome(self):
        """Stop metronome thread"""
        self._stop_event.set()
        if self._metronome_thread:
            self._metronome_thread.join(timeout=2)
        logger.info(f"Metronome node {self.name} stopped")


class CalculationNode(Node):
    """Node that performs calculations on input"""

    def __init__(self, name, config):
        super().__init__(name, config)

    # Uses default Node.compute() implementation


class SubgraphWrapperNode(Node):
    """
    Node that wraps a SubgraphNode for integration with the parent graph.
    
    This node acts as a bridge between the parent graph's node system and 
    the subgraph's internal execution. It handles:
    - Dirty propagation across subgraph boundaries
    - Input/output data marshaling
    - Subgraph state management (active/suspended)
    
    Patent Pending - DishtaYantra Framework
    """
    
    def __init__(self, name, config, subgraph_node):
        """
        Initialize the wrapper node.
        
        Args:
            name: Node name in parent graph
            config: Node configuration
            subgraph_node: The SubgraphNode to wrap
        """
        super().__init__(name, config)
        self.subgraph_node = subgraph_node
        self._node_type = 'SubgraphNode'
        
    @property
    def is_subgraph(self) -> bool:
        """Indicate this is a subgraph node."""
        return True
    
    @property
    def subgraph(self):
        """Get the underlying subgraph."""
        return self.subgraph_node.subgraph if self.subgraph_node else None
    
    @property
    def is_active(self) -> bool:
        """Check if subgraph is active."""
        return self.subgraph_node.is_active if self.subgraph_node else False
    
    @property
    def is_suspended(self) -> bool:
        """Check if subgraph is suspended."""
        return self.subgraph_node.is_suspended if self.subgraph_node else True
    
    def set_dirty(self):
        """Set this node as dirty and propagate to subgraph if active."""
        super().set_dirty()
        if self.subgraph_node and self.is_active:
            self.subgraph_node.mark_dirty()
    
    def pre_compute(self):
        """Pre-compute phase - check if any incoming edges have new data."""
        # Check incoming edges for data
        for edge in self._incoming_edges:
            if edge.from_node.isdirty() or self._input != edge.get_data():
                self.set_dirty()
                break
    
    def compute(self) -> bool:
        """
        Execute the subgraph and propagate output.
        
        If subgraph is suspended, cached output is used and downstream
        nodes are NOT marked dirty (preventing unnecessary recalculation).
        """
        if not self.isdirty():
            return False
        
        try:
            # Gather inputs from incoming edges
            inputs = {}
            for edge in self._incoming_edges:
                edge_data = edge.get_data()
                if edge_data is not None:
                    if edge.pname:
                        inputs[edge.pname] = edge_data
                    else:
                        inputs[edge.from_node.name] = edge_data
            
            # Merge into single input if only one source
            if len(inputs) == 1:
                input_data = list(inputs.values())[0]
            else:
                input_data = inputs
            
            self._input = input_data
            
            # Execute subgraph
            if self.subgraph_node:
                self._output = self.subgraph_node.execute(input_data)
            else:
                self._output = input_data
            
            # Propagate to outgoing edges
            for edge in self._outgoing_edges:
                edge.set_data(self._output)
                # Only mark downstream dirty if subgraph is active
                # When suspended, we use cached output but don't propagate dirty
                if self.is_active:
                    edge.to_node.set_dirty()
            
            self.set_clean()
            return True
            
        except Exception as e:
            error_info = {
                'time': datetime.now().isoformat(),
                'error': str(e),
                'subgraph': self.name
            }
            self._errors.append(error_info)
            logger.error(f"Error in subgraph node {self.name}: {str(e)}")
            return False
    
    def light_up(self, reason: str = None):
        """Activate the subgraph."""
        if self.subgraph_node:
            self.subgraph_node.light_up(reason)
            self.set_dirty()  # Trigger recalculation
    
    def light_down(self, reason: str = None):
        """Suspend the subgraph."""
        if self.subgraph_node:
            self.subgraph_node.light_down(reason)
    
    def details(self) -> dict:
        """Return detailed information about this node."""
        base_details = super().details()
        
        # Add subgraph-specific details
        base_details['node_type'] = 'SubgraphNode'
        base_details['is_subgraph'] = True
        
        if self.subgraph_node and self.subgraph:
            sg = self.subgraph
            base_details['subgraph'] = {
                'name': sg.name,
                'state': sg.state.value,
                'is_active': sg.is_active,
                'execution_mode': sg.execution_mode.value,
                'node_count': sg.node_count,
                'entry_node': sg.entry_node_name,
                'exit_node': sg.exit_node_name,
                'suspend_reason': sg.suspend_reason,
                'metrics': {
                    'execution_count': sg.metrics.execution_count,
                    'avg_latency_ms': sg.metrics.avg_execution_time_ms,
                    'last_latency_ms': sg.metrics.last_execution_time_ms,
                    'cache_hits': sg.metrics.cache_hit_count,
                    'errors': sg.metrics.error_count
                },
                'internal_nodes': [
                    {
                        'name': node.name,
                        'borrows_from': node.borrows_from,
                        'role': node.role,
                        'is_dirty': node.is_dirty,
                        'last_execution_ms': node.last_execution_time_ms
                    }
                    for node in sg.nodes.values()
                ]
            }
        
        return base_details

# ----------------------------------------------------------------------------
# A1 source-batching nodes (roadmap Phase 1). ADDITIVE and OPT-IN: these are new
# node types. SubscriptionNode / PublicationNode above are unchanged, so every
# existing DAG behaves exactly as before. Use these only when a DAG opts in by
# setting node "type" to "BatchingSubscriptionNode" / "FlatteningPublicationNode".
# ----------------------------------------------------------------------------
class BatchingSubscriptionNode(SubscriptionNode):
    """Source node that auto-batches messages for the Arrow columnar path.

    Drains up to ``max_batch_size`` messages from the subscriber per compute
    cycle and emits ONE batch-envelope message ``{batch_key: [...]}`` so
    downstream ArrowCalculators can vectorize without the application having to
    build envelopes itself. Load-adaptive: batches fill under backlog and flush
    immediately when the queue is idle (so a quiet stream keeps low latency).

    Config (all optional):
        {"batch": {"max_size": 500, "key": "batch"}}
      or flat: {"max_batch_size": 500, "batch_key": "batch"}
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        bc = cfg.get('batch', {}) or {}
        size = bc.get('max_size', bc.get('size', cfg.get('max_batch_size', cfg.get('batch_size', 500))))
        self._max_batch_size = max(1, int(size))
        self._batch_key = bc.get('key', cfg.get('batch_key', 'batch'))

    def compute(self) -> bool:
        """Drain up to ``max_batch_size`` messages into one batch-envelope and run
        the calculator/transformers once over it. Returns False and stays clean when
        the queue is empty, so an idle stream costs nothing; the equality gate still
        applies to the emitted envelope."""
        if not self.isdirty() or not self.subscriber:
            return False
        try:
            buffer = []
            while len(buffer) < self._max_batch_size:
                data = self.subscriber.get_data(block_time=0)
                if data is None:
                    break
                buffer.append(data)
            if not buffer:
                self.set_clean()
                return False

            self._messages_in += len(buffer)
            transformed_input = {self._batch_key: buffer}
            for transformer in self._input_transformers:
                transformed_input = transformer.transform(transformed_input)

            if self._calculator:
                calculated_output = self._calculator.calculate(transformed_input)
            else:
                calculated_output = transformed_input

            transformed_output = calculated_output
            for transformer in self._output_transformers:
                transformed_output = transformer.transform(transformed_output)

            if transformed_output != self._output:
                self._output = transformed_output
                self._messages_out += 1
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()
            self.set_clean()
            return True
        except Exception as e:
            self._errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            logger.error(f"Error in batching subscription node {self.name}: {str(e)}")
            return False


class FlatteningPublicationNode(PublicationNode):
    """Sink node that unbatches a batch-envelope before publishing.

    If its input is ``{batch_key: [...]}`` it publishes each record as a separate
    message, so the external/broker contract stays per-message even when the DAG
    batched internally. Otherwise it behaves exactly like PublicationNode. Pairs
    with BatchingSubscriptionNode.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        bc = cfg.get('batch', {}) or {}
        self._batch_key = bc.get('key', cfg.get('batch_key', 'batch'))

    def compute(self) -> bool:
        """Unbatch on publish: if the output is a ``{batch_key: [...]}`` envelope,
        publish each record separately so the external/broker contract stays
        per-message; otherwise publish the value as-is. Gated on change vs the last
        published value to avoid re-emitting unchanged state each sweep."""
        if not self.isdirty():
            return False
        try:
            Node.compute(self)  # consolidate incoming edges -> self._output
            out = self._output
            if out != self._last_published_output:
                if isinstance(out, dict) and isinstance(out.get(self._batch_key), list):
                    records = out[self._batch_key]
                    for rec in records:
                        for publisher in self.publishers:
                            try:
                                publisher.publish(rec)
                            except Exception as e:
                                logger.error(f"Error publishing from node {self.name}: {str(e)}")
                    self._published_count += len(records)
                else:
                    for publisher in self.publishers:
                        try:
                            publisher.publish(out)
                        except Exception as e:
                            logger.error(f"Error publishing from node {self.name}: {str(e)}")
                    self._published_count += 1
                self._last_published_output = dict(out) if isinstance(out, dict) else out
            return True
        except Exception as e:
            self._errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            logger.error(f"Error in flattening publication node {self.name}: {str(e)}")
            return False


# ---------------------------------------------------------------------------
# Arrow zero-copy transport nodes (v5.1.0, additive / opt-in)
#
# These pair with BatchingSubscriptionNode/FlatteningPublicationNode but carry a
# pyarrow.RecordBatch *on the edges* instead of the {batch_key: [...]} dict
# envelope. Because a RecordBatch is immutable, the engine shares it by reference
# (edge_value.ev_copy), so there is no per-stage deep-copy — the conversion
# dict<->Arrow happens once at ingress and once at egress, not at every hop.
# Reachable only by selecting these node types; existing DAGs are unaffected.
# ---------------------------------------------------------------------------

class ArrowBatchingSubscriptionNode(BatchingSubscriptionNode):
    """Source node that drains messages and emits ONE Arrow RecordBatch (not a
    dict envelope) for zero-copy columnar transport to downstream ArrowCalculators.

    Selected by node ``type`` ``ArrowBatchingSubscriptionNode``. Config matches
    BatchingSubscriptionNode (``{"batch": {"max_size": 500}}``). Node-level
    transformers are not supported on the Arrow source in this version (use edge
    transformers / ``transform_batch``); they fail fast rather than be ignored.
    """

    def compute(self) -> bool:
        """Drain messages, build ONE Arrow RecordBatch, and propagate by reference.

        Two Arrow-specific points: the equality gate uses ``ev_equals`` (RecordBatch
        value-equality) rather than dict ``!=``; and the output batch is shared
        downstream by reference (no copy), which is the whole point of the columnar
        path. Node-level transformers are rejected up front (fail fast) since the
        Arrow source can't apply them — use an edge transformer with transform_batch."""
        from core.dag.edge_value import ev_equals
        if not self.isdirty() or not self.subscriber:
            return False
        if self._input_transformers or self._output_transformers:
            raise TypeError(
                f"{self.name}: ArrowBatchingSubscriptionNode does not support "
                "node-level transformers; use an edge transformer with transform_batch."
            )
        try:
            from core.calculator.arrow_calculator import records_to_batch
            buffer = []
            while len(buffer) < self._max_batch_size:
                data = self.subscriber.get_data(block_time=0)
                if data is None:
                    break
                buffer.append(data)
            if not buffer:
                self.set_clean()
                return False

            self._messages_in += len(buffer)
            batch = records_to_batch(buffer)            # dict rows -> RecordBatch (once)
            if self._calculator:
                output = self._calculator.calculate(batch)
            else:
                output = batch

            if not ev_equals(output, self._output):
                self._output = output                   # shared by reference, no copy
                self._messages_out += 1
                for edge in self._outgoing_edges:
                    edge.to_node.set_dirty()
            self.set_clean()
            return True
        except Exception as e:
            self._errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            logger.error(f"Error in arrow batching subscription node {self.name}: {str(e)}")
            return False


class ArrowFlatteningPublicationNode(FlatteningPublicationNode):
    """Sink node that converts an incoming Arrow RecordBatch back to per-row dicts
    (``to_pylist``) and publishes each row, preserving the per-message external
    contract. If the input is not a batch it behaves exactly like
    FlatteningPublicationNode. Selected by node type ``ArrowFlatteningPublicationNode``.
    """

    def compute(self) -> bool:
        """Arrow unbatch on publish: convert an incoming RecordBatch to rows
        (``to_pylist``, once) and publish each, keeping the per-message external
        contract; uses ``ev_equals`` for the change gate. Falls back to the plain
        FlatteningPublicationNode path for non-batch input."""
        from core.dag.edge_value import is_batch, ev_equals
        if not self.isdirty():
            return False
        try:
            Node.compute(self)  # consolidate incoming edge(s) -> self._output
            out = self._output

            if is_batch(out):
                if not ev_equals(out, self._last_published_output):
                    records = out.to_pylist()           # RecordBatch -> rows (once)
                    for rec in records:
                        for publisher in self.publishers:
                            try:
                                publisher.publish(rec)
                            except Exception as e:
                                logger.error(f"Error publishing from node {self.name}: {str(e)}")
                    self._published_count += len(records)
                    self._last_published_output = out
                return True

            # non-batch input: identical to FlatteningPublicationNode's publish path
            if out != self._last_published_output:
                if isinstance(out, dict) and isinstance(out.get(self._batch_key), list):
                    records = out[self._batch_key]
                    for rec in records:
                        for publisher in self.publishers:
                            try:
                                publisher.publish(rec)
                            except Exception as e:
                                logger.error(f"Error publishing from node {self.name}: {str(e)}")
                    self._published_count += len(records)
                else:
                    for publisher in self.publishers:
                        try:
                            publisher.publish(out)
                        except Exception as e:
                            logger.error(f"Error publishing from node {self.name}: {str(e)}")
                    self._published_count += 1
                self._last_published_output = dict(out) if isinstance(out, dict) else out
            return True
        except Exception as e:
            self._errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            logger.error(f"Error in arrow flattening publication node {self.name}: {str(e)}")
            return False
