"""
Fanout Subscriber Introspection Mixin (v2.2 module split)
=========================================================

details() / routing statistics / child summaries / unrouted-file admin for
FanoutDataSubscriber, extracted verbatim from fanout_datapubsub.py to
respect the 500-line architecture limit. All state lives on the
subscriber.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import queue
import importlib
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, Protocol, runtime_checkable

from core.pubsub.datapubsub import DataSubscriber, DataPublisher, DataAwarePayload

logger = logging.getLogger(__name__)



class FanoutSubscriberStatsMixin:
    """Introspection, statistics, and unrouted-file administration."""

    def details(self):
        """
        Return detailed information about the router and all child subscribers

        Returns:
            Dictionary containing router details and all child subscriber details
        """
        with self._lock:
            base_details = {
                'name': self.name,
                'type': 'message_router',
                'source': self.source.replace('router://', ''),
                'source_subscriber': self.source_subscriber.name,
                'resolver_class': f"{self.resolver.__class__.__module__}.{self.resolver.__class__.__name__}",
                'max_depth': self.max_depth,
                'current_depth': self.get_queue_size(),
                'last_receive': self._last_receive,
                'receive_count': self._receive_count,
                'routed_count': dict(self._routed_count),
                'unrouted_count': self._unrouted_count,
                'error_count': self._error_count,
                'unrouted_file': self.unrouted_file,
                'suspended': not self._suspend_event.is_set(),
                'child_count': len(self.child_subscribers),
                'routing_keys': list(self.child_subscribers.keys()),
                'children': {}
            }

            # Add details from each child subscriber
            for routing_key, subscriber in self.child_subscribers.items():
                try:
                    base_details['children'][routing_key] = subscriber.details()
                except Exception as e:
                    logger.error(f"Error getting details for child subscriber '{routing_key}': {str(e)}")
                    base_details['children'][routing_key] = {'error': str(e)}

            return base_details

    def get_routing_statistics(self):
        """
        Get statistics about message routing

        Returns:
            Dictionary with routing statistics
        """
        with self._lock:
            total_routed = sum(self._routed_count.values())

            stats = {
                'total_received': self._receive_count,
                'total_routed': total_routed,
                'total_unrouted': self._unrouted_count,
                'total_errors': self._error_count,
                'routing_efficiency': (total_routed / self._receive_count * 100) if self._receive_count > 0 else 0,
                'routes': {}
            }

            # Per-route statistics
            for routing_key, count in self._routed_count.items():
                stats['routes'][routing_key] = {
                    'count': count,
                    'percentage': (count / total_routed * 100) if total_routed > 0 else 0
                }

            return stats

    def get_child_summary(self):
        """
        Get summary of all child subscribers

        Returns:
            Dictionary with child subscriber summary
        """
        summary = {
            'total_children': len(self.child_subscribers),
            'children': {}
        }

        for routing_key, subscriber in self.child_subscribers.items():
            try:
                details = subscriber.details()
                summary['children'][routing_key] = {
                    'name': details.get('name'),
                    'source': details.get('source'),
                    'queue_depth': details.get('current_depth', 0),
                    'receive_count': details.get('receive_count', 0),
                    'suspended': details.get('suspended', False)
                }
            except Exception as e:
                logger.error(f"Error getting summary for child '{routing_key}': {str(e)}")
                summary['children'][routing_key] = {'error': str(e)}

        return summary

    def get_total_child_receive_count(self):
        """
        Get total receive count across all child subscribers

        Returns:
            Total number of messages received by all child subscribers
        """
        total = 0
        for subscriber in self.child_subscribers.values():
            try:
                details = subscriber.details()
                total += details.get('receive_count', 0)
            except Exception as e:
                logger.error(f"Error getting receive count: {str(e)}")
        return total

    def clear_unrouted_file(self):
        """
        Clear the unrouted messages file

        This can be useful for maintenance or after analyzing unrouted messages

        Returns:
            Number of lines that were in the file before clearing
        """
        try:
            with open(self.unrouted_file, 'r') as f:
                line_count = sum(1 for _ in f)

            # Clear the file
            open(self.unrouted_file, 'w').close()

            logger.info(f"Cleared {line_count} unrouted messages from {self.unrouted_file}")
            return line_count

        except FileNotFoundError:
            logger.info(f"Unrouted file {self.unrouted_file} does not exist")
            return 0
        except Exception as e:
            logger.error(f"Error clearing unrouted file: {str(e)}")
            return -1


