"""
DAG Messaging & Subgraph Routes Mixin (v2.2)
============================================

Handlers for manual message publishing to a subscriber's external broker and
for subgraph light-up / light-down control. Split out of ``dag_routes.py`` to
keep each module within the 500-line architecture limit; mixed into
:class:`routes.dag_routes.DAGRoutes`, which registers the routes and provides
``self.dag_server`` / ``self.guards``.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback

from fastapi import Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import (
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


class DAGMessagingMixin:
    """Manual publish + subgraph control handlers for DAGRoutes."""


    # ------------------------------------------------------------------ #
    # Manual message publishing
    # ------------------------------------------------------------------ #

    def publish_message_page(self, request: Request, dag_name: str,
                             subscriber_name: str):
        """Render the manual publish page for one subscriber."""
        self.guards.admin_required(request)
        try:
            details = self.dag_server.details(dag_name)
            if subscriber_name not in details.get('subscribers', {}):
                return redirect_to(
                    request, 'dag_details', dag_name=dag_name,
                    flash_message=f'Subscriber {subscriber_name} not found',
                    flash_category='error')
            return render(request, 'dag/publish_message.html',
                          dag_name=dag_name,
                          subscriber_name=subscriber_name,
                          subscriber_info=details['subscribers'][
                              subscriber_name],
                          is_admin=True)
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading publish message '
                                         'page', e)
            return redirect_to(request, 'dag_details', dag_name=dag_name)

    async def publish_message_submit(self, request: Request, dag_name: str,
                                     subscriber_name: str):
        """Publish a message through the subscriber's EXTERNAL broker.

        v1.7.2: The message goes through the full pub/sub path
        (UI -> Publisher -> External Broker -> Subscriber -> DAG) so the
        external system is never bypassed.
        v1.5.2: Works with lazy-initialized DAGs by falling back to the DAG
        configuration when subscriber components aren't built.
        """
        self.guards.admin_required(request)
        try:
            form = await request.form()
            message = form.get('message')
            is_raw = (form.get('is_raw') or 'false').lower() == 'true'

            if not message:
                return JSONResponse({'error': 'No message provided'},
                                    status_code=400)

            if is_raw:
                message_data = message
                logger.info(f"Publishing raw message to {subscriber_name}: "
                            f"{message[:100]}...")
            else:
                message_data = json.loads(message)

            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return JSONResponse({'error': 'DAG not found'},
                                    status_code=404)

            # v1.5.2: Get subscriber config (built object or raw DAG config)
            subscriber_config = None
            source = None
            subscriber = dag.subscribers.get(subscriber_name)
            if subscriber:
                source = subscriber.source
                subscriber_config = subscriber.config.copy()
            else:
                for sub_cfg in dag.config.get('subscribers', []):
                    if sub_cfg.get('name') == subscriber_name:
                        subscriber_config = sub_cfg.get('config', {}).copy()
                        source = subscriber_config.get('source')
                        break

            if not subscriber_config or not source:
                return JSONResponse(
                    {'error': f'Subscriber {subscriber_name} not found in '
                              f'DAG configuration'}, status_code=404)

            supported_prefixes = ['mem://', 'inmemory://', 'memory://',
                                  'kafka://', 'redischannel://',
                                  'activemq://', 'rabbitmq://', 'tibcoems://']
            if not any(prefix in source for prefix in supported_prefixes):
                return JSONResponse(
                    {'error': 'Subscriber type does not support publishing'},
                    status_code=400)

            from core.pubsub.pubsubfactory import create_publisher
            subscriber_config['destination'] = source

            logger.info("")
            logger.info("=" * 70)
            logger.info("  UI MESSAGE PUBLISH - INITIATING")
            logger.info(f"  Creating REAL publisher to external system: "
                        f"{source}")
            logger.info("  Flow: UI -> Publisher -> External Broker -> "
                        "Subscriber -> DAG")
            logger.info("=" * 70)

            temp_publisher = create_publisher(f'ui_pub_{subscriber_name}',
                                              subscriber_config)
            temp_publisher.publish(message_data)
            temp_publisher.stop()

            msg_type = 'raw' if is_raw else 'JSON'
            logger.info("")
            logger.info("=" * 70)
            logger.info("  UI MESSAGE PUBLISH - SUCCESS")
            logger.info(f"  DAG: {dag_name}")
            logger.info(f"  Subscriber: {subscriber_name}")
            logger.info(f"  External Destination: {source}")
            logger.info(f"  Message Type: {msg_type}")
            if isinstance(message_data, str):
                preview = message_data[:100] + '...' \
                    if len(message_data) > 100 else message_data
                logger.info(f"  Message Preview: {preview}")
            else:
                logger.info(f"  Message Keys: "
                            f"{list(message_data.keys()) if isinstance(message_data, dict) else 'N/A'}")
            logger.info("  Message sent to EXTERNAL broker - DAG subscriber "
                        "will receive it")
            logger.info("=" * 70)

            return JSONResponse({'success': True,
                                 'message': f'{msg_type} message published '
                                            f'successfully'})
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': f'Invalid JSON: {str(e)}'},
                                status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error publishing message: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    # ------------------------------------------------------------------ #
    # Subgraph control
    # ------------------------------------------------------------------ #

    async def subgraph_control(self, request: Request, dag_name: str):
        """
        Control subgraph state (light up / light down).

        Expected JSON body::

            {"command": "light_up" | "light_down" | "light_up_all" |
                        "light_down_all",
             "subgraph": "subgraph_name",   // for single commands
             "reason": "optional reason"}
        """
        self.guards.admin_required(request)
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return JSONResponse({'success': False,
                                     'error': 'DAG not found'},
                                    status_code=404)

            data = await request.json()
            if not data:
                return JSONResponse({'success': False,
                                     'error': 'No JSON data provided'},
                                    status_code=400)

            command = data.get('command')
            subgraph_name = data.get('subgraph')
            reason = data.get('reason', 'Manual control via UI')

            if command == 'light_up':
                if not subgraph_name:
                    return JSONResponse({'success': False,
                                         'error': 'Subgraph name required'},
                                        status_code=400)
                dag.light_up_subgraph(subgraph_name, reason)
                logger.info(f"Subgraph '{subgraph_name}' in DAG '{dag_name}' "
                            f"activated. Reason: {reason}")
                return JSONResponse({'success': True,
                                     'message': f'Subgraph {subgraph_name} '
                                                f'activated'})
            if command == 'light_down':
                if not subgraph_name:
                    return JSONResponse({'success': False,
                                         'error': 'Subgraph name required'},
                                        status_code=400)
                dag.light_down_subgraph(subgraph_name, reason)
                logger.info(f"Subgraph '{subgraph_name}' in DAG '{dag_name}' "
                            f"suspended. Reason: {reason}")
                return JSONResponse({'success': True,
                                     'message': f'Subgraph {subgraph_name} '
                                                f'suspended'})
            if command == 'light_up_all':
                dag.light_up_all_subgraphs(reason)
                logger.info(f"All subgraphs in DAG '{dag_name}' activated. "
                            f"Reason: {reason}")
                return JSONResponse({'success': True,
                                     'message': 'All subgraphs activated'})
            if command == 'light_down_all':
                dag.light_down_all_subgraphs(reason)
                logger.info(f"All subgraphs in DAG '{dag_name}' suspended. "
                            f"Reason: {reason}")
                return JSONResponse({'success': True,
                                     'message': 'All subgraphs suspended'})
            return JSONResponse({'success': False,
                                 'error': f'Unknown command: {command}'},
                                status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error controlling subgraph: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def subgraph_status(self, request: Request, dag_name: str):
        """Get status of all subgraphs in a DAG."""
        self.guards.admin_required(request)
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return JSONResponse({'error': 'DAG not found'},
                                    status_code=404)
            status = dag.get_subgraph_status()
            return JSONResponse({'dag_name': dag_name,
                                 'subgraph_count': len(status),
                                 'subgraphs': status})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting subgraph status: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)
