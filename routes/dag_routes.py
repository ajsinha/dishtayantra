"""DAG management routes module"""
import json
import logging
from flask import render_template, request, redirect, url_for, flash, jsonify

logger = logging.getLogger(__name__)


class DAGRoutes:
    """Handles DAG management routes"""
    
    def __init__(self, app, dag_server, admin_required, worker_pool=None):
        self.app = app
        self.dag_server = dag_server
        self.admin_required = admin_required
        self.worker_pool = worker_pool  # v1.5.2: Worker pool for DAG dispatch
        self._register_routes()
    
    def _register_routes(self):
        """Register all DAG management routes"""
        self.app.add_url_rule('/dag/create', 'create_dag', 
                             self.admin_required(self.create_dag), 
                             methods=['GET', 'POST'])
        self.app.add_url_rule('/dag/<dag_name>/clone', 'clone_dag', 
                             self.admin_required(self.clone_dag), 
                             methods=['GET', 'POST'])
        self.app.add_url_rule('/dag/<dag_name>/delete', 'delete_dag', 
                             self.admin_required(self.delete_dag), 
                             methods=['POST'])
        self.app.add_url_rule('/dag/<dag_name>/start', 'start_dag', 
                             self.admin_required(self.start_dag), 
                             methods=['POST'])
        self.app.add_url_rule('/dag/<dag_name>/stop', 'stop_dag', 
                             self.admin_required(self.stop_dag), 
                             methods=['POST'])
        self.app.add_url_rule('/dag/<dag_name>/suspend', 'suspend_dag', 
                             self.admin_required(self.suspend_dag), 
                             methods=['POST'])
        self.app.add_url_rule('/dag/<dag_name>/resume', 'resume_dag', 
                             self.admin_required(self.resume_dag), 
                             methods=['POST'])
        self.app.add_url_rule('/dag/<dag_name>/subscriber/<subscriber_name>/publish', 
                             'publish_message', 
                             self.admin_required(self.publish_message), 
                             methods=['GET', 'POST'])
        
        # Subgraph control routes
        self.app.add_url_rule('/dag/<dag_name>/subgraph/control',
                             'dag_subgraph_control',
                             self.admin_required(self.subgraph_control),
                             methods=['POST'])
        self.app.add_url_rule('/dag/<dag_name>/subgraph/status',
                             'dag_subgraph_status',
                             self.subgraph_status,
                             methods=['GET'])
    
    def create_dag(self):
        """Create a new DAG"""
        if request.method == 'GET':
            return render_template('dag/create.html')

        try:
            if 'config_file' not in request.files:
                flash('No file provided', 'error')
                return redirect(url_for('create_dag'))

            file = request.files['config_file']
            if file.filename == '':
                flash('No file selected', 'error')
                return redirect(url_for('create_dag'))

            if not file.filename.endswith('.json'):
                flash('File must be a JSON file', 'error')
                return redirect(url_for('create_dag'))

            config_data = json.load(file)
            dag_name = self.dag_server.add_dag(config_data, file.filename)

            flash(f'DAG {dag_name} created successfully', 'success')
            return redirect(url_for('dashboard'))
        except Exception as e:
            logger.error(f"Error creating DAG: {str(e)}")
            flash(f'Error creating DAG: {str(e)}', 'error')
            return redirect(url_for('create_dag'))
    
    def clone_dag(self, dag_name):
        """Clone an existing DAG"""
        if request.method == 'GET':
            try:
                # Get original DAG details
                dag = self.dag_server.dags.get(dag_name)
                if not dag:
                    flash(f'DAG {dag_name} not found', 'error')
                    return redirect(url_for('dashboard'))

                return render_template('dag/clone.html',
                                       dag_name=dag_name,
                                       original_start=dag.start_time,
                                       original_end=dag.end_time,
                                       original_duration=dag.duration)
            except Exception as e:
                logger.error(f"Error loading clone page: {str(e)}")
                flash(f'Error: {str(e)}', 'error')
                return redirect(url_for('dashboard'))

        try:
            # Get form data
            start_time = request.form.get('start_time', '').strip()
            duration = request.form.get('duration', '').strip()

            # Convert empty strings to None
            if not start_time:
                start_time = None
            if not duration:
                duration = None

            # Clean start_time - remove colons if present
            if start_time:
                start_time = start_time.replace(':', '')

                # Validate start_time format
                if len(start_time) != 4 or not start_time.isdigit():
                    flash('Invalid start_time format. Use HHMM format (e.g., 0900)', 'error')
                    return redirect(url_for('clone_dag', dag_name=dag_name))

                hour = int(start_time[:2])
                minute = int(start_time[2:])
                if hour > 23 or minute > 59:
                    flash('Invalid start_time. Hour must be 0-23, minute must be 0-59', 'error')
                    return redirect(url_for('clone_dag', dag_name=dag_name))

            # Validate duration format if provided
            if duration:
                import re
                duration_pattern = r'^(\d+h)?(\d+m)?$'
                if not re.match(duration_pattern, duration.lower()):
                    flash('Invalid duration format. Use format like: 1h, 30m, or 1h30m', 'error')
                    return redirect(url_for('clone_dag', dag_name=dag_name))

                # Check that duration has at least hours or minutes
                if 'h' not in duration.lower() and 'm' not in duration.lower():
                    flash('Duration must include hours (h) or minutes (m)', 'error')
                    return redirect(url_for('clone_dag', dag_name=dag_name))

            # Clone the DAG with new time window
            cloned_name = self.dag_server.clone_dag(dag_name, start_time, duration)

            # Prepare success message
            if start_time and duration:
                flash(f'DAG cloned to {cloned_name} with start_time={start_time}, duration={duration}', 'success')
            elif start_time:
                flash(f'DAG cloned to {cloned_name} with start_time={start_time} (default duration: -5 minutes)', 'success')
            else:
                flash(f'DAG cloned to {cloned_name} with perpetual running (24/7)', 'success')

            return redirect(url_for('dashboard'))
        except Exception as e:
            logger.error(f"Error cloning DAG: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            flash(f'Error cloning DAG: {str(e)}', 'error')
            return redirect(url_for('clone_dag', dag_name=dag_name))
    
    def delete_dag(self, dag_name):
        """Delete a DAG"""
        try:
            self.dag_server.delete(dag_name)
            flash(f'DAG {dag_name} deleted', 'success')
        except Exception as e:
            logger.error(f"Error deleting DAG: {str(e)}")
            flash(f'Error deleting DAG: {str(e)}', 'error')

        return redirect(url_for('dashboard'))
    
    def start_dag(self, dag_name):
        """Start a DAG
        
        v1.5.2: DAGComputeServer.start() handles worker pool dispatch automatically
        """
        try:
            self.dag_server.start(dag_name)
            
            # Check where it's running to show appropriate message
            if self.worker_pool and self.worker_pool.is_running():
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    flash(f'DAG {dag_name} started on Worker {worker_id}', 'success')
                else:
                    flash(f'DAG {dag_name} started', 'success')
            else:
                flash(f'DAG {dag_name} started', 'success')
        except Exception as e:
            logger.error(f"Error starting DAG: {str(e)}")
            flash(f'Error starting DAG: {str(e)}', 'error')

        return redirect(url_for('dashboard'))
    
    def stop_dag(self, dag_name):
        """Stop a DAG
        
        v1.5.2: DAGComputeServer.stop() handles worker pool automatically
        """
        try:
            # Check where it was running before stopping
            worker_id = None
            if self.worker_pool and self.worker_pool.is_running():
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
            
            self.dag_server.stop(dag_name)
            
            if worker_id is not None:
                flash(f'DAG {dag_name} stopped (was on Worker {worker_id})', 'success')
            else:
                flash(f'DAG {dag_name} stopped', 'success')
        except Exception as e:
            logger.error(f"Error stopping DAG: {str(e)}")
            flash(f'Error stopping DAG: {str(e)}', 'error')

        return redirect(url_for('dashboard'))
    
    def suspend_dag(self, dag_name):
        """Suspend a DAG"""
        try:
            self.dag_server.suspend(dag_name)
            flash(f'DAG {dag_name} suspended', 'success')
        except Exception as e:
            logger.error(f"Error suspending DAG: {str(e)}")
            flash(f'Error suspending DAG: {str(e)}', 'error')

        return redirect(url_for('dashboard'))
    
    def resume_dag(self, dag_name):
        """Resume a DAG"""
        try:
            self.dag_server.resume(dag_name)
            flash(f'DAG {dag_name} resumed', 'success')
        except Exception as e:
            logger.error(f"Error resuming DAG: {str(e)}")
            flash(f'Error resuming DAG: {str(e)}', 'error')

        return redirect(url_for('dashboard'))
    
    def publish_message(self, dag_name, subscriber_name):
        """Display the publish message page (GET) or handle message submission (POST)
        
        v1.5.2: Updated to work with lazy-initialized DAGs. When worker pool is enabled,
        DAG components aren't built in main process, so we get subscriber config directly
        from the DAG configuration instead of the subscriber object.
        """
        if request.method == 'GET':
            try:
                details = self.dag_server.details(dag_name)

                # Check if subscriber exists
                if subscriber_name not in details.get('subscribers', {}):
                    flash(f'Subscriber {subscriber_name} not found', 'error')
                    return redirect(url_for('dag_details', dag_name=dag_name))

                subscriber_info = details['subscribers'][subscriber_name]

                return render_template(
                    'dag/publish_message.html',
                    dag_name=dag_name,
                    subscriber_name=subscriber_name,
                    subscriber_info=subscriber_info,
                    is_admin=True
                )
            except Exception as e:
                logger.error(f"Error loading publish message page: {str(e)}")
                flash(f'Error: {str(e)}', 'error')
                return redirect(url_for('dag_details', dag_name=dag_name))

        # POST method - handle message submission
        try:
            message = request.form.get('message')
            is_raw = request.form.get('is_raw', 'false').lower() == 'true'
            
            if not message:
                return jsonify({'error': 'No message provided'}), 400

            # Parse message based on format
            if is_raw:
                # v1.2.0: Raw message mode - send as string without JSON parsing
                # The subscriber with auto_package_non_dict=true will package it
                message_data = message
                logger.info(f"Publishing raw message to {subscriber_name}: {message[:100]}...")
            else:
                # JSON mode - parse and validate
                message_data = json.loads(message)

            # Get the DAG
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return jsonify({'error': 'DAG not found'}), 404

            # v1.5.2: Get subscriber config from DAG config (works with lazy-init)
            # When worker pool is enabled, dag.subscribers may be empty, but dag.config
            # always has the full configuration
            subscriber_config = None
            source = None
            
            # First try to get from built subscriber (if components are built)
            subscriber = dag.subscribers.get(subscriber_name)
            if subscriber:
                source = subscriber.source
                subscriber_config = subscriber.config.copy()
            else:
                # Fallback to DAG config (for lazy-initialized DAGs)
                for sub_cfg in dag.config.get('subscribers', []):
                    if sub_cfg.get('name') == subscriber_name:
                        subscriber_config = sub_cfg.get('config', {}).copy()
                        source = subscriber_config.get('source')
                        break
            
            if not subscriber_config or not source:
                return jsonify({'error': f'Subscriber {subscriber_name} not found in DAG configuration'}), 404

            # Check if subscriber type supports publishing
            supported_prefixes = ['mem://', 'inmemory://', 'memory://', 'kafka://', 'redischannel://', 'activemq://', 'rabbitmq://', 'tibcoems://']
            if not any(prefix in source for prefix in supported_prefixes):
                return jsonify({'error': 'Subscriber type does not support publishing'}), 400

            # v1.7.2: Create a real publisher to the ACTUAL external system (Kafka, ActiveMQ, etc.)
            # This ensures the message goes through the full pubsub path:
            #   UI → Publisher → External Broker → Subscriber → DAG
            # NOT bypassing the external system.
            from core.pubsub.pubsubfactory import create_publisher
            subscriber_config['destination'] = source

            logger.info("")
            logger.info("=" * 70)
            logger.info("  UI MESSAGE PUBLISH - INITIATING")
            logger.info(f"  Creating REAL publisher to external system: {source}")
            logger.info("  Flow: UI → Publisher → External Broker → Subscriber → DAG")
            logger.info("=" * 70)

            temp_publisher = create_publisher(f'ui_pub_{subscriber_name}', subscriber_config)
            temp_publisher.publish(message_data)
            temp_publisher.stop()

            msg_type = 'raw' if is_raw else 'JSON'
            
            # v1.7.2: Distinct log message for UI publish success
            logger.info("")
            logger.info("=" * 70)
            logger.info("  UI MESSAGE PUBLISH - SUCCESS")
            logger.info(f"  DAG: {dag_name}")
            logger.info(f"  Subscriber: {subscriber_name}")
            logger.info(f"  External Destination: {source}")
            logger.info(f"  Message Type: {msg_type}")
            if isinstance(message_data, str):
                preview = message_data[:100] + '...' if len(message_data) > 100 else message_data
                logger.info(f"  Message Preview: {preview}")
            else:
                logger.info(f"  Message Keys: {list(message_data.keys()) if isinstance(message_data, dict) else 'N/A'}")
            logger.info("  Message sent to EXTERNAL broker - DAG subscriber will receive it")
            logger.info("=" * 70)
            
            return jsonify({'success': True, 'message': f'{msg_type} message published successfully'})
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return jsonify({'error': f'Invalid JSON: {str(e)}'}), 400
        except Exception as e:
            logger.error(f"Error publishing message: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return jsonify({'error': str(e)}), 500
    
    def subgraph_control(self, dag_name):
        """
        Control subgraph state (light up / light down).
        
        Expected JSON body:
        {
            "command": "light_up" | "light_down" | "light_up_all" | "light_down_all",
            "subgraph": "subgraph_name",  // For single commands
            "reason": "optional reason"
        }
        """
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return jsonify({'success': False, 'error': 'DAG not found'}), 404
            
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'error': 'No JSON data provided'}), 400
            
            command = data.get('command')
            subgraph_name = data.get('subgraph')
            reason = data.get('reason', 'Manual control via UI')
            
            if command == 'light_up':
                if not subgraph_name:
                    return jsonify({'success': False, 'error': 'Subgraph name required'}), 400
                dag.light_up_subgraph(subgraph_name, reason)
                logger.info(f"Subgraph '{subgraph_name}' in DAG '{dag_name}' activated. Reason: {reason}")
                return jsonify({'success': True, 'message': f'Subgraph {subgraph_name} activated'})
            
            elif command == 'light_down':
                if not subgraph_name:
                    return jsonify({'success': False, 'error': 'Subgraph name required'}), 400
                dag.light_down_subgraph(subgraph_name, reason)
                logger.info(f"Subgraph '{subgraph_name}' in DAG '{dag_name}' suspended. Reason: {reason}")
                return jsonify({'success': True, 'message': f'Subgraph {subgraph_name} suspended'})
            
            elif command == 'light_up_all':
                dag.light_up_all_subgraphs(reason)
                logger.info(f"All subgraphs in DAG '{dag_name}' activated. Reason: {reason}")
                return jsonify({'success': True, 'message': 'All subgraphs activated'})
            
            elif command == 'light_down_all':
                dag.light_down_all_subgraphs(reason)
                logger.info(f"All subgraphs in DAG '{dag_name}' suspended. Reason: {reason}")
                return jsonify({'success': True, 'message': 'All subgraphs suspended'})
            
            else:
                return jsonify({'success': False, 'error': f'Unknown command: {command}'}), 400
                
        except Exception as e:
            logger.error(f"Error controlling subgraph: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def subgraph_status(self, dag_name):
        """Get status of all subgraphs in a DAG."""
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return jsonify({'error': 'DAG not found'}), 404
            
            status = dag.get_subgraph_status()
            return jsonify({
                'dag_name': dag_name,
                'subgraph_count': len(status),
                'subgraphs': status
            })
            
        except Exception as e:
            logger.error(f"Error getting subgraph status: {str(e)}")
            return jsonify({'error': str(e)}), 500
