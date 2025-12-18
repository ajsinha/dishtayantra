"""DAG management routes module"""
import json
import logging
from flask import render_template, request, redirect, url_for, flash, jsonify

logger = logging.getLogger(__name__)


class DAGRoutes:
    """Handles DAG management routes"""
    
    def __init__(self, app, dag_server, admin_required):
        self.app = app
        self.dag_server = dag_server
        self.admin_required = admin_required
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
        """Start a DAG"""
        try:
            self.dag_server.start(dag_name)
            flash(f'DAG {dag_name} started', 'success')
        except Exception as e:
            logger.error(f"Error starting DAG: {str(e)}")
            flash(f'Error starting DAG: {str(e)}', 'error')

        return redirect(url_for('dashboard'))
    
    def stop_dag(self, dag_name):
        """Stop a DAG"""
        try:
            self.dag_server.stop(dag_name)
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
        """Display the publish message page (GET) or handle message submission (POST)"""
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
            if not message:
                return jsonify({'error': 'No message provided'}), 400

            message_data = json.loads(message)

            # Get the subscriber from DAG
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return jsonify({'error': 'DAG not found'}), 404

            subscriber = dag.subscribers.get(subscriber_name)
            if not subscriber:
                return jsonify({'error': 'Subscriber not found'}), 404

            # Check if subscriber type supports publishing
            source = subscriber.source
            if not any(prefix in source for prefix in ['mem://', 'kafka://', 'redischannel://', 'activemq://']):
                return jsonify({'error': 'Subscriber type does not support publishing'}), 400

            # Create a temporary publisher to the same source
            from core.pubsub.pubsubfactory import create_publisher
            config = subscriber.config.copy()
            config['destination'] = source

            temp_publisher = create_publisher(f'temp_pub_{subscriber_name}', config)
            temp_publisher.publish(message_data)
            temp_publisher.stop()

            return jsonify({'success': True, 'message': 'Message published successfully'})
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return jsonify({'error': f'Invalid JSON: {str(e)}'}), 400
        except Exception as e:
            logger.error(f"Error publishing message: {str(e)}")
            return jsonify({'error': str(e)}), 500
