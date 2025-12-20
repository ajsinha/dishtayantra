"""
Non-authenticated routes module - accessible without login

Version: 1.5.0
"""
from flask import render_template


class NoAuthRoutes:
    """Handles routes that don't require authentication"""
    
    def __init__(self, app):
        self.app = app
        self._register_routes()
    
    def _register_routes(self):
        """Register all non-authenticated routes"""
        self.app.add_url_rule('/about', 'about', self.about)
        self.app.add_url_rule('/help', 'help', self.help_index)
        self.app.add_url_rule('/help/getting-started', 'help_getting_started', self.help_getting_started)
        self.app.add_url_rule('/help/dag-configuration', 'help_dag_configuration', self.help_dag_configuration)
        self.app.add_url_rule('/help/pubsub', 'help_pubsub', self.help_pubsub)
        self.app.add_url_rule('/help/calculators', 'help_calculators', self.help_calculators)
        self.app.add_url_rule('/help/transformers', 'help_transformers', self.help_transformers)
        self.app.add_url_rule('/help/time-windows', 'help_time_windows', self.help_time_windows)
        self.app.add_url_rule('/help/autoclone', 'help_autoclone', self.help_autoclone)
        self.app.add_url_rule('/help/dag-designer', 'help_dag_designer', self.help_dag_designer)
        self.app.add_url_rule('/help/cache', 'help_cache', self.help_cache)
        self.app.add_url_rule('/help/sample-dags', 'help_sample_dags', self.help_sample_dags)
        self.app.add_url_rule('/help/api-reference', 'help_api_reference', self.help_api_reference)
        self.app.add_url_rule('/help/glossary', 'help_glossary', self.help_glossary)
        self.app.add_url_rule('/help/free-threading', 'help_free_threading', self.help_free_threading)
        self.app.add_url_rule('/help/py4j-integration', 'help_py4j_integration', self.help_py4j_integration)
        self.app.add_url_rule('/help/pybind11-integration', 'help_pybind11_integration', self.help_pybind11_integration)
        self.app.add_url_rule('/help/rust-integration', 'help_rust_integration', self.help_rust_integration)
        self.app.add_url_rule('/help/rest-integration', 'help_rest_integration', self.help_rest_integration)
        self.app.add_url_rule('/help/architecture', 'help_architecture', self.help_architecture)
        self.app.add_url_rule('/help/lmdb-integration', 'help_lmdb_integration', self.help_lmdb_integration)
        self.app.add_url_rule('/help/kafka-integration', 'help_kafka_integration', self.help_kafka_integration)
        self.app.add_url_rule('/help/rabbitmq-integration', 'help_rabbitmq_integration', self.help_rabbitmq_integration)
        self.app.add_url_rule('/help/activemq-integration', 'help_activemq_integration', self.help_activemq_integration)
        self.app.add_url_rule('/help/redis-integration', 'help_redis_integration', self.help_redis_integration)
        self.app.add_url_rule('/help/tibco-integration', 'help_tibco_integration', self.help_tibco_integration)
        self.app.add_url_rule('/help/ibmmq-integration', 'help_ibmmq_integration', self.help_ibmmq_integration)
        self.app.add_url_rule('/help/inmemory-integration', 'help_inmemory_integration', self.help_inmemory_integration)
        self.app.add_url_rule('/help/subgraph', 'help_subgraph', self.help_subgraph)
        self.app.add_url_rule('/help/prometheus-monitoring', 'help_prometheus_monitoring', self.help_prometheus_monitoring)
    
    def about(self):
        """About page"""
        return render_template('about.html')
    
    def help_index(self):
        """Help index page"""
        return render_template('help/index.html')
    
    def help_getting_started(self):
        """Getting started help page"""
        return render_template('help/getting_started.html')
    
    def help_dag_configuration(self):
        """DAG configuration help page"""
        return render_template('help/dag_configuration.html')
    
    def help_pubsub(self):
        """Publishers and Subscribers help page"""
        return render_template('help/pubsub.html')
    
    def help_calculators(self):
        """Calculators help page"""
        return render_template('help/calculators.html')
    
    def help_transformers(self):
        """Transformers help page"""
        return render_template('help/transformers.html')
    
    def help_time_windows(self):
        """Time windows help page"""
        return render_template('help/time_windows.html')
    
    def help_autoclone(self):
        """AutoClone feature help page"""
        return render_template('help/autoclone.html')
    
    def help_dag_designer(self):
        """DAG Designer help page"""
        return render_template('help/dag_designer.html')
    
    def help_cache(self):
        """Cache management help page"""
        return render_template('help/cache.html')
    
    def help_sample_dags(self):
        """Sample DAGs help page"""
        return render_template('help/sample_dags.html')
    
    def help_api_reference(self):
        """API Reference help page"""
        return render_template('help/api_reference.html')
    
    def help_glossary(self):
        """Glossary help page"""
        return render_template('help/glossary.html')
    
    def help_free_threading(self):
        """Free-threading Python help page"""
        return render_template('help/free_threading.html')
    
    def help_py4j_integration(self):
        """Py4J Java integration help page"""
        return render_template('help/py4j_integration.html')
    
    def help_pybind11_integration(self):
        """pybind11 C++ integration help page"""
        return render_template('help/pybind11_integration.html')
    
    def help_rust_integration(self):
        """Rust PyO3 integration help page"""
        return render_template('help/rust_integration.html')
    
    def help_rest_integration(self):
        """REST API calculator integration help page"""
        return render_template('help/rest_integration.html')
    
    def help_architecture(self):
        """System architecture help page"""
        return render_template('help/architecture.html')
    
    def help_lmdb_integration(self):
        """LMDB zero-copy data exchange help page"""
        return render_template('help/lmdb_integration.html')
    
    def help_kafka_integration(self):
        """Kafka integration with dual library support help page"""
        return render_template('help/kafka_integration.html')
    
    def help_rabbitmq_integration(self):
        """RabbitMQ integration help page"""
        return render_template('help/rabbitmq_integration.html')
    
    def help_activemq_integration(self):
        """ActiveMQ integration help page"""
        return render_template('help/activemq_integration.html')
    
    def help_redis_integration(self):
        """Redis integration help page"""
        return render_template('help/redis_integration.html')
    
    def help_tibco_integration(self):
        """TIBCO EMS integration help page"""
        return render_template('help/tibco_integration.html')
    
    def help_ibmmq_integration(self):
        """IBM MQ integration help page"""
        return render_template('help/ibmmq_integration.html')
    
    def help_inmemory_integration(self):
        """In-Memory Pub/Sub integration help page"""
        return render_template('help/inmemory_integration.html')
    
    def help_subgraph(self):
        """Subgraph feature help page"""
        return render_template('help/subgraph.html')
    
    def help_prometheus_monitoring(self):
        """Prometheus monitoring integration help page"""
        return render_template('help/prometheus_monitoring.html')
