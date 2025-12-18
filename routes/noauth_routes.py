"""Non-authenticated routes module - accessible without login"""
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
