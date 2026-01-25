"""
Non-authenticated routes module - accessible without login

Version: 1.6.0
"""
import os
import logging
from flask import render_template, abort, request
from markupsafe import Markup

logger = logging.getLogger(__name__)


def render_markdown_to_html(markdown_text):
    """
    Convert markdown text to HTML with syntax highlighting.
    Uses markdown library with code highlighting extensions.
    """
    try:
        import markdown
        
        # Use simpler extension names that work reliably
        extensions = [
            'tables',           # Table support
            'fenced_code',      # ```code``` blocks
            'codehilite',       # Syntax highlighting
            'toc',              # Table of contents
            'sane_lists',       # Better list handling
            'nl2br',            # Newlines to <br>
        ]
        
        extension_configs = {
            'codehilite': {
                'css_class': 'highlight',
                'linenums': False,
                'guess_lang': True,
            },
            'toc': {
                'permalink': True,
            }
        }
        
        md = markdown.Markdown(extensions=extensions, extension_configs=extension_configs)
        html_content = md.convert(markdown_text)
        return html_content
    except ImportError as e:
        # Fallback: manual conversion including table support
        import re
        
        html = markdown_text
        
        # Process tables first (before other transformations)
        def convert_table(match):
            table_text = match.group(0)
            lines = table_text.strip().split('\n')
            if len(lines) < 2:
                return table_text
            
            result = ['<table>', '<thead>', '<tr>']
            
            # Header row
            header_cells = [cell.strip() for cell in lines[0].split('|') if cell.strip()]
            for cell in header_cells:
                # Convert inline formatting in headers
                cell = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', cell)
                cell = re.sub(r'`([^`]+)`', r'<code>\1</code>', cell)
                result.append(f'<th>{cell}</th>')
            result.extend(['</tr>', '</thead>', '<tbody>'])
            
            # Data rows (skip separator line)
            for line in lines[2:]:
                if line.strip() and not re.match(r'^[\|\-\:\s]+$', line):
                    result.append('<tr>')
                    cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                    for cell in cells:
                        # Convert inline formatting
                        cell = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', cell)
                        cell = re.sub(r'`([^`]+)`', r'<code>\1</code>', cell)
                        result.append(f'<td>{cell}</td>')
                    result.append('</tr>')
            
            result.extend(['</tbody>', '</table>'])
            return '\n'.join(result)
        
        # Match markdown tables (lines starting with |)
        html = re.sub(
            r'(?:^\|.+\|$\n?)+',
            convert_table,
            html,
            flags=re.MULTILINE
        )
        
        # Convert code blocks
        html = re.sub(r'```(\w+)?\n(.*?)```', r'<pre><code class="language-\1">\2</code></pre>', html, flags=re.DOTALL)
        
        # Convert inline code (but not inside <code> tags already)
        html = re.sub(r'`([^`]+)`', r'<code>\1</code>', html)
        
        # Convert headers
        html = re.sub(r'^#### (.+)$', r'<h4>\1</h4>', html, flags=re.MULTILINE)
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        
        # Convert bold and italic
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
        html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)
        
        # Convert horizontal rules
        html = re.sub(r'^---+$', r'<hr>', html, flags=re.MULTILINE)
        
        # Convert line breaks to paragraphs
        html = re.sub(r'\n\n+', r'</p>\n<p>', html)
        html = f'<p>{html}</p>'
        
        # Clean up empty paragraphs
        html = re.sub(r'<p>\s*</p>', '', html)
        html = re.sub(r'<p>\s*(<table>)', r'\1', html)
        html = re.sub(r'(</table>)\s*</p>', r'\1', html)
        html = re.sub(r'<p>\s*(<h[1-6]>)', r'\1', html)
        html = re.sub(r'(</h[1-6]>)\s*</p>', r'\1', html)
        html = re.sub(r'<p>\s*(<pre>)', r'\1', html)
        html = re.sub(r'(</pre>)\s*</p>', r'\1', html)
        html = re.sub(r'<p>\s*(<hr>)', r'\1', html)
        
        return html


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
        self.app.add_url_rule('/help/free-threading', 'help_free_threading', self.help_parallelism)
        self.app.add_url_rule('/help/parallelism', 'help_parallelism', self.help_parallelism)
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
        self.app.add_url_rule('/help/worker-pool', 'help_worker_pool', self.help_parallelism)
        self.app.add_url_rule('/help/configuration', 'help_configuration', self.help_configuration)
        
        # v1.5.2: User Guides (Markdown) routes
        self.app.add_url_rule('/help/userguides', 'help_userguides', self.help_userguides)
        self.app.add_url_rule('/help/userguides/<path:filename>', 'help_userguide_view', self.help_userguide_view)
        
        # v1.7.0: Research Paper route
        self.app.add_url_rule('/help/research', 'help_research', self.help_research)
    
    def about(self):
        """About page"""
        return render_template('about.html')
    
    def help_index(self):
        """Help index page"""
        return render_template('help/index.html')
    
    def help_userguides(self):
        """
        List all available user guides (markdown files).
        v1.5.2: Just-in-time markdown rendering.
        """
        docs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs')
        userguides_path = os.path.join(docs_path, 'userguides')
        
        guides = []
        categories = {
            'pubsub': [],      # PubSub integrations
            'calculator': [],  # Calculator integrations
            'feature': [],     # Core features
            'admin': [],       # Admin/System
            'other': []        # Other guides
        }
        
        # Scan for markdown files
        if os.path.exists(userguides_path):
            for filename in sorted(os.listdir(userguides_path)):
                if filename.endswith('.md'):
                    filepath = os.path.join(userguides_path, filename)
                    title = filename.replace('.md', '').replace('-', ' ').replace('_', ' ')
                    
                    # Get file size and modification time
                    stat = os.stat(filepath)
                    size_kb = stat.st_size / 1024
                    
                    guide_info = {
                        'filename': filename,
                        'title': title,
                        'size_kb': round(size_kb, 1),
                        'path': f'userguides/{filename}'
                    }
                    
                    # Categorize
                    lower_title = title.lower()
                    if any(x in lower_title for x in ['kafka', 'rabbit', 'redis', 'activemq', 'tibco', 'websphere', 'grpc', 'rest', 'file', 'inmemory', 'sql', 'aerospike', 'metronome', 'pubsub', 'subscriber', 'publisher']):
                        categories['pubsub'].append(guide_info)
                    elif any(x in lower_title for x in ['calculator', 'py4j', 'pybind', 'rust', 'java', 'c++']):
                        categories['calculator'].append(guide_info)
                    elif any(x in lower_title for x in ['admin', 'user', 'monitor', 'prometheus', 'log']):
                        categories['admin'].append(guide_info)
                    elif any(x in lower_title for x in ['subgraph', 'autoclone', 'cache', 'lmdb', 'router', 'packaging', 'resilient']):
                        categories['feature'].append(guide_info)
                    else:
                        categories['other'].append(guide_info)
                    
                    guides.append(guide_info)
        
        # Also check docs root for ARCHITECTURE.md etc
        root_docs = []
        if os.path.exists(docs_path):
            for filename in sorted(os.listdir(docs_path)):
                if filename.endswith('.md'):
                    filepath = os.path.join(docs_path, filename)
                    if os.path.isfile(filepath):
                        title = filename.replace('.md', '').replace('-', ' ').replace('_', ' ')
                        stat = os.stat(filepath)
                        root_docs.append({
                            'filename': filename,
                            'title': title,
                            'size_kb': round(stat.st_size / 1024, 1),
                            'path': filename
                        })
        
        return render_template(
            'help/userguides.html',
            guides=guides,
            categories=categories,
            root_docs=root_docs,
            total_count=len(guides) + len(root_docs)
        )
    
    def help_userguide_view(self, filename):
        """
        Render a single markdown file as HTML.
        v1.5.2: Just-in-time rendering with syntax highlighting.
        """
        docs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs')
        
        # Security: Prevent directory traversal
        filename = os.path.basename(filename)
        if '..' in filename or filename.startswith('/'):
            abort(403)
        
        # Check if it's in userguides or root docs
        filepath = os.path.join(docs_path, 'userguides', filename)
        if not os.path.exists(filepath):
            filepath = os.path.join(docs_path, filename)
        
        if not os.path.exists(filepath) or not filepath.endswith('.md'):
            abort(404)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
            
            # Convert markdown to HTML
            html_content = render_markdown_to_html(markdown_content)
            
            # Get title from first heading or filename
            title = filename.replace('.md', '').replace('-', ' ').replace('_', ' ')
            if markdown_content.startswith('# '):
                first_line = markdown_content.split('\n')[0]
                title = first_line.replace('# ', '').strip()
            
            return render_template(
                'help/userguide_view.html',
                title=title,
                filename=filename,
                content=Markup(html_content)
            )
        except Exception as e:
            logger.error(f"Error rendering markdown file {filename}: {e}")
            abort(500)
    
    def help_research(self):
        """
        Render the research paper markdown file.
        v1.7.0: Just-in-time rendering with syntax highlighting.
        """
        docs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs')
        filepath = os.path.join(docs_path, 'research', 'dishtayantra_paper.md')
        
        if not os.path.exists(filepath):
            abort(404)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                markdown_content = f.read()
            
            # Convert markdown to HTML
            html_content = render_markdown_to_html(markdown_content)
            
            # Title from the paper
            title = "DishtaYantra Research Paper"
            
            return render_template(
                'help/research.html',
                title=title,
                content=Markup(html_content)
            )
        except Exception as e:
            logger.error(f"Error rendering research paper: {e}")
            abort(500)
    
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
    
    def help_parallelism(self):
        """Parallelism Guide - Worker Pool & Free-Threading help page (v1.5.2)"""
        return render_template('help/parallelism.html')
    
    def help_free_threading(self):
        """Free-threading Python help page - redirects to parallelism guide"""
        return render_template('help/parallelism.html')
    
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
    
    def help_worker_pool(self):
        """Worker Pool & DAG Affinity help page - redirects to parallelism guide (v1.5.2)"""
        return render_template('help/parallelism.html')
    
    def help_configuration(self):
        """Configuration files reference (v1.5.2)"""
        return render_template('help/configuration.html')
