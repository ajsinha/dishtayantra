"""
Non-authenticated routes module - accessible without login

Version: 2.0.0 (FastAPI)
"""
import os
import logging
from fastapi import FastAPI, HTTPException, Request
from markupsafe import Markup

from web.fastapi_compat import render

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
    """Routes accessible without login: about, help pages, user guides,
    research paper.  Markdown documents are rendered just-in-time.

    v2.0.0: FastAPI registration; route names match the legacy Flask
    endpoint names so templates keep working unchanged.  Static help pages
    are generated from a single declarative table instead of one method per
    page (same URLs, names, and templates as before, far less duplication).
    """

    # (url_path, endpoint_name, template) - one row per static help page.
    STATIC_HELP_PAGES = [
        ('/help/getting-started', 'help_getting_started', 'help/getting_started.html'),
        ('/help/dag-configuration', 'help_dag_configuration', 'help/dag_configuration.html'),
        ('/help/pubsub', 'help_pubsub', 'help/pubsub.html'),
        ('/help/calculators', 'help_calculators', 'help/calculators.html'),
        ('/help/transformers', 'help_transformers', 'help/transformers.html'),
        ('/help/time-windows', 'help_time_windows', 'help/time_windows.html'),
        ('/help/autoclone', 'help_autoclone', 'help/autoclone.html'),
        ('/help/dag-designer', 'help_dag_designer', 'help/dag_designer.html'),
        ('/help/cache', 'help_cache', 'help/cache.html'),
        ('/help/sample-dags', 'help_sample_dags', 'help/sample_dags.html'),
        ('/help/api-reference', 'help_api_reference', 'help/api_reference.html'),
        ('/help/glossary', 'help_glossary', 'help/glossary.html'),
        ('/help/free-threading', 'help_free_threading', 'help/parallelism.html'),
        ('/help/parallelism', 'help_parallelism', 'help/parallelism.html'),
        ('/help/py4j-integration', 'help_py4j_integration', 'help/py4j_integration.html'),
        ('/help/pybind11-integration', 'help_pybind11_integration', 'help/pybind11_integration.html'),
        ('/help/rust-integration', 'help_rust_integration', 'help/rust_integration.html'),
        ('/help/rest-integration', 'help_rest_integration', 'help/rest_integration.html'),
        ('/help/architecture', 'help_architecture', 'help/architecture.html'),
        ('/help/lmdb-integration', 'help_lmdb_integration', 'help/lmdb_integration.html'),
        ('/help/kafka-integration', 'help_kafka_integration', 'help/kafka_integration.html'),
        ('/help/rabbitmq-integration', 'help_rabbitmq_integration', 'help/rabbitmq_integration.html'),
        ('/help/activemq-integration', 'help_activemq_integration', 'help/activemq_integration.html'),
        ('/help/redis-integration', 'help_redis_integration', 'help/redis_integration.html'),
        ('/help/tibco-integration', 'help_tibco_integration', 'help/tibco_integration.html'),
        ('/help/ibmmq-integration', 'help_ibmmq_integration', 'help/ibmmq_integration.html'),
        ('/help/inmemory-integration', 'help_inmemory_integration', 'help/inmemory_integration.html'),
        ('/help/subgraph', 'help_subgraph', 'help/subgraph.html'),
        ('/help/prometheus-monitoring', 'help_prometheus_monitoring', 'help/prometheus_monitoring.html'),
        ('/help/worker-pool', 'help_worker_pool', 'help/parallelism.html'),
        ('/help/configuration', 'help_configuration', 'help/configuration.html'),
        # New in v2.0.0
        ('/help/storage', 'help_storage', 'help/storage.html'),
        ('/help/ha', 'help_ha', 'help/ha.html'),
        ('/help/cloud-pubsub', 'help_cloud_pubsub', 'help/cloud_pubsub.html'),
        ('/help/database', 'help_database', 'help/database.html'),
        ('/help/scheduling', 'help_scheduling', 'help/scheduling.html'),
        # Tutorials (v2.2): step-by-step guides with full code
        ('/help/tutorial-1', 'help_tutorial_1', 'help/tutorial_1_basic.html'),
        ('/help/tutorial-2', 'help_tutorial_2', 'help/tutorial_2_pipeline.html'),
        ('/help/tutorial-3', 'help_tutorial_3', 'help/tutorial_3_advanced.html'),
        ('/help/tutorial-4', 'help_tutorial_4', 'help/tutorial_4_enterprise.html'),
        ('/help/tutorial-5', 'help_tutorial_5', 'help/tutorial_5_subgraphs.html'),
        ('/help/tutorial-6', 'help_tutorial_6', 'help/tutorial_6_coordination.html'),
        ('/help/tutorial-7', 'help_tutorial_7', 'help/tutorial_7_worker_pool.html'),
        ('/help/tutorial-8', 'help_tutorial_8', 'help/tutorial_8_jvm.html'),
    ]

    def __init__(self, app: FastAPI):
        self.app = app
        self._register_routes()

    def _register_routes(self):
        """Register all non-authenticated routes."""
        add = self.app.add_api_route
        add('/about', self.about, methods=['GET'], name='about',
            include_in_schema=False)
        add('/help', self.help_index, methods=['GET'], name='help',
            include_in_schema=False)

        for path, name, template in self.STATIC_HELP_PAGES:
            add(path, self._make_static_page_handler(template),
                methods=['GET'], name=name, include_in_schema=False)

        # v1.5.2: User Guides (Markdown) routes
        add('/help/userguides', self.help_userguides, methods=['GET'],
            name='help_userguides', include_in_schema=False)
        add('/help/userguides/{filename:path}', self.help_userguide_view,
            methods=['GET'], name='help_userguide_view',
            include_in_schema=False)

        # v1.7.0: Research Paper route
        add('/help/research', self.help_research, methods=['GET'],
            name='help_research', include_in_schema=False)

    def _make_static_page_handler(self, template: str):
        """Build a handler closure rendering one static help template."""
        def handler(request: Request):
            return render(request, template)
        return handler

    def about(self, request: Request):
        """About page."""
        return render(request, 'about.html')

    def help_index(self, request: Request):
        """Help index page."""
        return render(request, 'help/index.html')

    def help_userguides(self, request: Request):
        """
        List all available user guides (markdown files).
        v1.5.2: Just-in-time markdown rendering.
        """
        docs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                 'docs')
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
                    title = filename.replace('.md', '').replace('-', ' ') \
                        .replace('_', ' ')
                    stat = os.stat(filepath)
                    guide_info = {
                        'filename': filename,
                        'title': title,
                        'size_kb': round(stat.st_size / 1024, 1),
                        'path': f'userguides/{filename}'
                    }
                    lower_title = title.lower()
                    if any(x in lower_title for x in
                           ['kafka', 'rabbit', 'redis', 'activemq', 'tibco',
                            'websphere', 'grpc', 'rest', 'file', 'inmemory',
                            'sql', 'aerospike', 'metronome', 'pubsub',
                            'subscriber', 'publisher', 's3', 'azure', 'gcs',
                            'cloud']):
                        categories['pubsub'].append(guide_info)
                    elif any(x in lower_title for x in
                             ['calculator', 'py4j', 'pybind', 'rust', 'java',
                              'c++']):
                        categories['calculator'].append(guide_info)
                    elif any(x in lower_title for x in
                             ['admin', 'user', 'monitor', 'prometheus',
                              'log']):
                        categories['admin'].append(guide_info)
                    elif any(x in lower_title for x in
                             ['subgraph', 'autoclone', 'cache', 'lmdb',
                              'router', 'packaging', 'resilient', 'storage',
                              'ha', 'database']):
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
                        title = filename.replace('.md', '').replace('-', ' ') \
                            .replace('_', ' ')
                        stat = os.stat(filepath)
                        root_docs.append({
                            'filename': filename,
                            'title': title,
                            'size_kb': round(stat.st_size / 1024, 1),
                            'path': filename
                        })

        return render(request, 'help/userguides.html',
                      guides=guides,
                      categories=categories,
                      root_docs=root_docs,
                      total_count=len(guides) + len(root_docs))

    def help_userguide_view(self, request: Request, filename: str):
        """
        Render a single markdown file as HTML.
        v1.5.2: Just-in-time rendering with syntax highlighting.
        """
        docs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                 'docs')

        # Security: Prevent directory traversal
        filename = os.path.basename(filename)
        if '..' in filename or filename.startswith('/'):
            raise HTTPException(status_code=403, detail='Forbidden')

        # Check if it's in userguides or root docs
        filepath = os.path.join(docs_path, 'userguides', filename)
        if not os.path.exists(filepath):
            filepath = os.path.join(docs_path, filename)

        if not os.path.exists(filepath) or not filepath.endswith('.md'):
            raise HTTPException(status_code=404, detail='Document not found')

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                markdown_content = f.read()

            html_content = render_markdown_to_html(markdown_content)

            # Get title from first heading or filename
            title = filename.replace('.md', '').replace('-', ' ') \
                .replace('_', ' ')
            if markdown_content.startswith('# '):
                first_line = markdown_content.split('\n')[0]
                title = first_line.replace('# ', '').strip()

            return render(request, 'help/userguide_view.html',
                          title=title,
                          filename=filename,
                          content=Markup(html_content))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error rendering markdown file {filename}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500,
                                detail=f'Error rendering {filename}')

    def help_research(self, request: Request):
        """
        Render the research paper markdown file.
        v1.7.0: Just-in-time rendering with syntax highlighting.
        """
        docs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                 'docs')
        filepath = os.path.join(docs_path, 'research',
                                'dishtayantra_paper.md')

        if not os.path.exists(filepath):
            raise HTTPException(status_code=404,
                                detail='Research paper not found')

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                markdown_content = f.read()

            html_content = render_markdown_to_html(markdown_content)
            return render(request, 'help/research.html',
                          title="DishtaYantra Research Paper",
                          content=Markup(html_content))
        except Exception as e:
            logger.error(f"Error rendering research paper: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500,
                                detail='Error rendering research paper')
