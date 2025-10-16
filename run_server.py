#!/usr/bin/env python
"""
Main entry point for DAG Compute Server
"""

import os
import sys
import logging
from web.app import app, dag_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/dagserver.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point"""
    logger.info("Starting DAG Compute Server")

    # Create necessary directories
    directories = ['logs', 'config', 'config/dags']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

    try:
        # Run Flask app
        app.run(
            debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true',
            host=os.environ.get('FLASK_HOST', '0.0.0.0'),
            port=int(os.environ.get('FLASK_PORT', 5000))
        )
    except KeyboardInterrupt:
        logger.info("Shutting down DAG Compute Server")
        dag_server.shutdown()
    except Exception as e:
        logger.error(f"Error running server: {str(e)}")
        dag_server.shutdown()
        sys.exit(1)


if __name__ == '__main__':
    main()