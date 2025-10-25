#!/usr/bin/env python
"""
Main entry point for DishtaYantra Compute Server
"""

import os
import sys
import logging
from web.app import app, dag_server
from core.properties_configurator import PropertiesConfigurator

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
    logger.info("Starting DishtaYantra Compute Server")


    # Create necessary directories
    directories = ['logs', 'config', 'config/dags']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

    try:
        print("\n✓ Loading application properties...")
        props = PropertiesConfigurator(['config/application.properties'])

        host = props.get('server.host', '0.0.0.0')
        port = props.get_int('server.port', 5002)
        debug = props.get('server.debug', 'False').lower() == 'true'

        # Run Flask app
        app.run(
            app.run(host=host, port=port, debug=debug)
        )
    except KeyboardInterrupt:
        logger.info("Shutting down DishtaYantra Compute Server")
        dag_server.shutdown()
    except Exception as e:
        logger.error(f"Error running server: {str(e)}")
        dag_server.shutdown()
        sys.exit(1)


if __name__ == '__main__':
    main()