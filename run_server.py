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

        # Load external module paths and add to sys.path for dynamic imports
        external_module_paths = props.get_values_by_pattern(r'^external\.module\.path\.')

        if external_module_paths:
            print(f"\n✓ Loading external module paths...")
            for module_path in external_module_paths:
                # Expand environment variables and resolve path
                resolved_path = os.path.expandvars(module_path)
                resolved_path = os.path.expanduser(resolved_path)

                # Convert to absolute path if relative
                if not os.path.isabs(resolved_path):
                    resolved_path = os.path.abspath(resolved_path)

                # Add to sys.path if it exists and not already present
                if os.path.exists(resolved_path):
                    if resolved_path not in sys.path:
                        sys.path.insert(0, resolved_path)
                        logger.info(f"Added to Python path: {resolved_path}")
                        print(f"  • {resolved_path}")
                    else:
                        logger.debug(f"Path already in sys.path: {resolved_path}")
                else:
                    logger.warning(f"External module path does not exist: {resolved_path}")
                    print(f"  ⚠ Warning: Path does not exist: {resolved_path}")

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