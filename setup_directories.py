#!/usr/bin/env python
"""
Setup script to create all necessary directories and default files
"""

import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_directories():
    """Create all necessary directories"""
    directories = [
        'core',
        'core/pubsub',
        'core/calculator',
        'core/transformer',
        'core/dag',
        'web',
        'web/templates',
        'web/static',
        'web/static/css',
        'web/static/js',
        'config',
        'config/dags',
        'logs'
    ]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")
        else:
            logger.info(f"Directory already exists: {directory}")


def create_init_files():
    """Create __init__.py files"""
    init_files = [
        'core/__init__.py',
        'core/pubsub/__init__.py',
        'core/calculator/__init__.py',
        'core/transformer/__init__.py',
        'core/dag/__init__.py',
        'web/__init__.py'
    ]

    for init_file in init_files:
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                module_name = os.path.dirname(init_file).replace('/', '.')
                f.write(f'"""{module_name} module"""\n')
            logger.info(f"Created: {init_file}")
        else:
            logger.info(f"Already exists: {init_file}")


def create_users_config():
    """Create users.json if it doesn't exist"""
    users_file = 'config/users.json'

    if not os.path.exists(users_file):
        users = {
            "admin": {
                "password": "admin123",
                "role": "admin"
            },
            "user1": {
                "password": "user123",
                "role": "user"
            },
            "operator": {
                "password": "operator123",
                "role": "user"
            }
        }

        with open(users_file, 'w') as f:
            json.dump(users, f, indent=2)
        logger.info(f"Created: {users_file}")
    else:
        logger.info(f"Already exists: {users_file}")


def create_sample_dag():
    """Create a simple sample DAG configuration"""
    dag_file = 'config/dags/simple_inmemory_dag.json'

    if not os.path.exists(dag_file):
        dag_config = {
            "name": "simple_inmemory_pipeline",
            "start_time": None,
            "end_time": None,
            "subscribers": [
                {
                    "name": "input_sub",
                    "config": {
                        "source": "mem://queue/input_queue",
                        "max_depth": 1000
                    }
                }
            ],
            "publishers": [
                {
                    "name": "output_pub",
                    "config": {
                        "destination": "mem://queue/output_queue"
                    }
                }
            ],
            "calculators": [
                {
                    "name": "defaults_calculator",
                    "type": "ApplyDefaultsCalculator",
                    "config": {
                        "defaults": {
                            "status": "processed",
                            "timestamp": None
                        }
                    }
                }
            ],
            "transformers": [],
            "nodes": [
                {
                    "name": "input_node",
                    "type": "SubscriptionNode",
                    "config": {},
                    "subscriber": "input_sub"
                },
                {
                    "name": "process_node",
                    "type": "CalculationNode",
                    "config": {},
                    "calculator": "defaults_calculator"
                },
                {
                    "name": "output_node",
                    "type": "PublicationNode",
                    "config": {},
                    "publishers": ["output_pub"]
                }
            ],
            "edges": [
                {
                    "from_node": "input_node",
                    "to_node": "process_node"
                },
                {
                    "from_node": "process_node",
                    "to_node": "output_node"
                }
            ]
        }

        with open(dag_file, 'w') as f:
            json.dump(dag_config, f, indent=2)
        logger.info(f"Created: {dag_file}")
    else:
        logger.info(f"Already exists: {dag_file}")


def main():
    """Main setup function"""
    logger.info("=" * 60)
    logger.info("Starting DishtaYantra Compute Server Setup")
    logger.info("=" * 60)

    logger.info("\n1. Creating directories...")
    create_directories()

    logger.info("\n2. Creating __init__.py files...")
    create_init_files()

    logger.info("\n3. Creating users configuration...")
    create_users_config()

    logger.info("\n4. Creating sample DAG configuration...")
    create_sample_dag()

    logger.info("\n" + "=" * 60)
    logger.info("Setup completed successfully!")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. Install dependencies: pip install -r requirements.txt")
    logger.info("2. Run the server: python run_server.py")
    logger.info("3. Access web UI: http://localhost:5000")
    logger.info("4. Login with: admin / admin123")
    logger.info("\n")


if __name__ == '__main__':
    main()