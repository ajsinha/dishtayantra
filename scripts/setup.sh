#!/bin/bash

# Setup script for DAG Compute Server

echo "=== DAG Compute Server Setup ==="

# Create directory structure
echo "Creating directory structure..."
mkdir -p core/pubsub
mkdir -p core/calculator
mkdir -p core/transformer
mkdir -p core/dag
mkdir -p web/templates
mkdir -p web/static/css
mkdir -p web/static/js
mkdir -p config/dags
mkdir -p logs

# Create __init__.py files
echo "Creating __init__.py files..."
touch core/__init__.py
touch core/pubsub/__init__.py
touch core/calculator/__init__.py
touch core/transformer/__init__.py
touch core/dag/__init__.py
touch web/__init__.py

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install requirements
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create default users.json if it doesn't exist
if [ ! -f config/users.json ]; then
    echo "Creating default users.json..."
    cat > config/users.json << EOF
{
  "admin": {
    "password": "admin123",
    "role": "admin"
  },
  "user1": {
    "password": "user123",
    "role": "user"
  }
}
EOF
fi

# Make run_server.py executable
chmod +x run_server.py

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To start the server:"
echo "  1. Activate virtual environment: source venv/bin/activate"
echo "  2. Run server: python run_server.py"
echo ""
echo "Or use Docker:"
echo "  docker-compose up -d"
echo ""
echo "Access the web UI at: http://localhost:5000"
echo "Default admin credentials: admin / admin123"
echo ""