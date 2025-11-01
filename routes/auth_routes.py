"""Authentication routes module"""
from flask import render_template, request, redirect, url_for, session, flash
from functools import wraps


class AuthRoutes:
    """Handles authentication-related routes"""
    
    def __init__(self, app, user_registry):
        self.app = app
        self.user_registry = user_registry
        self._register_routes()
    
    def login_required(self, f):
        """Decorator to require login for routes"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'username' not in session:
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated_function
    
    def admin_required(self, f):
        """Decorator to require admin role for routes"""
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'username' not in session:
                return redirect(url_for('login'))
            # Check if user has admin role using UserRegistry
            if not self.user_registry.has_role(session.get('username'), 'admin'):
                flash('Admin access required', 'error')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    
    def _register_routes(self):
        """Register all authentication routes"""
        self.app.add_url_rule('/', 'index', self.index)
        self.app.add_url_rule('/login', 'login', self.login, methods=['GET', 'POST'])
        self.app.add_url_rule('/logout', 'logout', self.logout)
    
    def index(self):
        """Root route - redirects to dashboard or login"""
        if 'username' in session:
            return redirect(url_for('dashboard'))
        return redirect(url_for('login'))
    
    def login(self):
        """Login page and authentication"""
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')

            # Authenticate using UserRegistry
            user_data = self.user_registry.authenticate(username, password)
            if user_data:
                session['username'] = username
                session['full_name'] = user_data.get('full_name', username)
                session['roles'] = user_data.get('roles', ['user'])
                flash('Login successful', 'success')
                return redirect(url_for('dashboard'))
            else:
                flash('Invalid username or password', 'error')

        return render_template('login.html')
    
    def logout(self):
        """Logout and clear session"""
        session.clear()
        flash('Logged out successfully', 'success')
        return redirect(url_for('login'))
