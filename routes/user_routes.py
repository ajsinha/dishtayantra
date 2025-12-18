"""User management routes module"""
import logging
from flask import render_template, request, redirect, url_for, flash, jsonify, session

logger = logging.getLogger(__name__)


class UserRoutes:
    """Handles user management routes"""
    
    def __init__(self, app, user_registry, admin_required):
        self.app = app
        self.user_registry = user_registry
        self.admin_required = admin_required
        self._register_routes()
    
    def _register_routes(self):
        """Register all user management routes"""
        # View routes
        self.app.add_url_rule('/users', 'user_management', 
                             self.admin_required(self.user_management))
        self.app.add_url_rule('/users/create', 'user_create_page', 
                             self.admin_required(self.user_create_page), 
                             methods=['GET', 'POST'])
        self.app.add_url_rule('/users/edit/<username>', 'user_edit_page', 
                             self.admin_required(self.user_edit_page), 
                             methods=['GET'])
        
        # API routes
        self.app.add_url_rule('/users/api/list', 'users_list', 
                             self.admin_required(self.users_list), 
                             methods=['GET'])
        self.app.add_url_rule('/users/api/reload', 'users_reload', 
                             self.admin_required(self.users_reload), 
                             methods=['POST'])
        self.app.add_url_rule('/users/api/create', 'users_create', 
                             self.admin_required(self.users_create), 
                             methods=['POST'])
        self.app.add_url_rule('/users/api/update', 'users_update', 
                             self.admin_required(self.users_update), 
                             methods=['PUT', 'POST'])
        self.app.add_url_rule('/users/api/delete', 'users_delete', 
                             self.admin_required(self.users_delete), 
                             methods=['DELETE'])
        self.app.add_url_rule('/users/api/stats', 'users_stats', 
                             self.admin_required(self.users_stats), 
                             methods=['GET'])
    
    def user_management(self):
        """User management page"""
        return render_template('user/management.html')
    
    def users_list(self):
        """List all users"""
        try:
            users = self.user_registry.list_all_users(include_passwords=False)

            # Convert to list format with username included
            users_list = []
            for username, user_data in users.items():
                user_info = user_data.copy()
                user_info['username'] = username
                users_list.append(user_info)

            return jsonify({
                'success': True,
                'users': users_list,
                'total': len(users_list)
            })
        except Exception as e:
            logger.error(f"Error listing users: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def users_reload(self):
        """Force reload users from file"""
        try:
            logger.info(f"User registry reload triggered by: {session.get('username', 'unknown')}")
            success = self.user_registry.force_reload()

            if success:
                user_count = self.user_registry.get_user_count()
                logger.info(f"User registry reloaded successfully. Total users: {user_count}")
                return jsonify({
                    'success': True,
                    'message': f'User registry reloaded. Total users: {user_count}'
                })
            else:
                return jsonify({'success': False, 'error': 'Failed to reload user registry'}), 500
        except Exception as e:
            logger.error(f"Error reloading user registry: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def user_create_page(self):
        """User creation page"""
        if request.method == 'GET':
            return render_template('user/create.html')

        # POST - Handle form submission
        try:
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '').strip()
            full_name = request.form.get('full_name', '').strip()

            # Convert roles from form checkboxes
            roles = []
            if request.form.get('role_user'):
                roles.append('user')
            if request.form.get('role_operator'):
                roles.append('operator')
            if request.form.get('role_admin'):
                roles.append('admin')

            # Validation
            if not username:
                flash('Username is required', 'error')
                return redirect(url_for('user_create_page'))

            if not password:
                flash('Password is required', 'error')
                return redirect(url_for('user_create_page'))

            if len(password) < 6:
                flash('Password must be at least 6 characters long', 'error')
                return redirect(url_for('user_create_page'))

            if not roles or len(roles) == 0:
                flash('At least one role is required', 'error')
                return redirect(url_for('user_create_page'))

            if self.user_registry.user_exists(username):
                flash(f'User {username} already exists', 'error')
                return redirect(url_for('user_create_page'))

            # Create user data
            user_data = {
                'password': password,
                'full_name': full_name if full_name else username,
                'roles': roles
            }

            # Create user
            success = self.user_registry.create_user(username, user_data, session.get('username'))

            if success:
                logger.info(f"User {username} created by {session.get('username')}")
                flash(f'User {username} created successfully', 'success')
                return redirect(url_for('user_management'))
            else:
                flash('Failed to create user', 'error')
                return redirect(url_for('user_create_page'))

        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            flash(f'Error creating user: {str(e)}', 'error')
            return redirect(url_for('user_create_page'))
    
    def users_create(self):
        """Create a new user (API endpoint for backward compatibility)"""
        try:
            data = request.get_json()
            username = data.get('username', '').strip()
            password = data.get('password', '').strip()
            full_name = data.get('full_name', '').strip()
            roles = data.get('roles', [])

            # Validation
            if not username:
                return jsonify({'success': False, 'error': 'Username is required'}), 400

            if not password:
                return jsonify({'success': False, 'error': 'Password is required'}), 400

            if not roles or len(roles) == 0:
                return jsonify({'success': False, 'error': 'At least one role is required'}), 400

            if self.user_registry.user_exists(username):
                return jsonify({'success': False, 'error': f'User {username} already exists'}), 400

            # Create user data
            user_data = {
                'password': password,
                'full_name': full_name if full_name else username,
                'roles': roles if roles else ['user']
            }

            # Create user
            success = self.user_registry.create_user(username, user_data, session.get('username'))

            if success:
                logger.info(f"User {username} created by {session.get('username')}")
                return jsonify({'success': True, 'message': f'User {username} created successfully'})
            else:
                return jsonify({'success': False, 'error': 'Failed to create user'}), 500

        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def users_update(self):
        """Update an existing user"""
        try:
            # Handle both JSON (API) and form data (from edit page)
            if request.is_json:
                data = request.get_json()
            else:
                data = request.form.to_dict()
                # Convert roles from form checkboxes
                roles = []
                if request.form.get('role_user') == 'on':
                    roles.append('user')
                if request.form.get('role_operator') == 'on':
                    roles.append('operator')
                if request.form.get('role_admin') == 'on':
                    roles.append('admin')
                data['roles'] = roles

            username = data.get('username', '').strip()
            password = data.get('password', '').strip()
            full_name = data.get('full_name', '').strip()
            roles = data.get('roles', [])

            # Validation
            if not username:
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Username is required'}), 400
                else:
                    flash('Username is required', 'error')
                    return redirect(url_for('user_edit_page', username=username))

            # Protect admin user
            if username.lower() == 'admin':
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Cannot edit the protected admin user'}), 403
                else:
                    flash('Cannot edit the protected admin user', 'error')
                    return redirect(url_for('user_management'))

            if not self.user_registry.user_exists(username):
                if request.is_json:
                    return jsonify({'success': False, 'error': f'User {username} not found'}), 404
                else:
                    flash(f'User {username} not found', 'error')
                    return redirect(url_for('user_management'))

            # Build update data
            user_data = {}
            if password:
                user_data['password'] = password
            if full_name:
                user_data['full_name'] = full_name
            if roles is not None:
                if len(roles) == 0:
                    if request.is_json:
                        return jsonify({'success': False, 'error': 'At least one role is required'}), 400
                    else:
                        flash('At least one role is required', 'error')
                        return redirect(url_for('user_edit_page', username=username))
                user_data['roles'] = roles

            # Prevent removing admin role from the last admin
            if 'roles' in user_data and 'admin' not in user_data['roles']:
                if self.user_registry.has_role(username, 'admin'):
                    # Count total admins
                    all_users = self.user_registry.list_all_users()
                    admin_count = sum(1 for u in all_users.values() if 'admin' in u.get('roles', []))
                    if admin_count <= 1:
                        if request.is_json:
                            return jsonify(
                                {'success': False, 'error': 'Cannot remove admin role from the last admin user'}), 400
                        else:
                            flash('Cannot remove admin role from the last admin user', 'error')
                            return redirect(url_for('user_edit_page', username=username))

            # Update user
            success = self.user_registry.modify_user(username, user_data, session.get('username'))

            if success:
                logger.info(f"User {username} updated by {session.get('username')}")
                if request.is_json:
                    return jsonify({'success': True, 'message': f'User {username} updated successfully'})
                else:
                    flash(f'User {username} updated successfully', 'success')
                    return redirect(url_for('user_management'))
            else:
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Failed to update user'}), 500
                else:
                    flash('Failed to update user', 'error')
                    return redirect(url_for('user_edit_page', username=username))

        except Exception as e:
            logger.error(f"Error updating user: {str(e)}")
            if request.is_json:
                return jsonify({'success': False, 'error': str(e)}), 500
            else:
                flash(f'Error updating user: {str(e)}', 'error')
                return redirect(url_for('user_management'))
    
    def users_delete(self):
        """Delete a user"""
        try:
            data = request.get_json()
            username = data.get('username', '').strip()

            # Validation
            if not username:
                return jsonify({'success': False, 'error': 'Username is required'}), 400

            # Protect admin user
            if username.lower() == 'admin':
                return jsonify({'success': False, 'error': 'Cannot delete the protected admin user'}), 403

            # Prevent self-deletion
            if username == session.get('username'):
                return jsonify({'success': False, 'error': 'Cannot delete your own account'}), 400

            if not self.user_registry.user_exists(username):
                return jsonify({'success': False, 'error': f'User {username} not found'}), 404

            # Delete user (UserRegistry will prevent deleting last admin)
            success = self.user_registry.delete_user(username, session.get('username'))

            if success:
                logger.info(f"User {username} deleted by {session.get('username')}")
                return jsonify({'success': True, 'message': f'User {username} deleted successfully'})
            else:
                return jsonify(
                    {'success': False, 'error': 'Failed to delete user. Cannot delete the last admin user.'}), 400

        except Exception as e:
            logger.error(f"Error deleting user: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def users_stats(self):
        """Get user statistics"""
        try:
            all_users = self.user_registry.list_all_users()

            # Count users by role
            role_counts = {}
            for user_data in all_users.values():
                for role in user_data.get('roles', []):
                    role_counts[role] = role_counts.get(role, 0) + 1

            return jsonify({
                'success': True,
                'stats': {
                    'total_users': len(all_users),
                    'role_counts': role_counts
                }
            })
        except Exception as e:
            logger.error(f"Error getting user stats: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def user_edit_page(self, username):
        """User edit page"""
        try:
            # Check if user exists
            if not self.user_registry.user_exists(username):
                flash(f'User {username} not found', 'error')
                return redirect(url_for('user_management'))

            # Protect admin user
            if username.lower() == 'admin':
                flash('Cannot edit the protected admin user', 'error')
                return redirect(url_for('user_management'))

            # Get user data
            user_data = self.user_registry.get_user(username, include_password=False)

            return render_template('user/edit.html', user_data=user_data)
        except Exception as e:
            logger.error(f"Error loading user edit page: {str(e)}")
            flash(f'Error: {str(e)}', 'error')
            return redirect(url_for('user_management'))
