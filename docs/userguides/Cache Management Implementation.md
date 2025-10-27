# Cache Management Implementation

## © 2025-2030 Ashutosh Sinha

This implementation adds comprehensive cache management functionality to your DishtaYantra Compute Server application, allowing users to interact with the InMemoryRedis cache through a web interface.

## Files Created/Modified

1. **app.py** - Updated Flask application with cache management routes
2. **cache_management.html** - Cache management page template
3. **base.html** - Updated base template with cache navigation link

## Features Implemented

### For All Users

#### 1. Query Cache
- Search for cache entries using pattern matching
- Supports wildcard patterns (e.g., `user:*`, `*:session`, `*`)
- View key, value (truncated), type, and TTL information
- Click on any value to view the full content in a modal

#### 2. Cache Statistics
- Real-time statistics dashboard showing:
  - Total number of keys
  - Keys with TTL
  - Keys without TTL
  - Refresh button to update stats

#### 3. Pagination
- **Rows per page dropdown** (top left of table):
  - Options: 10, 50, 100, All
  - Dynamically adjusts table display
- **Pagination links** (bottom right):
  - Previous/Next buttons
  - Page numbers with ellipsis for large result sets
  - Shows current page range (e.g., "Showing 1-10 of 45 entries")

### For Admin Users Only

#### 1. Create Entry
- Modal form to create new cache entries
- Fields:
  - Key (required)
  - Value (required)
  - TTL in seconds (optional, -1 for no expiry)
- Validates input and provides feedback

#### 2. Delete Entry
- Delete individual cache entries
- Confirmation dialog before deletion
- Immediate feedback on success/failure

#### 3. Modify TTL
- Update TTL for existing entries
- Modal shows current TTL
- Set new TTL in seconds:
  - Positive number: Set expiry
  - -1: Remove expiry (persist key)

#### 4. Clear Entire Cache
- **WARNING**: Deletes ALL cache entries
- Double confirmation dialog
- Useful for testing or reset scenarios

#### 5. Download Cache
- Export entire cache as JSON file
- Includes:
  - All keys and values
  - Key types
  - TTL information
- Downloaded file named: `cache_export_<timestamp>.json`

## API Endpoints

### All Users
- `GET /cache` - Display cache management page
- `GET /cache/api/query` - Query cache entries (supports pattern, page, per_page)
- `GET /cache/api/stats` - Get cache statistics

### Admin Only
- `POST /cache/api/create` - Create new cache entry
- `DELETE /cache/api/delete` - Delete cache entry
- `PUT /cache/api/ttl` - Update entry TTL
- `POST /cache/api/clear` - Clear entire cache
- `GET /cache/api/download` - Download cache as JSON

## Installation Instructions

### Step 1: Replace/Update Files

1. **Replace app.py**:
   ```bash
   cp app.py /path/to/your/project/app.py
   ```

2. **Add cache_management.html**:
   ```bash
   cp cache_management.html /path/to/your/project/templates/cache_management.html
   ```

3. **Update base.html**:
   ```bash
   cp base.html /path/to/your/project/templates/base.html
   ```

### Step 2: Ensure Dependencies

Make sure you have these Python packages installed:

```bash
pip install flask
```

The `inmemory_redisclone` module should already be in your project.

### Step 3: Update Redis Instance Reference

**IMPORTANT**: In `app.py`, the code creates a new `InMemoryRedisClone()` instance. You need to ensure this is the SAME instance used by your DAG nodes.

If your DAG server already has a redis instance, update line 49 in app.py:

```python
# Option 1: If dag_server has a redis instance
redis_cache = dag_server.redis_instance  # Update based on your implementation

# Option 2: If you have a global redis instance
from your_module import redis_instance
redis_cache = redis_instance

# Option 3: Current implementation (creates new instance)
redis_cache = InMemoryRedisClone()
```

### Step 4: Run the Application

```bash
python app.py
```

The application will start on `http://0.0.0.0:5000`

## Usage Guide

### Accessing Cache Management

1. Log in to the application
2. Click on **"Cache"** in the navigation bar
3. You'll see the cache management interface

### Searching for Keys

1. Enter a pattern in the search box:
   - `*` - All keys
   - `user:*` - All keys starting with "user:"
   - `*:session` - All keys ending with ":session"
   - `cache:user:*` - Keys matching this pattern
2. Click **Search** or press Enter
3. Results will display in the table below

### Changing Rows Per Page

1. Use the dropdown in the top-left of the table
2. Select: 10, 50, 100, or All
3. Table updates automatically

### Viewing Full Values

1. Click on any value cell in the table
2. A modal will open showing:
   - Full key name
   - Complete value
   - Key type
   - TTL information
3. Use **"Copy Value"** button to copy to clipboard

### Admin Operations

#### Creating Entries
1. Click **"Create Entry"** button
2. Fill in the form:
   - Key: Unique identifier
   - Value: String value to store
   - TTL: Optional, leave empty for no expiry
3. Click **Create**

#### Updating TTL
1. Click the clock icon (⏰) next to an entry
2. Enter new TTL:
   - Positive number: Set expiry in seconds
   - -1: Remove expiry
3. Click **Update**

#### Deleting Entries
1. Click the trash icon (🗑️) next to an entry
2. Confirm deletion
3. Entry is immediately removed

#### Downloading Cache
1. Click **"Download Cache"** button
2. JSON file will download with all cache data
3. File can be imported or analyzed externally

#### Clearing Cache
1. Click **"Clear Entire Cache"** button
2. Confirm the warning dialog
3. ALL cache entries will be deleted

## Pattern Matching Examples

### Basic Patterns
- `*` - Match all keys
- `user:*` - All keys starting with "user:"
- `*:token` - All keys ending with ":token"
- `cache:*:data` - Keys with "cache:" prefix and ":data" suffix

### Complex Patterns
- `user:[0-9]*` - Would need custom logic (not directly supported)
- Multiple patterns require separate searches

## Security Considerations

1. **Authentication**: All routes require login
2. **Authorization**: Admin operations require admin role
3. **Input Validation**: All inputs are validated server-side
4. **Confirmation Dialogs**: Destructive operations require confirmation
5. **Logging**: All admin operations are logged

## Testing

### Test as Regular User
1. Login as: `user1` / `user123`
2. Should see:
   - Query functionality
   - View statistics
   - View full values
3. Should NOT see:
   - Create, Delete, Update TTL buttons
   - Clear Cache button
   - Download Cache button

### Test as Admin
1. Login as: `admin` / `admin123`
2. Should see all features
3. Test each operation:
   - Create test entries
   - Update TTL
   - Delete entries
   - Download cache
   - Clear cache

## Troubleshooting

### Issue: "redis_cache not found"
**Solution**: Ensure the redis instance is properly initialized in app.py

### Issue: No entries showing
**Solution**: 
- Check if cache has any entries: `redis_cache.keys('*')`
- Try searching with pattern `*`
- Create a test entry using admin controls

### Issue: Pagination not working
**Solution**: 
- Check browser console for JavaScript errors
- Ensure jQuery and Bootstrap JS are loaded

### Issue: Admin buttons not showing
**Solution**: 
- Verify user has 'admin' role in users.json
- Check session contains admin role: `'admin' in session.get('roles', [])`

## Performance Considerations

1. **Large Datasets**: Use specific patterns instead of `*` for better performance
2. **Pagination**: Adjust "rows per page" based on result size
3. **Auto-refresh**: Stats refresh on each search operation
4. **Full Values**: Only loaded when clicked (not in initial query)

## Future Enhancements

Potential improvements for future versions:

1. **Export/Import**: Import cache from JSON file
2. **Bulk Operations**: Delete multiple entries at once
3. **Key Analysis**: Show key usage patterns and statistics
4. **Real-time Updates**: WebSocket for live cache monitoring
5. **Search History**: Remember recent search patterns
6. **Filters**: Filter by TTL, type, or value content
7. **Batch Upload**: Upload multiple entries from CSV/JSON

## API Response Formats

### Query Response
```json
{
  "success": true,
  "results": [
    {
      "key": "user:123",
      "value": "truncated...",
      "full_value": "complete value",
      "type": "string",
      "ttl": 3600,
      "ttl_display": "3600s"
    }
  ],
  "total": 45,
  "page": 1,
  "per_page": 10,
  "total_pages": 5
}
```

### Stats Response
```json
{
  "success": true,
  "stats": {
    "total_keys": 100,
    "keys_with_ttl": 75,
    "keys_without_ttl": 25,
    "types": {
      "string": 90,
      "hash": 10
    }
  }
}
```

## License

Copyright © 2025 - 2030 Ashutosh Sinha

## Support

For issues or questions, please refer to the main project documentation or contact the development team.

## Copyright Notice

© 2025 - 2030 Ashutosh Sinha.

All rights reserved. No part of this publication may be reproduced, distributed, or transmitted in any form or by any means, including photocopying, recording, or other electronic or mechanical methods, without the prior written permission of the publisher, except in the case of brief quotations embodied in critical reviews and certain other noncommercial uses permitted by copyright law.
