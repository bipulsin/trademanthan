# Backend Control Scripts with Timeout Protection

## Problem Solved

Previously, backend start/stop/check commands would hang indefinitely, blocking the agent console. All operations now have strict timeout limits (10-20 seconds maximum).

## Scripts Created

### 1. `backend/scripts/backend_control.sh` (Server-side)
Server-side script that handles backend operations with built-in timeouts.

**Usage:**
```bash
# On EC2 server
bash /home/ubuntu/trademanthan/backend/scripts/backend_control.sh [start|stop|restart|status|check]
```

**Features:**
- Non-blocking operations
- Health checks with timeouts
- Proper process management
- Status reporting

### 2. `scripts/remote_backend_control.sh` (Local wrapper)
Local script that wraps SSH commands with timeout protection.

**Usage:**
```bash
# From local machine
./scripts/remote_backend_control.sh [start|stop|restart|status|check]
```

**Features:**
- 10-second timeout for status/check operations
- 20-second timeout for restart operations
- Automatic SSH connection management
- Prevents hanging

### 3. `scripts/deploy_with_timeout.sh` (Full deployment)
Complete deployment script with timeout protection for all operations.

**Usage:**
```bash
./scripts/deploy_with_timeout.sh
```

**Steps:**
1. Push to GitHub (with timeout)
2. Pull on server (15s timeout)
3. Restart backend (20s timeout)

## Timeout Limits

| Operation | Timeout | Reason |
|-----------|---------|--------|
| Status/Check | 10s | Quick operations |
| Start/Stop | 10s | Should complete quickly |
| Restart | 20s | Needs time to stop + start |
| Deployment | 15s per step | Multiple operations |

## Examples

### Check Backend Status
```bash
./scripts/remote_backend_control.sh status
# Output: ✅ Backend is running (PID: 12345)
#         ✅ Backend is healthy
# Completes in < 2 seconds
```

### Restart Backend
```bash
./scripts/remote_backend_control.sh restart
# Output: Stopping backend...
#         ✅ Backend stopped
#         Starting backend...
#         ✅ Backend started (PID: 12346)
# Completes in < 5 seconds
```

### Full Deployment
```bash
./scripts/deploy_with_timeout.sh
# Completes all steps within timeout limits
```

## Technical Details

### Timeout Implementation
- Uses background processes with monitoring
- Kills processes that exceed timeout
- Returns exit code 124 on timeout
- Works on macOS (no `timeout` command needed)

### SSH Configuration
- `ConnectTimeout=5`: Fast connection timeout
- `ServerAliveInterval=2`: Keep connection alive
- `ServerAliveCountMax=3`: Max keepalive failures

### Error Handling
- All operations return proper exit codes
- Timeout errors are clearly indicated
- Failed operations don't block subsequent steps

## Benefits

1. **No More Hanging**: All operations complete within defined timeouts
2. **Predictable Behavior**: Consistent timeout limits
3. **Better UX**: Agent console remains responsive
4. **Error Recovery**: Timeouts allow retry logic
5. **Cross-Platform**: Works on macOS without `timeout` command

## Future Improvements

- Add retry logic for failed operations
- Implement exponential backoff
- Add monitoring/alerting for timeouts
- Create dashboard for backend status

