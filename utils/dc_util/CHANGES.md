# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Persistent Logging**: Dual logging to both STDOUT and persistent file with automatic rotation
  - New `--log-file` CLI flag (default: `/resource/heapdump/dc_util.log`)
  - Automatic file rotation when approaching 1MB to prevent disk space issues
  - Failsafe design - continues STDOUT logging even if file logging fails
  - Essential for debugging Kubernetes lifecycle hooks where container logs may not be accessible
  - Creates directory structure if it doesn't exist

- **PostStart Hook Detection**: Intelligent detection of StatefulSet PostStart hooks
  - Automatically scans StatefulSet containers for PostStart hooks with `dc_util --reset-routing`
  - Prevents routing allocation changes when no PostStart hook exists to reset them
  - Solves historical issue where `NEW_PRIMARIES` routing allocation could not be reliably reset
  - Supports both single dash (`-reset-routing`) and double dash (`--reset-routing`) flag formats
  - Precise word boundary matching prevents false positives from similar flag names
  - Logs clear messages when PostStart hooks are found or missing

- **Single Node Cluster Detection**: Automatic detection and handling of single node clusters
  - Detects when StatefulSet has exactly 1 replica and skips decommission
  - Prevents unnecessary overhead and potential failures in single node deployments
  - Clear logging explains why decommission was skipped
  - Maintains existing behavior for multi-node clusters (≥2 replicas)

- **Configurable Lock File Path**: New `--lock-file` CLI flag
  - Default: `/resource/heapdump/dc_util.lock`
  - Allows customization for different deployment scenarios
  - All lock file operations now use configurable path

- **Enhanced Flag Support**: Improved command-line flag handling
  - Both `-reset-routing` and `--reset-routing` formats now supported
  - Maintains backward compatibility with existing deployments
  - Better error handling and validation

### Changed

- **Routing Allocation Logic**: Enhanced PreStop process with PostStart hook detection
  - Routing allocation changes now only occur when corresponding PostStart hook exists
  - Prevents permanent cluster misconfiguration in deployments without PostStart hooks
  - More intelligent decision making based on actual StatefulSet configuration

- **Replica Count Handling**: Improved logic for different cluster sizes
  - Zero replicas (scaled down): Skips decommission with clear logging
  - Single replica: Skips decommission to prevent failures
  - Multiple replicas: Proceeds with normal decommission process
  - Better log messages explaining the decision for each scenario

- **Function Signatures**: Updated internal functions to support configurable paths
  - `createLockFile()` now accepts lock file path parameter
  - `removeLockFile()` now accepts lock file path parameter
  - `lockFileExists()` now accepts lock file path parameter
  - `handleResetRouting()` now accepts lock file path parameter

### Improved

- **Logging Experience**: Comprehensive logging improvements
  - All log messages now appear in both STDOUT and persistent file
  - Better visibility into hook execution for debugging
  - Historical logs available even after pod restarts
  - Easier troubleshooting and operations monitoring

- **Documentation**: Extensively updated README.md
  - Added "Recent Updates" section highlighting new features
  - New "Replica Count Logic" section with examples
  - Updated CLI parameter table with new flags
  - Enhanced "PostStart Hook Detection" documentation
  - Added complete "Persistent Logging" section with usage examples
  - Updated sample logs sections to reflect new capabilities

- **Testing**: Comprehensive test coverage for all new features
  - `TestHasPostStartHookWithResetRouting`: PostStart hook detection with various scenarios
  - `TestPostStopRoutingAllocationIntegration`: Integration tests for routing allocation logic
  - `TestLoggingIntegration`: Dual logging functionality verification
  - `TestLogRotation`: File rotation behavior validation
  - `TestSingleNodeClusterBehavior`: Single node cluster detection tests
  - `TestReplicaCountBehavior`: Comprehensive replica count handling tests
  - All existing tests updated to work with new function signatures

### Technical Details

- **New CLI Flags**:
  - `--log-file string`: Path to persistent log file (default: `/resource/heapdump/dc_util.log`)
  - `--lock-file string`: Path to lock file (default: `/resource/heapdump/dc_util.lock`)

- **New Functions**:
  - `setupLogging(logFile string)`: Configures dual logging with rotation
  - `hasPostStartHookWithResetRouting(statefulSet *appsv1.StatefulSet)`: PostStart hook detection
  - Enhanced replica count logic in main decommission flow

- **Dependencies**: Added `path/filepath` import for directory handling

### Log Message Examples

```bash
# PostStart hook detection
Decommissioner: No postStart hook with dc_util --reset-routing or -reset-routing found, skipping pre-stop routing allocation change

# Single node cluster detection
Decommissioner: Single node cluster detected (replicas=1) -- Skipping decommission

# Persistent logging
Decommissioner: 2025/10/17 15:02:38 Using kubeconfig from /Users/walter/.kube/config
# (Same message appears in both STDOUT and /resource/heapdump/dc_util.log)
```

### Backward Compatibility

- All existing CLI flags and behavior remain unchanged
- Existing StatefulSet configurations continue to work without modification
- New features are opt-in via CLI flags or automatic detection
- No breaking changes to existing functionality

### Benefits

- **For Operations**: Persistent logs make debugging Kubernetes hooks significantly easier
- **For Development**: Enhanced testing capabilities with better dry-run logging
- **For Reliability**: Prevents cluster misconfigurations and single node failures
- **For Maintenance**: Clear logging and automatic file rotation reduce operational overhead
