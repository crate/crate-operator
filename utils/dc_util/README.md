# Rolling restart with `alter cluster decommission`

While working on a cloud issue, a small tool was created
to not only _terminate_ a pod by `kubelet` sending a SIGTERM, but by having the ability
to use a preStop hook and issue an `alter cluster decommission` for that node.

The cratedb Documentation explains the rolling restart process here: https://cratedb.com/docs/guide/admin/upgrade/rolling.html

## Recent Updates

**🎉 NEW FEATURES:**

- **Persistent Logging**: Dual logging to STDOUT and persistent file with auto-rotation (see "Persistent Logging" section)
- **PostStart Hook Detection**: Automatically detects PostStart hooks to prevent permanent routing allocation misconfiguration
- **Single Node Cluster Detection**: Automatically skips decommission for single node clusters (replicas=1) to prevent failures
- **Enhanced Flag Support**: Now supports both `-reset-routing` and `--reset-routing` flag formats
- **Configurable Paths**: New `--log-file` and `--lock-file` CLI flags for customized deployments

These updates solve the historical issue where _NEW_PRIMARIES_ routing allocation could not be reliably reset and make hook debugging significantly easier in Kubernetes environments.

# What does the tool do?

First, the decommission settings are configured for the cluster. By default, _force_
decommission is enabled - meaning: If CrateDB determines that the decommission failed,
it would roll it back. In the context of terminating a pod/process in Kubernetes, the
shutdown cannot be canceled - therefore _force_ is typically set on the CrateDB side.
However, this can now be controlled via the `dc-util-graceful-stop` StatefulSet label
and remains true by default.

**NEW**: The tool now intelligently detects if a StatefulSet has a PostStart hook configured
with dc_util reset-routing capability. If no such PostStart hook exists, routing allocation
changes are skipped to prevent permanent cluster misconfiguration.

Before doing that, the StatefulSet is checked for the number of replicas configured. This is
done to determine whether a full stop of all pods in the CrateDB cluster is _scheduled_. In
case of a full restart, there is **NO** decommission sent to the cluster and the Kubernetes
shutdown continues by sending `SIGTERM`.

**NEW**: The tool now also detects single node clusters (replicas=1) and automatically skips
decommissioning since there are no other nodes to migrate data to, preventing potential
failures or hanging operations.

For having access to the number of replicas on the sts, additional permission need to be granted
to the ServiceAccount:

```yaml
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "list", "watch"]
```

This needs to be created/setup manually, or by the crate-operator.

When the decommission is sent to the cluster, the command almost immediately returns. Nevertheless,
CrateDB starts the decommissioning in the background and we need to wait until CrateDB exits.
After that, control is _returned_ to `kubelet` which continues by sending SIGTERM.
`Termination Grace Period` needs to be set longer than the decommission timeout, as kubelet
is monitoring this timer and would eventually assume the preStop process _hangs_ and continue
with _TERMINATING_ the containers/pod.

# How to configure it?

## Complete Configuration with Both Hooks (Recommended)

For full routing allocation management and decommissioning:

```yaml
image: crate:5.8.3
lifecycle:
  postStart:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          ARCH=$(uname -m)
          case $ARCH in
            x86_64) BINARY_ARCH="amd64" ;;
            aarch64) BINARY_ARCH="arm64" ;;
            *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
          esac

          curl --insecure -sLO https://github.com/crate/crate-operator/releases/download/dc_util_v1.0.0/dc_util-linux-${BINARY_ARCH}
          curl --insecure -sLO https://github.com/crate/crate-operator/releases/download/dc_util_v1.0.0/dc_util-linux-${BINARY_ARCH}.sha256
          sha256sum -c dc_util-linux-${BINARY_ARCH}.sha256
          chmod u+x ./dc_util-linux-${BINARY_ARCH}
          ./dc_util-linux-${BINARY_ARCH} --reset-routing || true
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          ARCH=$(uname -m)
          case $ARCH in
            x86_64) BINARY_ARCH="amd64" ;;
            aarch64) BINARY_ARCH="arm64" ;;
            *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
          esac

          curl -sLO https://github.com/crate/crate-operator/releases/download/dc_util_v1.0.0/dc_util-linux-${BINARY_ARCH}
          curl -sLO https://github.com/crate/crate-operator/releases/download/dc_util_v1.0.0/dc_util-linux-${BINARY_ARCH}.sha256
          sha256sum -c dc_util-linux-${BINARY_ARCH}.sha256
          chmod u+x ./dc_util-linux-${BINARY_ARCH}
          ./dc_util-linux-${BINARY_ARCH} -min-availability PRIMARIES
terminationGracePeriodSeconds: 7230
```

**Architecture Detection**: Both configurations automatically detect the CPU architecture (x86_64/amd64 or aarch64/arm64) and download the appropriate binary. This ensures compatibility across different Kubernetes node architectures without manual configuration.

**Note**: The `|| true` in the postStart hook prevents container failures if dc_util encounters issues during startup.

## Understanding terminationGracePeriodSeconds

Proper configuration of `terminationGracePeriodSeconds` is critical for successful decommissioning:

**Key Considerations:**

- **Kubernetes default (30s) is insufficient** for CrateDB decommissioning operations
- **Must be longer than decommission timeout** - otherwise kubelet kills the pod before decommission completes
- **Acts as maximum time limit** before kubelet forcefully terminates the container
- **With graceful termination enabled**, decommission completion time becomes predictable

**Recommended Approach:**
Set `terminationGracePeriodSeconds` generously (e.g., 7200s) and configure sensible decommission timeouts (20-30 minutes). The decommission timeout should be long enough for data migration between nodes, which varies significantly based on:

- Data volume and cluster topology
- `min_availability` setting (PRIMARIES vs FULL)
- Number of available target nodes
- Network and disk performance

**Example Migration Scenarios:**

```bash
# Large data migration - may take hours
• Shards to move: 185 primaries, 41 replicas
• Data to move: 2.3TB, Target nodes: 2
• Estimated time: 16h 23m (40MB/sec)

# Fast conversion scenario - completes quickly
• Shards to move: 218 primaries (210 fast-convert)
• Data to move: 1.6GB, Target nodes: 2
• Estimated time: 4s (400MB/sec)
```

**Important**: Avoid excessively long timeouts as they can delay restarts, especially when stuck queries in `sys.jobs` prevent clean shutdown but will eventually time out anyway.

In this example, the binary is loaded from the GitHub repo as part of the pod's termination process. In case one of these commands fails, `kubelet` continues with SIGTERM immediately.

# How to build it?

```shell
GOOS=linux go build -o dc_util  dc_util.go
GOOS=linux GOARCH=amd64 go build -o dc_util-linux-amd64 dc_util.go
shasum -a 256 dc_util
```

This builds the binary for `Linux` in case you are building it on MacOS. For simplifying the testing `--dry-run` is availble. dc_util only works in kubernetes, as it relies on the Kubernetes API to interact with the cluster.

# Command Line

There are a bunch of CLI parameters that can be set to fine-tune the behavior. Most
are used for testing purpose:

| Parameter             | setting                                                                                                                                                                                                                                                                |
| --------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--crate-node-prefix` | allows customizing the CrateDB node names in the StatefulSet in case it is not the default `data-hot`. This is not to be confused with the _hostname_!                                                                                                                 |
| `--timeout`           | CrateDB's default timeout is 7200s - this is automatically adjusted based on `terminationGracePeriodSeconds` (see below)                                                                                                                                               |
| `--pid`               | For testing locally only                                                                                                                                                                                                                                               |
| `--hostname`          | Used to derive the name of the Kubernetes StatefulSet; the _replica number_ of the pod is _stripped_ from it, which returns the StatefulSet name. e.g., `crate-data-hot-eadf76b5-c634-4f0f-abcc-7442d01cb7dd-0 -> crate-data-hot-eadf76b5-c634-4f0f-abcc-7442d01cb7dd` |
| `--min-availability`  | Either `PRIMARIES`, `FULL`, or `NONE`. Can be overridden by StatefulSet labels (see below). Please refer to the CrateDB documentation.                                                                                                                                 |
| `--dry-run`           | **Testing mode**: Logs all SQL statements that would be sent but doesn't actually send them to the node. Skips process monitoring. Perfect for testing dc_util behavior in pods without affecting the CrateDB cluster.                                                 |
| `--reset-routing`     | **PostStart mode**: Resets routing allocation to 'all' if dc_util lock file exists. Used in PostStart hooks to restore routing allocation after dc_util shutdown. Includes 10-minute cluster readiness timeout.                                                        |
| `--log-file`          | **Path to persistent log file** (default: `/resource/heapdump/dc_util.log`). Logs are written to both STDOUT and this file simultaneously. File is auto-rotated when approaching 1MB. Essential for debugging Kubernetes hooks.                                        |
| `--lock-file`         | **Path to lock file** (default: `/resource/heapdump/dc_util.lock`). Used to track dc_util shutdowns and coordinate PostStart hook behavior. Customizable for different deployment scenarios.                                                                           |

# Timeout Logic

The tool automatically determines the appropriate decommission timeout based on the StatefulSet's `terminationGracePeriodSeconds`:

- **Default case (30s or nil)**: Uses `--timeout` flag value (30s is too small for CrateDB decommissioning)
- **Custom terminationGracePeriodSeconds**: Uses `terminationGracePeriodSeconds - 120s` (reserves 120s for shutdown)
- **Minimum safety**: Always enforces minimum 360s timeout regardless of calculated value
- **Logging**: Reports when using derived timeout instead of flag timeout

## Real-world scenarios:

- **Long-running workload** (1800s): Uses 1680s for decommission, keeps 120s for shutdown
- **Very long period** (3600s): Uses 3480s for decommission

## Statefuleset Replica Count Logic

The tool intelligently handles different cluster sizes:

- **Zero replicas** (replicas=0): Skips decommission - StatefulSet is scaled down
- **Single node cluster** (replicas=1): Skips decommission - no other nodes to migrate data to
- **Multi-node cluster** (replicas≥2): Proceeds with normal decommission process

### Sample Logs for Different Scenarios:

```
# Zero replicas
Decommissioner: No replicas are configured -- Skipping decommission

# Single node cluster
Decommissioner: Single node cluster detected (replicas=1) -- Skipping decommission

# Multi-node cluster
Decommissioner: Decommissioning node data-hot-2 with graceful_stop.timeout of 7200s...
```

# StatefulSet Label Configuration

The tool can read configuration from StatefulSet labels, overriding CLI parameters. This is especially useful to dynamically adjust decommission settings, without restarting the POD
to pick up settings changes in the statefulset - think "short-cut" when needed. The tool has sensible defaults and can be run without parameters (except the `--reset-routing`).

As already mentioned `terminationGracePeriodSeconds` MUST be set larger then `--tiemout` otherwise kubelet will SIGKILL the container before decommission finished!

## Labels:

- **`dc-util-min-availability`**: Sets min-availability (values: `NONE`, `PRIMARIES`, `FULL`)
- **`dc-util-graceful-stop`**: Controls graceful stop force setting (values: `true`, `false`)
- **`dc-util-disabled`**: Disables dc_util decommissioning entirely (values: `true`, `false`, default: `false`)
- **`dc-util-no-prestart`**: Disables PostStart reset-routing behavior (values: `true`, `false`, default: `false`)
- **`dc-util-pre-stop-routing-allocation`**: Sets routing allocation value during preStop (values: `new_primaries`, `all`, default: `new_primaries`)

## Example StatefulSet with labels:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: crate-data-hot
  labels:
    dc-util-min-availability: "PRIMARIES"
    dc-util-graceful-stop: "false"
    dc-util-disabled: "false"
    dc-util-no-prestart: "false"
    dc-util-pre-stop-routing-allocation: "new_primaries"
spec:
  # ... rest of StatefulSet spec
```

## Behavior:

- **No labels**: Uses CLI parameter values (`--min-availability`, default force=true, disabled=false)
- **Valid labels**: Uses label values, logs the override
- **Invalid labels**: Uses CLI defaults, logs the invalid value
- **Label precedence**: StatefulSet labels override CLI parameters
- **When disabled=true**: dc_util logs a message and exits without performing any decommission work
- **When no-prestart=true**: PostStart reset-routing behavior is skipped

## Sample Logs

**UPDATE**: With the new persistent logging feature (see "Persistent Logging" section below), dc_util now logs to both STDOUT and a persistent file (`/resource/heapdump/dc_util.log` by default), making hook debugging much easier!

Legacy note: Previously, you could not see PreStop hook log output as it wasn't logged to STDOUT where you would expect it.

```
bash-5.2# ./dc_util-linux-amd64 -min-availability PRIMARIES -timeout 120s
Decommissioner: 2025/10/09 17:16:46 Using in-cluster configuration
Decommissioner: 2025/10/09 17:16:46 Parsing hostname: crate-data-hot-d84c10e6-d8fb-4d10-bf60-f9f2ea919a73-2
Decommissioner: 2025/10/09 17:16:46 Extracted CrateDB node name: data-hot-2
Decommissioner: 2025/10/09 17:16:46 StatefulSet has 3 replicas configured
Decommissioner: 2025/10/09 17:16:46 Using min-availability from StatefulSet label: NONE
Decommissioner: 2025/10/09 17:16:46 Using graceful stop force from StatefulSet label: false
Decommissioner: 2025/10/09 17:16:46 Using timeout derived from terminationGracePeriodSeconds: 780s (terminationGracePeriodSeconds=900s, buffer=120s) instead of flag value: 120s
Decommissioner: 2025/10/09 17:16:46 StatefulSet terminationGracePeriodSeconds: 900s
Decommissioner: 2025/10/09 17:16:48 Decommissioning node data-hot-2 with graceful_stop.timeout of 780s, min_availability=NONE, force=false
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"set global transient \"cluster.graceful_stop.timeout\" = '780s';"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":24.105846}
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"set global transient \"cluster.graceful_stop.force\" = false;"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":18.95872}
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"set global transient \"cluster.graceful_stop.min_availability\"='NONE';"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":13.927663}
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"alter cluster decommission 'data-hot-2'"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":3.827284}
Decommissioner: 2025/10/09 17:16:48 Decommission command sent successfully
Decommissioner: 2025/10/09 17:16:48 Process 1 is still running (check count: 0)

```

## Dry-Run Mode

For testing purposes, you can use the `--dry-run` flag to simulate the decommission process without actually sending SQL commands to the CrateDB node:

```bash
./dc_util-linux-amd64 --dry-run -min-availability PRIMARIES -timeout 120s
```

### Sample Dry-Run Logs

```
Decommissioner: 2025/10/16 14:41:10 Using in-cluster configuration
Decommissioner: 2025/10/16 14:41:10 Parsing hostname: crate-data-hot-abc123-0
Decommissioner: 2025/10/16 14:41:10 Extracted CrateDB node name: data-hot-0
Decommissioner: 2025/10/16 14:41:10 StatefulSet has 3 replicas configured
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Running in dry-run mode - no SQL commands will be sent
Decommissioner: 2025/10/16 14:41:10 StatefulSet terminationGracePeriodSeconds: 900s
Decommissioner: 2025/10/16 14:41:10 Decommissioning node data-hot-0 with graceful_stop.timeout of 780s, min_availability=PRIMARIES, force=true
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send SQL statement: set global transient "cluster.graceful_stop.timeout" = '780s';
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send to URL: https://127.0.0.1:4200/_sql
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send SQL statement: set global transient "cluster.graceful_stop.force" = true;
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send to URL: https://127.0.0.1:4200/_sql
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send SQL statement: set global transient "cluster.graceful_stop.min_availability"='PRIMARIES';
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send to URL: https://127.0.0.1:4200/_sql
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send SQL statement: alter cluster decommission 'data-hot-0'
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would send to URL: https://127.0.0.1:4200/_sql
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would have sent decommission commands successfully
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Would monitor process 1 until it stops
Decommissioner: 2025/10/16 14:41:10 [DRY-RUN] Dry-run completed successfully
```

In dry-run mode:

- All StatefulSet parsing and label reading happens normally
- All decommission logic is executed (timeout calculation, statement preparation)
- SQL statements are logged with `[DRY-RUN]` prefix but not sent to CrateDB
- Process monitoring is skipped since no actual decommission occurs
- Perfect for testing in pods without impacting the cluster

## PostStart Hook - Routing Allocation Reset

dc_util now supports a postStart hook mode to automatically restore routing allocation after a graceful shutdown:

### Usage in postStart Hook:

```yaml
spec:
  template:
    spec:
      containers:
        - name: crate
          lifecycle:
            postStart:
              exec:
                command:
                  - /bin/sh
                  - -c
                  - |
                    ARCH=$(uname -m)
                    case $ARCH in
                      x86_64) BINARY_ARCH="amd64" ;;
                      aarch64) BINARY_ARCH="arm64" ;;
                      *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
                    esac

                    curl --insecure -sLO https://example.com/dc_util-linux-${BINARY_ARCH}
                    curl --insecure -sLO https://example.com/dc_util-linux-${BINARY_ARCH}.sha256
                    sha256sum -c dc_util-linux-${BINARY_ARCH}.sha256
                    chmod u+x dc_util-linux-${BINARY_ARCH}
                    ./dc_util-linux-${BINARY_ARCH} --reset-routing || true
            preStop:
              exec:
                command:
                  - /bin/sh
                  - -c
                  - |
                    ARCH=$(uname -m)
                    case $ARCH in
                      x86_64) BINARY_ARCH="amd64" ;;
                      aarch64) BINARY_ARCH="arm64" ;;
                      *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
                    esac

                    curl --insecure -sLO https://example.com/dc_util-linux-${BINARY_ARCH}
                    curl --insecure -sLO https://example.com/dc_util-linux-${BINARY_ARCH}.sha256
                    sha256sum -c dc_util-linux-${BINARY_ARCH}.sha256
                    chmod u+x dc_util-linux-${BINARY_ARCH}
                    ./dc_util-linux-${BINARY_ARCH} --min-availability PRIMARIES
```

**Important postStart Hook Failure Behavior**: postStart hooks are **NOT failsafe** - if any command fails, Kubernetes kills the container, causing CrashLoopBackOff. The `|| true` at the end is crucial to prevent this:

- **Without `|| true`**: Download failures, network issues, or dc_util errors will kill the container
- **With `|| true`**: Container starts successfully even if postStart hook fails
- **Trade-off**: Hook failures become silent - check container logs to see what actually happened

For production deployments, consider embedding dc_util in your container image to eliminate download dependencies.

### How it Works:

**PostStart Hook Detection** (NEW):

- dc_util now automatically detects if a StatefulSet has a postStart hook configured with `dc_util --reset-routing` (or `-reset-routing`)
- If no postStart hook is found, the preStop process skips routing allocation changes to prevent permanent cluster misconfiguration
- Supports both single dash (`-reset-routing`) and double dash (`--reset-routing`) flag formats
- Prevents false positives from similar flag names using precise word boundary matching

**preStop Process (Enhanced)**:

1. **Checks for postStart hook** with dc_util reset-routing capability
2. Sets routing allocation to restricted value (`new_primaries` by default) **only if postStart hook exists**
3. Creates lock file (configurable via `--lock-file`)
4. Continues with normal decommission process

**postStart Process (`--reset-routing`)**:

1. Checks `dc-util-no-prestart` label (exit if `true`)
2. Checks for lock file existence (exit if not found)
3. Waits for cluster readiness (10-minute timeout)
4. Executes: `SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = 'all';`
5. Removes lock file (prevents retry loops)

### Sample postStart Logs:

```
ResetRouting: 2025/10/16 19:23:49 Starting reset-routing mode for hostname: crate-data-hot-abc123-0
ResetRouting: 2025/10/16 19:23:49 Using in-cluster configuration
ResetRouting: 2025/10/16 19:23:49 Lock file found at /resource/heapdump/dc_util.lock, proceeding with routing allocation reset
ResetRouting: 2025/10/16 19:23:50 Waiting for cluster readiness (timeout: 10m0s)...
ResetRouting: 2025/10/16 19:23:52 Cluster is ready after 3 attempts
ResetRouting: 2025/10/16 19:23:52 Executing routing allocation reset
ResetRouting: 2025/10/16 19:23:52 Payload: {"stmt":"SET GLOBAL TRANSIENT \"cluster.routing.allocation.enable\" = 'all';"}
ResetRouting: 2025/10/16 19:23:52 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":15.234}
ResetRouting: 2025/10/16 19:23:52 Routing allocation reset completed successfully
ResetRouting: 2025/10/16 19:23:52 Removed lock file: /resource/heapdump/dc_util.lock
ResetRouting: 2025/10/16 19:23:52 Reset-routing completed
```

### Configuration Options:

- **Disable postStart**: Set `dc-util-no-prestart: "true"`
- **Custom preStop routing**: Set `dc-util-pre-stop-routing-allocation: "all"`
- **Testing**: Use `--reset-routing --dry-run` for safe testing

## Persistent Logging

**NEW FEATURE**: dc_util now supports persistent logging to both STDOUT and a file simultaneously, essential for debugging Kubernetes lifecycle hooks where container logs may not be easily accessible.

### Key Features:

- **Dual output**: Every log message appears in both STDOUT and the specified log file
- **Automatic rotation**: Log file is truncated when approaching 1MB to prevent disk space issues
- **Configurable path**: Use `--log-file` to customize the log location (default: `/resource/heapdump/dc_util.log`)
- **Failsafe design**: If file logging fails, STDOUT logging continues uninterrupted
- **Zero code changes**: All existing `log.Printf()` statements automatically use dual logging

### Usage Examples:

```bash
# Default logging (logs to both STDOUT and /resource/heapdump/dc_util.log)
dc_util --dry-run --hostname crate-0

# Custom log file location
dc_util --reset-routing --log-file /custom/debug/dc_util.log

# Custom lock file location
dc_util --dry-run --lock-file /tmp/custom.lock
```

### Benefits for Kubernetes:

- **Hook debugging**: postStart and preStop hook logs are now persistent and accessible
- **Troubleshooting**: Historical logs available even after pod restarts
- **Operations**: Easy to collect logs from multiple pods for analysis
- **Development**: Simplified testing and validation of hook behavior
