package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultCrateNodePrefix               = "data-hot"
	defaultPID                           = 1
	defaultProto                         = "https"
	defaultMinAvailability               = "FULL"
	defaultTimeout                       = "7200s"
	defaultTerminationGracePeriodSeconds = 30  // Kubernetes default
	gracePeriodBuffer                    = 120 // seconds to subtract from terminationGracePeriodSeconds
	minimumTimeout                       = 360 // minimum effective timeout in seconds

	// StatefulSet label keys (using dashes instead of underscores)
	labelMinAvailability = "dc-util-min-availability"
	labelGracefulStop    = "dc-util-graceful-stop"
	labelDisabled        = "dc-util-disabled"
)

type customError struct{ msg string }

func (e *customError) Error() string { return e.msg }

// ErrMalformedHostname is returned when hostname cannot be parsed
var ErrMalformedHostname = &customError{"malformed hostname"}

// splitHostname splits hostname by "-"
func splitHostname(hostname string) []string {
	return strings.Split(hostname, "-")
}

// makeDecommissionStmt creates the decommission statement
func makeDecommissionStmt(nodeName string) string {
	return fmt.Sprintf("alter cluster decommission '%s'", nodeName)
}

// extractNodeName extracts the CrateDB node name from hostname
func extractNodeName(hostname, crateNodePrefix, defaultPrefix string) (string, error) {
	parts := splitHostname(hostname)

	// If custom prefix provided (not default), use it
	if crateNodePrefix != defaultPrefix && crateNodePrefix != "" {
		if len(parts) > 0 {
			podNumber := parts[len(parts)-1]
			return crateNodePrefix + "-" + podNumber, nil
		}
		return "", ErrMalformedHostname
	}

	// Extract from hostname if using default prefix
	// Expected format: crate-<prefix-parts>-<uuid-parts>-<pod-number>
	// We want: <prefix-parts>-<pod-number>
	if len(parts) >= 4 && parts[0] == "crate" {
		podNumber := parts[len(parts)-1]

		// Look for the node prefix pattern after "crate"
		// Use the provided crateNodePrefix (which equals defaultPrefix in this case)
		prefixParts := strings.Split(crateNodePrefix, "-")

		// Check if the hostname contains the expected prefix parts after "crate"
		if len(parts) >= len(prefixParts)+2 { // crate + prefix parts + pod number (minimum)
			// Extract the prefix parts that match our expected pattern
			prefixMatches := true
			for i, expectedPart := range prefixParts {
				if parts[1+i] != expectedPart {
					prefixMatches = false
					break
				}
			}

			if prefixMatches {
				return crateNodePrefix + "-" + podNumber, nil
			}
		}
	}

	return "", ErrMalformedHostname
}

// calculateEffectiveTimeout determines the timeout to use based on terminationGracePeriodSeconds
func calculateEffectiveTimeout(flagTimeout string, terminationGracePeriodSeconds *int64) (string, error) {
	// Parse the flag timeout value
	flagTimeoutDuration, err := time.ParseDuration(flagTimeout)
	if err != nil {
		return "", fmt.Errorf("invalid timeout format: %w", err)
	}
	flagTimeoutSeconds := int(flagTimeoutDuration.Seconds())

	// If terminationGracePeriodSeconds is not set or is the default value (30s), use flag timeout
	// The default 30s is too small for CrateDB decommissioning, so we rely on the flag timeout
	if terminationGracePeriodSeconds == nil || *terminationGracePeriodSeconds == defaultTerminationGracePeriodSeconds {
		return flagTimeout, nil
	}

	// Calculate effective timeout: terminationGracePeriodSeconds - buffer
	effectiveTimeoutSeconds := int(*terminationGracePeriodSeconds) - gracePeriodBuffer

	// Ensure minimum timeout
	if effectiveTimeoutSeconds < minimumTimeout {
		effectiveTimeoutSeconds = minimumTimeout
		log.Printf("Calculated timeout (%ds) is below minimum, using %ds instead",
			int(*terminationGracePeriodSeconds)-gracePeriodBuffer, minimumTimeout)
	}

	// Log when using different timeout than flag
	if effectiveTimeoutSeconds != flagTimeoutSeconds {
		log.Printf("Using timeout derived from terminationGracePeriodSeconds: %ds (terminationGracePeriodSeconds=%ds, buffer=%ds) instead of flag value: %ds",
			effectiveTimeoutSeconds, *terminationGracePeriodSeconds, gracePeriodBuffer, flagTimeoutSeconds)
	}

	return fmt.Sprintf("%ds", effectiveTimeoutSeconds), nil
}

// getMinAvailabilityFromLabels reads min-availability from StatefulSet labels or returns default
func getMinAvailabilityFromLabels(labels map[string]string, defaultValue string) string {
	if value, exists := labels[labelMinAvailability]; exists {
		// Validate the value
		switch value {
		case "NONE", "FULL", "PRIMARIES":
			log.Printf("Using min-availability from StatefulSet label: %s", value)
			return value
		default:
			log.Printf("Invalid min-availability value in StatefulSet label '%s': %s, using default: %s",
				labelMinAvailability, value, defaultValue)
		}
	}
	return defaultValue
}

// getGracefulStopForceFromLabels reads graceful stop force setting from StatefulSet labels
func getGracefulStopForceFromLabels(labels map[string]string) bool {
	if value, exists := labels[labelGracefulStop]; exists {
		switch value {
		case "TRUE", "true", "True":
			log.Printf("Using graceful stop force from StatefulSet label: true")
			return true
		case "FALSE", "false", "False":
			log.Printf("Using graceful stop force from StatefulSet label: false")
			return false
		default:
			log.Printf("Invalid graceful stop value in StatefulSet label '%s': %s, using default: true",
				labelGracefulStop, value)
		}
	}
	return true // default behavior
}

// getDisabledFromLabels reads disabled setting from StatefulSet labels
func getDisabledFromLabels(labels map[string]string) bool {
	if value, exists := labels[labelDisabled]; exists {
		switch value {
		case "TRUE", "true", "True":
			log.Printf("Using disabled from StatefulSet label: true")
			return true
		case "FALSE", "false", "False":
			log.Printf("Using disabled from StatefulSet label: false")
			return false
		default:
			log.Printf("Invalid disabled value in StatefulSet label '%s': %s, using default: false",
				labelDisabled, value)
		}
	}
	return false // default behavior
}

func sendSQLStatement(proto, stmt string, dryRun bool) error {
	if dryRun {
		log.Printf("[DRY-RUN] Would send SQL statement: %s", stmt)
		log.Printf("[DRY-RUN] Would send to URL: %s://127.0.0.1:4200/_sql", proto)
		return nil
	}
	payload := map[string]string{"stmt": stmt}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON payload: %w", err)
	}
	log.Printf("Payload: %s", string(payloadBytes))

	// Create an HTTP client with TLS configuration to skip certificate verification
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	// Make the HTTP POST request
	url := fmt.Sprintf("%s://127.0.0.1:4200/_sql", proto)
	resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to make HTTP POST request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status: %s", resp.Status)
	}

	// Read and print the response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	log.Printf("Response from server: %s", string(respBody))

	return nil
}

func isProcessRunning(pid int) bool {
	err := syscall.Kill(pid, syscall.Signal(0))
	return err == nil
}

func getNamespace(inCluster bool, kubeconfig clientcmd.ClientConfig) (string, error) {
	if inCluster {
		namespaceBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			return "", fmt.Errorf("failed to read namespace: %w", err)
		}
		return strings.TrimSpace(string(namespaceBytes)), nil
	}
	namespace, _, err := kubeconfig.Namespace()
	if err != nil {
		return "", fmt.Errorf("failed to get namespace from kubeconfig: %w", err)
	}
	return namespace, nil
}

func run() error {
	log.SetPrefix("Decommissioner: ")
	log.SetOutput(os.Stdout)

	envHostname := os.Getenv("HOSTNAME")

	var (
		crateNodePrefix     string
		decommissionTimeout string
		pid                 int
		proto               string
		hostname            string
		minAvailability     string
		dryRun              bool
	)

	flag.StringVar(&crateNodePrefix, "crate-node-prefix", defaultCrateNodePrefix, "Prefix of the CrateDB node name")
	flag.StringVar(&decommissionTimeout, "timeout", defaultTimeout, "Timeout for decommission statement in seconds")
	flag.IntVar(&pid, "pid", defaultPID, "PID of the process to check")
	flag.StringVar(&proto, "proto", defaultProto, "Protocol to use for the HTTP server")
	flag.StringVar(&hostname, "hostname", envHostname, "Hostname of the pod")
	flag.StringVar(&minAvailability, "min-availability", defaultMinAvailability, "Minimum availability during decommission (FULL/PRIMARIES)")
	flag.BoolVar(&dryRun, "dry-run", false, "Log all actions without sending SQL commands to the node")
	flag.Parse()

	if hostname == "" {
		return fmt.Errorf("hostname is required")
	}

	// Determine if we are running in-cluster or using kubeconfig
	kubeconfigPath := os.Getenv("KUBECONFIG")
	inClusterConfig := kubeconfigPath == ""

	var (
		config     *rest.Config
		kubeconfig clientcmd.ClientConfig
		err        error
	)

	if inClusterConfig {
		log.Println("Using in-cluster configuration")
		config, err = rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("failed to create in-cluster config: %w", err)
		}
	} else {
		log.Printf("Using kubeconfig from %s", kubeconfigPath)
		kubeconfig = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		)
		config, err = kubeconfig.ClientConfig()
		if err != nil {
			return fmt.Errorf("failed to load kubeconfig: %w", err)
		}
	}

	// Create a Kubernetes client
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	// Get the namespace
	namespace, err := getNamespace(inClusterConfig, kubeconfig)
	if err != nil {
		return err
	}

	// Construct the StatefulSet name from HOSTNAME
	hostnameParts := strings.Split(hostname, "-")
	if len(hostnameParts) < 2 {
		return fmt.Errorf("invalid HOSTNAME format: %s", hostname)
	}
	statefulSetName := strings.Join(hostnameParts[:len(hostnameParts)-1], "-")

	// Determine crateNodeName using extracted logic
	log.Printf("Parsing hostname: %s", hostname)
	actualCrateNodeName, err := extractNodeName(hostname, crateNodePrefix, defaultCrateNodePrefix)
	if err != nil {
		return fmt.Errorf("failed to extract node name from hostname %s: %w", hostname, err)
	}
	log.Printf("Extracted CrateDB node name: %s", actualCrateNodeName)

	ctx := context.Background()
	statefulSet, err := clientset.AppsV1().StatefulSets(namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get StatefulSet: %w", err)
	}
	replicas := int(*statefulSet.Spec.Replicas)

	log.Printf("StatefulSet has %d replicas configured", replicas)

	// Get configuration from StatefulSet labels
	effectiveMinAvailability := getMinAvailabilityFromLabels(statefulSet.Labels, minAvailability)
	gracefulStopForce := getGracefulStopForceFromLabels(statefulSet.Labels)
	disabled := getDisabledFromLabels(statefulSet.Labels)

	// Check if dc_util is disabled
	if disabled {
		log.Println("dc_util is disabled via StatefulSet label, exiting without performing decommission")
		return nil
	}

	if dryRun {
		log.Println("[DRY-RUN] Running in dry-run mode - no SQL commands will be sent")
	}

	// Calculate effective timeout based on terminationGracePeriodSeconds
	effectiveTimeout, err := calculateEffectiveTimeout(decommissionTimeout, statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds)
	if err != nil {
		return fmt.Errorf("failed to calculate effective timeout: %w", err)
	}

	if statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds != nil {
		log.Printf("StatefulSet terminationGracePeriodSeconds: %ds", *statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds)
	} else {
		log.Printf("StatefulSet terminationGracePeriodSeconds: not set (using Kubernetes default)")
	}

	time.Sleep(2 * time.Second) // Sleep to catch up with the replica settings

	if replicas > 0 {
		// Send the SQL statements to decommission the node
		log.Printf("Decommissioning node %s with graceful_stop.timeout of %s, min_availability=%s, force=%t",
			actualCrateNodeName, effectiveTimeout, effectiveMinAvailability, gracefulStopForce)

		statements := []string{
			fmt.Sprintf(`set global transient "cluster.graceful_stop.timeout" = '%s';`, effectiveTimeout),
			fmt.Sprintf(`set global transient "cluster.graceful_stop.force" = %t;`, gracefulStopForce),
			fmt.Sprintf(`set global transient "cluster.graceful_stop.min_availability"='%s';`, effectiveMinAvailability),
			makeDecommissionStmt(actualCrateNodeName),
		}

		for _, stmt := range statements {
			if err := sendSQLStatement(proto, stmt, dryRun); err != nil {
				return err
			}
		}

		if dryRun {
			log.Println("[DRY-RUN] Would have sent decommission commands successfully")
			log.Printf("[DRY-RUN] Would monitor process %d until it stops", pid)
			log.Println("[DRY-RUN] Dry-run completed successfully")
		} else {
			log.Println("Decommission command sent successfully")

			// Loop to check if the process is running
			counter := 0
			for isProcessRunning(pid) {
				if counter%10 == 0 {
					log.Printf("Process %d is still running (check count: %d)", pid, counter)
				}
				counter++
				time.Sleep(2 * time.Second)
			}

			log.Printf("Process %d has stopped", pid)
		}
	} else {
		log.Printf("No replicas are configured -- Skipping decommission")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
