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
	defaultCrateNodePrefix = "data-hot"
	defaultPID             = 1
	defaultProto           = "https"
	defaultMinAvailability = "FULL"
	defaultTimeout         = "7200s"
)

func sendSQLStatement(proto, stmt string) error {
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
	)

	flag.StringVar(&crateNodePrefix, "crate-node-prefix", defaultCrateNodePrefix, "Prefix of the CrateDB node name")
	flag.StringVar(&decommissionTimeout, "timeout", defaultTimeout, "Timeout for decommission statement in seconds")
	flag.IntVar(&pid, "pid", defaultPID, "PID of the process to check")
	flag.StringVar(&proto, "proto", defaultProto, "Protocol to use for the HTTP server")
	flag.StringVar(&hostname, "hostname", envHostname, "Hostname of the pod")
	flag.StringVar(&minAvailability, "min-availability", defaultMinAvailability, "Minimum availability during decommission (FULL/PRIMARIES)")
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

	ctx := context.Background()
	statefulSet, err := clientset.AppsV1().StatefulSets(namespace).Get(ctx, statefulSetName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get StatefulSet: %w", err)
	}
	replicas := int(*statefulSet.Spec.Replicas)

	log.Printf("StatefulSet has %d replicas configured", replicas)

	time.Sleep(2 * time.Second) // Sleep to catch up with the replica settings

	if replicas > 0 {
		podNumber := hostnameParts[len(hostnameParts)-1]

		// Send the SQL statements to decommission the node
		log.Printf("Decommissioning node %s with graceful_stop.timeout of %s", podNumber, decommissionTimeout)

		statements := []string{
			fmt.Sprintf(`set global transient "cluster.graceful_stop.timeout" = '%s';`, decommissionTimeout),
			`set global transient "cluster.graceful_stop.force" = True;`,
			fmt.Sprintf(`set global transient "cluster.graceful_stop.min_availability"='%s';`, minAvailability),
			fmt.Sprintf(`alter cluster decommission '%s-%s'`, crateNodePrefix, podNumber),
		}

		for _, stmt := range statements {
			if err := sendSQLStatement(proto, stmt); err != nil {
				return err
			}
		}

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
