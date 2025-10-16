package main

import (
	"fmt"
	"testing"
	"time"
)

// Test the extractNodeName function directly with comprehensive cases
func TestExtractNodeName(t *testing.T) {
	testCases := []struct {
		name             string
		hostname         string
		crateNodePrefix  string
		defaultPrefix    string
		expectedNodeName string
		shouldError      bool
	}{
		{
			name:             "Real world case with UUID",
			hostname:         "crate-data-hot-d84c10e6-d8fb-4d10-bf60-f9f2ea919a73-2",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "data-hot-2",
			shouldError:      false,
		},
		{
			name:             "Simple case without UUID",
			hostname:         "crate-data-hot-0",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "data-hot-0",
			shouldError:      false,
		},
		{
			name:             "Multiple UUID parts",
			hostname:         "crate-data-hot-uuid1-uuid2-1",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "data-hot-1",
			shouldError:      false,
		},
		{
			name:             "Custom prefix case",
			hostname:         "data-hot-2",
			crateNodePrefix:  "custom-prefix",
			defaultPrefix:    "data-hot",
			expectedNodeName: "custom-prefix-2",
			shouldError:      false,
		},
		{
			name:             "Custom prefix with complex hostname",
			hostname:         "crate-data-hot-uuid-0",
			crateNodePrefix:  "custom",
			defaultPrefix:    "data-hot",
			expectedNodeName: "custom-0",
			shouldError:      false,
		},
		{
			name:             "Invalid hostname format",
			hostname:         "invalid-hostname",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "",
			shouldError:      true,
		},
		{
			name:             "Hostname too short",
			hostname:         "crate-data-0",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "",
			shouldError:      true,
		},
		{
			name:             "Hostname not starting with crate",
			hostname:         "notcrate-data-hot-uuid-0",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "",
			shouldError:      true,
		},
		{
			name:             "Empty hostname",
			hostname:         "",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "",
			shouldError:      true,
		},
		{
			name:             "Single part prefix",
			hostname:         "crate-master-uuid-0",
			crateNodePrefix:  "master",
			defaultPrefix:    "master",
			expectedNodeName: "master-0",
			shouldError:      false,
		},
		{
			name:             "Multi-part prefix with UUID",
			hostname:         "crate-data-warm-d84c10e6-d8fb-4d10-bf60-f9f2ea919a73-1",
			crateNodePrefix:  "data-warm",
			defaultPrefix:    "data-warm",
			expectedNodeName: "data-warm-1",
			shouldError:      false,
		},
		{
			name:             "Hostname without crate prefix but with UUID pattern",
			hostname:         "master-d84c10e6-d8fb-4d10-bf60-2",
			crateNodePrefix:  "master",
			defaultPrefix:    "data-hot",
			expectedNodeName: "master-2",
			shouldError:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := extractNodeName(tc.hostname, tc.crateNodePrefix, tc.defaultPrefix)

			if tc.shouldError && err == nil {
				t.Errorf("Expected error but got none. Result: %s", result)
			} else if !tc.shouldError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			} else if !tc.shouldError && result != tc.expectedNodeName {
				t.Errorf("Expected '%s' but got '%s'", tc.expectedNodeName, result)
			}

			// Log successful cases for visibility
			if !tc.shouldError && err == nil {
				t.Logf("Successfully extracted node name: %s", result)
			}
		})
	}
}

func TestDecommissionIntegrationCases(t *testing.T) {
	cases := []struct {
		name            string
		hostname        string
		crateNodePrefix string
		defaultPrefix   string
		wantNodeName    string
		wantError       bool
	}{
		{
			name:            "Custom prefix overrides extraction",
			hostname:        "crate-data-hot-uuid-0",
			crateNodePrefix: "custom",
			defaultPrefix:   "data-hot",
			wantNodeName:    "custom-0",
			wantError:       false,
		},
		{
			name:            "Default prefix uses extraction",
			hostname:        "crate-data-hot-uuid-0",
			crateNodePrefix: "data-hot",
			defaultPrefix:   "data-hot",
			wantNodeName:    "data-hot-0",
			wantError:       false,
		},
		{
			name:            "Malformed hostname (too short)",
			hostname:        "crate-data-0",
			crateNodePrefix: "data-hot",
			defaultPrefix:   "data-hot",
			wantNodeName:    "",
			wantError:       true,
		},
		{
			name:            "Malformed hostname (not crate)",
			hostname:        "notcrate-data-hot-uuid-0",
			crateNodePrefix: "data-hot",
			defaultPrefix:   "data-hot",
			wantNodeName:    "",
			wantError:       true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, err := extractNodeName(c.hostname, c.crateNodePrefix, c.defaultPrefix)

			if c.wantError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !c.wantError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !c.wantError && result != c.wantNodeName {
				t.Errorf("Got nodeName=%q; want nodeName=%q", result, c.wantNodeName)
			}
		})
	}
}

func TestNodeNameExtractionEdgeCases(t *testing.T) {
	cases := []struct {
		hostname        string
		crateNodePrefix string
		defaultPrefix   string
		expectedNode    string
		shouldError     bool
	}{
		// Single prefix part
		{"crate-data-12345-0", "data", "data", "data-0", false},
		// Multiple prefix parts
		{"crate-foo-bar-baz-12345-0", "foo-bar-baz", "foo-bar-baz", "foo-bar-baz-0", false},
		// Missing UUID (should still work with our new logic)
		{"crate-data-hot-0", "data-hot", "data-hot", "data-hot-0", false},
		// Not starting with crate-
		{"notcrate-data-hot-uuid-0", "data-hot", "data-hot", "", true},
		// Empty hostname
		{"", "data-hot", "data-hot", "", true},
	}

	for _, c := range cases {
		t.Run(c.hostname, func(t *testing.T) {
			result, err := extractNodeName(c.hostname, c.crateNodePrefix, c.defaultPrefix)

			if c.shouldError && err == nil {
				t.Errorf("Expected error but got none. Result: %s", result)
			} else if !c.shouldError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			} else if !c.shouldError && result != c.expectedNode {
				t.Errorf("Expected '%s' but got '%s'", c.expectedNode, result)
			}
		})
	}
}

func TestDecommissionStatement(t *testing.T) {
	tests := []struct {
		name             string
		hostname         string
		crateNodePrefix  string
		defaultPrefix    string
		expectedNodeName string
		expectError      bool
	}{
		{
			name:             "Use prefix from hostname if flag is default",
			hostname:         "crate-master-bdc9bebd-d0c6-49a3-bcae-142f34d125fa-0",
			crateNodePrefix:  "master",
			defaultPrefix:    "master",
			expectedNodeName: "master-0",
			expectError:      false,
		},
		{
			name:             "Use prefix from hostname with data-hot",
			hostname:         "crate-data-hot-bdc9bebd-d0c6-49a3-bcae-142f34d125fa-0",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "data-hot-0",
			expectError:      false,
		},
		{
			name:             "Use provided flag if not default",
			hostname:         "crate-master-bdc9bebd-d0c6-49a3-bcae-142f34d125fa-0",
			crateNodePrefix:  "custom",
			defaultPrefix:    "data-hot",
			expectedNodeName: "custom-0",
			expectError:      false,
		},
		{
			name:             "Real world UUID case",
			hostname:         "crate-data-hot-d84c10e6-d8fb-4d10-bf60-f9f2ea919a73-2",
			crateNodePrefix:  "data-hot",
			defaultPrefix:    "data-hot",
			expectedNodeName: "data-hot-2",
			expectError:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualNodeName, err := extractNodeName(tt.hostname, tt.crateNodePrefix, tt.defaultPrefix)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if !tt.expectError {
				stmt := makeDecommissionStmt(actualNodeName)
				want := "alter cluster decommission '" + tt.expectedNodeName + "'"
				if stmt != want {
					t.Errorf("Got statement %q, want %q", stmt, want)
				}
				t.Logf("Generated correct statement: %q", stmt)
			}
		})
	}
}

func TestMakeDecommissionStmt(t *testing.T) {
	tests := []struct {
		nodeName string
		expected string
	}{
		{"data-hot-0", "alter cluster decommission 'data-hot-0'"},
		{"master-1", "alter cluster decommission 'master-1'"},
		{"custom-prefix-2", "alter cluster decommission 'custom-prefix-2'"},
	}

	for _, tt := range tests {
		result := makeDecommissionStmt(tt.nodeName)
		if result != tt.expected {
			t.Errorf("makeDecommissionStmt(%q) = %q, want %q", tt.nodeName, result, tt.expected)
		}
	}
}

func TestSplitHostname(t *testing.T) {
	tests := []struct {
		hostname string
		expected []string
	}{
		{"crate-data-hot-0", []string{"crate", "data", "hot", "0"}},
		{"crate-data-hot-d84c10e6-d8fb-4d10-bf60-f9f2ea919a73-2", []string{"crate", "data", "hot", "d84c10e6", "d8fb", "4d10", "bf60", "f9f2ea919a73", "2"}},
		{"simple", []string{"simple"}},
		{"", []string{""}},
	}

	for _, tt := range tests {
		result := splitHostname(tt.hostname)
		if len(result) != len(tt.expected) {
			t.Errorf("splitHostname(%q) returned %d parts, want %d", tt.hostname, len(result), len(tt.expected))
			continue
		}
		for i, part := range result {
			if part != tt.expected[i] {
				t.Errorf("splitHostname(%q)[%d] = %q, want %q", tt.hostname, i, part, tt.expected[i])
			}
		}
	}
}

func TestCalculateEffectiveTimeout(t *testing.T) {
	tests := []struct {
		name                          string
		flagTimeout                   string
		terminationGracePeriodSeconds *int64
		expectedTimeout               string
		expectError                   bool
	}{
		{
			name:                          "Use flag timeout when terminationGracePeriodSeconds is nil",
			flagTimeout:                   "7200s",
			terminationGracePeriodSeconds: nil,
			expectedTimeout:               "7200s",
			expectError:                   false,
		},
		{
			name:                          "Use flag timeout when terminationGracePeriodSeconds is default (30s)",
			flagTimeout:                   "3600s",
			terminationGracePeriodSeconds: int64Ptr(30),
			expectedTimeout:               "3600s",
			expectError:                   false,
		},
		{
			name:                          "Use derived timeout when terminationGracePeriodSeconds is higher",
			flagTimeout:                   "1800s",
			terminationGracePeriodSeconds: int64Ptr(600),
			expectedTimeout:               "480s", // 600 - 120 = 480
			expectError:                   false,
		},
		{
			name:                          "Apply minimum timeout when calculated value is too low",
			flagTimeout:                   "1800s",
			terminationGracePeriodSeconds: int64Ptr(300),
			expectedTimeout:               "360s", // 300 - 120 = 180, but minimum is 360
			expectError:                   false,
		},
		{
			name:                          "Large terminationGracePeriodSeconds",
			flagTimeout:                   "1800s",
			terminationGracePeriodSeconds: int64Ptr(1800),
			expectedTimeout:               "1680s", // 1800 - 120 = 1680
			expectError:                   false,
		},
		{
			name:                          "Invalid flag timeout format",
			flagTimeout:                   "invalid",
			terminationGracePeriodSeconds: int64Ptr(600),
			expectedTimeout:               "",
			expectError:                   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := calculateEffectiveTimeout(tt.flagTimeout, tt.terminationGracePeriodSeconds)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if !tt.expectError && result != tt.expectedTimeout {
				t.Errorf("Expected timeout %q, got %q", tt.expectedTimeout, result)
			}

			if !tt.expectError {
				t.Logf("Successfully calculated timeout: %s", result)
			}
		})
	}
}

func TestGetMinAvailabilityFromLabels(t *testing.T) {
	tests := []struct {
		name         string
		labels       map[string]string
		defaultValue string
		expected     string
	}{
		{
			name:         "No label present - use default",
			labels:       map[string]string{},
			defaultValue: "FULL",
			expected:     "FULL",
		},
		{
			name:         "Valid PRIMARIES value",
			labels:       map[string]string{"dc-util-min-availability": "PRIMARIES"},
			defaultValue: "FULL",
			expected:     "PRIMARIES",
		},
		{
			name:         "Valid NONE value",
			labels:       map[string]string{"dc-util-min-availability": "NONE"},
			defaultValue: "FULL",
			expected:     "NONE",
		},
		{
			name:         "Valid FULL value",
			labels:       map[string]string{"dc-util-min-availability": "FULL"},
			defaultValue: "PRIMARIES",
			expected:     "FULL",
		},
		{
			name:         "Invalid value - use default",
			labels:       map[string]string{"dc-util-min-availability": "INVALID"},
			defaultValue: "FULL",
			expected:     "FULL",
		},
		{
			name:         "Other labels present but not target label",
			labels:       map[string]string{"other-label": "value", "another": "test"},
			defaultValue: "PRIMARIES",
			expected:     "PRIMARIES",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getMinAvailabilityFromLabels(tt.labels, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			} else {
				t.Logf("Successfully got min-availability: %s", result)
			}
		})
	}
}

func TestGetGracefulStopForceFromLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{
			name:     "No label present - use default true",
			labels:   map[string]string{},
			expected: true,
		},
		{
			name:     "TRUE value",
			labels:   map[string]string{"dc-util-graceful-stop": "TRUE"},
			expected: true,
		},
		{
			name:     "true value",
			labels:   map[string]string{"dc-util-graceful-stop": "true"},
			expected: true,
		},
		{
			name:     "True value",
			labels:   map[string]string{"dc-util-graceful-stop": "True"},
			expected: true,
		},
		{
			name:     "FALSE value",
			labels:   map[string]string{"dc-util-graceful-stop": "FALSE"},
			expected: false,
		},
		{
			name:     "false value",
			labels:   map[string]string{"dc-util-graceful-stop": "false"},
			expected: false,
		},
		{
			name:     "False value",
			labels:   map[string]string{"dc-util-graceful-stop": "False"},
			expected: false,
		},
		{
			name:     "Invalid value - use default true",
			labels:   map[string]string{"dc-util-graceful-stop": "maybe"},
			expected: true,
		},
		{
			name:     "Other labels present but not target label",
			labels:   map[string]string{"other-label": "value", "another": "test"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getGracefulStopForceFromLabels(tt.labels)
			if result != tt.expected {
				t.Errorf("Expected %t, got %t", tt.expected, result)
			} else {
				t.Logf("Successfully got graceful stop force: %t", result)
			}
		})
	}
}

func TestStatefulSetLabelIntegration(t *testing.T) {
	tests := []struct {
		name                    string
		labels                  map[string]string
		flagMinAvailability     string
		expectedMinAvailability string
		expectedForce           bool
		description             string
	}{
		{
			name:                    "No labels - use CLI defaults",
			labels:                  map[string]string{},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "FULL",
			expectedForce:           true,
			description:             "Default behavior when no labels present",
		},
		{
			name: "Both labels present - override CLI",
			labels: map[string]string{
				"dc-util-min-availability": "PRIMARIES",
				"dc-util-graceful-stop":    "false",
			},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "PRIMARIES",
			expectedForce:           false,
			description:             "Labels override CLI parameters",
		},
		{
			name: "Only min-availability label",
			labels: map[string]string{
				"dc-util-min-availability": "NONE",
			},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "NONE",
			expectedForce:           true,
			description:             "Partial label override - force uses default",
		},
		{
			name: "Only force label",
			labels: map[string]string{
				"dc-util-graceful-stop": "false",
			},
			flagMinAvailability:     "PRIMARIES",
			expectedMinAvailability: "PRIMARIES",
			expectedForce:           false,
			description:             "Partial label override - min-availability uses CLI",
		},
		{
			name: "Invalid values fallback to defaults",
			labels: map[string]string{
				"dc-util-min-availability": "INVALID",
				"dc-util-graceful-stop":    "maybe",
			},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "FULL",
			expectedForce:           true,
			description:             "Invalid label values use defaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test min-availability
			resultMinAvail := getMinAvailabilityFromLabels(tt.labels, tt.flagMinAvailability)
			if resultMinAvail != tt.expectedMinAvailability {
				t.Errorf("Min-availability: expected %q, got %q", tt.expectedMinAvailability, resultMinAvail)
			}

			// Test graceful stop force
			resultForce := getGracefulStopForceFromLabels(tt.labels)
			if resultForce != tt.expectedForce {
				t.Errorf("Graceful stop force: expected %t, got %t", tt.expectedForce, resultForce)
			}

			t.Logf("%s: min-availability=%s, force=%t", tt.description, resultMinAvail, resultForce)
		})
	}
}

func TestStatefulSetLabelIntegrationWithDisabled(t *testing.T) {
	tests := []struct {
		name                    string
		labels                  map[string]string
		flagMinAvailability     string
		expectedMinAvailability string
		expectedForce           bool
		expectedDisabled        bool
		description             string
	}{
		{
			name:                    "No labels - all defaults",
			labels:                  map[string]string{},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "FULL",
			expectedForce:           true,
			expectedDisabled:        false,
			description:             "Default behavior when no labels present",
		},
		{
			name: "All labels present - complete override",
			labels: map[string]string{
				"dc-util-min-availability": "PRIMARIES",
				"dc-util-graceful-stop":    "false",
				"dc-util-disabled":         "true",
			},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "PRIMARIES",
			expectedForce:           false,
			expectedDisabled:        true,
			description:             "All labels override defaults",
		},
		{
			name: "Disabled true with other settings",
			labels: map[string]string{
				"dc-util-disabled":         "true",
				"dc-util-min-availability": "NONE",
			},
			flagMinAvailability:     "FULL",
			expectedMinAvailability: "NONE",
			expectedForce:           true,
			expectedDisabled:        true,
			description:             "When disabled, other settings still parsed but won't be used",
		},
		{
			name: "Disabled false - normal operation",
			labels: map[string]string{
				"dc-util-disabled":      "false",
				"dc-util-graceful-stop": "false",
			},
			flagMinAvailability:     "PRIMARIES",
			expectedMinAvailability: "PRIMARIES",
			expectedForce:           false,
			expectedDisabled:        false,
			description:             "Explicitly disabled=false allows normal operation",
		},
		{
			name: "Invalid disabled value - use default false",
			labels: map[string]string{
				"dc-util-disabled":         "maybe",
				"dc-util-min-availability": "FULL",
			},
			flagMinAvailability:     "PRIMARIES",
			expectedMinAvailability: "FULL",
			expectedForce:           true,
			expectedDisabled:        false,
			description:             "Invalid disabled value falls back to false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test min-availability
			resultMinAvail := getMinAvailabilityFromLabels(tt.labels, tt.flagMinAvailability)
			if resultMinAvail != tt.expectedMinAvailability {
				t.Errorf("Min-availability: expected %q, got %q", tt.expectedMinAvailability, resultMinAvail)
			}

			// Test graceful stop force
			resultForce := getGracefulStopForceFromLabels(tt.labels)
			if resultForce != tt.expectedForce {
				t.Errorf("Graceful stop force: expected %t, got %t", tt.expectedForce, resultForce)
			}

			// Test disabled
			resultDisabled := getDisabledFromLabels(tt.labels)
			if resultDisabled != tt.expectedDisabled {
				t.Errorf("Disabled: expected %t, got %t", tt.expectedDisabled, resultDisabled)
			}

			t.Logf("%s: min-availability=%s, force=%t, disabled=%t",
				tt.description, resultMinAvail, resultForce, resultDisabled)
		})
	}
}

func TestGetDisabledFromLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{
			name:     "No label present - use default false",
			labels:   map[string]string{},
			expected: false,
		},
		{
			name:     "TRUE value",
			labels:   map[string]string{"dc-util-disabled": "TRUE"},
			expected: true,
		},
		{
			name:     "true value",
			labels:   map[string]string{"dc-util-disabled": "true"},
			expected: true,
		},
		{
			name:     "True value",
			labels:   map[string]string{"dc-util-disabled": "True"},
			expected: true,
		},
		{
			name:     "FALSE value",
			labels:   map[string]string{"dc-util-disabled": "FALSE"},
			expected: false,
		},
		{
			name:     "false value",
			labels:   map[string]string{"dc-util-disabled": "false"},
			expected: false,
		},
		{
			name:     "False value",
			labels:   map[string]string{"dc-util-disabled": "False"},
			expected: false,
		},
		{
			name:     "Invalid value - use default false",
			labels:   map[string]string{"dc-util-disabled": "maybe"},
			expected: false,
		},
		{
			name:     "Other labels present but not target label",
			labels:   map[string]string{"other-label": "value", "another": "test"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDisabledFromLabels(tt.labels)
			if result != tt.expected {
				t.Errorf("Expected %t, got %t", tt.expected, result)
			} else {
				t.Logf("Successfully got disabled: %t", result)
			}
		})
	}
}

func TestSendSQLStatementDryRun(t *testing.T) {
	tests := []struct {
		name   string
		proto  string
		stmt   string
		dryRun bool
	}{
		{
			name:   "Dry run mode - logs statement without sending",
			proto:  "https",
			stmt:   "alter cluster decommission 'data-hot-0'",
			dryRun: true,
		},
		{
			name:   "Dry run mode - set global statement",
			proto:  "http",
			stmt:   `set global transient "cluster.graceful_stop.timeout" = '7200s';`,
			dryRun: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// In dry-run mode, sendSQLStatement should not return an error
			// and should just log the statements
			err := sendSQLStatement(tt.proto, tt.stmt, tt.dryRun)
			if err != nil {
				t.Errorf("sendSQLStatement in dry-run mode should not return error, got: %v", err)
			}
			t.Logf("Successfully tested dry-run for statement: %s", tt.stmt)
		})
	}
}

func TestDisabledAndDryRunInteraction(t *testing.T) {
	tests := []struct {
		name        string
		labels      map[string]string
		dryRun      bool
		shouldSkip  bool
		description string
	}{
		{
			name:        "Disabled=true, dry-run=false - should skip entirely",
			labels:      map[string]string{"dc-util-disabled": "true"},
			dryRun:      false,
			shouldSkip:  true,
			description: "When disabled, dry-run flag is irrelevant",
		},
		{
			name:        "Disabled=true, dry-run=true - should skip entirely",
			labels:      map[string]string{"dc-util-disabled": "true"},
			dryRun:      true,
			shouldSkip:  true,
			description: "When disabled, dry-run flag is irrelevant",
		},
		{
			name:        "Disabled=false, dry-run=true - should run in dry-run mode",
			labels:      map[string]string{"dc-util-disabled": "false"},
			dryRun:      true,
			shouldSkip:  false,
			description: "When not disabled, dry-run mode should work normally",
		},
		{
			name:        "No disabled label, dry-run=true - should run in dry-run mode",
			labels:      map[string]string{},
			dryRun:      true,
			shouldSkip:  false,
			description: "Default disabled=false allows dry-run to work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disabled := getDisabledFromLabels(tt.labels)

			if disabled != tt.shouldSkip {
				t.Errorf("Expected disabled=%t, got disabled=%t", tt.shouldSkip, disabled)
			}

			// Test that sendSQLStatement works correctly in dry-run mode when not disabled
			if !disabled && tt.dryRun {
				err := sendSQLStatement("https", "test statement", tt.dryRun)
				if err != nil {
					t.Errorf("sendSQLStatement in dry-run mode should not error when not disabled, got: %v", err)
				}
			}

			t.Logf("%s: disabled=%t, dry-run=%t", tt.description, disabled, tt.dryRun)
		})
	}
}

func TestGetNoPreStartFromLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected bool
	}{
		{
			name:     "No label present - use default false",
			labels:   map[string]string{},
			expected: false,
		},
		{
			name:     "TRUE value",
			labels:   map[string]string{"dc-util-no-prestart": "TRUE"},
			expected: true,
		},
		{
			name:     "true value",
			labels:   map[string]string{"dc-util-no-prestart": "true"},
			expected: true,
		},
		{
			name:     "True value",
			labels:   map[string]string{"dc-util-no-prestart": "True"},
			expected: true,
		},
		{
			name:     "FALSE value",
			labels:   map[string]string{"dc-util-no-prestart": "FALSE"},
			expected: false,
		},
		{
			name:     "false value",
			labels:   map[string]string{"dc-util-no-prestart": "false"},
			expected: false,
		},
		{
			name:     "False value",
			labels:   map[string]string{"dc-util-no-prestart": "False"},
			expected: false,
		},
		{
			name:     "Invalid value - use default false",
			labels:   map[string]string{"dc-util-no-prestart": "maybe"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getNoPreStartFromLabels(tt.labels)
			if result != tt.expected {
				t.Errorf("Expected %t, got %t", tt.expected, result)
			}
		})
	}
}

func TestGetPreStopRoutingAllocationFromLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected string
	}{
		{
			name:     "No label present - use default",
			labels:   map[string]string{},
			expected: "new_primaries",
		},
		{
			name:     "Valid new_primaries value",
			labels:   map[string]string{"dc-util-pre-stop-routing-allocation": "new_primaries"},
			expected: "new_primaries",
		},
		{
			name:     "Valid all value",
			labels:   map[string]string{"dc-util-pre-stop-routing-allocation": "all"},
			expected: "all",
		},
		{
			name:     "Invalid value - use default",
			labels:   map[string]string{"dc-util-pre-stop-routing-allocation": "invalid"},
			expected: "new_primaries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPreStopRoutingAllocationFromLabels(tt.labels)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestCreateAndRemoveLockFile(t *testing.T) {
	// Test dry-run mode
	t.Run("Create lock file - dry run", func(t *testing.T) {
		err := createLockFile(true)
		if err != nil {
			t.Errorf("createLockFile in dry-run should not error, got: %v", err)
		}
	})

	t.Run("Remove lock file - dry run", func(t *testing.T) {
		err := removeLockFile(true)
		if err != nil {
			t.Errorf("removeLockFile in dry-run should not error, got: %v", err)
		}
	})
}

func TestLockFileExists(t *testing.T) {
	// This test just verifies the function doesn't panic
	// Actual file operations would require filesystem setup
	exists := lockFileExists()
	t.Logf("Lock file exists: %t", exists)
}

func TestWaitForClusterReadiness(t *testing.T) {
	// Test dry-run mode
	t.Run("Wait for cluster readiness - dry run", func(t *testing.T) {
		err := waitForClusterReadiness("https", time.Minute, true)
		if err != nil {
			t.Errorf("waitForClusterReadiness in dry-run should not error, got: %v", err)
		}
	})
}

func TestResetRoutingIntegration(t *testing.T) {
	tests := []struct {
		name                      string
		labels                    map[string]string
		dryRun                    bool
		expectedNoPreStart        bool
		expectedRoutingAllocation string
		description               string
	}{
		{
			name:                      "Normal operation - all defaults",
			labels:                    map[string]string{},
			dryRun:                    false,
			expectedNoPreStart:        false,
			expectedRoutingAllocation: "new_primaries",
			description:               "Default behavior",
		},
		{
			name: "PreStart disabled",
			labels: map[string]string{
				"dc-util-no-prestart": "true",
			},
			dryRun:                    false,
			expectedNoPreStart:        true,
			expectedRoutingAllocation: "new_primaries",
			description:               "PostStart should be skipped",
		},
		{
			name: "Custom routing allocation with dry-run",
			labels: map[string]string{
				"dc-util-pre-stop-routing-allocation": "all",
				"dc-util-no-prestart":                 "false",
			},
			dryRun:                    true,
			expectedNoPreStart:        false,
			expectedRoutingAllocation: "all",
			description:               "Custom routing with dry-run mode",
		},
		{
			name: "All routing labels configured",
			labels: map[string]string{
				"dc-util-no-prestart":                 "false",
				"dc-util-pre-stop-routing-allocation": "new_primaries",
			},
			dryRun:                    false,
			expectedNoPreStart:        false,
			expectedRoutingAllocation: "new_primaries",
			description:               "Complete routing configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test no-prestart label parsing
			noPreStart := getNoPreStartFromLabels(tt.labels)
			if noPreStart != tt.expectedNoPreStart {
				t.Errorf("Expected no-prestart=%t, got %t", tt.expectedNoPreStart, noPreStart)
			}

			// Test pre-stop routing allocation label parsing
			routingAllocation := getPreStopRoutingAllocationFromLabels(tt.labels)
			if routingAllocation != tt.expectedRoutingAllocation {
				t.Errorf("Expected routing allocation=%s, got %s", tt.expectedRoutingAllocation, routingAllocation)
			}

			// Test lock file operations in dry-run mode
			if tt.dryRun {
				err := createLockFile(true)
				if err != nil {
					t.Errorf("createLockFile in dry-run should not error: %v", err)
				}

				err = removeLockFile(true)
				if err != nil {
					t.Errorf("removeLockFile in dry-run should not error: %v", err)
				}
			}

			t.Logf("%s: no-prestart=%t, routing=%s, dry-run=%t",
				tt.description, noPreStart, routingAllocation, tt.dryRun)
		})
	}
}

func TestCompleteDryRunWorkflow(t *testing.T) {
	// This test demonstrates what a complete dry-run should look like
	// when dc_util is NOT disabled
	tests := []struct {
		name        string
		labels      map[string]string
		description string
	}{
		{
			name: "Complete workflow with routing allocation",
			labels: map[string]string{
				"dc-util-disabled":                    "false", // Important: NOT disabled
				"dc-util-pre-stop-routing-allocation": "new_primaries",
				"dc-util-min-availability":            "PRIMARIES",
				"dc-util-graceful-stop":               "true",
			},
			description: "Shows complete dry-run workflow including routing allocation",
		},
		{
			name: "Workflow with custom routing allocation",
			labels: map[string]string{
				"dc-util-pre-stop-routing-allocation": "all",
				"dc-util-min-availability":            "FULL",
			},
			description: "Shows workflow with custom routing allocation value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the label parsing functions that would be used
			disabled := getDisabledFromLabels(tt.labels)
			if disabled {
				t.Skip("Test is for non-disabled scenarios")
			}

			preStopRouting := getPreStopRoutingAllocationFromLabels(tt.labels)
			minAvail := getMinAvailabilityFromLabels(tt.labels, "FULL")
			gracefulStop := getGracefulStopForceFromLabels(tt.labels)

			t.Logf("Expected dry-run workflow for: %s", tt.description)
			t.Logf("1. Pre-stop routing allocation: SET GLOBAL TRANSIENT \"cluster.routing.allocation.enable\" = '%s';", preStopRouting)
			t.Logf("2. Lock file creation")
			t.Logf("3. Graceful stop settings (timeout, force=%t, min_availability='%s')", gracefulStop, minAvail)
			t.Logf("4. Decommission statement")

			// Test that dry-run mode works for these operations
			routingStmt := fmt.Sprintf(`SET GLOBAL TRANSIENT "cluster.routing.allocation.enable" = '%s';`, preStopRouting)
			err := sendSQLStatement("https", routingStmt, true) // dry-run mode
			if err != nil {
				t.Errorf("Routing allocation statement in dry-run should not error: %v", err)
			}

			err = createLockFile(true) // dry-run mode
			if err != nil {
				t.Errorf("Lock file creation in dry-run should not error: %v", err)
			}
		})
	}
}

func int64Ptr(i int64) *int64 {
	return &i
}
