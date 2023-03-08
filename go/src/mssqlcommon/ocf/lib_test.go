// Copyright (C) Microsoft Corporation.

package ocf

import (
	"os"
	"testing"
)

func TestImportOcfExitCodes(t *testing.T) {
	// Note the values here are intentionally not the real values of these env vars.
	// In particular OCF_SUCCESS is not set to its real value of 0 to be able to check if it's correctly initialized
	var requiredEnvironmentVariables = map[string]string{
		"OCF_SUCCESS":           "1",
		"OCF_ERR_ARGS":          "2",
		"OCF_ERR_CONFIGURED":    "3",
		"OCF_ERR_GENERIC":       "4",
		"OCF_ERR_PERM":          "5",
		"OCF_ERR_UNIMPLEMENTED": "6",
		"OCF_FAILED_MASTER":     "7",
		"OCF_NOT_RUNNING":       "8",
		"OCF_RUNNING_MASTER":    "9",
	}

	// All vars should be 0 initially
	if OCF_SUCCESS != 0 {
		t.Fatal("OCF_SUCCESS is not 0. Test is not starting from clean slate.")
	}

	for key, value := range requiredEnvironmentVariables {
		os.Setenv(key, value)
	}

	// All vars set to valid values
	err := ImportOcfExitCodes()
	if err != nil {
		t.Fatalf("Expected ImportOcfExitCodes to succeed but it failed: %s", err)
	}

	// One var not set
	os.Unsetenv("OCF_SUCCESS")
	err = ImportOcfExitCodes()
	if err == nil {
		t.Fatal("Expected ImportOcfExitCodes to fail but it succeeded")
	}
	if err.Error() != "OCF_SUCCESS is set to an invalid value []" {
		t.Fatalf("ImportOcfExitCodes did not fail with an error about OCF_SUCCESS being unset: %s", err.Error())
	}

	// One var set to invalid value
	os.Setenv("OCF_SUCCESS", "A")
	err = ImportOcfExitCodes()
	if err == nil {
		t.Fatal("Expected ImportOcfExitCodes to fail but it succeeded")
	}
	if err.Error() != "OCF_SUCCESS is set to an invalid value [A]" {
		t.Fatalf("ImportOcfExitCodes did not fail with an error about OCF_SUCCESS being set to A: %s", err.Error())
	}
}
