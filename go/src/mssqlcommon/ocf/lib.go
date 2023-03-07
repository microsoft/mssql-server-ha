// Copyright (C) Microsoft Corporation.

// Package ocf contains items related to SQL Server on Pacemaker.
package ocf

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
)

type OcfExitCode int

var (
	OCF_ERR_CONFIGURED    OcfExitCode
	OCF_ERR_GENERIC       OcfExitCode
	OCF_ERR_ARGS          OcfExitCode
	OCF_ERR_PERM          OcfExitCode
	OCF_ERR_UNIMPLEMENTED OcfExitCode
	OCF_FAILED_MASTER     OcfExitCode
	OCF_NOT_RUNNING       OcfExitCode
	OCF_RUNNING_MASTER    OcfExitCode
	OCF_SUCCESS           OcfExitCode
)

// --------------------------------------------------------------------------------------
// Function: ImportOcfExitCodes
//
// Description:
//    Imports the OCF exit codes from corresponding environment variables.
//
func ImportOcfExitCodes() error {
	var err error

	OCF_ERR_CONFIGURED, err = importOcfExitCode("OCF_ERR_CONFIGURED")
	if err != nil {
		return err
	}

	OCF_ERR_GENERIC, err = importOcfExitCode("OCF_ERR_GENERIC")
	if err != nil {
		return err
	}

	OCF_ERR_ARGS, err = importOcfExitCode("OCF_ERR_ARGS")
	if err != nil {
		return err
	}

	OCF_ERR_PERM, err = importOcfExitCode("OCF_ERR_PERM")
	if err != nil {
		return err
	}

	OCF_ERR_UNIMPLEMENTED, err = importOcfExitCode("OCF_ERR_UNIMPLEMENTED")
	if err != nil {
		return err
	}

	OCF_FAILED_MASTER, err = importOcfExitCode("OCF_FAILED_MASTER")
	if err != nil {
		return err
	}

	OCF_NOT_RUNNING, err = importOcfExitCode("OCF_NOT_RUNNING")
	if err != nil {
		return err
	}

	OCF_RUNNING_MASTER, err = importOcfExitCode("OCF_RUNNING_MASTER")
	if err != nil {
		return err
	}

	OCF_SUCCESS, err = importOcfExitCode("OCF_SUCCESS")
	if err != nil {
		return err
	}

	return nil
}

func importOcfExitCode(name string) (OcfExitCode, error) {
	stringValue := os.Getenv(name)
	intValue, err := strconv.Atoi(stringValue)
	if err != nil {
		return 0, fmt.Errorf("%s is set to an invalid value [%s]", name, stringValue)
	}

	return OcfExitCode(intValue), nil
}

// Function: Exit
//
// Description:
//    Helper to exit with the given exit code and error.
//
func Exit(logger *log.Logger, exitCode int, err error) error {
	if err != nil {
		// Print each line individually to ensure that each line is prefixed with the logger prefix
		for _, line := range strings.Split(err.Error(), "\n") {
			logger.Println(line)
		}
	}

	os.Exit(exitCode)

	return nil
}

// KillCurrentProcessWhenParentExits uses prctl to request that the current process receive a SIGKILL if its parent process dies.
// This ensures that the helper processes spawned by the resource agent shell script gets cleaned up if Pacemaker kills the resource agent's shell process
// (due to op timeout, etc.)
//
// This uses syscall.Syscall instead of unix.Prctl since we don't have golang.org/x/sys/unix
func KillCurrentProcessWhenParentExits() error {
	_, _, errno := syscall.Syscall(syscall.SYS_PRCTL, syscall.PR_SET_PDEATHSIG, uintptr(syscall.SIGKILL), 0)
	if errno != 0 {
		return fmt.Errorf("prctl failed with errno %d", errno)
	}

	return nil
}

// Function: OcfExit
//
// Description:
//    Helper to exit with the given OCF exit code and error.
//
//    To distinguish OCF exit codes from other exit codes (like 1 for panics),
//    the actual exit code is 10 + the OCF exit code.
//
func OcfExit(logger *log.Logger, ocfExitCode OcfExitCode, err error) error {
	return Exit(logger, int(ocfExitCode)+10, err)
}
