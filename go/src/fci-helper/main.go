// Copyright (C) Microsoft Corporation.

package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"

	"mssqlcommon"
	mssqlocf "mssqlcommon/ocf"
)

/*
	Program to be called from the mssql:fci resource agent to monitor SQL Server health.

	Determines the health of the specified SQL Server instance based on
		1) whether a connection can be established to the instance, and
		2) the results of the 'sp_server_diagnostics' stored procedure
*/

func main() {
	stdout := log.New(os.Stdout, "", log.LstdFlags)
	stderr := log.New(os.Stderr, "ERROR: ", log.LstdFlags)

	err := mssqlocf.KillCurrentProcessWhenParentExits()
	if err != nil {
		mssqlocf.Exit(stderr, 1, fmt.Errorf("Unexpected error: %s", err))
	}

	err = doMain(stdout, stderr)
	if err != nil {
		mssqlocf.Exit(stderr, 1, fmt.Errorf("Unexpected error: %s", err))
	}
}

func doMain(stdout *log.Logger, stderr *log.Logger) error {
	var (
		hostname             string
		sqlPort              uint64
		credentialsFile      string
		applicationName      string
		rawConnectionTimeout int64
		rawHealthThreshold   uint
		rawMonitorTimeout    int64		

		action string

		virtualServerName string
	)

	flag.StringVar(&hostname, "hostname", "localhost", "The hostname of the SQL Server instance to connect to. Default: localhost")
	flag.Uint64Var(&sqlPort, "port", 0, "The port on which the instance is listening for logins.")
	flag.StringVar(&credentialsFile, "credentials-file", "", "The path to the credentials file.")
	flag.StringVar(&applicationName, "application-name", "", "The application name to use for the T-SQL connection.")
	flag.Int64Var(&rawConnectionTimeout, "connection-timeout", 30, "The connection timeout in seconds. "+
		"The application will retry connecting to the instance until this time elapses. Default: 30")
	flag.UintVar(&rawHealthThreshold, "health-threshold", uint(mssqlcommon.ServerCriticalError), "The instance health threshold. Default: 3 (SERVER_CRITICAL_ERROR)")

	flag.StringVar(&action, "action", "", `One of --start, --monitor
	start: Start the replica on this node.
	monitor: Monitor the replica on this node.`)

	flag.StringVar(&virtualServerName, "virtual-server-name", "", "The virtual server name that should be set on the SQL Server instance.")
	flag.Int64Var(&rawMonitorTimeout, "monitor-interval-timeout", 0, "The monitor interval timeout in seconds. "+
	"For FCI this is expected to be always Default: 0")

	flag.Parse()

	stdout.Printf(
		"fci-helper invoked with hostname [%s]; port [%d]; credentials-file [%s]; application-name [%s]; connection-timeout [%d]; health-threshold [%d]; action [%s]\n",
		hostname, sqlPort,
		credentialsFile,
		applicationName,
		rawConnectionTimeout, rawHealthThreshold,
		action)

	switch action {
	case "start":
		stdout.Printf(
			"fci-helper invoked with virtual-server-name [%s]\n",
			virtualServerName)

	case "monitor":
		stdout.Printf(
			"fci-helper invoked with virtual-server-name [%s]\n",
			virtualServerName)
	}

	if hostname == "" {
		return errors.New("a valid hostname must be specified using --hostname")
	}

	if sqlPort == 0 {
		return errors.New("a valid port number must be specified using --port")
	}

	if credentialsFile == "" {
		return errors.New("a valid path to a credentials file must be specified using --credentials-file")
	}

	if applicationName == "" {
		return errors.New("a valid application name must be specified using --application-name")
	}

	if action == "" {
		return errors.New("a valid action must be specified using --action")
	}

	if action == "start" || action == "monitor" {
		if virtualServerName == "" {
			return errors.New("a valid virtual server name must be specified using --virtual-server-name")
		}
	}

	err := mssqlocf.ImportOcfExitCodes()
	if err != nil {
		return err
	}

	connectionTimeout := time.Duration(rawConnectionTimeout) * time.Second
	monitorTimeout := time.Duration(rawMonitorTimeout) * time.Second	
	healthThreshold := mssqlcommon.ServerHealth(rawHealthThreshold)

	sqlUsername, sqlPassword, err := mssqlcommon.ReadCredentialsFile(credentialsFile)
	if err != nil {
		return mssqlocf.OcfExit(stderr, mssqlocf.OCF_ERR_ARGS, fmt.Errorf("Could not read credentials file: %s", err))
	}

	db, err := mssqlcommon.OpenDBWithHealthCheck(
		hostname, sqlPort,
		sqlUsername, sqlPassword,
		applicationName,
		connectionTimeout, connectionTimeout,
		monitorTimeout,
		stdout)
	if err != nil {
		switch serverUnhealthyError := err.(type) {
		case *mssqlcommon.ServerUnhealthyError:
			if serverUnhealthyError.RawValue <= healthThreshold {
				return mssqlocf.OcfExit(stderr, mssqlocf.OCF_ERR_GENERIC, fmt.Errorf(
					"Instance health status %d is at or below the threshold value of %d",
					serverUnhealthyError.RawValue, healthThreshold))
			}

			stdout.Printf("Instance health status %d is greater than the threshold value of %d\n", serverUnhealthyError.RawValue, healthThreshold)

		default:
			return err
		}
	}
	defer db.Close()

	var ocfExitCode mssqlocf.OcfExitCode

	switch action {
	case "start":
		ocfExitCode, err = start(db, virtualServerName, stdout)

	case "monitor":
		ocfExitCode, err = monitor(db, virtualServerName, stdout)

	default:
		return fmt.Errorf("unknown value for --action %s", action)
	}

	return mssqlocf.OcfExit(stderr, ocfExitCode, err)
}

// Function: start
//
// Description:
//    Implements the OCF "start" action
//
func start(db *sql.DB, virtualServerName string, stdout *log.Logger) (mssqlocf.OcfExitCode, error) {
	stdout.Printf("Setting local server name to %s...\n", virtualServerName)

	err := mssqlcommon.SetLocalServerName(db, virtualServerName)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not set local server name: %s", err)
	}

	return monitor(db, virtualServerName, stdout)
}

// Function: monitor
//
// Description:
//    Implements the OCF "monitor" action
//
func monitor(db *sql.DB, virtualServerName string, stdout *log.Logger) (mssqlocf.OcfExitCode, error) {
	stdout.Println("Querying local server name...")

	currentServerName, err := mssqlcommon.GetLocalServerName(db)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not query local server name: %s", err)
	}

	stdout.Printf("Local server name is %s\n", currentServerName)

	if !strings.EqualFold(currentServerName, virtualServerName) {
		return mssqlocf.OCF_ERR_ARGS, fmt.Errorf("Expected local server name to be %s but it was %s", virtualServerName, currentServerName)
	}

	return mssqlocf.OCF_SUCCESS, nil
}
