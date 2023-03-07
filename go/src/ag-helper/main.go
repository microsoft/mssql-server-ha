// Copyright (C) Microsoft Corporation.

package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"

	"mssqlcommon"
	mssqlag "mssqlcommon/ag"
	mssqlocf "mssqlcommon/ocf"
)

const (
	// promotionScoreCurrentMaster is the promotion score set on a replica that's already the master.
	// This is the highest value to motivate Pacemaker to keep it the master.
	promotionScoreCurrentMaster = "20"

	// promotionScoreCanBePromoted is the promotion score set on a replica that can be promoted if necessary.
	// This is lower than the score set on the current master but still greater than 0.
	promotionScoreCanBePromoted = "10"

	// promotionScoreShouldNotBePromoted is the promotion score set on a replica that should not be promoted.
	promotionScoreShouldNotBePromoted = "-INFINITY"
)

func main() {
	stdout := log.New(os.Stdout, "", log.LstdFlags)
	stderr := log.New(os.Stderr, "ERROR: ", log.LstdFlags)
	promotionScoreOut := log.New(os.Stderr, "PROMOTION_SCORE: ", 0)
	sequenceNumberOut := log.New(os.Stderr, "SEQUENCE_NUMBER: ", 0)
	leaseExpiryOut := log.New(os.Stderr, "LEASE_EXPIRY: ", 0)

	err := mssqlocf.KillCurrentProcessWhenParentExits()
	if err != nil {
		mssqlocf.Exit(stderr, 1, fmt.Errorf("Unexpected error: %s", err))
	}

	err = doMain(stdout, stderr, promotionScoreOut, sequenceNumberOut, leaseExpiryOut)
	if err != nil {
		mssqlocf.Exit(stderr, 1, fmt.Errorf("Unexpected error: %s", err))
	}
}

func doMain(stdout *log.Logger, stderr *log.Logger, promotionScoreOut *log.Logger, sequenceNumberOut *log.Logger, leaseExpiryOut *log.Logger) error {
	var (
		hostname             string
		sqlPort              uint64
		agName               string
		credentialsFile      string
		applicationName      string
		rawConnectionTimeout int64
		rawHealthThreshold   uint

		action string

		skipPreCheck                               bool
		sequenceNumbers                            string
		newMaster                                  string
		requiredSynchronizedSecondariesToCommitArg int
		currentMaster                              string
		disablePrimaryOnQuorumTimeoutAfter         int64
		primaryWriteLeaseDuration                  int64
		rawMonitorTimeout                          int64
		leaseExpiry                                string
	)

	flag.StringVar(&hostname, "hostname", "localhost", "The hostname of the SQL Server instance to connect to. Default: localhost")
	flag.Uint64Var(&sqlPort, "port", 0, "The port on which the instance is listening for logins.")
	flag.StringVar(&agName, "ag-name", "", "The name of the Availability Group")
	flag.StringVar(&credentialsFile, "credentials-file", "", "The path to the credentials file.")
	flag.StringVar(&applicationName, "application-name", "", "The application name to use for the T-SQL connection.")
	flag.Int64Var(&rawConnectionTimeout, "connection-timeout", 30, "The connection timeout in seconds. "+
		"The application will retry connecting to the instance until this time elapses. Default: 30")
	flag.UintVar(&rawHealthThreshold, "health-threshold", uint(mssqlcommon.ServerCriticalError), "The instance health threshold. Default: 3 (SERVER_CRITICAL_ERROR)")

	flag.StringVar(&action, "action", "", `One of --start, --stop, --monitor, --pre-promote, --promote, --demote
	start: Start the replica on this node.
	stop: Stop the replica on this node.
	monitor: Monitor the replica on this node.
	pre-start: Before starting a new clone.
	post-stop: After stopping an existing clone.
	pre-promote: Fetch the sequence number of the replica on this node.
	promote: Promote the replica on this node to master.
	demote: Demote the replica on this node to slave.`)

	flag.BoolVar(&skipPreCheck, "skip-precheck", false, "Promote the replica on this node to master even if its availability mode is ASYNCHRONOUS_COMMIT.")
	flag.StringVar(&sequenceNumbers, "sequence-numbers", "", "The sequence numbers of each replica as stored in the cluster. The value is expected to be in the format returned by attrd_updater -QA")
	flag.StringVar(&newMaster, "new-master", "", "The name of the node that is being promoted.")
	flag.IntVar(&requiredSynchronizedSecondariesToCommitArg, "required-synchronized-secondaries-to-commit", -1, "Explicit value for REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT. If not provided, the value will be derived from the number of SYNCHRONOUS_COMMIT replicas.")
	flag.StringVar(&currentMaster, "current-master", "", "The name of the node that is currently the master.")
	flag.Int64Var(&disablePrimaryOnQuorumTimeoutAfter, "disable-primary-on-quorum-timeout-after", 60, "How long the primary can block on committing a configuration update before the agent stops renewing its write lease, in seconds. Default: 60")
	flag.Int64Var(&primaryWriteLeaseDuration, "primary-write-lease-duration", -1, "Primary write lease duration, in seconds.")
	flag.Int64Var(&rawMonitorTimeout, "monitor-interval-timeout", 0, "The monitor interval timeout in seconds. "+
		"The aghelper will attempt new connection request to the instance after connection timeout, this goes until this time elapses. Default: 0")
	flag.StringVar(&leaseExpiry, "lease-expiry", "", "The lease expiry time. The value is expected to be in the format returned by attrd_updater -QA")

	flag.Parse()
	if hostname == "" {
		hostname = "localhost"
	}
	stdout.Printf(
		"ag-helper invoked with hostname [%s]; port [%d]; ag-name [%s]; credentials-file [%s]; application-name [%s]; connection-timeout [%d]; health-threshold [%d]; action [%s]\n",
		hostname,
		sqlPort,
		agName,
		credentialsFile,
		applicationName,
		rawConnectionTimeout,
		rawHealthThreshold,
		action)

	switch action {
	case "start":
		stdout.Printf(
			"ag-helper invoked with sequence-numbers [...]; required-synchronized-secondaries-to-commit [%d]; current-master [%s]; disable-primary-on-quorum-timeout-after [%d]; primary-write-lease-duration [%d]",
			requiredSynchronizedSecondariesToCommitArg, currentMaster, disablePrimaryOnQuorumTimeoutAfter, primaryWriteLeaseDuration,
		)

	case "monitor":
		stdout.Printf(
			"ag-helper invoked with required-synchronized-secondaries-to-commit [%d]; current-master [%s]; disable-primary-on-quorum-timeout-after [%d]; primary-write-lease-duration [%d]; monitor-interval-timeout [%d]",
			requiredSynchronizedSecondariesToCommitArg, currentMaster, disablePrimaryOnQuorumTimeoutAfter, primaryWriteLeaseDuration, rawMonitorTimeout,
		)

	case "pre-start":
		stdout.Printf(
			"ag-helper invoked with required-synchronized-secondaries-to-commit [%d]\n",
			requiredSynchronizedSecondariesToCommitArg)

	case "post-stop":
		stdout.Printf(
			"ag-helper invoked with required-synchronized-secondaries-to-commit [%d]\n",
			requiredSynchronizedSecondariesToCommitArg)

	case "promote":
		stdout.Printf(
			"ag-helper invoked with skip-precheck [%t]; sequence-numbers [...]; new-master [%s]; required-synchronized-secondaries-to-commit [%d]; disable-primary-on-quorum-timeout-after [%d]; primary-write-lease-duration [%d]",
			skipPreCheck, newMaster, requiredSynchronizedSecondariesToCommitArg, disablePrimaryOnQuorumTimeoutAfter, primaryWriteLeaseDuration,
		)
	}

	if hostname == "" {
		return errors.New("a valid hostname must be specified using --hostname")
	}

	if sqlPort == 0 {
		return errors.New("a valid port number must be specified using --port")
	}

	if agName == "" {
		return errors.New("a valid AG name must be specified using --ag-name")
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

	if (action == "start" || action == "monitor" || action == "promote") && primaryWriteLeaseDuration < 0 {
		return errors.New("a valid value must be specified using --primary-write-lease-duration")
	}

	if (action == "promote") && newMaster == "" {
		return errors.New("a valid hostname must be specified using --new-master")
	}

	err := mssqlocf.ImportOcfExitCodes()
	if err != nil {
		return err
	}

	connectionTimeout := time.Duration(rawConnectionTimeout) * time.Second
	monitorTimeout := time.Duration(rawMonitorTimeout) * time.Second	
	healthThreshold := mssqlcommon.ServerHealth(rawHealthThreshold)

	var requiredSynchronizedSecondariesToCommit *uint
	if requiredSynchronizedSecondariesToCommitArg != -1 {
		if requiredSynchronizedSecondariesToCommitArg < 0 || requiredSynchronizedSecondariesToCommitArg > math.MaxInt32 {
			return mssqlocf.OcfExit(stderr, mssqlocf.OCF_ERR_CONFIGURED, errors.New(
				"--required-synchronized-secondaries-to-commit must be set to a valid integer between 0 and one less than the number of SYNCHRONOUS_COMMIT replicas (both inclusive)"))
		}

		requiredSynchronizedSecondariesToCommitUint := uint(requiredSynchronizedSecondariesToCommitArg)
		requiredSynchronizedSecondariesToCommit = &requiredSynchronizedSecondariesToCommitUint
	}

	sqlUsername, sqlPassword, err := mssqlcommon.ReadCredentialsFile(credentialsFile)
	if err != nil {
		return mssqlocf.OcfExit(stderr, mssqlocf.OCF_ERR_ARGS, fmt.Errorf("Could not read credentials file: %s", err))
	}

	var db *sql.DB

	switch action {
	case "start", "monitor", "promote", "pre-promote":
		// Ensure instance is healthy before checking AG health
		db, err = mssqlcommon.OpenDBWithHealthCheck(
			hostname,
			sqlPort,
			sqlUsername,
			sqlPassword,
			applicationName,
			connectionTimeout,
			connectionTimeout,
			monitorTimeout,
			stdout,
		)
		if err != nil {
			switch serverUnhealthyError := err.(type) {
			case *mssqlcommon.ServerUnhealthyError:
				if serverUnhealthyError.RawValue <= healthThreshold {
					return mssqlocf.OcfExit(stderr, mssqlocf.OCF_ERR_GENERIC, fmt.Errorf(
						"Instance is unhealthy: status %d is at or below monitor policy %d",
						serverUnhealthyError.RawValue, healthThreshold))
				}

				stdout.Printf("Instance is healthy: status %d is above monitor policy %d", serverUnhealthyError.RawValue, healthThreshold)

			default:
				stdout.Printf("OpenDBWithHealthCheck failed during %s: %s", action, mssqlcommon.FormatErrorString(err))
				return err
			}
		}

	default:
		// Don't check instance health for other actions

		db, err = mssqlcommon.OpenDB(
			hostname,
			sqlPort,
			sqlUsername,
			sqlPassword,
			"",
			applicationName,
			connectionTimeout,
			connectionTimeout,
		)
		if err != nil {
			stdout.Printf("OpenDB failed during %s: %s", action, mssqlcommon.FormatErrorString(err))
			return mssqlocf.OcfExit(stderr, mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not connect to instance: %s", mssqlcommon.FormatErrorString(err)))
		}
	}
	defer db.Close()

	var ocfExitCode mssqlocf.OcfExitCode

	switch action {
	case "start":
		ocfExitCode, err =
			start(
				db,
				agName,
				sequenceNumbers,
				requiredSynchronizedSecondariesToCommit,
				currentMaster,
				disablePrimaryOnQuorumTimeoutAfter,
				primaryWriteLeaseDuration,
				stdout,
				promotionScoreOut,
				leaseExpiryOut)

	case "stop":
		ocfExitCode, err = stop(db, agName, stdout)

	case "monitor":
		ocfExitCode, err =
			monitor(
				db,
				agName,
				requiredSynchronizedSecondariesToCommit,
				currentMaster,
				"monitor",
				disablePrimaryOnQuorumTimeoutAfter,
				primaryWriteLeaseDuration,
				stdout,
				promotionScoreOut,
				leaseExpiryOut)

	case "pre-start":
		ocfExitCode, err = preStart(db, agName, requiredSynchronizedSecondariesToCommit, stdout, sequenceNumberOut)

	case "post-stop":
		ocfExitCode, err = postStop(db, agName, requiredSynchronizedSecondariesToCommit, stdout)

	case "pre-promote":
		ocfExitCode, err = prePromote(db, agName, stdout, sequenceNumberOut)
		
	case "post-promote":
		ocfExitCode, err = postPromote(db, agName, stdout, sequenceNumberOut)	

	case "promote":
		ocfExitCode, err =
			promote(
				db,
				agName,
				sequenceNumbers,
				newMaster,
				skipPreCheck,
				requiredSynchronizedSecondariesToCommit,
				disablePrimaryOnQuorumTimeoutAfter,
				primaryWriteLeaseDuration,
				stdout,
				promotionScoreOut,
				leaseExpiryOut)

	case "demote":
		ocfExitCode, err = demote(db, agName, leaseExpiry, stdout)

	default:
		return fmt.Errorf("unknown value for --action %s", action)
	}
	
	if err != nil {
		stdout.Printf("Failed action %s: %s", action, mssqlcommon.FormatErrorString(err))
	}
	return mssqlocf.OcfExit(stderr, ocfExitCode, err)
}

// Function: start
//
// Description:
//    Implements the OCF "start" action by ensuring the AG replica exists and is in SECONDARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica exists and is in SECONDARY role.
//    OCF_ERR_GENERIC: Propagated from `monitor()`
//
func start(
	db *sql.DB, agName string,
	sequenceNumbers string,
	requiredSynchronizedSecondariesToCommit *uint,
	currentMaster string,
	disablePrimaryOnQuorumTimeoutAfter int64,
	primaryWriteLeaseDuration int64,
	stdout *log.Logger,
	promotionScoreOut *log.Logger,
	leaseExpiryOut *log.Logger,
) (mssqlocf.OcfExitCode, error) {
	
	instancename, err := mssqlcommon.GetServerInstanceName(db)
	if err != nil {
		stdout.Printf("[DEBUG] AG Helper Start Role GetServerInstanceName error: %v", err)
		instancename = ""
	}
	stdout.Printf("[DEBUG] AG Helper Start Role info: AVAILABILITY GROUP %s on instance %s", agName, instancename)
	
	role, err := getRole(db, agName, stdout)
	if err == sql.ErrNoRows {
		return mssqlocf.OCF_ERR_GENERIC, errors.New("Did not find AG row in sys.availability_groups")
	}
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	switch role {
	case mssqlag.RoleRESOLVING:
		if currentMaster == "" {
			// There is no master right now. Don't run ALTER AG SET ROLE = SECONDARY because we don't want to wait for recovery.
			// Just pretend the replica is healthy so the replica is available for pre-promote.
			//
			// If this replica gets chosen to be promoted to master, `promote` will run ALTER AG FAILOVER and bring it out of RESOLVING.
			//
			// If another replica gets chosen to be promoted to master, --current-master will be set,
			// so `monitor` will return OCF_NOT_RUNNING and trigger Pacemaker to stop the resource.
			// The subsequent `start` will run ALTER AG SET ROLE = SECONDARY and bring it out of RESOLVING.
		} else {
			// There is already a master, so run ALTER AG SET ROLE = SECONDARY and wait for DBs to finish recovery
			err := setRoleToSecondaryAndWait(db, agName, stdout)
			if err != nil {
				return mssqlocf.OCF_ERR_GENERIC, err
			}
		}

	case mssqlag.RolePRIMARY:
		// Don't expect to be a primary. Tell Pacemaker so that it stops the resource.
		return mssqlocf.OCF_RUNNING_MASTER, nil

	default:
		// Do nothing
	}

	sequenceNumber, err := mssqlag.GetSequenceNumber(db, agName)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not query sequence number: %s", mssqlcommon.FormatErrorString(err))
	}
	stdout.Printf("Sequence number is %s", humanReadableSequenceNumber(sequenceNumber))

	parsedSequenceNumbers, err := parseSequenceNumbers(sequenceNumbers, nil, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	maxSequenceNumber := parsedSequenceNumbers.Max

	if sequenceNumber < maxSequenceNumber {
		stdout.Printf(
			"Replica has sequence number %s but max sequence number is %s, so it cannot be promoted",
			humanReadableSequenceNumber(sequenceNumber), humanReadableSequenceNumber(maxSequenceNumber))

		promotionScoreOut.Println(promotionScoreShouldNotBePromoted)

		promotionScoreOut = nil // Don't let `monitor` set a different promotion score
	}

	// Check health to confirm successful startup
	return monitor(
		db,
		agName,
		requiredSynchronizedSecondariesToCommit,
		currentMaster,
		"start",
		disablePrimaryOnQuorumTimeoutAfter,
		primaryWriteLeaseDuration,
		stdout,
		promotionScoreOut,
		leaseExpiryOut)
}

// Function: stop
//
// Description:
//    Implements the OCF "stop" action by ensuring the AG replica doesn't exist or is in SECONDARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica does not exist, or was successfully set to SECONDARY role (if necessary).
//    OCF_ERR_GENERIC: Any error from trying to set the AG replica to SECONDARY role.
//
func stop(db *sql.DB, agName string, stdout *log.Logger) (mssqlocf.OcfExitCode, error) {
	instancename, err := mssqlcommon.GetServerInstanceName(db)
	if err != nil {
		stdout.Printf("[DEBUG] AG Helper Stop Role GetServerInstanceName error: %v", err)
		instancename = ""
	}
	stdout.Printf("[DEBUG] AG Helper Stop Role info: AVAILABILITY GROUP %s on instance %s", agName, instancename)
	
	role, err := getRole(db, agName, stdout)
	if err == sql.ErrNoRows {
		return mssqlocf.OCF_SUCCESS, nil
	}
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	// Only set role to SECONDARY if it's a PRIMARY.
	// We don't care to change role if it's RESOLVING. That will be handled by a subsequent `start`, if any.
	if role == mssqlag.RolePRIMARY {
		err := offlineAndWait(db, agName, stdout)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, err
		}
	}

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: monitor
//
// Description:
//    Implements the OCF "monitor" action.
//
// Returns:
//    OCF_SUCCESS: AG replica on this instance is in SECONDARY role.
//    OCF_RUNNING_MASTER: AG replica on this instance is in PRIMARY role. If DB_FAILOVER is ON for this AG,
//        then all databases on this replica are ONLINE.
//    OCF_NOT_RUNNING: The AG is not found in sys.availability_groups, or its role is RESOLVING.
//    OCF_ERR_GENERIC: One of the above is not true.
//
func monitor(
	db *sql.DB, agName string,
	requiredSynchronizedSecondariesToCommit *uint,
	currentMaster string,
	caller string,
	disablePrimaryOnQuorumTimeoutAfter int64,
	primaryWriteLeaseDuration int64,
	stdout *log.Logger,
	promotionScoreOut *log.Logger,
	leaseExpiryOut *log.Logger,
) (mssqlocf.OcfExitCode, error) {
	stdout.Printf("Monitor Caller is: %s.", caller)
	
	instancename, err := mssqlcommon.GetServerInstanceName(db)
	if err != nil {
		stdout.Printf("[DEBUG] AG Helper Monitor Role GetServerInstanceName error: %v", err)
		instancename = ""
	}
	stdout.Printf("[DEBUG] AG Helper Monitor Role info: AVAILABILITY GROUP %s on instance %s", agName, instancename)
	
	role, err := getRole(db, agName, stdout)
	if err == sql.ErrNoRows {
		return mssqlocf.OCF_NOT_RUNNING, nil
	}
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}
	
	// We need to skip monitor logic in primary directly if:
	// 1) currentMaster does not have a value yet
	// 2) monitor is called from monitor, not start or promote
	noMasterFlag := currentMaster == "" || currentMaster == " " 
	if role == mssqlag.RolePRIMARY && caller == "monitor" && noMasterFlag {
		stdout.Printf("Skipping monitor for primary...")
		
		// These printlns will be used by pengine to decide the following steps after monitor()
		// so we need to add them here to avoid unnecessary offline 
		if leaseExpiryOut == nil {
			stdout.Printf("nomaster - Lease Expiry Log is null,return error...")
			return mssqlocf.OCF_ERR_GENERIC, err
		}
		
		fmt.Printf("nomaster - timenow: %v \n",time.Now().UTC().Format("20060102150405"))
		leaseExpiryOut.Println(time.Now().UTC().Add(time.Duration(primaryWriteLeaseDuration)*time.Second).Format("20060102150405"))
		
		if promotionScoreOut != nil {
			promotionScoreOut.Println(promotionScoreCurrentMaster)
		}
		
		return mssqlocf.OCF_RUNNING_MASTER, nil
	}
	
	instancename, err = mssqlcommon.GetServerInstanceName(db)
	if err == sql.ErrNoRows {
		return mssqlocf.OCF_NOT_RUNNING, nil
	}
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}
	
	if role == mssqlag.RolePRIMARY && !strings.EqualFold(instancename, currentMaster) && caller == "monitor"  {
	
		err := offlineAndWait(db, agName, stdout)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, err
		}
		
		// We need to manually reset role to resolving if offlineAndWait() does not return any errors.
		role = mssqlag.RoleRESOLVING
	}
	
	stdout.Printf("Instance name is %s.", instancename)
	stdout.Printf("Current master is %s.", currentMaster)

	if role == mssqlag.RolePRIMARY {
		currentConfigurationCommitStartTime, err := mssqlag.GetCurrentConfigurationCommitStartTime(db, agName)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("could not get last configuration commit time: %s", mssqlcommon.FormatErrorString(err))
		}

		// If there's a current configuration with start time < now() - disablePrimaryOnQuorumTimeoutAfter, then a quorum timeout has occurred
		quorumTimeoutOccurred :=
			currentConfigurationCommitStartTime != nil &&
				(*currentConfigurationCommitStartTime).Before(time.Now().Add(-time.Duration(disablePrimaryOnQuorumTimeoutAfter)*time.Second))
		if quorumTimeoutOccurred {
			stdout.Printf("There is a configuration commit in progress since %s. Not renewing lease.", currentConfigurationCommitStartTime.Local())
			
			// We need to offline primay to resolving role if quorum timeout happens.
			err := offlineAndWait(db, agName, stdout)
			if err != nil {
				return mssqlocf.OCF_ERR_GENERIC, err
			}
			
		} else {
			ocfExitCode, err := updateExternalLease(db, agName, primaryWriteLeaseDuration, stdout, leaseExpiryOut)
			
			if err != nil {
				return ocfExitCode, err
			}
		}

		dbFailoverMode, err := mssqlag.GetDBFailoverMode(db, agName)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not query DB_FAILOVER setting: %s", mssqlcommon.FormatErrorString(err))
		}

		var dbFailoverModeString string
		if dbFailoverMode {
			dbFailoverModeString = "ON"
		} else {
			dbFailoverModeString = "OFF"
		}

		stdout.Printf("DB_FAILOVER is %s.", dbFailoverModeString)

		// Update REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT if necessary
		err = setRequiredSynchronizedSecondariesToCommit(db, agName, requiredSynchronizedSecondariesToCommit, stdout)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, err
		}

		if promotionScoreOut != nil {
			promotionScoreOut.Println(promotionScoreCurrentMaster)
		}

		return mssqlocf.OCF_RUNNING_MASTER, nil
	}

	//We need to return error if currentMaster is not correct. 
	//It is possible when quorum timeout happens, db replica is already resolving 
	//But pacemaker resource still thinks current replica is primary.
	if strings.EqualFold(instancename, currentMaster) && caller == "monitor" {
		stdout.Printf("test - Role is %s.", role)
		stdout.Printf("test - Instance name is %s.", instancename)
		stdout.Printf("test - Current master is %s.", currentMaster)
		return mssqlocf.OCF_ERR_GENERIC, nil
	}

	// Ideally we would check if this is a SYNCHRONOUS_COMMIT replica and all DBs are SYNCHRONIZED, otherwise set promotion score
	// to `-INFINITY`. This doesn't work since if the PRIMARY is down, all DB replicas report themselves as NOT SYNCHRONIZING in
	// sys.dm_hadr_database_replica_states even if their copy of the AG configuration indicates they were synchronized before the
	// PRIMARY went down. Even if sys.dm_hadr_database_replica_states did tell the truth, it wouldn't know about databases that
	// don't even exist on the local replica, ie databases that were never seeded from the PRIMARY to the local replica for any reason.
	//
	// The FAILOVER DDL has access to this information, which is how it's able to fail with 41142, but the DMVs don't expose it.

	if role == mssqlag.RoleRESOLVING && caller == "monitor" {
		// AG is neither PRIMARY nor SECONDARY, which means it's waiting to be explicitly set to one or the other via start / promote.

		if currentMaster == "" {
			// There is no master right now. Don't report this as a failure since we don't want the replica to stop and restart.
		} else {
			// There is already a master, so run ALTER AG SET ROLE = SECONDARY and wait for DBs to finish recovery
			
			stdout.Printf("Setting the role to Secondary.")
			err := setRoleToSecondaryAndWait(db, agName, stdout)
			if err != nil {
				return mssqlocf.OCF_ERR_GENERIC, err
			}
			
			if promotionScoreOut != nil {
				promotionScoreOut.Println(promotionScoreShouldNotBePromoted)
			}

			return mssqlocf.OCF_SUCCESS, nil
		}
	}

	if promotionScoreOut != nil {
		availabilityMode, _, err := mssqlag.GetAvailabilityMode(db, agName)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not query availability mode: %s", mssqlcommon.FormatErrorString(err))
		}

		if availabilityMode == mssqlag.AmSYNCHRONOUS_COMMIT {
			promotionScoreOut.Println(promotionScoreCanBePromoted)
		} else {
			promotionScoreOut.Println(promotionScoreShouldNotBePromoted)
		}
	}

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: preStart
//
// Description:
//    Invoked to handle pre-start notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS
//    OCF_ERR_GENERIC
//
func preStart(
	db *sql.DB, agName string,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger,
	sequenceNumberOut *log.Logger,
) (mssqlocf.OcfExitCode, error) {
	isPrimary, err := isPrimary(db, agName, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	if isPrimary {
		// A replica is going to start. If it's starting because a new replica was added to the AG, then we need to update REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT.
		err := setRequiredSynchronizedSecondariesToCommit(db, agName, requiredSynchronizedSecondariesToCommit, stdout)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, err
		}
	}

	// Write out local replica's sequence number to attrd so that the replica being started can set its promotion score
	sequenceNumber, err := getSequenceNumberAdjustedForAvailabilityMode(db, agName, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}
	sequenceNumberOut.Println(sequenceNumber)

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: postStop
//
// Description:
//    Invoked to handle post-stop notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS
//    OCF_ERR_GENERIC
//
func postStop(
	db *sql.DB, agName string,
	requiredSynchronizedSecondariesToCommit *uint,
	stdout *log.Logger) (mssqlocf.OcfExitCode, error) {

	isPrimary, err := isPrimary(db, agName, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	if isPrimary {
		// A replica has stopped. If it stopped because a replica was removed from the AG, then we need to update REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT.
		err := setRequiredSynchronizedSecondariesToCommit(db, agName, requiredSynchronizedSecondariesToCommit, stdout)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, err
		}
	}

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: postPromote
//
// Description:
//    Invoked to handle post-promote notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS
//    OCF_ERR_GENERIC
//
func postPromote(
	db *sql.DB, agName string,
	stdout *log.Logger, sequenceNumberOut *log.Logger) (mssqlocf.OcfExitCode, error) {
	
	instancename, err := mssqlcommon.GetServerInstanceName(db)
	if err != nil {
		stdout.Printf("[DEBUG] AG Helper PostPromote Role GetServerInstanceName error: %v", err)
		instancename = ""
	}
	stdout.Printf("[DEBUG] AG Helper PostPromote Role info: AVAILABILITY GROUP %s on instance %s", agName, instancename)

	role, err := getRole(db, agName, stdout)
	if err == sql.ErrNoRows {
		return mssqlocf.OCF_SUCCESS, nil
	}
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	// If role is secondary we will bring it offline in post-promote.
	if role == mssqlag.RoleSECONDARY {
		stdout.Println("Setting secondary to offline")
		err := offlineAndWait(db, agName, stdout)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, err
		}
	}
	return mssqlocf.OCF_SUCCESS, nil
}

// Function: prePromote
//
// Description:
//    Invoked to handle pre-promote notifications from the OCF "notify" action.
//
// Returns:
//    OCF_SUCCESS: Sequence number was fetched successfully.
//    OCF_ERR_GENERIC: Could not query sequence number of the AG replica.
//
func prePromote(
	db *sql.DB, agName string,
	stdout *log.Logger, sequenceNumberOut *log.Logger) (mssqlocf.OcfExitCode, error) {

	sequenceNumber, err := getSequenceNumberAdjustedForAvailabilityMode(db, agName, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}
	sequenceNumberOut.Println(sequenceNumber)

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: promote
//
// Description:
//    Implements the OCF "promote" action by failing over the AG replica to PRIMARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica is already in PRIMARY role or was successfully failed over to PRIMARY role.
//    OCF_FAILED_MASTER: AG replica could not be failed over to PRIMARY role and is now in unknown state.
//    OCF_ERR_GENERIC: Could not determine initial role of AG replica, or --skip-precheck was not passed and the availability mode is
//        ASYNCHRONOUS_COMMIT or could not be successfully retrieved, or the sequence number of the AG replica is lower than the
//        sequence number of some other replica.
//
func promote(
	db *sql.DB, agName string,
	sequenceNumbers string,
	newMaster string,
	skipPreCheck bool,
	requiredSynchronizedSecondariesToCommit *uint,
	disablePrimaryOnQuorumTimeoutAfter int64,
	primaryWriteLeaseDuration int64,
	stdout *log.Logger,
	promotionScoreOut *log.Logger,
	leaseExpiryOut *log.Logger,
) (mssqlocf.OcfExitCode, error) {

	isPrimary, err := isPrimary(db, agName, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}
	if isPrimary {
		return mssqlocf.OCF_SUCCESS, nil
	}

	if skipPreCheck {
		stdout.Println("Skipping pre-check since --skip-precheck was specified.")
	} else {
		availabilityMode, availabilityModeDesc, err := mssqlag.GetAvailabilityMode(db, agName)
		if err != nil {
			return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not query availability mode: %s", mssqlcommon.FormatErrorString(err))
		}

		if availabilityMode != mssqlag.AmSYNCHRONOUS_COMMIT {
			return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf(
				"%s (%d) replica cannot be promoted to PRIMARY",
				availabilityModeDesc, availabilityMode)
		}
	}

	parsedSequenceNumbers, err := parseSequenceNumbers(sequenceNumbers, &newMaster, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	numSequenceNumbers := parsedSequenceNumbers.Count
	maxSequenceNumber := parsedSequenceNumbers.Max
	newMasterSequenceNumber := parsedSequenceNumbers.NewMaster

	if newMasterSequenceNumber < maxSequenceNumber {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf(
			"Replica has sequence number %s but max sequence number is %s, so it cannot be promoted",
			humanReadableSequenceNumber(newMasterSequenceNumber), humanReadableSequenceNumber(maxSequenceNumber))
	}

	if newMasterSequenceNumber == 0 {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Replica has sequence number %s, so it cannot be promoted", humanReadableSequenceNumber(newMasterSequenceNumber))
	}

	numSyncCommitAndConfigurationOnlyReplicas, err := mssqlag.GetNumSyncCommitAndConfigurationOnlyReplicas(db, agName)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("Could not query number of SYNCHRONOUS_COMMIT or CONFIGURATION_ONLY replicas: %s", mssqlcommon.FormatErrorString(err))
	}

	stdout.Printf("AG has %d SYNCHRONOUS_COMMIT or CONFIGURATION_ONLY replicas.", numSyncCommitAndConfigurationOnlyReplicas)

	requiredNumSequenceNumbers := mssqlag.CalculateNumRequiredSequenceNumbers(numSyncCommitAndConfigurationOnlyReplicas)
	if numSequenceNumbers < requiredNumSequenceNumbers {
		return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf(
			"Not enough replicas are online to safely promote this replica: need %d but have %d",
			requiredNumSequenceNumbers, numSequenceNumbers)
	}
	
	// We need to renew lease before and after failover so db will not out of sync
	// renew lease after failover will be done in monitor() function
	ocfExitCode, err := updateExternalLease(db, agName, primaryWriteLeaseDuration, stdout, leaseExpiryOut)
			
	if err != nil {
		return ocfExitCode, err
	}
	
	instancename, err := mssqlcommon.GetServerInstanceName(db)
	if err != nil {
		stdout.Printf("[DEBUG] AG Helper Promote Role GetServerInstanceName error: %v", err)
		instancename = ""
	}
	stdout.Printf("[DEBUG] Promote info: AVAILABILITY GROUP %s on instance %s", agName, instancename)

	stdout.Println("Promoting replica to PRIMARY role...")

	err = mssqlag.Failover(db, agName)
	if err != nil {
		if e, ok := err.(mssql.Error); ok && e.Number == mssqlcommon.SQLError_AGCannotFailover_UnsynchronizedDBs {
			// Write a shorter error message prefix so that it's readable when truncated by `pcs resource status` or `crm_mon`
			return mssqlocf.OCF_ERR_GENERIC, fmt.Errorf("One or more DBs are unsynchronized or not joined to the AG: %s", mssqlcommon.FormatErrorString(err))
		}

		return mssqlocf.OCF_FAILED_MASTER, fmt.Errorf("Could not promote replica to PRIMARY role: %s", mssqlcommon.FormatErrorString(err))
	}

	// `FAILOVER` DDL returns before role change finishes, so wait till it completes.
	err = waitUntilRoleSatisfies(db, agName, stdout, func(role mssqlag.Role) bool { return role == mssqlag.RolePRIMARY })
	if err != nil {
		return mssqlocf.OCF_FAILED_MASTER, fmt.Errorf("Failed while waiting for replica to be in PRIMARY role: %s", mssqlcommon.FormatErrorString(err))
	}

	stdout.Println("Replica is now PRIMARY")

	err = setRequiredSynchronizedSecondariesToCommit(db, agName, requiredSynchronizedSecondariesToCommit, stdout)
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	// Wait for databases to be healthy before considering `promote` complete
	ocfExitCode, err =
		monitor(
			db,
			agName,
			requiredSynchronizedSecondariesToCommit,
			newMaster,
			"promote",
			disablePrimaryOnQuorumTimeoutAfter,
			primaryWriteLeaseDuration,
			stdout,
			promotionScoreOut,
			leaseExpiryOut)
	if err != nil {
		return ocfExitCode, err
	}

	if ocfExitCode == mssqlocf.OCF_RUNNING_MASTER {
		// `promote` should return OCF_SUCCESS since Pacemaker treats OCF_RUNNING_MASTER as an error
		ocfExitCode = mssqlocf.OCF_SUCCESS
	}

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: demote
//
// Description:
//    Implements the OCF "demote" action by setting the AG replica to SECONDARY role.
//
// Returns:
//    OCF_SUCCESS: AG replica was successfully set to SECONDARY role.
//    OCF_ERR_GENERIC: Could not set AG replica to SECONDARY role.
//
func demote(db *sql.DB, agName string, leaseExpiry string, stdout *log.Logger) (mssqlocf.OcfExitCode, error) {
	instancename, err := mssqlcommon.GetServerInstanceName(db)
	if err != nil {
		stdout.Printf("[DEBUG] AG Helper Demote Role GetServerInstanceName error: %v", err)
		instancename = ""
	}
	stdout.Printf("[DEBUG] AG Helper Demote Role info: AVAILABILITY GROUP %s on instance %s", agName, instancename)
	
	role, err := getRole(db, agName, stdout)
	if err == sql.ErrNoRows {
		return mssqlocf.OCF_NOT_RUNNING, nil
	}
	if err != nil {
		return mssqlocf.OCF_ERR_GENERIC, err
	}

	if role == mssqlag.RolePRIMARY {
		// We don't want to ALTER AG SET ROLE = SECONDARY since that will start recovery of AG DBs, causing subsequent
		// ALTER AG FAILOVER DDL to block. Offlining the replica is better.
		err := offlineAndWait(db, agName, stdout)
		if err != nil {
			leaseExpiryTime, err := parseLeaseExpiryTime(leaseExpiry, stdout)
			if err != nil {
				return mssqlocf.OCF_ERR_GENERIC, err
			}
			
			stdout.Printf("Lease Expiry %s", leaseExpiryTime)
			
			//Here leaseExpiryTime means the TIME when lease expires
			if len(leaseExpiryTime) > 0 {
				layout := "20060102150405"
				t, err := time.Parse(layout, leaseExpiryTime)
				if err != nil {
    					return mssqlocf.OCF_ERR_GENERIC, err
				}
		
				timenow := time.Now().UTC()
			
				// Based on doc, we sleep 1 more sec. But in real life, sleep 5 more sec is safer
				diff := t.Sub(timenow.Add(time.Second * (-5)))
				fmt.Printf("Offline of AG didn't succeed, waiting for %v so that the lease expires \n", diff)
			
				if diff > 0{
					time.Sleep(diff)
				}
			
			}
			return mssqlocf.OCF_SUCCESS, nil
		}
	}

	return mssqlocf.OCF_SUCCESS, nil
}

// Function: waitForDatabasesToBeOnline
//
// Description:
//    Waits for all databases in the AG to be ONLINE.
//    Periodically prints a message detailing the number of databases that are not ONLINE.
//
func waitForDatabasesToBeOnline(
	db *sql.DB, agName string,
	stdout *log.Logger,
) error {
	for {
		transientNonOnlineDatabasesMessage, permanentNonOnlineDatabasesMessage, err := mssqlag.GetDatabaseStates(db, agName)
		if err != nil {
			return fmt.Errorf("Failed while waiting for databases to be online: %s", mssqlcommon.FormatErrorString(err))
		}

		if len(permanentNonOnlineDatabasesMessage) > 0 {
			return errors.New(permanentNonOnlineDatabasesMessage)
		}

		if len(transientNonOnlineDatabasesMessage) > 0 {
			stdout.Println(transientNonOnlineDatabasesMessage)
			time.Sleep(1 * time.Second)
			continue
		}

		// All ready
		stdout.Println("All databases are ONLINE.")
		return nil
	}
}

func isPrimary(db *sql.DB, agName string, stdout *log.Logger) (bool, error) {
	role, err := getRole(db, agName, stdout)
	if err != nil {
		return false, err
	}

	return (role == mssqlag.RolePRIMARY), nil
}

func getRole(db *sql.DB, agName string, stdout *log.Logger) (mssqlag.Role, error) {
	role, roleDesc, err := mssqlag.GetRole(db, agName)
	if err == sql.ErrNoRows {
		stdout.Println("Did not find AG row in sys.availability_groups")
		stdout.Println("Either the AG replica does not exist on the instance, or the SQL user does not have ALTER, CONTROL and VIEW DEFINITION permissions on the AG.")
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("Could not query replica role: %s", mssqlcommon.FormatErrorString(err))
	}

	stdout.Printf("Replica is %s (%d)", roleDesc, role)

	return role, nil
}

func updateExternalLease(db *sql.DB, agName string, primaryWriteLeaseDuration int64, stdout *log.Logger, leaseExpiryOut *log.Logger) (mssqlocf.OcfExitCode, error) {
	stdout.Printf("[DEBUG] Lease is in the process of being renewed for AVAILABILITY GROUP %s for %d seconds", agName, primaryWriteLeaseDuration)
	err := mssqlag.UpdateExternalWriteLease(db, agName, time.Duration(primaryWriteLeaseDuration)*time.Second)
	if err != nil {
		if e, ok := err.(mssql.Error); ok {
			if e.Number == mssqlcommon.SQLError_AGDoesNotAllowExternalLeaseUpdates {
				stdout.Println(
					"WARNING (Ignore this if External Lease is not used) : The AG does not allow updating external write lease. Not updating the external write lease. " +
						"Recreate the AG with the WRITE_LEASE_VALIDITY option in the CREATE AVAILABILITY GROUP DDL.")
			} else if e.Number == mssqlcommon.SQLError_AGExternalLeaseUpdate_NewExpiryIsOlderThanCurrentExpiry {
				stdout.Printf("Cannot renew the external write lease to %ds because it's already valid for a longer time.", primaryWriteLeaseDuration)
			} else {
				stdout.Printf("[DEBUG] Update external lease failed with mssql error: %v", err)
				return mssqlocf.OCF_ERR_GENERIC, err
			}
		} else {
			stdout.Printf("[DEBUG] Update external lease failed with non-mssql error: %v", err)
			return mssqlocf.OCF_ERR_GENERIC, err
		}
	} else {
		if leaseExpiryOut == nil {
			stdout.Printf("Lease Expiry Log is null,return error...")
			return mssqlocf.OCF_ERR_GENERIC, err
		}
		
		stdout.Printf("[DEBUG] Lease update success.")
		
		//We may not need to save "start lease time" and "primaryWriteLeaseDuration(Monitor_Interval)" by global variables.
		//Since we already have "leaseExpiryTime" as time formation in demote()
		fmt.Printf("timenow: %v \n",time.Now().UTC().Format("20060102150405"))
		leaseExpiryOut.Println(time.Now().UTC().Add(time.Duration(primaryWriteLeaseDuration)*time.Second).Format("20060102150405"))
	}
	
	return mssqlocf.OCF_SUCCESS, nil
}

func getSequenceNumberAdjustedForAvailabilityMode(db *sql.DB, agName string, stdout *log.Logger) (int64, error) {
	availabilityMode, availabilityModeDesc, err := mssqlag.GetAvailabilityMode(db, agName)
	if err != nil {
		return 0, fmt.Errorf("Could not query availability mode: %s", mssqlcommon.FormatErrorString(err))
	}

	stdout.Printf("Replica is %s (%d).", availabilityModeDesc, availabilityMode)

	var sequenceNumber int64
	if availabilityMode == mssqlag.AmSYNCHRONOUS_COMMIT || availabilityMode == mssqlag.AmCONFIGURATION_ONLY {
		sequenceNumber, err = mssqlag.GetSequenceNumber(db, agName)
		if err != nil {
			return 0, fmt.Errorf("Could not query sequence number: %s", mssqlcommon.FormatErrorString(err))
		}
	} else {
		sequenceNumber = 0
	}

	stdout.Printf("Sequence number is %s", humanReadableSequenceNumber(sequenceNumber))

	return sequenceNumber, nil
}

func humanReadableSequenceNumber(sequenceNumber int64) string {
	majorNumber := (sequenceNumber >> 32) & 0xFFFFFFFF
	minorNumber := sequenceNumber & 0xFFFFFFFF
	return fmt.Sprintf("%X:%X (%d)", majorNumber, minorNumber, sequenceNumber)
}

// Function: offlineAndWait
//
// Description:
//    Runs ALTER AG OFFLINE DDL and waits for the role to change to RESOLVING
//
func offlineAndWait(db *sql.DB, agName string, stdout *log.Logger) error {
	stdout.Println("Offlining replica...")

	err := mssqlag.Offline(db, agName)
	if err != nil {
		return fmt.Errorf("Could not offline replica: %s", mssqlcommon.FormatErrorString(err))
	}

	// Ensure role is RESOLVING before continuing.
	err = waitUntilRoleSatisfies(db, agName, stdout, func(role mssqlag.Role) bool { return role == mssqlag.RoleRESOLVING })
	if err != nil {
		return fmt.Errorf("Failed while waiting for replica to be in RESOLVING role: %s", mssqlcommon.FormatErrorString(err))
	}
	
	return nil
}

type parsedSequenceNumbers struct {
	Count     uint
	Max       int64
	NewMaster int64
}

func parseSequenceNumbers(
	sequenceNumbers string,
	newMaster *string,
	stdout *log.Logger,
) (parsedSequenceNumbers, error) {
	stdout.Println("Verifying replica's sequence number vs all sequence numbers...")

	var result parsedSequenceNumbers

	lineRegex := regexp.MustCompile(`^name="[^"]+" host="([^"]+)" value="(\d+)"$`)

	for _, line := range strings.Split(sequenceNumbers, "\n") {
		stdout.Printf("Sequence number line [%s]", line)

		match := lineRegex.FindStringSubmatch(line)
		if match == nil {
			stdout.Println("Line does not match expected syntax. Ignoring.")
			continue
		}

		host := match[1]
		value, err := strconv.ParseInt(match[2], 10, 64)
		if err != nil {
			return result, fmt.Errorf("Could not parse sequence number line: %s", mssqlcommon.FormatErrorString(err))
		}

		if newMaster != nil && host == *newMaster {
			result.NewMaster = value
		}

		if value > result.Max {
			result.Max = value
		}

		if value > 0 {
			result.Count++
		}
	}

	stdout.Printf("%d sequence numbers were found", result.Count)
	stdout.Printf("Max sequence number is %s", humanReadableSequenceNumber(result.Max))
	if newMaster != nil {
		stdout.Printf("Sequence number of %s is %s", *newMaster, humanReadableSequenceNumber(result.NewMaster))
	}

	return result, nil
}

func parseLeaseExpiryTime(
	leaseExpiry string,
	stdout *log.Logger,
) (string, error) {
	stdout.Println("Verifying replica's lease expiry time..")

	var leaseExpiryTime string
	
	leaseExpiryTime = ""

	lineRegex := regexp.MustCompile(`^name="[^"]+" host="([^"]+)" value="(\d+)"$`)

	for _, line := range strings.Split(leaseExpiry, "\n") {
		stdout.Printf("Lease Expiry line [%s]", line)

		match := lineRegex.FindStringSubmatch(line)
		if match == nil {
			stdout.Println("Line does not match expected syntax. Ignoring.")
			continue
		}
		if len(match)>2 {
			leaseExpiryTime = match[2]
		}

	}

	if len(leaseExpiryTime)<1 {
		return leaseExpiryTime, fmt.Errorf("Lease expiry time not present.")
	}
	
	return leaseExpiryTime, nil
}

func setRequiredSynchronizedSecondariesToCommit(
	db *sql.DB, agName string,
	override *uint,
	stdout *log.Logger,
) error {
	var requiredSynchronizedSecondariesToCommit uint

	if override == nil {
		numSyncCommitReplicas, err := mssqlag.GetNumSyncCommitReplicas(db, agName)
		if err != nil {
			return fmt.Errorf("Could not query number of SYNCHRONOUS_COMMIT replicas: %s", mssqlcommon.FormatErrorString(err))
		}

		stdout.Printf("AG has %d SYNCHRONOUS_COMMIT replicas.", numSyncCommitReplicas)

		requiredSynchronizedSecondariesToCommit = mssqlag.CalculateRequiredSynchronizedSecondariesToCommit(numSyncCommitReplicas)
	} else {
		requiredSynchronizedSecondariesToCommit = *override
	}

	stdout.Printf("Setting REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT to %d...", requiredSynchronizedSecondariesToCommit)

	err := mssqlag.SetRequiredSynchronizedSecondariesToCommit(db, agName, int32(requiredSynchronizedSecondariesToCommit))
	if err != nil {
		return fmt.Errorf("Could not set REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT: %s", mssqlcommon.FormatErrorString(err))
	}

	return nil
}

// Function: setRoleToSecondaryAndWait
//
// Description:
//    Runs ALTER AG SET ROLE = SECONDARY DDL and waits for the role to change to SECONDARY
//
func setRoleToSecondaryAndWait(db *sql.DB, agName string, stdout *log.Logger) error {
	stdout.Println("Setting replica to SECONDARY role...")

	err := mssqlag.SetRoleToSecondary(db, agName)
	
	type ErrorWithNumber interface {
    		SQLErrorNumber() int32
	}	
		
	if err != nil {
		if errorWithNumber, ok := err.(ErrorWithNumber); ok {
    			if errorWithNumber.SQLErrorNumber() == 41104 {
				stdout.Println("Could not set replica to SECONDARY role. Failover Failed.")
				return nil
			} 
		}
		
		return fmt.Errorf("Could not set replica to SECONDARY role: %s", mssqlcommon.FormatErrorString(err))
	}

	// `SET (ROLE = SECONDARY)` DDL returns before role change finishes, so wait till it completes.
	err = waitUntilRoleSatisfies(db, agName, stdout, func(role mssqlag.Role) bool { return role == mssqlag.RoleSECONDARY })
	if err != nil {
		return fmt.Errorf("Failed while waiting for replica to be in SECONDARY role: %s", mssqlcommon.FormatErrorString(err))
	}

	return nil
}

func waitUntilRoleSatisfies(db *sql.DB, agName string, stdout *log.Logger, predicate func(mssqlag.Role) bool) error {
	for {
		role, err := getRole(db, agName, stdout)
		if err != nil {
			return err
		}

		if predicate(role) {
			return nil
		}

		time.Sleep(1 * time.Second)
	}
}
