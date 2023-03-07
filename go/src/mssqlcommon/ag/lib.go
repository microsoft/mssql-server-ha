// Copyright (C) Microsoft Corporation.

// Package ag contains items related to SQL Server Availability Groups.
package ag

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"mssqlcommon"
)

// An AvailabilityMode represents an AG replica's availability mode.
//
// See the availability_mode field in https://msdn.microsoft.com/en-us/library/ff877883.aspx for details.
type AvailabilityMode byte

const (
	// The replica has ASYNCHRONOUS_COMMIT availability mode
	AmASYNCHRONOUS_COMMIT AvailabilityMode = 0

	// The replica has SYNCHRONOUS_COMMIT availability mode
	AmSYNCHRONOUS_COMMIT AvailabilityMode = 1

	// The replica has CONFIGURATION_ONLY availability mode
	AmCONFIGURATION_ONLY AvailabilityMode = 4
)

const (
	// AmASYNCHRONOUS_COMMIT_JSON is the JSON string that AmASYNCHRONOUS_COMMIT is serialized to
	AmASYNCHRONOUS_COMMIT_JSON = "asynchronousCommit"

	// AmSYNCHRONOUS_COMMIT_JSON is the JSON string that AmSYNCHRONOUS_COMMIT is serialized to
	AmSYNCHRONOUS_COMMIT_JSON = "synchronousCommit"

	// AmCONFIGURATION_ONLY_JSON is the JSON string that AmCONFIGURATION_ONLY is serialized to
	AmCONFIGURATION_ONLY_JSON = "configurationOnly"
)

const (
	// AmASYNCHRONOUS_COMMIT_TSQL is the T-SQL keyword for AmASYNCHRONOUS_COMMIT
	AmASYNCHRONOUS_COMMIT_TSQL = "ASYNCHRONOUS_COMMIT"

	// AmSYNCHRONOUS_COMMIT_TSQL is the T-SQL keyword for AmSYNCHRONOUS_COMMIT
	AmSYNCHRONOUS_COMMIT_TSQL = "SYNCHRONOUS_COMMIT"

	// AmCONFIGURATION_ONLY_TSQL is the T-SQL keyword for AmCONFIGURATION_ONLY
	AmCONFIGURATION_ONLY_TSQL = "CONFIGURATION_ONLY"
)

// TSQL returns the T-SQL keyword corresponding to this AvailabilityMode
func (availabilityMode AvailabilityMode) TSQL() (string, error) {
	switch availabilityMode {
	case AmASYNCHRONOUS_COMMIT:
		return AmASYNCHRONOUS_COMMIT_TSQL, nil
	case AmSYNCHRONOUS_COMMIT:
		return AmSYNCHRONOUS_COMMIT_TSQL, nil
	case AmCONFIGURATION_ONLY:
		return AmCONFIGURATION_ONLY_TSQL, nil
	default:
		return "", fmt.Errorf("unexpected availabilty mode %d", availabilityMode)
	}
}

// MarshalJSON serializes this AvailabilityMode as a JSON value
func (availabilityMode AvailabilityMode) MarshalJSON() ([]byte, error) {
	var availabilityModeString string

	switch availabilityMode {
	case AmASYNCHRONOUS_COMMIT:
		availabilityModeString = AmASYNCHRONOUS_COMMIT_JSON
	case AmSYNCHRONOUS_COMMIT:
		availabilityModeString = AmSYNCHRONOUS_COMMIT_JSON
	case AmCONFIGURATION_ONLY:
		availabilityModeString = AmCONFIGURATION_ONLY_JSON
	default:
		return nil, fmt.Errorf("unrecognized availability mode %d", availabilityMode)
	}

	return json.Marshal(availabilityModeString)
}

// UnmarshalJSON deserializes this AvailabilityMode from a JSON value
func (availabilityMode *AvailabilityMode) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	switch s {
	case AmASYNCHRONOUS_COMMIT_JSON:
		*availabilityMode = AmASYNCHRONOUS_COMMIT
	case AmSYNCHRONOUS_COMMIT_JSON:
		*availabilityMode = AmSYNCHRONOUS_COMMIT
	case AmCONFIGURATION_ONLY_JSON:
		*availabilityMode = AmCONFIGURATION_ONLY
	default:
		return fmt.Errorf("Expected one of %s, %s, %s", AmASYNCHRONOUS_COMMIT_JSON, AmCONFIGURATION_ONLY_JSON, AmSYNCHRONOUS_COMMIT_JSON)
	}

	return nil
}

// A DatabaseState represents a database state.
//
// See the state field in https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql for details.
type DatabaseState byte

const (
	// The database is in RESTORING state
	DatabaseStateRESTORING DatabaseState = 1

	// The database is in RECOVERING state
	DatabaseStateRECOVERING DatabaseState = 2

	// The database is in RECOVERY_PENDING state
	DatabaseStateRECOVERY_PENDING DatabaseState = 3

	// The database is in OFFLINE state
	DatabaseStateOFFLINE DatabaseState = 6
)

// A Role represents an AG replica's role.
//
// See the role field in https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-hadr-availability-replica-states-transact-sql for details.
type Role byte

const (
	// The replica is in RESOLVING role.
	RoleRESOLVING Role = 0

	// The replica is in PRIMARY role.
	RolePRIMARY Role = 1

	// The replica is in SECONDARY role.
	RoleSECONDARY Role = 2
)

// The seeding mode of an AG replica
//
// See the seeding_mode field in https://msdn.microsoft.com/en-us/library/ff877883.aspx for details
type SeedingMode byte

const (
	// The replica is in automatic seeding mode
	SmAUTOMATIC SeedingMode = 0

	// The replica is in manual seeding mode
	SmMANUAL SeedingMode = 1
)

// Replica contains the properties of an AG replica.
type Replica struct {
	ID               string
	Name             string
	EndpointURL      string
	AvailabilityMode AvailabilityMode
}

// AddReplica adds the given replica to the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//    replica: The replica to add.
//
func AddReplica(db *sql.DB, agName string, replica Replica) error {
	alterDdl := `
		ALTER AVAILABILITY GROUP %s ADD REPLICA ON %s WITH (
			ENDPOINT_URL = %s,
			AVAILABILITY_MODE = %s`

	availabilityMode, err := replica.AvailabilityMode.TSQL()
	if err != nil {
		return err
	}

	// Only add these options if it is not an AmCONFIGURATION_ONLY replica
	var miscOptions string
	if replica.AvailabilityMode != AmCONFIGURATION_ONLY {
		miscOptions = `,
			FAILOVER_MODE = EXTERNAL,
			SEEDING_MODE = AUTOMATIC,
			SECONDARY_ROLE (
				ALLOW_CONNECTIONS = READ_ONLY
			)`
	}

	alterDdl = fmt.Sprintf("%s%s)", alterDdl, miscOptions)

	_, err = db.Exec(fmt.Sprintf(
		alterDdl,
		mssqlcommon.QuoteNameBracket(agName),
		mssqlcommon.QuoteNameQuote(replica.Name),
		mssqlcommon.QuoteNameQuote(replica.EndpointURL),
		availabilityMode,
	))
	return err
}

// CalculateNumRequiredSequenceNumbers calculates the number of sequence numbers required for a safe promotion for the given number of
// SYNCHRONOUS_COMMIT or CONFIGURATION_ONLY replicas.
//
// Params:
//    numReplicas: The number of SYNCHRONOUS_COMMIT or CONFIGURATION_ONLY replicas.
//
func CalculateNumRequiredSequenceNumbers(numReplicas uint) uint {
	// num replicas which must commit = quorum count = floor(numReplicas / 2) + 1
	// num replicas which may not commit = numReplicas - num replicas which must commit = ceil(numReplicas / 2) - 1 = floor((numReplicas + 1) / 2) - 1
	// required sequence numbers = num replicas which may not commit + 1 = floor((numReplicas / 2) + 1)

	return (numReplicas / 2) + 1
}

// CalculateRequiredSynchronizedSecondariesToCommit Calculates the optimal value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT for the given number of SYNCHRONOUS_COMMIT replicas.
//
// Params:
//    numReplicas: The number of SYNCHRONOUS_COMMIT replicas.
//
func CalculateRequiredSynchronizedSecondariesToCommit(numReplicas uint) uint {
	// quorum count = floor(numReplicas / 2) + 1
	// required synchronized secondaries to commit = quorum count - 1 (value doesn't count the primary)
	//
	// But for two replicas, customers prefer RSSTC = 0 since they don't want unavailablility on the single S to block writes on P

	if numReplicas == 2 {
		return 0
	}

	return numReplicas / 2
}

// Create creates an Availability Group with the given name.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func Create(db *sql.DB, agName string, sqlMajorVersion int, externalWriteLeaseValidTime time.Duration, replica Replica) error {
	var createDdl string

	if sqlMajorVersion >= 15 {
		createDdl = `
			CREATE AVAILABILITY GROUP %s
			WITH (DB_FAILOVER = ON, CLUSTER_TYPE = EXTERNAL, WRITE_LEASE_VALIDITY = ` + fmt.Sprintf("%d", externalWriteLeaseValidTime/time.Second) + `)
			FOR REPLICA ON %s WITH (
				ENDPOINT_URL = %s,
				AVAILABILITY_MODE = %s`
	} else {
		createDdl = `
			CREATE AVAILABILITY GROUP %s
			WITH (DB_FAILOVER = ON, CLUSTER_TYPE = EXTERNAL)
			FOR REPLICA ON %s WITH (
				ENDPOINT_URL = %s,
				AVAILABILITY_MODE = %s`
	}

	availabilityMode, err := replica.AvailabilityMode.TSQL()
	if err != nil {
		return err
	}

	// Only add these options if it is not an AmCONFIGURATION_ONLY replica
	var miscOptions string
	if replica.AvailabilityMode != AmCONFIGURATION_ONLY {
		miscOptions = `,
			FAILOVER_MODE = EXTERNAL,
			SEEDING_MODE = AUTOMATIC,
			SECONDARY_ROLE (
				ALLOW_CONNECTIONS = READ_ONLY
			)`
	}

	createDdl = fmt.Sprintf("%s%s)", createDdl, miscOptions)

	_, err = db.Exec(fmt.Sprintf(
		createDdl,
		mssqlcommon.QuoteNameBracket(agName),
		mssqlcommon.QuoteNameQuote(replica.Name),
		mssqlcommon.QuoteNameQuote(replica.EndpointURL),
		availabilityMode,
	))
	return err
}

// CreateDbmUser creates a SQL user for the given login if it doesn't already exist, and grants it permissions required for the DBM Kubernetes agent.
func CreateDbmUser(db *sql.DB, username string, loginName string) error {
	err := mssqlcommon.CreateUser(db, username, loginName)
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf(`
		GRANT CREATE CERTIFICATE TO %[1]s;
		GRANT CREATE ENDPOINT TO %[2]s; -- CREATE/DROP DBM ENDPOINT
	`, mssqlcommon.QuoteNameBracket(username), mssqlcommon.QuoteNameBracket(loginName)))
	return err
}

// DropIfExists drops the given Availability Group if it exists.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func DropIfExists(db *sql.DB, agName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF EXISTS(SELECT * FROM sys.availability_groups WHERE name = ?)
			DROP AVAILABILITY GROUP %s
		;
	`, mssqlcommon.QuoteNameBracket(agName)), agName)
	return err
}

// Failover performs a failover of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func Failover(db *sql.DB, agName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes'
		ALTER AVAILABILITY GROUP %s FAILOVER
	`, mssqlcommon.QuoteNameBracket(agName)))
	return err
}

// FailoverWithDataLoss forces a failover of the given Availability Group, accepting data loss.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func FailoverWithDataLoss(db *sql.DB, agName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		EXEC sp_set_session_context @key = N'external_cluster', @value = N'yes'
		ALTER AVAILABILITY GROUP %s FORCE_FAILOVER_ALLOW_DATA_LOSS
	`, mssqlcommon.QuoteNameBracket(agName)))
	return err
}

// GetNumHealthySyncCommitSecondaries gets the number of sync seconaries that are connected and synchronized
func GetNumHealthySyncCommitSecondaries(db *sql.DB, agName string) (uint, error) {
	// See https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-hadr-availability-replica-states-transact-sql
	var healthSyncSecondaries uint
	err := db.QueryRow(`
		DECLARE @numAgDbs INT
		SELECT @numAgDbs = COUNT(*)
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_database_replica_states drs ON drs.group_id = ag.group_id
		WHERE ag.name = ? AND drs.is_local = 1

		SELECT COUNT(*)
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE ag.name = ? AND ars.is_local = 0 AND ar.availability_mode = 1 AND ars.connected_state = 1 AND (synchronization_health = 2 OR @numAgDbs = 0)`,
		agName, agName).Scan(&healthSyncSecondaries)
	if err != nil {
		return 0, err
	}

	return healthSyncSecondaries, nil
}

// GetNumConnectedSyncCommitSecondaries gets the number of sync seconaries that are connected and synchronized
func GetNumConnectedSyncCommitSecondaries(db *sql.DB, agName string) (uint, error) {
	// See https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-hadr-availability-replica-states-transact-sql
	var connectedSyncSecondaries uint
	err := db.QueryRow(`
		SELECT COUNT(*)
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE ag.name = ? AND ars.is_local = 0 AND ar.availability_mode = 1 AND ars.connected_state = 1`,
		agName).Scan(&connectedSyncSecondaries)
	if err != nil {
		return 0, err
	}

	return connectedSyncSecondaries, nil
}

// PrintReplicaAgState prints the availiblity groups state
func PrintReplicaAgState(db *sql.DB, agName string, stdout *log.Logger) {
	// See https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-hadr-availability-replica-states-transact-sql
	rows, err := db.Query(`
		SELECT ar.replica_server_name, ars.synchronization_health_desc, ars.connected_state_desc
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE ag.name = ? AND ar.availability_mode = 1`,
		agName)

	if err != nil {
		panic(err)
	}

	defer rows.Close()
	result := "AG replica states:\nreplica_server_name, synchronization_health_desc, connected_state_desc\n"
	for rows.Next() {
		var serverName string
		var syncHealth string
		var conected string
		err = rows.Scan(&serverName, &syncHealth, &conected)
		if err != nil {
			return
		}

		result += fmt.Sprintf("%s, %s, %s\n", serverName, syncHealth, conected)
	}
	stdout.Print(result)

	var hadDbs int
	err = db.QueryRow(`
		SELECT COUNT(*)
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_database_replica_states drs ON drs.group_id = ag.group_id
		WHERE ag.name = ? AND drs.is_local = 1`,
		agName).Scan(&hadDbs)
	if err != nil {
		panic(err)
	}
	stdout.Printf("AG has dbs: %v", hadDbs > 0)
}

// GetReplicaHealthState determines if the instance, replicaName, is healthy and connected respectively
// return values:
// - bool noting if the replica is connected
// - bool noting if the replica is health
// - error if an error occurted
func GetReplicaHealthState(db *sql.DB, replicaName string, agName string) (bool, bool, error) {
	var connectedState byte
	var synchronizationState byte
	err := db.QueryRow(`
		DECLARE @numAgDbs INT
		SELECT @numAgDbs = COUNT(*)
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id
			INNER JOIN sys.dm_hadr_database_replica_states drs ON drs.group_id = ag.group_id
		WHERE ag.name = ? AND drs.is_local = 1

		SELECT ars.connected_state, CASE @numAgDbs WHEN 0 THEN 2 ELSE ars.synchronization_health END AS synchronization_health
			FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE ag.name = ? AND ar.replica_server_name = ?`, agName, agName, replicaName).Scan(&connectedState, &synchronizationState)

	if err != nil {
		return false, false, err
	}
	// See https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-hadr-availability-replica-states-transact-sql
	return connectedState == 1, synchronizationState == 2, nil
}

// GetAvailabilityMode gets the availability mode of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The numeric value and string name of the availability mode, or an error if the AG was not found.
//
func GetAvailabilityMode(db *sql.DB, agName string) (availabilityMode AvailabilityMode, availabilityModeDesc string, err error) {
	err = db.QueryRow(`
		SELECT ar.availability_mode, ar.availability_mode_desc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&availabilityMode, &availabilityModeDesc)

	return
}

// GetCurrentConfigurationCommitStartTime gets the start timestamp of the current configuration commit, if any, of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The start timestamp, or nil if there is no configuration commit in progress.
//
func GetCurrentConfigurationCommitStartTime(db *sql.DB, agName string) (currentConfigurationCommitStartTime *time.Time, err error) {
	err = db.QueryRow(`
		SELECT ars.current_configuration_commit_start_time_utc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
		WHERE
			ag.name = ?`, agName).Scan(&currentConfigurationCommitStartTime)

	return
}

// GetCurrentReplicaID gets the ID of the local replica of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetCurrentReplicaID(db *sql.DB, agName string) (currentReplicaID string, err error) {
	err = db.QueryRow(`
		SELECT CAST(ar.replica_id as NCHAR(36))
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&currentReplicaID)

	return
}

// GetCurrentReplicaName gets the name of the local replica of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetCurrentReplicaName(db *sql.DB, agName string) (currentReplicaName string, err error) {
	err = db.QueryRow(`
		SELECT ar.replica_server_name
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&currentReplicaName)

	return
}

// GetDatabaseStates returns two messages containing the number of databases that belong to the given Availability Group and are not ONLINE.
//
// The first message contains the number of databases in states that are transient, like RECOVERING. The caller will likely want to
// wait for these databases to come ONLINE on their own.
//
// The second message contains the number of databases in states that are permanent, like SUSPECT. The caller will likely want to fail
// immediately.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetDatabaseStates(db *sql.DB, agName string) (transient string, permanent string, err error) {
	stmt, err := db.Prepare(`
		SELECT drs.database_state, drs.database_state_desc, COUNT(*) FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_database_replica_states drs ON drs.group_id = ag.group_id AND drs.is_local = 1
		WHERE
			ag.name = ? AND drs.database_state <> 0
		GROUP BY drs.database_state, drs.database_state_desc`)
	if err != nil {
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query(agName)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var state DatabaseState
		var stateDesc string
		var numDatabases int
		err = rows.Scan(&state, &stateDesc, &numDatabases)
		if err != nil {
			return
		}

		message := fmt.Sprintf("%d databases are %s, ", numDatabases, stateDesc)
		switch state {
		case DatabaseStateRESTORING, DatabaseStateRECOVERING, DatabaseStateRECOVERY_PENDING, DatabaseStateOFFLINE:
			transient += message
		default:
			permanent += message
		}
	}

	transient = strings.TrimSuffix(transient, ", ")
	permanent = strings.TrimSuffix(permanent, ", ")

	err = rows.Err()

	return
}

// GetDBFailoverMode gets the DB_FAILOVER setting of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    `true` means ON, `false` means OFF.
//
func GetDBFailoverMode(db *sql.DB, agName string) (dbFailoverMode bool, err error) {
	err = db.QueryRow(`
		SELECT ag.db_failover
		FROM
			sys.availability_groups ag
		WHERE
			ag.name = ?`, agName).Scan(&dbFailoverMode)

	return
}

// GetNumSyncCommitReplicas gets the number of SYNCHRONOUS_COMMIT replicas in the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetNumSyncCommitReplicas(db *sql.DB, agName string) (numReplicas uint, err error) {
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM
			sys.availability_replicas ar
			INNER JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
		WHERE ag.name = ? AND ar.availability_mode = ?`, agName, AmSYNCHRONOUS_COMMIT).Scan(&numReplicas)

	return
}

// GetNumSyncCommitAndConfigurationOnlyReplicas gets the number of SYNCHRONOUS_COMMIT and CONFIGURATION_ONLY replicas in the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetNumSyncCommitAndConfigurationOnlyReplicas(db *sql.DB, agName string) (numReplicas uint, err error) {
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM
			sys.availability_replicas ar
			INNER JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
		WHERE ag.name = ? AND ar.availability_mode IN (?, ?)`, agName, AmSYNCHRONOUS_COMMIT, AmCONFIGURATION_ONLY).Scan(&numReplicas)

	return
}

// GetPrimaryReplicaName gets the name of the primary replica of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetPrimaryReplicaName(db *sql.DB, agName string) (primaryReplicaName string, err error) {
	err = db.QueryRow(`
		SELECT ags.primary_replica
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_group_states ags ON ags.group_id = ag.group_id
		WHERE
			ag.name = ?`, agName).Scan(&primaryReplicaName)

	return
}

// GetReplicas gets the name and IP of the replicas of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GetReplicas(db *sql.DB, agName string) ([]Replica, error) {
	rows, err := db.Query(`
		SELECT CAST(ar.replica_id as NCHAR(36)), ar.replica_server_name, ar.endpoint_url, ar.availability_mode
		FROM
			sys.availability_replicas ar
			INNER JOIN sys.availability_groups ag ON ag.group_id = ar.group_id
		WHERE
			ag.name = ?
		`, agName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := []Replica{}
	for rows.Next() {
		var replicaID string
		var replicaName string
		var endpointURL string
		var availabilityMode AvailabilityMode
		err = rows.Scan(&replicaID, &replicaName, &endpointURL, &availabilityMode)
		if err != nil {
			return nil, err
		}

		result = append(result, Replica{
			ID:               replicaID,
			Name:             replicaName,
			EndpointURL:      endpointURL,
			AvailabilityMode: availabilityMode,
		})
	}

	return result, nil
}

// GetRole gets the role of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The numeric value and name of the role, or an error if the AG was not found.
//
func GetRole(db *sql.DB, agName string) (role Role, roleDesc string, err error) {
	err = db.QueryRow(`
		SELECT ars.role, ars.role_desc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
		WHERE
			ag.name = ?`, agName).Scan(&role, &roleDesc)

	return
}

// GetSeedingMode gets the seeding mode of the current replica of the given Availability Group
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The numeric value and string name of the seeding mode, or an error if the AG was not found.
//
func GetSeedingMode(db *sql.DB, agName string) (seedingMode SeedingMode, seedingModeDesc string, err error) {
	err = db.QueryRow(`
		SELECT ar.seeding_mode, ar.seeding_mode_desc
		FROM
			sys.availability_groups ag
			INNER JOIN sys.dm_hadr_availability_replica_states ars ON ars.group_id = ag.group_id AND ars.is_local = 1
			INNER JOIN sys.availability_replicas ar ON ar.replica_id = ars.replica_id
		WHERE
			ag.name = ?`, agName).Scan(&seedingMode, &seedingModeDesc)

	return
}

// GetSequenceNumber gets the sequence number of the current replica of the given Availability Group
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
// Returns:
//    The sequence number.
//
func GetSequenceNumber(db *sql.DB, agName string) (sequenceNumber int64, err error) {
	err = db.QueryRow(`
		SELECT ag.sequence_number
		FROM
			sys.availability_groups ag
		WHERE
			ag.name = ?`, agName).Scan(&sequenceNumber)

	return
}

// GrantCreateAnyDatabase grants the given Availability Group's replica the permission to create any databases in the AG that aren't present.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func GrantCreateAnyDatabase(db *sql.DB, agName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s GRANT CREATE ANY DATABASE", mssqlcommon.QuoteNameBracket(agName)))
	return
}

// Join joins the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//
func Join(db *sql.DB, agName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s JOIN WITH (CLUSTER_TYPE = EXTERNAL)", mssqlcommon.QuoteNameBracket(agName)))
	return
}

// Offline offlines the local replica of the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
func Offline(db *sql.DB, agName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s OFFLINE", mssqlcommon.QuoteNameBracket(agName)))
	return
}

// RemoveReplica removes the given replica from the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//    replicaName: The name of the replica to add.
//
func RemoveReplica(db *sql.DB, agName string, replicaName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s REMOVE REPLICA ON %s", mssqlcommon.QuoteNameBracket(agName), mssqlcommon.QuoteNameQuote(replicaName)))
	return
}

// SetRequiredSynchronizedSecondariesToCommit sets the value of REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT on the given Availability Group on the instance.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//    newValue: The new REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT value.
//
func SetRequiredSynchronizedSecondariesToCommit(db *sql.DB, agName string, newValue int32) (err error) {
	_, err = db.Exec(fmt.Sprintf(`
		DECLARE @num_ags INT;
		SELECT @num_ags = COUNT(*) FROM sys.availability_groups WHERE name = ? AND required_synchronized_secondaries_to_commit = ?;
		IF @num_ags = 0
			ALTER AVAILABILITY GROUP %s SET (REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = %d)
		;
	`, mssqlcommon.QuoteNameBracket(agName), newValue), agName, newValue)
	return
}

// SetRoleToSecondary sets the role of the given Availability Group to SECONDARY.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
func SetRoleToSecondary(db *sql.DB, agName string) (err error) {
	_, err = db.Exec(fmt.Sprintf("ALTER AVAILABILITY GROUP %s SET (ROLE = SECONDARY)", mssqlcommon.QuoteNameBracket(agName)))
	return
}

// UpdateExternalWriteLease updates the valid time of external write lease of the specified availability group to the given value
func UpdateExternalWriteLease(db *sql.DB, agName string, validTime time.Duration) error {
	_, err := db.Exec(fmt.Sprintf(
		"ALTER AVAILABILITY GROUP %s SET (WRITE_LEASE_VALIDITY = %d)",
		mssqlcommon.QuoteNameBracket(agName), validTime/time.Second))
	return err
}

// UpdateReplica adds the given replica to the given Availability Group.
//
// Params:
//    db: A connection to a SQL Server instance hosting a replica of the AG.
//    agName: The name of the AG.
//    replica: The replica to update.
//
func UpdateReplica(db *sql.DB, agName string, replica Replica) (err error) {
	_, err = db.Exec(fmt.Sprintf(
		"ALTER AVAILABILITY GROUP %s MODIFY REPLICA ON %s WITH (ENDPOINT_URL = %s)",
		mssqlcommon.QuoteNameBracket(agName), mssqlcommon.QuoteNameQuote(replica.Name), mssqlcommon.QuoteNameQuote(replica.EndpointURL)))
	return
}
