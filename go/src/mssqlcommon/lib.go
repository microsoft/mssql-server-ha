// Copyright (C) Microsoft Corporation.

// Package mssqlcommon contains items that are common to all SQL Server golang packages.
package mssqlcommon

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Diagnostics represents whether the SQL Server subsystems are healthy or not
type Diagnostics struct {
	System          bool
	Resource        bool
	QueryProcessing bool
}

// ServerHealth represents the various thresholds for server health
type ServerHealth uint

// SQLConnectionInfo contains properties related to opening a T-SQL connection to a SQL Server instance
type SQLConnectionInfo struct {
	Hostname            string
	Port                uint64
	Username            string
	Password            string
	ApplicationName     string
	ConnectionTimeout   time.Duration
	QueryCommandTimeout time.Duration
	JoinCommandTimeout  time.Duration
}

const (
	// The instance is down or refusing connections
	//
	// This library can't distinguish between down or unresponsive, which is why a single health code is used for both,
	// and why there is no enum member with a value of `2`.
	ServerDownOrUnresponsive ServerHealth = 1

	// sp_server_diagnostics detected a critical system error
	ServerCriticalError ServerHealth = 3

	// sp_server_diagnostics detected a moderate resources error
	ServerModerateError ServerHealth = 4

	// sp_server_diagnostics detected an error that's neither moderate nor critical
	ServerAnyQualifiedError ServerHealth = 5
)

// ServerUnhealthyError is an error that means the SQL Server instance is unhealthy
type ServerUnhealthyError struct {
	RawValue ServerHealth
	Inner    error
}

func (err *ServerUnhealthyError) Error() string {
	switch err.RawValue {
	case ServerAnyQualifiedError:
		return fmt.Sprintf("AnyQualified %s", err.Inner)

	case ServerModerateError:
		return fmt.Sprintf("Moderate %s", err.Inner)

	case ServerCriticalError:
		return fmt.Sprintf("Critical %s", err.Inner)

	case ServerDownOrUnresponsive:
		return fmt.Sprintf("Unresponsive or down %s", err.Inner)

	default:
		return fmt.Sprintf("Unknown (%d) %s", err.RawValue, err.Inner)
	}
}

// DiagnosticsChannelItem represents values returned by a QueryDiagnosticsChannel channel
type DiagnosticsChannelItem struct {
	Diagnostics Diagnostics
	Error       error
}

const (
	// SQLError_InvalidSyntax corresponds to SQL error 102:
	// "Incorrect syntax near ..."
	SQLError_InvalidSyntax int32 = 102

	// SQLError_AGCannotFailover_UnsynchronizedDBs corresponds to SQL error 41142:
	// "The availability replica ... cannot become the primary replica. One or more databases are not synchronized or have not joined the availability group."
	SQLError_AGCannotFailover_UnsynchronizedDBs = 41142

	// SQLError_AlterAGAddRemoveReplica_NotReady corresponds to SQL error 41190:
	// "Availability group ... failed to process ... command. The local availability replica is not in a state that could process the command."
	SQLError_AlterAGAddRemoveReplica_NotReady int32 = 41190

	// SQLError_AGDoesNotAllowExternalLeaseUpdates corresponds to SQL error 47116:
	// "The external lease cannot be set on availability group .... External Lease updates are not enabled for this availability group."
	SQLError_AGDoesNotAllowExternalLeaseUpdates int32 = 47116

	// SQLError_AGExternalLeaseUpdate_NewExpiryIsOlderThanCurrentExpiry corresponds to SQL error 47119:
	// "The current write lease of the availability group ... is still valid. The lease expiration time cannot be set to an earlier time than its current value."
	SQLError_AGExternalLeaseUpdate_NewExpiryIsOlderThanCurrentExpiry int32 = 47119
)

// GetCertInfo gets the properties of the given SQL Server certificate.
func GetCertInfo(db *sql.DB, certName string, encryptionPassword string) (publicKey []byte, privateKey []byte, thumbprint []byte, err error) {
	err = db.QueryRow(`
		IF EXISTS (SELECT * FROM sys.certificates WHERE name = ? )
		BEGIN
			DECLARE @cert_id INT = CERT_ID(?);
			SELECT CERTENCODED(@cert_id) AS public_key, CERTPRIVATEKEY(@cert_id, ?) AS private_key, thumbprint FROM sys.certificates WHERE certificate_id = @cert_id;
		END
		ELSE
		BEGIN
			SELECT 0,0,0
		END
			`, certName, certName, encryptionPassword).Scan(&publicKey, &privateKey, &thumbprint)
	return
}

// CreateCert creates a certificate with the given properties if it doesn't already exist, and returns its properties.
//
// The subject is set to the cert name.
func CreateCert(db *sql.DB, certName string, username string, encryptionPassword string) (publicKey []byte, privateKey []byte, thumbprint []byte, err error) {
	err = db.QueryRow(fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = ?)
		BEGIN
			CREATE CERTIFICATE %s
				AUTHORIZATION %s
				WITH SUBJECT = %s
		END

		DECLARE @cert_id INT = CERT_ID(?);
		SELECT CERTENCODED(@cert_id) AS public_key, CERTPRIVATEKEY(@cert_id, ?) AS private_key, thumbprint FROM sys.certificates WHERE certificate_id = @cert_id;
	`, QuoteNameBracket(certName), QuoteNameBracket(username), QuoteNameQuote(certName)), certName, certName, encryptionPassword).Scan(&publicKey, &privateKey, &thumbprint)
	return
}

// CreateDBMirroringEndpoint creates a DATABASE_MIRRORING endpoint using the given properties.
func CreateDBMirroringEndpoint(db *sql.DB, endpointName string, endpointPort uint64, certName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.endpoints WHERE name = ?)
		BEGIN
			CREATE ENDPOINT %s
				STATE = STARTED
				AS TCP (LISTENER_IP = (0.0.0.0), LISTENER_PORT = %d)
				FOR DATA_MIRRORING (
					ROLE = ALL,
					AUTHENTICATION = CERTIFICATE %s,
					ENCRYPTION = REQUIRED ALGORITHM AES
				)
		END
	`, QuoteNameBracket(endpointName), endpointPort, QuoteNameBracket(certName)), endpointName)
	return err
}

// LockSQL creates a T-SQL application lock and holds it
func LockSQL(db *sql.DB, lockName string) (success int, err error) {
	err = db.QueryRow(fmt.Sprintf(`
		BEGIN TRANSACTION;
		DECLARE @result int;
		EXEC @result = sp_getapplock @Resource = %s, @LockMode = 'Exclusive', @LockTimeout= '0';
		IF @result < 0
		BEGIN
			SELECT 0
			ROLLBACK TRANSACTION;
		END
		ELSE
		BEGIN
			SELECT 1
		END;
	`, QuoteNameQuote(lockName))).Scan(&success)
	return
}

// CreateOrUpdateLogin creates a login with the given name and password. If the login already exists, updates its password.
func CreateOrUpdateLogin(db *sql.DB, loginName string, loginPassword string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.syslogins WHERE name = ?)
		BEGIN
			CREATE LOGIN %s WITH PASSWORD = %s
		END
		ELSE
		BEGIN
			ALTER LOGIN %s WITH PASSWORD = %s
		END
	`, QuoteNameBracket(loginName), QuoteNameQuote(loginPassword), QuoteNameBracket(loginName), QuoteNameQuote(loginPassword)), loginName)
	return err
}

// CreateMasterKey creates a master key with the given password if it doesn't already exist.
func CreateMasterKey(db *sql.DB, masterKeyPassword string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name like '%%DatabaseMasterKey%%')
		BEGIN
			CREATE MASTER KEY ENCRYPTION BY PASSWORD = %s
		END
	`, QuoteNameQuote(masterKeyPassword)))
	return err
}

// RegenerateMasterKey regenerates the master key with the new password.
func RegenerateMasterKey(db *sql.DB, masterKeyPassword string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF EXISTS (SELECT * FROM sys.symmetric_keys WHERE name like '%%DatabaseMasterKey%%')
		BEGIN
			ALTER MASTER KEY REGENERATE WITH ENCRYPTION BY PASSWORD = %s
		END
	`, QuoteNameQuote(masterKeyPassword)))
	return err
}

// CreateUser creates a SQL user for the given login if it doesn't already exist.
func CreateUser(db *sql.DB, userName string, loginName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sysusers WHERE name = ?)
		BEGIN
			CREATE USER %s FOR LOGIN %s
		END
	`, QuoteNameBracket(userName), QuoteNameBracket(loginName)), userName)
	return err
}

// Diagnose uses the server health diagnostics object returned by `QueryDiagnostics` to determine server health
func Diagnose(diagnostics Diagnostics) error {
	if !diagnostics.System {
		return &ServerUnhealthyError{RawValue: ServerCriticalError, Inner: fmt.Errorf("sp_server_diagnostics result indicates system error")}
	}

	if !diagnostics.Resource {
		return &ServerUnhealthyError{RawValue: ServerModerateError, Inner: fmt.Errorf("sp_server_diagnostics result indicates resource error")}
	}

	if !diagnostics.QueryProcessing {
		return &ServerUnhealthyError{RawValue: ServerAnyQualifiedError, Inner: fmt.Errorf("sp_server_diagnostics result indicates query processing error")}
	}

	return nil
}

// DropCert drops the specified certificate if it exists
func DropCert(db *sql.DB, certName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF EXISTS (SELECT * FROM sys.certificates WHERE name = ?)
		BEGIN
			DROP CERTIFICATE %s
		END
	`, QuoteNameBracket(certName)), certName)
	return err
}

// DropDBMirroringEndpoint drops the DBMirroring endpoint if it exists
func DropDBMirroringEndpoint(db *sql.DB) error {
	var dbMirrorName string
	err := db.QueryRow(`
		SELECT name FROM sys.database_mirroring_endpoints
	`).Scan(&dbMirrorName)

	if err == nil && dbMirrorName != "" {
		_, err = db.Exec(fmt.Sprintf(`
			DROP ENDPOINT %s
		`, QuoteNameBracket(dbMirrorName)))
	} else if err == sql.ErrNoRows {
		err = nil
	}

	return err
}

// GetEnvDuration gets the environment variable value and attempds to convert it to a duration
func GetEnvDuration(envVarName string, defaultValue time.Duration) time.Duration {
	envValue := defaultValue
	envValueString := os.Getenv(envVarName)
	if envValueString != "" {
		envValueFloat, err := strconv.ParseFloat(envValueString, 64)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert environment variable %s to float, value: %s, %s", envVarName, envValueString, err.Error()))
		}
		envValue = time.Duration(envValueFloat*1000) * time.Millisecond
	}

	return envValue
}

// GetEnvOrPanic gets value of the specified environment variable, and panics if the variable is not set
func GetEnvOrPanic(envVarName string) string {
	envVar := os.Getenv(envVarName)
	if envVar == "" {
		panic(fmt.Errorf("%s env var does not have valid value", envVarName))
	}
	return envVar
}

// GetLocalServerName gets the server name of the given SQL Server
func GetLocalServerName(db *sql.DB) (serverName string, err error) {
	err = db.QueryRow("SELECT @@SERVERNAME").Scan(&serverName)
	return
}

// GetServerInstanceName gets the server instance name of the given SQL Server
func GetServerInstanceName(db *sql.DB) (hostname string, err error) {
	err = db.QueryRow("SELECT SERVERPROPERTY('ServerName')").Scan(&hostname)
	return
}

// GetServerVersion gets the server version
func GetServerVersion(db *sql.DB) (serverVersion string, err error) {
	err = db.QueryRow("SELECT SERVERPROPERTY('ProductVersion')").Scan(&serverVersion)
	return
}

// ParseVersionInfo parses VersionInfo from ProductVersion
// the 0th index stores the major version number, the second index stores the minor version number ect.
func ParseVersionInfo(version string) ([4]int, error) {
	var version0, version1, version2, version3 int
	_, err := fmt.Sscanf(version, "%d.%d.%d.%d", &version0, &version1, &version2, &version3)
	return [4]int{version0, version1, version2, version3}, err
}

// CompareVersionInfo compares sql version information
// returns -1 if version1 is less than version2
// returns 0 if version1 is equal to version2
// returns 1 if version1 is greater than version2
func CompareVersionInfo(version1 [4]int, version2 [4]int) int {
	for i := 0; i < 4; i++ {
		if version1[i] > version2[i] {
			return 1
		} else if version1[i] < version2[i] {
			return -1
		}
	}

	return 0
}

// GrantConnectOnEndpoint grants CONNECT permission to the specified login on the specified endpoint
func GrantConnectOnEndpoint(db *sql.DB, endpointName string, loginName string) error {
	_, err := db.Exec(fmt.Sprintf("GRANT CONNECT ON ENDPOINT::%s TO %s", QuoteNameBracket(endpointName), QuoteNameBracket(loginName)))
	return err
}

// GrantAgControl grants availability group control to a sql login if the ag exists
func GrantAgControl(db *sql.DB, agName string, loginName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF EXISTS(SELECT * FROM sys.availability_groups WHERE name = ?)
			GRANT CONTROL ON AVAILABILITY GROUP :: %s TO %s
		;
	`, QuoteNameBracket(agName), QuoteNameBracket(loginName)), agName)

	return err
}

// GrantCertificateControl grants endpoint control to a sql login
func GrantCertificateControl(db *sql.DB, certName string, loginName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF EXISTS(SELECT * FROM sys.certificates)
			GRANT CONTROL ON CERTIFICATE :: %s TO %s
		;
	`, QuoteNameBracket(certName), QuoteNameBracket(loginName)))

	return err
}

// GrantEndpointCertificates grants loginName control permissions to all of the certificates associated with the endpoint
func GrantEndpointCertificates(db *sql.DB, loginName string) error {
	rows, err := db.Query(`
		WITH dbm_endpoints 
		AS (
			SELECT * FROM sys.database_mirroring_endpoints
			WHERE connection_auth_desc = 'CERTIFICATE'
		)
		SELECT certs.name FROM dbm_endpoints
		INNER JOIN sys.certificates certs
		ON dbm_endpoints.certificate_id = certs.certificate_id
	`)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var certName string
		err = rows.Scan(&certName)
		if err != nil {
			return err
		}

		// Grant control to all certificates
		err = GrantCertificateControl(db, certName, loginName)
		if err != nil {
			return err
		}
	}
	return err
}

// GrantEndpointControl grants endpoint control to a sql login
func GrantEndpointControl(db *sql.DB, endpointName string, loginName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF EXISTS(SELECT * FROM sys.database_mirroring_endpoints)
			GRANT CONTROL ON ENDPOINT :: %s TO %s
		;
	`, QuoteNameBracket(endpointName), QuoteNameBracket(loginName)))

	return err
}

// GrantAgentPermissions grants permissions to the AG agent login.
//
// Params:
//    username: the username
//    loginName: the login name
//
func GrantAgentPermissions(db *sql.DB, username string, loginName string) error {
	_, err := db.Exec(fmt.Sprintf(`
		GRANT ALTER ANY LOGIN TO %[2]s; -- CREATE/ALTER DBM LOGIN
		GRANT ALTER ANY USER TO %[1]s; -- CREATE DBM USER
		GRANT CREATE CERTIFICATE TO %[1]s WITH GRANT OPTION; -- Grant CREATE/DROP CERTIFICATE to dbm users
		GRANT CREATE ENDPOINT TO %[2]s WITH GRANT OPTION; -- Grant CREATE/DROP DBM ENDPOINT to local dbm user
		GRANT CREATE AVAILABILITY GROUP TO %[1]s; -- CREATE AG / ALTER AG JOIN
		GRANT ALTER ANY DATABASE TO %[1]s; -- ALTER AG GRANT CREATE ANY DATABASE
		GRANT VIEW SERVER STATE TO %[2]s; -- sys.dm_hadr_availability_replica_states
	`, QuoteNameBracket(username), QuoteNameBracket(loginName)))
	return err
}

// ImportCert creates a certificate with the given name and keys, if it doesn't already exist
func ImportCert(db *sql.DB, certName string, username string, publicKey []byte, privateKey []byte, decryptionPassword string) error {
	_, err := db.Exec(fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = ?)
		BEGIN
			CREATE CERTIFICATE %s
				AUTHORIZATION %s
				FROM BINARY = 0x%s
				WITH PRIVATE KEY (
					BINARY = 0x%s,
					DECRYPTION BY PASSWORD = %s
				)
		END
	`, QuoteNameBracket(certName), QuoteNameBracket(username), hex.EncodeToString(publicKey), hex.EncodeToString(privateKey), QuoteNameQuote(decryptionPassword)), certName)
	return err
}

// MonitorDBHealth opens a connection to the SQL Server instance using the given parameters,
// and repeatedly performs health checks.
//
// The returned channel yields:
// 1. errors from opening the connection or running the health check
// 2. ServerUnhealthyError values corresponding to the health check results if they violate the given healthThreshold
//
// The channel will be closed after yielding an error of the first category.
func MonitorDBHealth(connectionInfo SQLConnectionInfo, repeatInterval time.Duration, healthThreshold ServerHealth, stdout *log.Logger) <-chan DiagnosticsChannelItem {
	result := make(chan DiagnosticsChannelItem)

	// https://msdn.microsoft.com/en-us/library/ff878233.aspx
	const stateError = 3

	go func() {
		var db *sql.DB
		defer close(result)
		err := RetryExecute(connectionInfo.ConnectionTimeout, func(i uint) (bool, error) {
			stdout.Printf("Attempt %d to connect to the instance at %s:%d and run sp_server_diagnostics", i, connectionInfo.Hostname, connectionInfo.Port)

			var err error
			db, err = OpenDB(
				connectionInfo.Hostname,
				connectionInfo.Port,
				connectionInfo.Username,
				connectionInfo.Password,
				"",
				connectionInfo.ApplicationName,
				connectionInfo.ConnectionTimeout,
				connectionInfo.QueryCommandTimeout,
			)
			if err != nil {
				stdout.Printf("Attempt %d returned error: %s", i, err)
				return false, err
			}

			stdout.Printf("Connected to the instance at %s:%d", connectionInfo.Hostname, connectionInfo.Port)

			return true, nil
		})
		if err != nil {
			result <- DiagnosticsChannelItem{Error: err}
			return
		}
		defer db.Close()

		rows, err := db.Query(fmt.Sprintf("EXEC sp_server_diagnostics %d", int(repeatInterval.Seconds())))
		if err != nil {
			result <- DiagnosticsChannelItem{Error: err}
			return
		}
		defer rows.Close()

		for {
			var diagnostics Diagnostics

			for rows.Next() {
				var creationTime, componentType, componentName, stateDesc, data string
				var state int

				err = rows.Scan(&creationTime, &componentType, &componentName, &state, &stateDesc, &data)
				if err != nil {
					break
				}

				switch strings.ToLower(componentName) {
				case "system":
					diagnostics.System = state != stateError
				case "resource":
					diagnostics.Resource = state != stateError
				case "query_processing":
					diagnostics.QueryProcessing = state != stateError
				}
			}

			if err == nil {
				err = rows.Err()
			}
			if err != nil {
				result <- DiagnosticsChannelItem{Error: err}
				return
			}

			result <- DiagnosticsChannelItem{
				Diagnostics: diagnostics,
			}

			if !rows.NextResultSet() {
				result <- DiagnosticsChannelItem{
					Error: errors.New("unexpected end of resultsets"),
				}
				return
			}
		}
	}()

	return result
}

// OpenDB opens a connection to a SQL Server instance using the given parameters.
//
// Params:
//    hostname: Hostname of the instance.
//    port: Port number for the T-SQL endpoint of the instance.
//    username: Username to use to connect to the instance.
//    password: Password to use to connect to the instance.
//    applicationName: The application name that the connection will use.
//    connectionTimeout: Connection timeout.
//    commandTimeout: Command timeout.
func OpenDB(
	hostname string,
	port uint64,
	username string,
	password string,
	databaseName string,
	applicationName string,
	connectionTimeout time.Duration,
	commandTimeout time.Duration,
) (*sql.DB, error) {
	query := url.Values{}

	if databaseName != "" {
		query.Add("database", databaseName)
	}

	query.Add("app name", applicationName)
	// golang calls connection timeout "dial timeout", and go-mssqldb reuses it for the same meaning.
	// SqlClient's CommandTimeout maps to go-mssqldb's "connection timeout", since it gives up on the connection if no data is received for that time.
	query.Add("dial timeout", fmt.Sprintf("%d", connectionTimeout/time.Second))
	query.Add("connection timeout", fmt.Sprintf("%d", commandTimeout/time.Second))
	query.Add("encrypt", "true")
	query.Add("TrustServerCertificate", "true") // Otherwise TLS will fail because sql is using a self signed certificate

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(username, password),
		Host:     fmt.Sprintf("%s:%d", hostname, port),
		RawQuery: query.Encode(),
	}

	connectionString := u.String()

	db, err := sql.Open("mssql", connectionString)
	if err != nil {
		return nil, &ServerUnhealthyError{RawValue: ServerDownOrUnresponsive, Inner: err}
	}

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, &ServerUnhealthyError{RawValue: ServerDownOrUnresponsive, Inner: err}
	}

	return db, nil
}

// OpenDBWithHealthCheck opens a connection to a SQL Server instance using the given parameters, and performs a health check.
//
// Params:
//    hostname: Hostname of the instance.
//    port: Port number for the T-SQL endpoint of the instance.
//    username: Username to use to connect to the instance.
//    password: Password to use to connect to the instance.
//    connectionTimeout: Connection timeout.
//        If connection fails, this function will retry until this time has elapsed.
//        If this time elapses, the last error encountered will be returned.
//    monitorTimeout: monitor interval timeout. This will be 0 from other actions, but greater than 0 for monitor action
func OpenDBWithHealthCheck(
	hostname string,
	port uint64,
	username string,
	password string,
	applicationName string,
	connectionTimeout time.Duration,
	commandTimeout time.Duration,
	monitorTimeout time.Duration,
	stdout *log.Logger,
) (db *sql.DB, err error) {


	if monitorTimeout > 0 && monitorTimeout > connectionTimeout {
		err = RetryExecuteWithTimeout(connectionTimeout, monitorTimeout, stdout, func(i uint) (bool, error) {
			stdout.Printf("From RetryExecuteWithTimeout - Attempt %d to connect to the instance at %s:%d\n", i, hostname, port)
			var err error

			db, err = OpenDB(hostname, port, username, password, "", applicationName, connectionTimeout, commandTimeout)
			if err != nil {
				stdout.Printf("Attempt %d returned error: %s\n", i, FormatErrorString(err))

				return false, err
			}

			stdout.Printf("Connected to the instance at %s:%d\n", hostname, port)

			return true, nil
		})
	} else {
		err = RetryExecute(connectionTimeout, func(i uint) (bool, error) {
			stdout.Printf("From RetryExecute - Attempt %d to connect to the instance at %s:%d\n", i, hostname, port)
			var err error
			
			db, err = OpenDB(hostname, port, username, password, "", applicationName, connectionTimeout, commandTimeout)
			if err != nil {
				stdout.Printf("Attempt %d returned error: %s\n", i, FormatErrorString(err))
	
				return false, err
			}

			stdout.Printf("Connected to the instance at %s:%d\n", hostname, port)

			return true, nil
		})
	}
	
	
	if err != nil {
		return
	}

	diagnostics, err := QueryDiagnostics(db)
	if err != nil {
		return
	}

	err = Diagnose(diagnostics)

	return
}

// QueryDiagnostics gets the server health diagnostics of a SQL Server instance
func QueryDiagnostics(db *sql.DB) (result Diagnostics, err error) {
	// https://msdn.microsoft.com/en-us/library/ff878233.aspx
	const stateError = 3

	rows, err := db.Query("EXEC sp_server_diagnostics")
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var creationTime, componentType, componentName, stateDesc, data string
		var state int // https://msdn.microsoft.com/en-us/library/ff878233.aspx

		err = rows.Scan(&creationTime, &componentType, &componentName, &state, &stateDesc, &data)
		if err != nil {
			break
		}

		switch strings.ToLower(componentName) {
		case "system":
			result.System = state != stateError
		case "resource":
			result.Resource = state != stateError
		case "query_processing":
			result.QueryProcessing = state != stateError
		}
	}

	err = rows.Err()

	return
}

// QuoteNameBracket performs the equivalent operation of QUOTENAME with quote_character = '['
func QuoteNameBracket(s string) string {
	return fmt.Sprintf("[%s]", strings.Replace(s, "]", "]]", -1))
}

// QuoteNameQuote performs the equivalent operation of QUOTENAME with quote_character = '\''
func QuoteNameQuote(s string) string {
	return fmt.Sprintf("'%s'", strings.Replace(s, "'", "''", -1))
}

// ReadCredentialsFile reads the specified credentials file to extract a SQL username and password.
//
// The first line contains the username.
// The second line contains the password.
// Lines are separated by LF.
// The second line can end with LF or EOF.
func ReadCredentialsFile(filename string) (username string, password string, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if !scanner.Scan() {
		err = fmt.Errorf("could not read first line to extract username")
		return
	}
	username = scanner.Text()

	if !scanner.Scan() {
		err = fmt.Errorf("could not read second line to extract password")
		return
	}
	password = scanner.Text()

	return
}

// RetryExecute retries the execution of a function until the function returns true or the specified timeout elapses.
// If the timeout elapsed, the last error returned by the function is returned.
//
// The function receives a uint that contains the iteration number (starting from 1).
func RetryExecute(
	retryTimeout time.Duration,
	retryFn func(uint) (bool, error),
) (err error) {
	successChannel := make(chan struct{})
	errChannel := make(chan error)
	timeoutChannel := time.After(retryTimeout)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := r.(error) // Panics if r is not of type error, which is desirable
				errChannel <- err
			}
		}()

		for i := uint(1); ; i++ {
			success, err := retryFn(i)

			if err == nil {
				if success {
					successChannel <- struct{}{}
					return
				}
			} else {
				errChannel <- err
			}

			time.Sleep(1 * time.Second)
		}
	}()

	// Loop until success or timeout
	for {
		select {
		case _ = <-successChannel:
			err = nil
			return

		case err = <-errChannel:
			// Store the latest error so that it can be returned on timeout

		case _ = <-timeoutChannel:
			if err == nil {
				// Goroutine timed out without failing even once, so construct a timeout error to return to the caller
				err = fmt.Errorf("Connection attempt timed out. Either SQL Server is unresponsive or not accepting connection request")
			}

			return
		}
	}
}


// RetryExecuteWithTimeout is an extention of RetryExecute. If a connection timeout occurs then we will retry the connection attempt until the monitor timeout elapses.
// If the timeout elapsed, the last error returned by the function is returned.
//
// The function receives a uint that contains the iteration number (starting from 1).

func RetryExecuteWithTimeout(
	retryTimeout time.Duration,
	monitorTimeout time.Duration,
	stdout *log.Logger,
	retryFn func(uint) (bool, error),
) (err error) {
	successChannel := make(chan struct{})
	errChannel := make(chan error)
	timeoutChannel := time.After(monitorTimeout)
	quit := make(chan bool)
	
	var reterr error
	var finner func()
	var fouter func() (err error)

	finner = func() {
		retryChannel := time.After(retryTimeout)
		defer func() {
			if r := recover(); r != nil {
				err := r.(error) // Panics if r is not of type error, which is desirable
				errChannel <- err
			}
		}()

		for i := uint(1); ; i++ {
		
			select {
			
			case <-retryChannel:
				quit <- true
				return
				
			default:				
				success, err := retryFn(i)

				if err == nil {
					if success {
						successChannel <- struct{}{}
						return
					}
				} else {
					errChannel <- err
				}

				time.Sleep(1 * time.Second)
				
			}
		}
	}
	
	fouter = func() (err error) {
		go finner()
	
		// Loop until success or timeout
		for {
			select {
			case _ = <-successChannel:
				err = nil
				return

			case err = <-errChannel:
				// Store the latest error so that it can be returned on timeout
							
				
			case _ = <-quit:
				err = fmt.Errorf("Attempt retry")
				return				

			case _ = <-timeoutChannel:
				if err == nil {
					// Goroutine timed out without failing even once, so construct a timeout error to return to the caller
					err = fmt.Errorf("Connection attempts timed out. Either SQL Server is unresponsive or not accepting connection request")
				}

				return
			}
		}
	}
	
	reterr = fouter()
	for reterr != nil {
		if strings.Index(reterr.Error(), "retry") > 0 {
			stdout.Printf("Connection request timed out - attempting retry \n")
			reterr = fouter()
		} else {
			return reterr
		}
	}
	
return reterr
}

// SetLocalServerName sets the local server name to the given name via sp_dropserver + sp_addserver
func SetLocalServerName(db *sql.DB, serverName string) error {
	var currentServerName string
	err := db.QueryRow(`SELECT name FROM sys.servers WHERE server_id = 0`).Scan(&currentServerName)

	if err == nil && strings.EqualFold(currentServerName, serverName) {
		// Existing sys.servers row already has the specified name
		return nil
	}

	if err != nil && err != sql.ErrNoRows {
		// Unexpected error
		return err
	}

	if err == nil {
		// There is an existing sys.servers row and it has a different name than the specified name. Drop it.
		_, err = db.Exec("EXEC sp_dropserver ?", currentServerName)
		if err != nil {
			return err
		}
	}

	// At this point there is no sys.servers row for a local server. Add a row with the specified name.
	_, err = db.Exec("EXEC sp_addserver ?, local", serverName)

	return err
}

// GetReplicaNameRetry gets the replica name, will retry until success
func GetReplicaNameRetry(connectionInfo SQLConnectionInfo, stdout *log.Logger) string {
	var replicaName string
	// Retry until SQL Server is up
	for {
		err := WithDbConnection(connectionInfo, stdout, func(db *sql.DB) error {
			var err error
			stdout.Printf("Getting replica name...")
			replicaName, err = GetServerInstanceName(db)
			return err
		})
		if err != nil {
			stdout.Printf("could not connect to local sqlservr: %s", err)
			time.Sleep(1 * time.Second)
		} else {
			return replicaName
		}
	}
}

// WithDbConnection connects to a SQL Server instance using the specified connection info, runs the given function against it, and closes the connection.
//
// The QueryCommandTimeout is used for the command timeout.
func WithDbConnection(connectionInfo SQLConnectionInfo, stdout *log.Logger, f func(*sql.DB) error) error {
	return WithDB(
		connectionInfo.Hostname,
		connectionInfo.Port,
		connectionInfo.Username,
		connectionInfo.Password,
		"",
		connectionInfo.ApplicationName,
		connectionInfo.ConnectionTimeout,
		connectionInfo.QueryCommandTimeout,
		stdout,
		f,
	)
}

// WithDbJoinConnection connects to a SQL Server instance using the specified connection info, runs the given function against it, and closes the connection.
//
// The JoinCommandTimeout is used for the command timeout.
func WithDbJoinConnection(connectionInfo SQLConnectionInfo, stdout *log.Logger, f func(*sql.DB) error) error {
	return WithDB(
		connectionInfo.Hostname,
		connectionInfo.Port,
		connectionInfo.Username,
		connectionInfo.Password,
		"",
		connectionInfo.ApplicationName,
		connectionInfo.ConnectionTimeout,
		connectionInfo.JoinCommandTimeout,
		stdout,
		f,
	)
}

// WithDB connects to a SQL Server instance, runs the given function against it, and closes the connection.
func WithDB(
	hostname string,
	port uint64,
	username string,
	password string,
	databaseName string,
	applicationName string,
	connectionTimeout time.Duration,
	commandTimeout time.Duration,
	stdout *log.Logger,
	f func(*sql.DB) error,
) error {
	db, err := OpenDB(hostname, port, username, password, databaseName, applicationName, connectionTimeout, commandTimeout)
	if err != nil {
		return err
	}

	defer db.Close()

	err = f(db)

	return err
}

// Format the error string
//
func FormatErrorString(err error) error {
	type SqlErrorWithNumber interface {
		SQLErrorNumber() int32
		SQLErrorState() uint8
	}

	if err != nil {
		if sqlErrorWithNumber, ok := err.(SqlErrorWithNumber); ok {		
			var errVal int
			var errSt int
			var errNo string
			var errState string
			errSt = int(sqlErrorWithNumber.SQLErrorState())	
			errVal = int(sqlErrorWithNumber.SQLErrorNumber())	
			errNo = strconv.Itoa(errVal)
			errState = strconv.Itoa(errSt)
			return fmt.Errorf("Error %s, State %s: %s", errNo, errState, err)
		}
		return fmt.Errorf("%s", err)
	}
	return nil
}
