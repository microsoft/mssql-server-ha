// Copyright (C) Microsoft Corporation.

package mssqlcommon

import (
	"fmt"
	"testing"
)

func TestDiagnose(t *testing.T) {
	t.Parallel()

	for _, system := range []bool{true, false} {
		for _, resource := range []bool{true, false} {
			for _, queryProcessing := range []bool{true, false} {
				// Local copies of loop variables for the closure to capture
				system := system
				resource := resource
				queryProcessing := queryProcessing

				t.Run(fmt.Sprintf("system = %t, resource = %t, queryProcessing = %t", system, resource, queryProcessing), func(t *testing.T) {
					t.Parallel()

					diagnostics := Diagnostics{System: system, Resource: resource, QueryProcessing: queryProcessing}
					err := Diagnose(diagnostics)

					if system && resource && queryProcessing {
						if err != nil {
							t.Fatalf("Expected Diagnose to succeed but it failed: %s", err)
						}
					} else {
						if err == nil {
							t.Fatal("Expected Diagnose to fail but it succeeded")
						}

						switch serverUnhealthyError := err.(type) {
						case *ServerUnhealthyError:
							if !system {
								if serverUnhealthyError.RawValue != ServerCriticalError {
									t.Fatalf("Diagnose did not fail with ServerCriticalError: %d", serverUnhealthyError.RawValue)
								}

								if serverUnhealthyError.Inner.Error() != "sp_server_diagnostics result indicates system error" {
									t.Fatalf("Diagnose did not fail with an error about system error: %s", serverUnhealthyError.Inner.Error())
								}
							} else if !resource {
								if serverUnhealthyError.RawValue != ServerModerateError {
									t.Fatalf("Diagnose did not fail with ServerModerateError: %d", serverUnhealthyError.RawValue)
								}

								if serverUnhealthyError.Inner.Error() != "sp_server_diagnostics result indicates resource error" {
									t.Fatalf("Diagnose did not fail with an error about resource error: %s", serverUnhealthyError.Inner.Error())
								}
							} else if !queryProcessing {
								if serverUnhealthyError.RawValue != ServerAnyQualifiedError {
									t.Fatalf("Diagnose did not fail with ServerAnyQualifiedError: %d", serverUnhealthyError.RawValue)
								}

								if serverUnhealthyError.Inner.Error() != "sp_server_diagnostics result indicates query processing error" {
									t.Fatalf("Diagnose did not fail with an error about query processing error: %s", serverUnhealthyError.Inner.Error())
								}
							} else {
								t.Fatal("Unreachable")
							}

						default:
							t.Fatal("Diagnose did not return an error of type ServerUnhealthyError")
						}
					}
				})
			}
		}
	}
}
