This repository contains the source code of the Pacemaker resource agents that ship in the mssql-server-ha package.

This is a snapshot of our SQL Server-internal repository where the actual development takes place. As the commit histories are completely different, we cannot currently accept pull requests to this repository. This snapshot is provided so that users can see the source of the agents, make changes to suit any specific scenarios they have, write agents for other clustering systems following the same protocol, and so on. We intend to migrate development to this repository at a future date.

We're happy to receive bug reports, suggestions and feedback for the code in this repository as Github issues.

Kubernetes agents for monitoring SQL Server instances and Availability Groups are also coming, and will be added to this repository at a future date.


# Availability Group resource agent `ocf:mssql:ag`

This is made up of a golang binary `go/src/ag-helper` and a shell script `ag/ag`. `ag-helper` can be built by running `GOPATH=$PWD/go go install ag-helper`

The agent can be installed by moving the files to these locations:

- `ag/ag` to `/usr/lib/ocf/resource.d/mssql/ag`
- `ag/docs/*` to `/usr/lib/ocf/lib/mssql/*`
- `go/bin/ag-helper` to `/usr/lib/ocf/lib/mssql/ag-helper`

The shell script is the entry point for the resource agent and delegates to the helper binary for most tasks. The helper binary monitors the instance health by running `sp_server_diagnostics` and the AG health by querying `sys.databases`. It also implements the promote and demote actions by running the `ALTER AVAILABILITY GROUP FAILOVER` and `ALTER AVAILABILITY GROUP SET (ROLE = SECONDARY)` DDLs.


# Failover Cluster Instance resource agent `ocf:mssql:fci`

This is made up of a golang binary `go/src/fci-helper` and a shell script `fci/fci`. `fci-helper` can be built by running `GOPATH=$PWD/go go install fci-helper`

The agent can be installed by moving the files to these locations:

- `fci/fci` to `/usr/lib/ocf/resource.d/mssql/fci`
- `fci/docs/*` to `/usr/lib/ocf/lib/mssql/*`
- `go/bin/fci-helper` to `/usr/lib/ocf/lib/mssql/fci-helper`

The shell script is the entry point for the resource agent and handles starting and stopping the `sqlservr` process. The script invokes the `fci-helper` binary to fixup the server name after starting the resource (if necessary), and to monitor the instance health by running `sp_server_diagnostics`


# License

MIT


# Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
