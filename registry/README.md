# Feathr Registry

This is a enhanced SQL database based [Feathr](https://github.com/linkedin/feathr) registry implementation.

The registry has compatible API as [the one in the main Feathr repo](https://github.com/linkedin/feathr/tree/main/registry), also it supports **clustering** and more SQL databases:

* Microsoft SQL Server (including AzureSQL and Azure SQL Data Warehouse)
* MySQL
* PostgreSQL
* SQLite (for testing purpose)

To run the registry service, use:
```
feathr-registry --http-addr 0.0.0.0:8000
```

You can also use `Dockerfile` to build a Docker image with UI fetched from [the main Feathr repo](https://github.com/linkedin/feathr).

## Common Settings and Environment Variables

### Command line options

* `--http-addr`: Listening address, default to `0.0.0.0:8000`.
* `--api-base`: API base URL, default to `/api`, and the V1 and V2 API endpoint start with `/api/v1` and `/api/v2`.
* `--ext-http-addr`: Use if you have reverse proxy in front of the node and it is also a member of a cluster. The value of this option will be published to other nodes in the cluster so they can communicate to each other. Default value is same as `--http-addr`.
* `--load-db`: Add this option to load data from the database on start.
* `--write-db`: Add this option to write all updates to database, use with `--load-db` to enable fully sync with the database.
* `--node-id`: Node id in the cluster, default to `1`, each node must use unique value in the same cluster, otherwise it will not be able to join the cluster.
* `--seeds`: Comma separated list of seed nodes, new node will contact seeds to get the full picture of the whole cluster.
* `--no-init`: By default a node will try to start a new cluster if it cannot join existing one, use this option to disable this behavior.

### Environment variables

* `CONNECTION_STR`: Database connection string, can be either ADO connection string format (for SQLServer or AzureSQL) or URL (for MySQL/PostgreSQL/SQLite).
* `ENTITY_TABLE`: The name of the table that stores entities, default to `entities`.
* `EDGE_TABLE`: The name of the table that stores relationship between entities, default to `edges`.
* `RBAC_TABLE`: The name of the table that stores user permissions, default to `userroles`.
* `ENABLE_RBAC`: Set this variable to any non-empty string to enable access control, otherwise the access control is disabled.

The database schema can be created with the SQL script under `scripts` directory.

Check out for more command line options with `feathr-registry --help`, detailed documents are coming soon.
### Notes to clustering

To enable registry clustering, you should:

1. Start the initial node with `--node-id` equals `1`.
2. Start other nodes with unique node ids, and with `--seeds` option pointing to running nodes, this option can either be an `IP:port` combination, e.g. `1.2.3.4:8000`, or you can use DNS name instead of the IP address, the node will try to resolve all IP addresses of this DNS name to get as many seeds as possible.
3. Only 1 node should use `--load-db` and `--write-db` option, otherwise there could be race conditions and lead to corrupted data. Another use case is to use multiple nodes to write multiple different databases, if you need HA or geo-replication.
4. In case the database connected node is down, you can simply restart it and all missing operations will be replicated to this node, and database should be updated.
5. If you have reversed proxy such as nginx in front of the node, you may need to specify `--ext-http-addr`, then the node will report the value of this option as the external endpoint when joining the cluster, so other nodes can connect to it.