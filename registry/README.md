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

Check out for more command line options with `feathr-registry --help`, detailed documents are coming soon.