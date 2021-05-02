**SqlRog** is a tool for managing schema changes in SQL relational databases. Using SqlRog you can:

* Create local SQL project and export your database (tables, procedures, functions, etc) in Yaml files in order to track changes in version control systems
* Compare the changes in local project against Database and generate SQL statements for update
* Compare the changes from one Database to another and generate SQL statements for update
* Create multiple local projects

SqlRog currently supports: 
* MySQL 5.6
* Firebird 2.6

To support other databases you can create your own adapter.
