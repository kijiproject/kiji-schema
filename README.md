KijiSchema
==========

KijiSchema is a layout and schema management layer on top of Apache
HBase. KijiSchema supports complex, compound data types using Avro
serialization, as well as cell-level evolving schemas that dynamically
encode version information.

KijiSchema includes both commandline tools and a simple Java API to
manipulate tables, layouts, and data. It also provide native MapReduce
Input/Output formats for Kiji tables.

For more information about KijiSchema, see
[the Kiji project homepage](http://www.kiji.org).

Compilation
-----------

KijiSchema requires [Apache Maven 3](http://maven.apache.org/download.html) to build. It
may be built with the command

    mvn clean package

or by executing `bin/kiji` from the root of a checkout. This will create a release in the
target directory.

Installation
------------

KijiSchema requires an HBase and Hadoop cluster to run. Confirm that `$HADOOP_HOME` and
`$HBASE_HOME` are set appropriately. Then issue the following command from your KijiSchema
root directory:

    bin/kiji install

This will install a Kiji instance on the hbase cluster named 'default'.

Usage
-----

For commandline tool usage, issue the command:

    bin/kiji

Further documentation is available at the Kiji project
[Documentation Portal](http://docs.kiji.org)

Note for Cassandra users
------------------------

The `kiji-schema-cassandra` submodule of this project contains the implementation of a
Cassandra-backed version of KijiSchema.  Because Cassandra (and therefore also the Cassandra version
of KijiSchema) requires Java 7, we have disabled building this submodule by default.

If you have Java 7, you may build the Cassandra version of KijiSchema by using the "cassandra"
profile:

    mvn clean package -Pcassandra
