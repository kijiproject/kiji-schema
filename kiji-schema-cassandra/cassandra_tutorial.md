Cassandra KijiSchema Tutorial
=============================

Welcome to the Kiji Cassandra tutorial!  In this tutorial, you will learn to use the Cassandra version
of KijiSchema.  We assume that you, as a Cassandra user and Kiji aficionado, are incredibly popular,
and that you cannot fit all of your contacts into a traditional phone book.  Below, we show you how
to use Kiji to build a phone book on top of Cassandra.  In doing so, you will learn to create a new
Kiji instance, create Kiji tables, and put, get, and delete data in Kiji.

Download the Cassandra Bento Box
--------------------------------

Please begin by downloading the
[Cassandra Bento Box](https://www.google.com/url?q=https%3A%2F%2Fs3.amazonaws.com%2Fwww.kiji.org%2Fcb%2Fcassandra-bento.tar.gz&sa=D&sntz=1&usg=AFQjCNFPgUt8vcWoXjX5_Ah-6lHsHPq_Cg).

Note for OS X users
-------------------

OS X users will likely see errors like the following after executing Kiji commands:

    java.lang.reflect.InvocationTargetException
            at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
            at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
            ...
    Caused by: java.lang.UnsatisfiedLinkError: no snappyjava in java.library.path
            at java.lang.ClassLoader.loadLibrary(ClassLoader.java:1886)
            at java.lang.Runtime.loadLibrary0(Runtime.java:849)
            at java.lang.System.loadLibrary(System.java:1088)
            at org.xerial.snappy.SnappyNativeLoader.loadLibrary(SnappyNativeLoader.java:52)
            ... 20 more

Such commands are not a cause for worry.  You should not have any problems running the tutorial without Snappy installed.

Setup
-----

#### Set environment variables

Let's begin by unpacking the Bento Box and setting up some environment variables that we'll use throughout the rest of the tutorial.

Unpack the Bento Box:

    tar -zxvf cassandra-bento.tar.gz

Set your `KIJI_HOME` environment variable to the root of your Bento Box:

    export KIJI_HOME=/path/to/kiji-bento-ebi

Create an environment variable to use for our Kiji URI:

    export KIJI=kiji-cassandra://localhost:2181/localhost/9042/tutorial

Because this URI starts with `kiji-cassandra://` instead of just `kiji://`, it indicates to Kiji
that it references a Cassandra-backed Kiji instance.

The source code for this example is located in `$KIJI_HOME/examples/cassandra-phonebook.`  This
tutorial assumes that you will use the prebuilt JARs located in
`$KIJI_HOME/examples/cassandra-phonebook/lib`, however you can build your own in
`$KIJI_HOME/examples/cassandra-phonebook/target` by running `mvn package` from
`$KIJI_HOME/examples/cassandra-phonebook`

To work through this tutorial, various Kiji tools will require that Avro data type definitions
particular to the working phonebook example be on the classpath. You can add your artifacts to the
Kiji classpath by running:

    export KIJI_CLASSPATH="$KIJI_HOME/examples/cassandra-phonebook/lib/*"

Now we are ready to start up our Bento Box.  The following command will create a cluster running
Hadoop, ZooKeeper, HBase, and Cassandra:

    source ${KIJI_HOME}/bin/kiji-env.sh
    bento start


Create a Kiji instance
----------------------

Now that our Bento Box is up and running, let's create a Kiji instance:

    kiji install --kiji=${KIJI}

Executing the command should produce the following output:

    Successfully created Kiji instance: kiji-cassandra://localhost:2181/localhost/9042/tutorial/


Create a table
--------------

We need to create a table to store your numerous phonebook contacts. To create a table in Kiji, we
specify a layout and register it with Kiji.  We can specify layouts using JSON or the Kiji Data
Definition Language, DDL.  For this tutorial, we create a table with JSON:

    $KIJI_HOME/bin/kiji create-table --table=${KIJI}/phonebook --layout=$KIJI_HOME/examples/phonebook/layout.json

Most users should instead use the Kiji schema shell and DDL for creating tables, but the
Cassandra version of the schema shell is not yet complete.

After creating the table, we can verify that it exists.  Run the `kiji ls` command to see all of the
tables in our `$KIJI` Kiji instance:

    kiji ls ${KIJI}

You should see the following:

    kiji-cassandra://localhost:2181/localhost/9042/tutorial/phonebook

Hooray, we now have a Kiji table set up!


Read and Write in Kiji
----------------------

Now that we have successfully created a Kiji table, let's start adding contacts.  KijiSchema
supports writing to Kiji tables with the `KijiTableWriter` class.  The phonebook example includes
code that uses a `KijiTableWriter` to write to the phonebook table.

### AddEntry.java

The class `AddEntry.java` is included in the phonebook example source.  It implements a command-line
tool that asks a user for contact information and then uses that information to populate the columns
in a row in the Kiji table `phonebook` for that contact.  To start, `AddEntry.java` connects to Kiji
and opens the phonebook table for writing.  A Kiji instance is specified by a Kiji URI.  A Cassandra
Kiji URI specifies a ZooKeeper quorum and one or more Cassandra nodes to which to connect.  For our
example, we use the following Kiji URI:

    kiji-cassandra://localhost:2181/localhost/9042/tutorial/

This URI indicates that we connect to ZooKeeper on localhost with port 2181, the we connect to
Cassandra on the local host with port 9042, and that our Kiji instance name is "tutorial."

To creat a Kiji URI, we can use `KijiURI.KijiURIBuilder`:

    // Connect to Kiji and open the table.
    kiji = Kiji.Factory.open(
        KijiURI.newBuilder(Fields.PHONEBOOK_URI).build(),
        getConf());
    table = kiji.openTable(TABLE_NAME); // TABLE_NAME = 'phonebook'
    writer = table.openTableWriter();

#### Adding the phonebook entry

We then create an Entity ID using the contact's first and last name.  The Entity ID uniquely
identifies the row for the contact in the Kiji table:

    // Create a row ID with the first and last name.
    final EntityId user = table.getEntityId(first + "," + last);

We write the contact information gained from the user to the appropriate columns in the contact's
row of the Kiji table `phonebook.`  The column names are specified as constants in the `Fields.java`
file.  For example, the first name is written as:

    writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, first);

#### Cleanup

When we are done writing to a Kiji instance, table, or writer that we have previously opened, we
close or release these objects to free resources, in the reverse order that we opened them:

    // Safely free up resources by closing in reverse order.
    ResourceUtils.closeOrLog(writer);
    ResourceUtils.releaseOrLog(table);
    ResourceUtils.releaseOrLog(kiji);
    ResourceUtils.closeOrLog(console);

Something important to note is that Kiji instances and Kiji tables are *released* rather than
closed.  Kiji instances and tables are long-lived objects and many components in your system may
hold references to them.  Rather than require you to define a single "owner" of these objects, Kiji
performs reference counting to determine when to actually close resources.

#### Running the example

You can run the `AddEntry` class on the command line as follows:

    kiji jar $KIJI_HOME/examples/cassandra-phonebook/lib/kiji-phonebook-1.1.5-SNAPSHOT.jar \
        org.kiji.examples.phonebook.AddEntry

This syntax is the preferred mechanism for running your own `main(...)` methods with Kiji and its
dependencies properly on the classpath.

The interactive prompts and reponses should look like the following:

    First name: John
    Last name: Doe
    Email address: jd@wibidata.com
    Telephone: 415-111-2222
    Address line 1: 375 Alabama St
    Apartment:
    Address line 2:
    City: SF
    State: CA
    Zip: 94110

#### Verify

Now let's verify that our entry is present in the phonebook with Kiji scan:

    kiji scan $KIJI/phonebook

You should see a result like the following:

    Scanning kiji table: kiji://localhost:2181/default/phonebook/
    entity-id=['John,Doe'] [1384235579766] info:firstname
                                    John
    entity-id=['John,Doe'] [1384235579766] info:lastname
                                    Doe
    entity-id=['John,Doe'] [1384235579766] info:email
                                    jd@wibidata.com
    entity-id=['John,Doe'] [1384235579766] info:telephone
                                    415-111-2222
    entity-id=['John,Doe'] [1384235579766] info:address
                                    {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}

### Reading from a table

Now that we've added a contact to your phonebook, we should be able to read this contact's
information from the Kiji table.  KijiSchema supports reading from Kiji tables with the
`KijiTableReader` class.  We have included an example of retrieving a single contact from the Kiji
table using the contact's first and last names.

#### Lookup.java

We connect to Kiji and our phonebook table in the same way as we did when writing:

    // Connect to Kiji, open the table and reader.
    kiji = Kiji.Factory.open(
        KijiURI.newBuilder(Fields.PHONEBOOK_URI).build(),
        getConf());
    table = kiji.openTable(TABLE_NAME);

Since we are interested in reading data from Kiji, this time we open a table reader:

    reader = table.openTableReader();

### Looking up an entry

Create an entity ID to retrive a contact using the contact's first and last names:

    final EntityId entityId = table.getEntityId(mFirst + "," + mLast);

Create a data request to specify the desired columns from the Kiji Table.

  final KijiDataRequestBuilder reqBuilder = KijiDataRequest.builder();
  reqBuilder.newColumnsDef()
    .add(Fields.INFO_FAMILY, Fields.FIRST_NAME)
    .add(Fields.INFO_FAMILY, Fields.LAST_NAME)
    .add(Fields.INFO_FAMILY, Fields.EMAIL)
    .add(Fields.INFO_FAMILY, Fields.TELEPHONE)
    .add(Fields.INFO_FAMILY, Fields.ADDRESS);

  final KijiDataRequest dataRequest = reqBuilder.build();

We now retrieve our result by passing the EntityId and data request to our table reader. Doing so results in a KijiRowData containing the data read from the table.

  final KijiRowData rowData = reader.get(entityId, dataRequest);

#### Running the Example

You can run the following command to perform a lookup using the Lookup.java example:

    kiji jar $KIJI_HOME/examples/cassandra-phonebook/lib/kiji-phonebook-1.1.5-SNAPSHOT.jar \
        org.kiji.examples.phonebook.Lookup --first=John --last=Doe

Or use a Kiji command-line get:

    kiji get $KIJI/phonebook --entity-id="['John,Doe']"

### Deleting an Entry
A `KijiTableWriter` is used to perform point deletions on a Kiji table:

    writer = table.openTableWriter();
    writer.deleteRow(entityId);

#### Running the example

Run the `DeleteEntry` class from the command line:


    kiji jar $KIJI_HOME/examples/cassandra-phonebook/lib/kiji-phonebook-1.1.5-SNAPSHOT.jar \
      org.kiji.examples.phonebook.DeleteEntry

and enter the user you wish to remove:

    >   org.kiji.examples.phonebook.DeleteEntry
    First name: John
    Last name: Doe

Now check the user has been removed:

    kiji get $KIJI/phonebook --entity-id="['John,Doe']"

Wrapping up
-----------
This concludes the KijiSchema with Cassandra tutorial. If you are done, you can shut down the cluster by issuing the command:
    bento stop
