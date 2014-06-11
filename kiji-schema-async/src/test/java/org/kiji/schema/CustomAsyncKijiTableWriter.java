package scratch;

import java.io.IOException;

import com.stumbleupon.async.Deferred;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class CustomAsyncKijiTableWriter  {

  //Logger for logging messages
  static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CustomAsyncKijiTableWriter.class);

  public static void main(String[] args) throws IOException {

    //Prepare commonly used byte arrays
    byte[] mTable = Bytes.toBytes("Tmp_Table");
    byte[] r1 = Bytes.toBytes("row1");
    byte[] r2 = Bytes.toBytes("row2");
    byte[] r3 = Bytes.toBytes("row3");
    byte[] c1 = Bytes.toBytes("colfam1");
    byte[] q1 = Bytes.toBytes("qual1");
    byte[] q2 = Bytes.toBytes("qual2");
    byte[] v1 = Bytes.toBytes("Hello [Async] World");
    byte[] v2 = Bytes.toBytes("Hello [Async] World2");
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    //Is this needed with async?
    Configuration conf = HBaseConfiguration.create();
    String zkQuorum = ZKConfig.getZKQuorumServersString(conf);

    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.TestHBaseKijiTableWriterTest-1").build();
    KijiInstaller.get().install(uri,conf);

    Kiji mKiji = new InstanceBuilder(Kiji.Factory.open(uri,conf))
        .withTable("user", layout)
        .withRow("foo")
        .withFamily("info")
        .withQualifier("name").withValue(1L, "foo-val")
        .withQualifier("visits").withValue(1L, 42L)
        .withRow("bar")
        .withFamily("info")
        .withQualifier("visits").withValue(1L, 100L)
        .build();


    //LOG
    System.out.println("Configuration created: " + zkQuorum);

    //Setup Table
    HBaseClient hbclient = new HBaseClient("localhost");
    Deferred<Object> ensureTableExists = hbclient.ensureTableExists("user");
    try {
      ensureTableExists.joinUninterruptibly();
    } catch (Exception e) {
      e.printStackTrace();
    }

    /*
    //LOG
    System.out.println("Created table");

    //Create a Put
    //final CountDownLatch latch = new CountDownLatch(1);
    PutRequest put = new PutRequest(mTable,r1,c1,q1,v1);

    //LOG
    System.out.println("Created Put");

    //Put the items in the table
    hbclient.put(put);

    //LOG
    System.out.println("Added to table");


    //Use a Get to read from the table
    GetRequest get = new GetRequest(mTable,r1,c1,q1);
    ArrayList<KeyValue> result = null;
    try {
      result = hbclient.get(get).join();
    } catch (Exception e) {
      e.printStackTrace();
    }
    for (int i = 0; i < result.size(); i++){
      byte[] m_val = result.get(i).value();
      System.out.println("Value" + i + ": " + Bytes.toString(m_val));
    }

    //Scan through the table
    Scanner scanner = hbclient.newScanner(mTable);
    scanner.setFamily(c1);
    scanner.setQualifier(q1);

    ArrayList<ArrayList<KeyValue>> rows = null;
    ArrayList<Deferred<Boolean>> workers = new ArrayList<Deferred<Boolean>>();
    try {
      rows = scanner.nextRows().joinUninterruptibly();
    } catch (Exception e) {
      e.printStackTrace();
    }
    while (rows != null) {

      System.out.println("Received a page of users.");
      for (ArrayList<KeyValue> row : rows){
        KeyValue kv = row.get(0);
        byte[] expected = kv.value();
        String tmp_val = new String(expected);
        PutRequest mPut = new PutRequest(mTable, kv.key(), kv.family(),
            kv.qualifier(),Bytes.toBytes("NEW::" + tmp_val));
        System.out.println("VALUE: " + tmp_val);
        Deferred<Boolean> d = hbclient.compareAndSet(mPut, expected)
            .addCallback(new InterpretResponse(new String(kv.key())))
            .addCallbacks(new ResultToMessage(), new FailureToMessage())
            .addCallback(new SendMessage());
        workers.add(d);
      }
      try {
        rows = scanner.nextRows().joinUninterruptibly();
      } catch (Exception e) {
        e.printStackTrace();
      }

    }

    try {
      Deferred.group(workers).join();
    } catch (DeferredGroupException e){
      LOG.info(e.getCause().getMessage());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }





    */
    System.out.println("Finished.");


    //Close the client
    try {
      hbclient.shutdown().joinUninterruptibly();
    } catch (Exception e) {
      e.printStackTrace();
    }


  }

}
