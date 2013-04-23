#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
CREATE TABLE 'users' WITH DESCRIPTION 'example for maven archetype'
ROW KEY FORMAT HASH PREFIXED(2)
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = 10,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'information about the user' (
    name "string" WITH DESCRIPTION 'name of user',
    example_record CLASS ${package}.ExampleRecord WITH DESCRIPTION 'Our avro record'
  )
);
