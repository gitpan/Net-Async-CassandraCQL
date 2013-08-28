#!/usr/bin/perl

use strict;
use warnings;

my %CONFIG;
use Test::More;

BEGIN {
   # This test attempts to talk to a real Cassandra database, and will
   # create its own keyspace to work in.
   #
   # It is disabled by default, but to enable it create a t/local.yaml file
   # containing the host, keyspace, and optionally service port number to
   # connect to.
   -e "t/local.yaml" or plan skip_all => "No t/local.yaml config";
   require YAML;
   %CONFIG = %{ YAML::LoadFile( "t/local.yaml" ) };
   defined $CONFIG{host} or plan skip_all => "t/local.yaml does not define a host";
   defined $CONFIG{keyspace} or plan skip_all => "t/local.yaml does not define a keyspace";
}

use Test::HexString;

use IO::Async::Test;
use IO::Async::Loop;

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL qw( CONSISTENCY_ONE );

my $loop = IO::Async::Loop->new();
testing_loop( $loop );

my $cass = Net::Async::CassandraCQL->new;
$loop->add( $cass );

$cass->connect( host => $CONFIG{host}, service => $CONFIG{port} )->get;

$cass->query( "USE $CONFIG{keyspace};", CONSISTENCY_ONE )->get;

$cass->query( "CREATE TABLE tbl1 (key varchar PRIMARY KEY, t1 varchar, i1 int);", CONSISTENCY_ONE )->get;
my $table = 1;
END { $table and $cass->query( "DROP TABLE tbl1;", CONSISTENCY_ONE )->await }

pass( "CREATE TABLE" );

$cass->query( "INSERT INTO tbl1 (key, t1) VALUES ('the-key', 'the-value');", CONSISTENCY_ONE )->get;
pass( "INSERT INTO tbl" );

# SELECT as ->query
{
   my ( $type, $result ) = $cass->query( "SELECT key, t1 FROM tbl1;", CONSISTENCY_ONE )->get;

   is( $type, "rows", "SELECT query result type is rows" );

   is( $result->columns, 2, '$result has 2 columns' );
   is( $result->rows, 1, '$result has 1 row' );
   is( scalar $result->column_name(0), "$CONFIG{keyspace}.tbl1.key", 'column_name 0' );
   is( scalar $result->column_name(1), "$CONFIG{keyspace}.tbl1.t1",  'column_name 1' );
   is( $result->column_type(0), "VARCHAR", 'column_type 0' );
   is( $result->column_type(1), "VARCHAR", 'column_type 1' );

   is( $result->find_column( "key" ), 0, 'find_column key' );

   is_deeply( $result->row_array(0),
              [ "the-key", "the-value" ],
              'row_array(0)' );

   is_deeply( $result->row_hash(0),
              { key => "the-key", t1 => "the-value" },
              'row_hash(0)' );
}

# INSERT as ->prepare / ->execute
{
   my $query = $cass->prepare( "INSERT INTO tbl1 (key, i1) VALUES (?, ?);" )->get;

   ok( length $query->id, '$query->id is set for prepared INSERT' );

   is( $query->columns, 2, '$query has 2 columns for prepared INSERT' );
   is( scalar $query->column_name(0), "$CONFIG{keyspace}.tbl1.key", 'column_name 0' );
   is( scalar $query->column_name(1), "$CONFIG{keyspace}.tbl1.i1",  'column_name 1' );
   is( $query->column_type(0), "VARCHAR", 'column_type 0' );
   is( $query->column_type(1), "INT",     'column_type 1' );

   # ARRAY
   $query->execute( [ "another-key", 123456789 ], CONSISTENCY_ONE )->get;

   # HASH
   $query->execute( { key => "second-key", i1 => 987654321 }, CONSISTENCY_ONE )->get;
}

# SELECT as ->prepare / ->execute
{
   my $query = $cass->prepare( "SELECT i1 FROM tbl1 WHERE key = ?;" )->get;

   ok( length $query->id, '$query->id is set for prepared SELECT' );

   is( $query->columns, 1, '$query has 1 column for prepared SELECT' );

   my ( $type, $result ) = $query->execute( [ "another-key" ], CONSISTENCY_ONE )->get;

   is( $type, "rows", 'SELECT prepare/execute result type is rows' );

   is_deeply( $result->row_array(0),
              [ 123456789 ],
              'row_array(0) for SELECT prepare/execute' );
}

# Now test we have the right (de)serialisation form for all the numeric types
{
   $cass->query( <<'EOCQL', CONSISTENCY_ONE )->get;
CREATE TABLE numbers (
   key text PRIMARY KEY,
   i int, bi bigint, vi varint,
   flt float, dbl double, d decimal);
EOCQL
   my $table = 1;
   END { $table and $cass->query( "DROP TABLE numbers;", CONSISTENCY_ONE )->await }

   my %ints = (
      zero => 0,
      one  => 1,
      two  => 2,
      ten  => 10,
      minus_one => -1,
      million => 1_000_000,
   );

   my $getn_stmt = $cass->prepare( "SELECT * FROM numbers WHERE key = ?;" )->get;

   foreach my $name ( keys %ints ) {
      my $n = $ints{$name};
      $cass->query( "INSERT INTO numbers (key, i, bi, vi, flt, dbl, d) VALUES ('$name', $n, $n, $n, $n, $n, $n);", CONSISTENCY_ONE )->get;
   }

   foreach my $name ( keys %ints ) {
      my $n = $ints{$name};
      my ( undef, $result ) = $getn_stmt->execute( { key => $name }, CONSISTENCY_ONE )->get;

      is_deeply( $result->row_hash(0),
                 { key => $name, map { +$_ => $n } qw( i bi vi flt dbl d ) },
                 "Number deserialisation for $name" );
   }

   my %reals = (
      tenth => 0.1,
      half  => 0.5,
      e     => exp(1),
   );

   foreach my $name ( keys %reals ) {
      my $n = $reals{$name};
      $cass->query( "INSERT INTO numbers (key, flt, dbl, d) VALUES ('$name', $n, $n, $n);", CONSISTENCY_ONE )->get;
   }

   foreach my $name ( keys %reals ) {
      my $n = $reals{$name};
      my ( undef, $result ) = $getn_stmt->execute( { key => $name }, CONSISTENCY_ONE )->get;

      # floats aren't digit-wise exact
      my $row = $result->row_hash(0);
      is( sprintf( "%.5g", delete $row->{flt} ), sprintf( "%.5g", $n ),
          "Float deserialisation for $name" );

      is_deeply( $row,
                 { key => $name,
                   dbl => $n, d => $n,
                   i => undef, bi => undef, vi => undef },
                 "Number deserialisation for $name" );
   }
}

done_testing;
