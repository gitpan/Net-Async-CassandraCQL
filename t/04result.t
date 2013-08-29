#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::Result;

# Single column/row
{
   my $result = Protocol::CassandraCQL::Result->from_frame(
      Protocol::CassandraCQL::Frame->new(
         "\0\0\0\1\0\0\0\1\0\4test\0\5table\0\6column\0\x0a" . # metadata
         "\0\0\0\1" .   # row count
         "\0\0\0\4data" # row 0
      )
   );

   is( scalar $result->columns, 1, '$result->columns is 1' );

   is( scalar $result->rows, 1, '$result->rows is 1' );

   is_deeply( [ $result->rowbytes( 0 ) ],
              [ "data" ],
              '$result->rowbytes(0)' );

   is_deeply( $result->row_array( 0 ),
              [ "data" ],
              '$result->row_array(0)' );

   is_deeply( $result->row_hash( 0 ),
              { column => "data" },
              '$result->row_hash(0)' );
}

# Multiple columns
{
   my $result = Protocol::CassandraCQL::Result->from_frame(
      Protocol::CassandraCQL::Frame->new(
         "\0\0\0\1\0\0\0\2\0\4test\0\5table\0\3key\0\x0a\0\1i\0\x09" . # metadata
         "\0\0\0\1" .   # row count
         "\0\0\0\4aaaa\0\0\0\4\x00\x00\x00\x64" # row 0
      )
   );

   is( scalar $result->columns, 2, '$result->columns is 2' );

   is_deeply( $result->row_array( 0 ),
              [ "aaaa", 100 ],
              '$result->row_array(0)' );

   is_deeply( $result->row_hash( 0 ),
              { key => "aaaa", i => 100 },
              '$result->row_hash(0)' );
}

# Multiple rows
{
   my $result = Protocol::CassandraCQL::Result->from_frame(
      Protocol::CassandraCQL::Frame->new(
         "\0\0\0\1\0\0\0\2\0\4test\0\7numbers\0\4name\0\x0a\0\1i\0\x09" . # metadata
         "\0\0\0\3" . # row count
         "\0\0\0\4zero\0\0\0\4\x00\x00\x00\x00" . # row 0
         "\0\0\0\3one\0\0\0\4\x00\x00\x00\x01"  . # row 1
         "\0\0\0\3two\0\0\0\4\x00\x00\x00\x02"    # row 2
      )
   );

   is( scalar $result->rows, 3, '$result->rows is 3' );

   is_deeply( [ $result->rows_array ],
              [ [ "zero", 0 ], [ "one", 1 ], [ "two", 2 ] ],
              '$result->rows_array' );

   is_deeply( [ $result->rows_hash ],
              [ { name => "zero", i => 0 }, { name => "one", i => 1 }, { name => "two", i => 2 } ],
              '$result->rows_hash' );
}

done_testing;
