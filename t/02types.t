#!/usr/bin/perl

use strict;
use warnings;
use utf8;

use Test::More;
use Test::HexString;
use Math::BigInt;

use Protocol::CassandraCQL;
BEGIN {
   *encode = \&Protocol::CassandraCQL::encode;
   *decode = \&Protocol::CassandraCQL::decode;
}

is_hexstr( encode( ASCII => "hello" ), "hello", 'encode ASCII' );
is       ( decode( ASCII => "hello" ), "hello", 'decode ASCII' );
ok( !defined eval { encode( ASCII => "crème" ); 1 }, 'encode ASCII reject' );

is_hexstr( encode( BIGINT => 1234567890123 ), "\x00\x00\x01\x1f\x71\xfb\x04\xcb", 'encode BIGINT' );
is       ( decode( BIGINT => "\x00\x00\x01\x1f\x71\xfb\x04\xcb" ), 1234567890123, 'decode BIGINT' );

is_hexstr( encode( BLOB => "\x01\x23" ), "\x01\x23", 'encode BLOB' );
is_hexstr( decode( BLOB => "\x01\x23" ), "\x01\x23", 'decode BLOB' );

is_hexstr( encode( BOOLEAN => 1 == 1 ), "\x01", 'encode BOOLEAN true' );
is_hexstr( encode( BOOLEAN => 1 == 2 ), "\x00", 'encode BOOLEAN false' );
ok(        decode( BOOLEAN => "\x01" ),         'decode BOOLEAN true' );
ok(       !decode( BOOLEAN => "\x00" ),         'decode BOOLEAN false' );

is_hexstr( encode( DOUBLE => 12.3456 ), "\x40\x28\xb0\xf2\x7b\xb2\xfe\xc5", 'encode DOUBLE' );
is       ( decode( DOUBLE => "\x40\x28\xb0\xf2\x7b\xb2\xfe\xc5" ), 12.3456, 'decode DOUBLE' );

is_hexstr( encode( FLOAT => 1.234 ), "\x3f\x9d\xf3\xb6", 'encode FLOAT' );
# FLOAT decode might not be exact
ok(   abs( decode( FLOAT => "\x3f\x9d\xf3\xb6" ) - 1.234 ) < 0.001, 'decode FLOAT' );

is_hexstr( encode( INT => 12345678 ), "\x00\xbc\x61\x4e", 'encode INT' );
is       ( decode( INT => "\x00\xbc\x61\x4e" ), 12345678, 'decode INT' );

# UNIX epoch timestamps 1377686281 == 2013/08/28 11:38:01
is_hexstr( encode( TIMESTAMP => 1377686281 ), "\x00\x00\x01\x40\xc4\x80\x5b\x28", 'encode TIMESTAMP' );
is       ( decode( TIMESTAMP => "\x00\x00\x01\x40\xc4\x80\x5b\x28" ), 1377686281, 'decode TIMESTAMP' );

is_hexstr( encode( VARCHAR => "café" ), "caf\xc3\xa9", 'encode VARCHAR' );
is       ( decode( VARCHAR => "caf\xc3\xa9" ), "café", 'decode VARCHAR' );

is_hexstr( encode( VARINT => 123456 ), "\x01\xe2\x40", 'encode VARINT +ve small' );
is       ( decode( VARINT => "\x01\xe2\x40" ), 123456, 'decode VARINT +ve small' );
is_hexstr( encode( VARINT => -123 ), "\x85", 'encode VARINT -ve' );
is       ( decode( VARINT => "\x85" ), -123, 'decode VARINT -ve' );

is_hexstr( encode( VARINT => Math::BigInt->new("1234567890987654321") ), "\x11\x22\x10\xf4\xb1\x6c\x1c\xb1", 'encode VARCHAR +ve large' );
is       ( decode( VARINT => "\x11\x22\x10\xf4\xb1\x6c\x1c\xb1" ), "1234567890987654321", 'decode VARCHAR +ve large' );

# boundary cases of VARINT encoding
{
   sub test_VARINT {
      my $n = shift;
      is( decode( VARINT => encode( VARINT => $n ) ), $n, "encode/decode VARINT $n" );
   }

   test_VARINT( 0 );
   test_VARINT( 1 );
   test_VARINT( -1 );
   test_VARINT( 0xff ); # test zero-extension in the +ve case
   test_VARINT( -0xff ); # test sign-extension in the -ve case
}

# DECIMAL depends on VARINT so do it afterwards
is_hexstr( encode( DECIMAL => 0 ), "\0\0\0\0\x00", 'encode DECIMAL zero' );
is       ( decode( DECIMAL => "\0\0\0\0\x00" ), 0, 'decode DECIMAL zero' );
is_hexstr( encode( DECIMAL => 100 ), "\0\0\0\0\x64", 'encode DECIMAL 100' );
is       ( decode( DECIMAL => "\0\0\0\0\x64" ), 100, 'decode DECIMAL 100' );
is_hexstr( encode( DECIMAL => 0.25 ), "\0\0\0\2\x19", 'encode DECIMAL 0.25' );
is       ( decode( DECIMAL => "\0\0\0\2\x19" ), 0.25, 'decode DECIMAL 0.25' );

done_testing;
