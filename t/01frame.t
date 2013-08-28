#!/usr/bin/perl

use strict;
use warnings;
use utf8;

use Test::More;
use Test::HexString;

use Protocol::CassandraCQL::Frame;

# Empty
is( Protocol::CassandraCQL::Frame->new->bytes, "", '->bytes empty' );

# short
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_short( 0x1234 );
   is_hexstr( $frame->bytes, "\x12\x34", '->pack_short' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_short, 0x1234, '->unpack_short' );
}

# int
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_int( 0x12345678 );
   $frame->pack_int( -100 );
   is_hexstr( $frame->bytes, "\x12\x34\x56\x78\xff\xff\xff\x9c", '->pack_int and -ve' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_int, 0x12345678, '->unpack_int' );
   is( $frame->unpack_int, -100, '->unpack_int -ve' );
}

# string
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_string( "hello" );
   $frame->pack_string( "sandviĉon" );
   is_hexstr( $frame->bytes, "\x00\x05hello\x00\x0asandvi\xc4\x89on", '->pack_string and UTF-8' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_string, "hello", '->unpack_string' );
   is( $frame->unpack_string, "sandviĉon", '->unpack_string UTF-8' );
}

# long string
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_lstring( "hello" );
   $frame->pack_lstring( "sandviĉon" );
   is_hexstr( $frame->bytes, "\x00\x00\x00\x05hello\x00\x00\x00\x0asandvi\xc4\x89on",
              '->pack_lstring and UTF-8' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_lstring, "hello", '->unpack_lstring' );
   is( $frame->unpack_lstring, "sandviĉon", '->unpack_lstring UTF-8' );
}

# UUID
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_uuid( "X"x16 );
   is_hexstr( $frame->bytes, "X"x16, '->pack_uuid' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_uuid, "X"x16, '->unpack_uuid' );
}

# string list
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_string_list( [qw( one two three )] );
   is_hexstr( $frame->bytes, "\x00\x03\x00\x03one\x00\x03two\x00\x05three", '->pack_string_list' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is_deeply( $frame->unpack_string_list, [qw( one two three )], '->unpack_string_list' );
}

# bytes
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_bytes( "abcd" );
   $frame->pack_bytes( undef );
   is_hexstr( $frame->bytes, "\x00\x00\x00\x04abcd" . "\xff\xff\xff\xff", '->pack_bytes and undef' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_bytes, "abcd", '->unpack_bytes' );
   is( $frame->unpack_bytes, undef,  '->unpack_bytes undef' );
}

# short bytes
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_short_bytes( "efgh" );
   is_hexstr( $frame->bytes, "\x00\x04efgh", '->pack_short_bytes' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is( $frame->unpack_short_bytes, "efgh", '->unpack_short_bytes' );
}

# string map
{
   my $frame = Protocol::CassandraCQL::Frame->new;
   $frame->pack_string_map( { one => "ONE", two => "TWO" } );
   is_hexstr( $frame->bytes, "\x00\x02" . "\x00\x03one\x00\x03ONE" .
                                          "\x00\x03two\x00\x03TWO", '->pack_string_map' );

   $frame = Protocol::CassandraCQL::Frame->new( $frame->bytes );
   is_deeply( $frame->unpack_string_map, { one => "ONE", two => "TWO" }, '->unpack_string_map' );
}

done_testing;
