#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::HexString;

use IO::Async::Test;
use IO::Async::OS;
use IO::Async::Loop;
use IO::Async::Stream;

use Net::Async::CassandraCQL::Connection;

my $loop = IO::Async::Loop->new();
testing_loop( $loop );

# Snappy
SKIP: {
   skip "Compress::Snappy is unavailable", 1 unless Net::Async::CassandraCQL::Connection::HAVE_SNAPPY;

   my ( $S1, $S2 ) = IO::Async::OS->socketpair() or die "Cannot create socket pair - $!";

   my $conn = Net::Async::CassandraCQL::Connection->new( handle => $S1 );

   $loop->add( $conn );

   my $f = $conn->startup;

   my $stream = "";
   wait_for_stream { length $stream > 8 } $S2 => $stream;

   $S2->syswrite( "\x81\x00\x01\x02\0\0\0\0" );

   $stream = "";

   # Something sure to compress
   $f = $conn->send_message( 10, Protocol::CassandraCQL::Frame->new->pack_string( "A" x 100 ) );

   wait_for_stream { length $stream >= 8 } $S2 => $stream;

   # Unpack just the framing
   my ( $version, $flags, $id, $opcode, $len ) = unpack "C C C C N", $stream;
   substr $stream, 0, 8, "";

   is( $version, 1, 'version 1 request' );
   is( $flags, 1, 'FLAG_COMPRESS' );

   wait_for_stream { length $stream >= $len } $S2 => $stream;

   my $body = Compress::Snappy::decompress( $stream );

   is_hexstr( $body,
              "\x00\x64" . ( "A"x100 ),
              'Decompressed body' );

   $body = Compress::Snappy::compress( "\x00\x64" . ( "B"x100 ) );

   $S2->syswrite( pack "C C C C N a*", $version|0x80, 1, $id, 11, length $body, $body );

   wait_for { $f->is_ready };

   my ( $op, $response ) = $f->get;

   is( $response->unpack_string, "B" x 100, 'Response body decompressed' );

   $loop->remove( $conn );
}

done_testing;
