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
use Protocol::CassandraCQL::Frame;

my $loop = IO::Async::Loop->new();
testing_loop( $loop );

my ( $S1, $S2 ) = IO::Async::OS->socketpair() or die "Cannot create socket pair - $!";

my $conn = Net::Async::CassandraCQL::Connection->new(
   handle => $S1,
);

ok( defined $conn, 'defined $conn' );

$loop->add( $conn );

# Simulate the smallest possible STARTUP/READY exchange
{
   my $f = $conn->send_message( 1,
      Protocol::CassandraCQL::Frame->new->pack_string_list( [] ) );

   isa_ok( $f, "Future", '$f isa Future for ->send_message' );

   my $stream = "";
   wait_for_stream { length $stream >= 10 } $S2 => $stream;

   is_hexstr( $stream,
              "\x01\x00\x01\x01\0\0\0\2\0\0",
              'stream after ->send_message( 1, "\0\0" )' );

   ok( !$f->is_ready, '$f not ready yet before server replies' );

   $S2->syswrite( "\x81\x00\x01\x02\0\0\0\0" );

   wait_for { $f->is_ready };

   ok( $f->is_ready, '$f now ready after server replies' );
   is( scalar $f->get, 2, '$f->get returns reply opcode' );
   is( ref +( $f->get )[1], "Protocol::CassandraCQL::Frame", '$f->get [1] is reply Frame' );
}

# Two in flight
{
   my $f1 = $conn->send_message( 3,
      Protocol::CassandraCQL::Frame->new->pack_string( "ONE" ) );
   my $f2 = $conn->send_message( 4,
      Protocol::CassandraCQL::Frame->new->pack_string( "TWO" ) );

   my $stream = "";
   wait_for_stream { length $stream >= 8+5 + 8+5 } $S2 => $stream;

   is_hexstr( $stream,
              "\x01\x00\x01\x03\0\0\0\5\0\3ONE" .
              "\x01\x00\x02\x04\0\0\0\5\0\3TWO",
              'stream after two concurrent ->send_message calls' );

   $S2->syswrite( "\x81\x00\x02\x06\0\0\0\0" );

   wait_for { $f2->is_ready };

   ok( !$f1->is_ready, '$f1 is not ready yet' );
   is( scalar $f2->get, 6, '$f2->get is 6' );

   $S2->syswrite( "\x81\x00\x01\x05\0\0\0\0" );

   wait_for { $f1->is_ready };

   is( scalar $f1->get, 5, '$f1->get is 5' );
}

# Error
{
   my $f = $conn->send_message( 5, Protocol::CassandraCQL::Frame->new );

   my $stream = "";
   wait_for_stream{ length $stream >= 8 } $S2 => $stream;

   $S2->syswrite( "\x81\x00\x01\x00\0\0\0\x0a\0\0\0\0\0\4Bad!" );

   wait_for { $f->is_ready };

   ok( $f->failure, '$f has failed' );
   is_deeply( [ $f->failure ], [ "OPCODE_ERROR: Bad!\n", 0, Protocol::CassandraCQL::Frame->new ],
              '$f->failure' );
}

done_testing;
