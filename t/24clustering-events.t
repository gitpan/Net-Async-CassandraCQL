#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Identity;
use Test::Refcount;

use t::MockConnection;
use Socket qw( pack_sockaddr_in inet_aton );

use Net::Async::CassandraCQL;
use Protocol::CassandraCQL::Result 0.06;

# Mock the ->_connect_node method
no warnings 'redefine';
my %conns;
local *Net::Async::CassandraCQL::_connect_node = sub {
   my $self = shift;
   my ( $connect_host, $connect_service ) = @_;
   $conns{$connect_host} = my $conn = t::MockConnection->new( $connect_host );
   return Future->new->done( $conn );
};

my $cass = Net::Async::CassandraCQL->new(
   host => "10.0.0.1",
);

my $f = $cass->connect;

ok( my $c = $conns{"10.0.0.1"}, 'Connected to 10.0.0.1' );

# Initial nodelist query
$c->send_nodelist(
   local => { dc => "DC1", rack => "rack1" },
   peers => {
      "10.0.0.2" => { dc => "DC1", rack => "rack1" },
   },
);

$f->get;

ok( $c->is_registered, 'Using 10.0.0.1 for events' );

ok( !defined $cass->{nodes}{"10.0.0.2"}{down_time}, 'Node 10.0.0.2 does not yet have down_time' );

$conns{"10.0.0.1"}->invoke_event(
   on_status_change => DOWN => pack_sockaddr_in( 0, inet_aton( "10.0.0.2" ) ),
);

ok( defined $cass->{nodes}{"10.0.0.2"}{down_time}, 'Node 10.0.0.2 has down_time after STATUS_CHANGE DOWN' );

done_testing;
