#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Identity;
use Test::Refcount;

use t::MockConnection;

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
   primaries => 3,
   prefer_dc => "DC1",
);

my $f = $cass->connect;

ok( my $c = $conns{"10.0.0.1"}, 'Connected to 10.0.0.1' );

# Initial nodelist query
$c->send_nodelist(
   local => { dc => "DC1", rack => "rack1" },
   peers => {
      "10.0.0.2" => { dc => "DC1", rack => "rack1" },
      "10.0.0.3" => { dc => "DC1", rack => "rack1" },
      "10.0.1.1" => { dc => "DC2", rack => "rack1" },
      "10.0.1.2" => { dc => "DC2", rack => "rack1" },
      "10.0.1.3" => { dc => "DC2", rack => "rack1" },
   },
);

$f->get;

ok( defined $conns{"10.0.0.2"}, 'Connected to 10.0.0.2' );
ok( defined $conns{"10.0.0.3"}, 'Connected to 10.0.0.3' );

ok( !defined $conns{"10.0.1.1"} &&
    !defined $conns{"10.0.1.2"} &&
    !defined $conns{"10.0.1.3"}, 'Not connected to DC2 hosts' );

# Only a preference; should still fall back to other DCs if one of these fails

# Fake closure
undef $conns{"10.0.0.3"};
$cass->_closed_node( "10.0.0.3" );

is( defined($conns{"10.0.1.1"}) + defined($conns{"10.0.1.2"}) + defined($conns{"10.0.1.3"}),
    1, 'One of the DC2 hosts now connected after DC1 failure' );

done_testing;
