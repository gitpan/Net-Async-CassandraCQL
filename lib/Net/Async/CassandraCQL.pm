#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::CassandraCQL;

use strict;
use warnings;
use 5.010;

our $VERSION = '0.06';

use base qw( IO::Async::Notifier );

use Carp;

use Future::Utils qw( fmap_void );
use List::Util qw( shuffle );
use Scalar::Util qw( weaken );
use Socket qw( inet_ntop AF_INET AF_INET6 );

use Protocol::CassandraCQL qw( CONSISTENCY_ONE );

use Net::Async::CassandraCQL::Connection;

use constant DEFAULT_CQL_PORT => 9042;

# Time after which down nodes will be retried
use constant NODE_RETRY_TIME => 60;

=head1 NAME

C<Net::Async::CassandraCQL> - use Cassandra databases with L<IO::Async> using CQL

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::CassandraCQL;
 use Protocol::CassandraCQL qw( CONSISTENCY_QUORUM );

 my $loop = IO::Async::Loop->new;

 my $cass = Net::Async::CassandraCQL->new(
    host => "localhost",
    keyspace => "my-keyspace",
    default_consistency => CONSISTENCY_QUORUM,
 );
 $loop->add( $cass );


 $cass->connect->get;


 my @f;
 foreach my $number ( 1 .. 100 ) {
    push @f, $cass->query( "INSERT INTO numbers (v) VALUES $number" );
 }
 Future->needs_all( @f )->get;


 my $get_stmt = $cass->prepare( "SELECT v FROM numbers" )->get;

 my ( undef, $result ) = $get_stmt->execute( [] )->get;

 foreach my $row ( $result->rows_hash ) {
    say "We have a number " . $row->{v};
 }

=head1 DESCRIPTION

This module allows use of the C<CQL3> interface of a Cassandra database. It
fully supports asynchronous operation via L<IO::Async>, allowing both direct
queries and prepared statements to be managed concurrently, if required.
Alternatively, as the interface is entirely based on L<Future> objects, it can
be operated synchronously in a blocking fashion by simply awaiting each
individual operation by calling the C<get> method.

It is based on L<Protocol::CassandraCQL>, which more completely documents the
behaviours and limits of its ability to communicate with Cassandra.

=cut

=head1 PARAMETERS

The following named parameters may be passed to C<new> or C<configure>:

=over 8

=item host => STRING

The hostname of the Cassandra node to connect to

=item service => STRING

Optional. The service name or port number to connect to.

=item username => STRING

=item password => STRING

Optional. Authentication details to use for C<PasswordAuthenticator>.

=item keyspace => STRING

Optional. If set, a C<USE keyspace> query will be issued as part of the
connect method.

=item default_consistency => INT

Optional. Default consistency level to use if none is provided to C<query> or
C<execute>.

=back

=cut

sub configure
{
   my $self = shift;
   my %params = @_;

   foreach (qw( host service username password keyspace default_consistency )) {
      $self->{$_} = delete $params{$_} if exists $params{$_};
   }

   $self->SUPER::configure( %params );
}

=head1 METHODS

=cut

=head2 $str = $cass->quote( $str )

Quotes a string argument suitable for inclusion in an immediate CQL query
string.

In general, it is better to use a prepared query and pass the value as an
execute parameter though.

=cut

sub quote
{
   my $self = shift;
   my ( $str ) = @_;

   # CQL's 'quoting' handles any character except quote marks, which have to
   # be doubled
   $str =~ s/'/''/g;
   return qq('$str');
}

=head2 $str = $cass->quote_identifier( $str )

Quotes an identifier name suitable for inclusion in a CQL query string.

=cut

sub quote_identifier
{
   my $self = shift;
   my ( $str ) = @_;

   return $str if $str =~ m/^[a-z_][a-z0-9_]+$/;

   # CQL's "quoting" handles any character except quote marks, which have to
   # be doubled
   $str =~ s/"/""/g;
   return qq("$str");
}

=head2 $cass->connect( %args ) ==> ()

Connects to the Cassandra node and starts up the connection. The returned
Future will yield nothing on success.

Takes the following named arguments:

=over 8

=item host => STRING

=item service => STRING

=back

A host name is required, either as a named argument or as a configured value
on the object. If the service name is missing, the default CQL port will be
used instead.

=cut

# ->_connect_node( $host, $service ) ==> $conn
# mocked during unit testing
sub _connect_node
{
   my $self = shift;
   my ( $host, $service ) = @_;

   $service //= $self->{service} // DEFAULT_CQL_PORT;

   my $conn = Net::Async::CassandraCQL::Connection->new(
      on_closed => sub {
         my $node = shift;
         $self->remove_child( $node );
         $self->_closed_node( $node->nodeid );
      },
      map { $_ => $self->{$_} } qw( username password ),
   );
   $self->add_child( $conn );

   $conn->connect(
      host    => $host,
      service => $service,
   )->on_fail( sub {
      # Some kinds of failure have already removed it
      $self->remove_child( $conn ) if $conn->parent;
   });
}

# invoked during unit testing
sub _closed_node
{
   my $self = shift;
   my ( $nodeid ) = @_;

   my $now = time();

   my $node = $self->{nodes}{$nodeid} or return;

   undef $node->{conn};
   undef $node->{ready_f};
   $node->{down_time} = $now;

   if( $nodeid eq $self->{primary} ) {
      $self->debug_printf( "PRIMARY DOWN %s", $nodeid );
      undef $self->{primary};

      $self->_pick_new_primary( $now );
   }
}

sub connect
{
   my $self = shift;
   my %args = @_;

   my $conn;

   $self->_connect_node(
      $args{host}    // $self->{host},
      $args{service},
   )->then( sub {
      ( $conn ) = @_;
      $self->_list_nodes( $conn );
   })->then( sub {
      my @nodes = @_;

      $self->{nodes} = \my %nodes;
      foreach my $node ( @nodes ) {
         my $n = $nodes{$node->{host}} = {
            data_center => $node->{data_center},
            rack        => $node->{rack},
         };

         $n->{conn} = $conn if $node->{host} eq $conn->nodeid;
      }

      # Initial primary on the seed
      $self->{primary} = ( grep { $nodes{$_}{conn} } keys %nodes )[0];
      my $node = $nodes{$self->{primary}};

      $self->debug_printf( "PRIMARY PICKED %s", $self->{primary} );
      $node->{ready_f} = $self->_ready_node( $self->{primary} );
   });
}

sub _pick_new_primary
{
   my $self = shift;
   my ( $now ) = @_;

   my $nodes = $self->{nodes};

   my $new_primary;

   # Expire old down statuses and try to find a non-down node
   foreach my $nodeid ( shuffle keys %$nodes ) {
      my $node = $nodes->{$nodeid};

      delete $node->{down_time} if defined $node->{down_time} and $now - $node->{down_time} > NODE_RETRY_TIME;
      $new_primary ||= $nodeid if !$node->{down_time};
   }

   if( !defined $new_primary ) {
      die "ARGH! TODO: can't find a new node to be primary\n";
   }

   $self->debug_printf( "PRIMARY PICKED %s", $new_primary );
   $self->{primary} = $new_primary;

   my $node = $nodes->{$new_primary};

   my $f = $node->{ready_f} = $self->_connect_node( $new_primary )->then( sub {
      my ( $conn ) = @_;
      $node->{conn} = $conn;

      $self->_ready_node( $new_primary )
   })->on_fail( sub {
      print STDERR "ARGH! NEW PRIMARY FAILED: @_\n";
   })->on_done( sub {
      $self->debug_printf( "PRIMARY UP %s", $new_primary );
   });
}

sub _ready_node
{
   my $self = shift;
   my ( $nodeid ) = @_;

   my $node = $self->{nodes}{$nodeid} or die "Don't have a node id $nodeid";
   my $conn = $node->{conn} or die "Expected node to have a {conn} but it doesn't";

   my $keyspace = $self->{keyspace};

   my $keyspace_f =
      $keyspace ? $conn->query( "USE " . $self->quote_identifier( $keyspace ), CONSISTENCY_ONE )
                : Future->new->done;

   $keyspace_f->then( sub {
      my $conn_f = Future->new->done( $conn );
      return $conn_f unless my $queries = $self->{queries};

      # Expire old ones
      defined $queries->{$_} or delete $queries->{$_} for keys %$queries;
      return $conn_f unless keys %$queries;

      ( fmap_void {
         my $query = shift;
         $conn->prepare( $query->cql, $self );
      } foreach => [ values %$queries ] )
         ->then( sub { $conn_f } );
   });
}

sub _get_a_node
{
   my $self = shift;

   defined $self->{primary} or die "ARGH: $self -> _get_a_node called with no defined PRIMARY";

   my $nodes = $self->{nodes};

   if( my $node = $nodes->{$self->{primary}} ) {
      return $node->{ready_f};
   }

   die "ARGH: don't have a primary node";
}

=head2 $cass->query( $cql, $consistency ) ==> ( $type, $result )

Performs a CQL query. On success, the values returned from the Future will
depend on the type of query.

For C<USE> queries, the type is C<keyspace> and C<$result> is a string giving
the name of the new keyspace.

For C<CREATE>, C<ALTER> and C<DROP> queries, the type is C<schema_change> and
C<$result> is a 3-element ARRAY reference containing the type of change, the
keyspace and the table name.

For C<SELECT> queries, the type is C<rows> and C<$result> is an instance of
L<Protocol::CassandraCQL::Result> containing the returned row data.

For other queries, such as C<INSERT>, C<UPDATE> and C<DELETE>, the future
returns nothing.

=cut

sub query
{
   my $self = shift;
   my ( $cql, $consistency ) = @_;

   $consistency //= $self->{default_consistency};
   defined $consistency or croak "'query' needs a consistency level";

   $self->_get_a_node->then( sub {
      shift->query( $cql, $consistency );
   });
}

=head2 $cass->query_rows( $cql, $consistency ) ==> $result

A shortcut wrapper for C<query> which expects a C<rows> result and returns it
directly. Any other result is treated as an error. The returned Future returns
a C<Protocol::CassandraCQL::Result> directly

=cut

sub query_rows
{
   my $self = shift;
   my ( $cql, $consistency ) = @_;

   $self->query( $cql, $consistency )->then( sub {
      my ( $type, $result ) = @_;
      $type eq "rows" or Future->new->fail( "Expected 'rows' result" );
      Future->new->done( $result );
   });
}

=head2 $cass->prepare( $cql ) ==> $query

Prepares a CQL query for later execution. On success, the returned Future
yields an instance of a prepared query object (see below).

=cut

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;

   my $queries = $self->{queries} ||= {};

   $self->_get_a_node->then( sub {
      shift->prepare( $cql, $self )
   })->on_done( sub {
      my ( $query ) = @_;

      # Expire old ones
      defined $queries->{$_} or delete $queries->{$_} for keys %$queries;
      weaken( $queries->{$query->id} = $query );
   });
}

=head2 $cass->execute( $id, $data, $consistency ) ==> ( $type, $result )

Executes a previously-prepared statement, given its ID and the binding data.
On success, the returned Future will yield results of the same form as the
C<query> method. C<$data> should contain a list of encoded byte-string values.

Normally this method is not directly required - instead, use the C<execute>
method on the query object itself, as this will encode the parameters
correctly.

=cut

sub execute
{
   my $self = shift;
   my ( $id, $data, $consistency ) = @_;

   $consistency //= $self->{default_consistency};
   defined $consistency or croak "'execute' needs a consistency level";

   $self->_get_a_node->then( sub {
      shift->execute( $id, $data, $consistency );
   });
}

=head1 CONVENIENT WRAPPERS

The following wrapper methods all wrap the basic C<query> operation.

=cut

=head2 $cass->schema_keyspaces ==> $result

A shortcut to a C<SELECT> query on C<system.schema_keyspaces>, which returns a
result object listing all the keyspaces.

Exact details of the returned columns will depend on the Cassandra version,
but the result should at least be keyed by the first column, called
C<keyspace_name>.

 my $keyspaces = $result->rowmap_hash( "keyspace_name" )

=cut

sub schema_keyspaces
{
   my $self = shift;

   $self->query_rows(
      "SELECT * FROM system.schema_keyspaces",
      CONSISTENCY_ONE
   );
}

=head2 $cass->schema_columnfamilies( $keyspace ) ==> $result

A shortcut to a C<SELECT> query on C<system.schema_columnfamilies>, which
returns a result object listing all the columnfamilies of the given keyspace.

Exact details of the returned columns will depend on the Cassandra version,
but the result should at least be keyed by the first column, called
C<columnfamily_name>.

 my $columnfamilies = $result->rowmap_hash( "columnfamily_name" )

=cut

sub schema_columnfamilies
{
   my $self = shift;
   my ( $keyspace ) = @_;

   $self->query_rows(
      "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = " . $self->quote( $keyspace ),
      CONSISTENCY_ONE
   );
}

=head2 $cass->schema_columns( $keyspace, $columnfamily ) ==> $result

A shortcut to a C<SELECT> query on C<system.schema_columns>, which returns a
result object listing all the columns of the given columnfamily.

Exact details of the returned columns will depend on the Cassandra version,
but the result should at least be keyed by the first column, called
C<column_name>.

 my $columns = $result->rowmap_hash( "column_name" )

=cut

sub schema_columns
{
   my $self = shift;
   my ( $keyspace, $columnfamily ) = @_;

   $self->query_rows(
      "SELECT * FROM system.schema_columns WHERE keyspace_name = " . $self->quote( $keyspace ) . " AND columnfamily_name = " . $self->quote( $columnfamily ),
      CONSISTENCY_ONE,
   );
}

sub _list_nodes
{
   my $self = shift;
   my ( $conn ) = @_;

   # The system.peers table doesn't include the node we actually connect to.
   # So we'll have to look up its own information from system.local and add
   # the socket address manually.
   Future->needs_all(
      $conn->query( "SELECT data_center, rack FROM system.local", CONSISTENCY_ONE )
         ->then( sub {
            my ( $type, $result ) = @_;
            $type eq "rows" or Future->new->fail( "Expected 'rows' result" );
            my $local = $result->row_hash( 0 );
            $local->{host} = $conn->nodeid;
            Future->new->done( $local );
         }),
      $conn->query( "SELECT peer, data_center, rack FROM system.peers", CONSISTENCY_ONE )
         ->then( sub {
            my ( $type, $result ) = @_;
            $type eq "rows" or Future->new->fail( "Expected 'rows' result" );
            my @nodes = $result->rows_hash;
            foreach my $node ( @nodes ) {
               my $addrlen = length $node->{peer};
               my $family = $addrlen ==  4 ? AF_INET :
                            $addrlen == 16 ? AF_INET6 :
                            die "Expected ADDRLEN 4 or 16";
               $node->{host} = inet_ntop( $family, delete $node->{peer} );
            }
            Future->new->done( @nodes );
         }),
   )
}

=head1 TODO

=over 8

=item *

Allow storing multiple Cassandra seed node hostnames for startup.

=item *

Allow more than one primary node - roundrobin, or other load balancing
strategies.

=item *

Allow backup nodes, for faster connection failover.

=item *

Use C<TOPOLGY_CHANGE> and C<STATUS_CHANGE> events to keep the nodelist
updated.

=item *

Node preference by C<data_center>.

=back

=cut

=head1 SPONSORS

This code was paid for by

=over 2

=item *

Perceptyx L<http://www.perceptyx.com/>

=item *

Shadowcat Systems L<http://www.shadow.cat>

=back

=head1 AUTHOR

Paul Evans <leonerd@leonerd.org.uk>

=cut

0x55AA;
