#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::CassandraCQL;

use strict;
use warnings;
use 5.010;

our $VERSION = '0.02';

use base qw( IO::Async::Protocol::Stream );

use Carp;

use Protocol::CassandraCQL qw( :opcodes :results );
use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::ColumnMeta;
use Protocol::CassandraCQL::Result;

use constant DEFAULT_CQL_PORT => 9042;

=head1 NAME

C<Net::Async::CassandraCQL> - use Cassandra databases with L<IO::Async> using CQL

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::CassandraCQL;
 use Protocol::CassandraCQL qw( CONSISTENCY_QUORUM );

 my $loop = IO::Async::Loop->new;

 my $cass = Net::Async::CassandraCQL->new(
    host => "localhost",
 );
 $loop->add( $cass );


 $cass->connect->then( sub {
    $cass->query( "USE my-keyspace;" );
 })->get;


 my @f;
 foreach my $number ( 1 .. 100 ) {
    push @f, $cass->query( "INSERT INTO numbers (v) VALUES $number;",
       CONSISTENCY_QUORUM );
 }
 Future->needs_all( @f )->get;


 my $get_stmt = $cass->prepare( "SELECT v FROM numbers;" )->get;

 my ( undef, $result ) = $get_stmt->execute( [], CONSISTENCY_QUORUM )->get;

 foreach my $row ( $result->rows_hash) {
    say "We have a number " . $row->{v};
 }

=head1 DESCRIPTION

This module allows use of the C<CQLv3> interface of a Cassandra database. It
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

=back

=cut

sub _init
{
   my $self = shift;
   $self->SUPER::_init( @_ );

   $self->{streams} = []; # map [1 .. 127] to Future
   $self->{pending} = []; # queue of [$opcode, $frame, $f]
}

sub configure
{
   my $self = shift;
   my %params = @_;

   foreach (qw( host service )) {
      $self->{$_} = delete $params{$_} if exists $params{$_};
   }

   $self->SUPER::configure( %params );
}

=head1 METHODS

=cut

# function
sub _decode_result
{
   my ( $response ) = @_;

   my $result = $response->unpack_int;

   if( $result == RESULT_VOID ) {
      return Future->new->done();
   }
   elsif( $result == RESULT_ROWS ) {
      return Future->new->done( rows => Protocol::CassandraCQL::Result->from_frame( $response ) );
   }
   elsif( $result == RESULT_SET_KEYSPACE ) {
      return Future->new->done( keyspace => $response->unpack_string );
   }
   elsif( $result == RESULT_SCHEMA_CHANGE ) {
      return Future->new->done( schema_change => [ map { $response->unpack_string } 1 .. 3 ] );
   }
   else {
      return Future->new->done( "??" => $response->bytes );
   }
}

=head2 $f = $cass->connect( %args )

Connects to the Cassandra node an send the C<OPCODE_STARTUP> message. The
returned Future will yield nothing on success.

Takes the following named arguments:

=over 8

=item host => STRING

=item service => STRING

Optional. Overrides the configured values.

=back

A host name is required, either as a named argument or as a configured value
on the object. If the service name is missing, the default CQL port will be
used instead.

=cut

sub connect
{
   my $self = shift;
   my %args = @_;

   $args{host}    //= $self->{host}    or croak "Require 'host'";
   $args{service} //= $self->{service} // DEFAULT_CQL_PORT;

   return ( $self->{connect_f} ||=
      $self->SUPER::connect( %args )->on_fail( sub { undef $self->{connect_f} } ) )
      ->and_then( sub {
         $self->startup
      });
}

sub on_read
{
   my $self = shift;
   my ( $buffref, $eof ) = @_;

   return 0 unless length $$buffref >= 8;

   my $bodylen = unpack( "x4 N", $$buffref );
   return 0 unless length $$buffref >= 8 + $bodylen;

   my ( $version, $flags, $streamid, $opcode ) = unpack( "C C C C x4", substr $$buffref, 0, 8, "" );
   my $body = substr $$buffref, 0, $bodylen, "";

   # v1 response
   $version == 0x81 or
      $self->fail_all_and_close( sprintf "Unexpected message version %#02x\n", $version ), return;

   my $frame = Protocol::CassandraCQL::Frame->new( $body );

   # TODO: flags
   if( my $f = $self->{streams}[$streamid] ) {
      if( $opcode == OPCODE_ERROR ) {
         my $err     = $frame->unpack_int;
         my $message = $frame->unpack_string;
         $f->fail( "OPCODE_ERROR: $message\n", $err, $frame );
      }
      else {
         $f->done( $opcode, $frame );
      }

      undef $self->{streams}[$streamid];

      if( my $next = shift @{ $self->{pending} } ) {
         my ( $opcode, $frame, $f ) = @$next;
         $self->_send( $opcode, $streamid, $frame, $f );
      }
   }
   else {
      print STDERR "Received a message opcode=$opcode for unknown stream $streamid\n";
   }

   return 1;
}

sub on_closed
{
   my $self = shift;
   $self->fail_all( "Connection closed" );
}

sub fail_all
{
   my $self = shift;
   my ( $failure ) = @_;

   foreach ( @{ $self->{streams} } ) {
      $_->fail( $failure ) if $_;
   }
   @{ $self->{streams} } = ();

   foreach ( @{ $self->{pending} } ) {
      $_->[2]->fail( $failure );
   }
   @{ $self->{pending} } = ();
}

sub fail_all_and_close
{
   my $self = shift;
   my ( $failure ) = @_;

   $self->fail_all( $failure );

   $self->close;

   return Future->new->fail( $failure );
}

=head2 $f = $cass->send_message( $opcode, $frame )

Sends a message with the given opcode and L<Protocol::CassandraCQL::Frame> for
the message body. The returned Future will yield the response opcode and
frame.

  ( $reply_opcode, $reply_frame ) = $f->get

This is a low-level method; applications should instead use one of the wrapper
methods below.

=cut

sub send_message
{
   my $self = shift;
   my ( $opcode, $frame ) = @_;

   my $f = $self->loop->new_future;

   my $streams = $self->{streams} ||= [];
   my $id;
   foreach ( 1 .. $#$streams ) {
      $id = $_ and last if !defined $streams->[$_];
   }

   if( !defined $id ) {
      if( $#$streams == 127 ) {
         push @{ $self->{pending} }, [ $opcode, $frame, $f ];
         return $f;
      }
      $id = @$streams;
      $id = 1 if !$id; # can't use 0
   }

   $self->_send( $opcode, $id, $frame, $f );

   return $f;
}

sub _send
{
   my $self = shift;
   my ( $opcode, $id, $frame, $f ) = @_;

   my $version = 0x01;
   my $flags   = 0;
   my $body    = $frame->bytes;
   $self->write( pack "C C C C N a*", $version, $flags, $id, $opcode, length $body, $body );

   $self->{streams}[$id] = $f;
}

=head2 $f = $cass->startup

Sends the initial connection setup message. On success, the returned Future
yields nothing.

Normally this is not required as the C<connect> method performs it implicitly.

=cut

sub startup
{
   my $self = shift;

   $self->send_message( OPCODE_STARTUP,
      Protocol::CassandraCQL::Frame->new->pack_string_map( {
            CQL_VERSION => "3.0.0",
      } )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_READY or return $self->fail_all_and_close( "Expected OPCODE_READY" );

      return Future->new->done;
   });
}

=head2 $f = $cass->options

Requests the list of supported options from the server node. On success, the
returned Future yields a HASH reference mapping option names to ARRAY
references containing valid values.

=cut

sub options
{
   my $self = shift;

   $self->send_message( OPCODE_OPTIONS,
      Protocol::CassandraCQL::Frame->new
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_SUPPORTED or return Future->new->fail( "Expected OPCODE_SUPPORTED" );

      my %opts;
      # $response contains a multimap; short * { string, string list }
      foreach ( 1 .. $response->unpack_short ) {
         my $name = $response->unpack_string;
         $opts{$name} = $response->unpack_string_list;
      }
      return Future->new->done( \%opts );
   });
}

=head2 $f = $cass->query( $cql, $consistency )

Performs a CQL query. On success, the values returned from the Future will
depend on the type of query.

 ( $type, $result ) = $f->get

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

   $self->send_message( OPCODE_QUERY,
      Protocol::CassandraCQL::Frame->new->pack_lstring( $cql )
                                        ->pack_short( $consistency )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or return Future->new->fail( "Expected OPCODE_RESULT" );
      return _decode_result( $response );
   });
}

=head2 $f = $cass->prepare( $cql )

Prepares a CQL query for later execution. On success, the returned Future
yields an instance of a prepared query object (see below).

 ( $query ) = $f->get

=cut

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;

   $self->send_message( OPCODE_PREPARE,
      Protocol::CassandraCQL::Frame->new->pack_lstring( $cql )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or return Future->new->fail( "Expected OPCODE_RESULT" );

      $response->unpack_int == RESULT_PREPARED or return Future->new->fail( "Expected RESULT_PREPARED" );

      return Future->new->done( Net::Async::CassandraCQL::Query->from_frame( $self, $response ) );
   });
}

=head2 $f = $cass->execute( $id, $data, $consistency )

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

   my $frame = Protocol::CassandraCQL::Frame->new
      ->pack_short_bytes( $id )
      ->pack_short( scalar @$data );
   $frame->pack_bytes( $_ ) for @$data;
   $frame->pack_short( $consistency );

   $self->send_message( OPCODE_EXECUTE, $frame )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or return Future->new->fail( "Expected OPCODE_RESULT" );
      return _decode_result( $response );
   });
}

package # hide from CPAN
   Net::Async::CassandraCQL::Query;
use base qw( Protocol::CassandraCQL::ColumnMeta );
use Carp;

=head1 PREPARED QUERIES

Prepared query objects are returned by C<prepare>, and have the following
methods, in addition to those of its parent class,
L<Protocol::CassandraCQL::ColumnMeta>.

=cut

sub from_frame
{
   my $class = shift;
   my ( $cassandra, $response ) = @_;

   my $id = $response->unpack_short_bytes;

   my $self = $class->SUPER::from_frame( $response );

   $self->{cassandra} = $cassandra;
   $self->{id} = $id;

   return $self;
}

=head2 $id = $query->id

Returns the query ID.

=cut

sub id
{
   my $self = shift;
   return $self->{id};
}

=head2 $f = $query->execute( $data, $consistency )

Executes the query on the Cassandra connection object that created it,
returning a future yielding the result the same way as the C<query> or
C<execute> methods.

The contents of the C<$data> reference will be encoded according to the types
given in the underlying column metadata. C<$data> may be given as a positional
ARRAY reference, or a named HASH reference where the keys give column names.

=cut

sub execute
{
   my $self = shift;
   my ( $data, $consistency ) = @_;

   my @data;
   if( ref $data eq "ARRAY" ) {
      @data = @$data;
   }
   elsif( ref $data eq "HASH" ) {
      @data = ( undef ) x $self->columns;
      foreach my $name ( keys %$data ) {
         my $idx = $self->find_column( $name );
         defined $idx or croak "Unknown bind column name '$name'";
         defined $data[$idx] and croak "Cannot bind column ".$self->column_name($idx)." twice";
         $data[$idx] = $data->{$name};
      }
   }

   my @bytes = $self->encode_data( @data );

   return $self->{cassandra}->execute( $self->id, \@bytes, $consistency );
}

=head1 TODO

=over 8

=item *

Handle OPCODE_AUTHENTICATE and OPCODE_REGISTER

=item *

Support frame compression

=item *

Move L<Protocol::CassandraCQL> to its own distribution

=item *

Allow storing multiple Cassandra node hostnames and perform some kind of
balancing or failover of connections.

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
