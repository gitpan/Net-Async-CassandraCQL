#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::CassandraCQL::Query;

use strict;
use warnings;
use base qw( Protocol::CassandraCQL::ColumnMeta );

our $VERSION = '0.05';

use Carp;

=head1 NAME

C<Net::Async::CassandraCQL::Query> - a Cassandra CQL prepared query

=head1 DESCRIPTION

Prepared query objects are returned by the C<prepare> of
L<Net::Async::CassandraCQL> to represent a prepared query in the server. They
can be executed multiple times, if required, by passing the values of the
placeholders to the C<execute> method.

This is a subclass of L<Protocol::CassandraCQL::ColumnMeta>.

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

=head1 METHODS

=cut

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
