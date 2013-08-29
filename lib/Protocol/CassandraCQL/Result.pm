#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::Result;

use strict;
use warnings;
use base qw( Protocol::CassandraCQL::ColumnMeta );

our $VERSION = '0.02';

use Protocol::CassandraCQL qw( :types );

=head1 NAME

C<Protocol::CassandraCQL::Result> - stores the result of a Cassandra CQL query

=head1 DESCRIPTION

Objects in this class store the result of a direct query or executed prepared
statement, as returned by an C<OPCODE_RESULT> giving C<RESULT_ROWS>. It allows
convenient access to the decoded row data.

As a subclass of L<Protocol::CassandraCQL::ColumnMeta> it also provides
information about column metadata, such as column names and types.

=cut

=head1 CONSTRUCTOR

=head2 $result = Protocol::CassandraCQL::Result->from_frame( $frame )

Returns a new result object initialised from the given C<OPCODE_RESULT> /
C<RESULT_ROWS> message frame.

=cut

sub from_frame
{
   my $class = shift;
   my ( $frame ) = @_;
   my $self = $class->SUPER::from_frame( $frame );

   my $n_rows = $frame->unpack_int;
   my $n_columns = scalar @{$self->{columns}};

   $self->{rows} = [];
   foreach ( 1 .. $n_rows ) {
      push @{$self->{rows}}, [ map { $frame->unpack_bytes } 1 .. $n_columns ];
   }

   return $self;
}

=head2 $n = $result->rows

Returns the number of rows

=cut

sub rows
{
   my $self = shift;
   return scalar @{ $self->{rows} };
}

=head2 @columns = $result->rowbytes( $idx )

Returns a list of the raw bytestrings containing the row's data

=cut

sub rowbytes
{
   my $self = shift;
   my ( $idx ) = @_;

   return @{ $self->{rows}[$idx] };
}

=head2 $data = $result->row_array( $idx )

Returns the row's data decoded, as an ARRAY reference

=cut

sub row_array
{
   my $self = shift;
   my ( $idx ) = @_;

   return [ $self->decode_data( $self->rowbytes( $idx ) ) ];
}

=head2 $data = $result->row_hash( $idx )

Returns the row's data decoded, as a HASH reference mapping column short names
to values.

=cut

sub row_hash
{
   my $self = shift;
   my ( $idx ) = @_;

   my @data = $self->decode_data( $self->rowbytes( $idx ) );
   return { map { $self->column_shortname( $_ ) => $data[$_] } 0 .. $#data };
}

=head2 @data = $result->rows_array

Returns a list of all the rows' data decoded as ARRAY references.

=cut

sub rows_array
{
   my $self = shift;
   return map { $self->row_array( $_ ) } 0 .. $self->rows-1;
}

=head2 @data = $result->rows_hash

Returns a list of all the rows' data decoded as HASH references.

=cut

sub rows_hash
{
   my $self = shift;
   return map { $self->row_hash( $_ ) } 0 .. $self->rows-1;
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
