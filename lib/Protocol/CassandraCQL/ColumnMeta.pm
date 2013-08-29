#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::ColumnMeta;

use strict;
use warnings;

our $VERSION = '0.02';

use Protocol::CassandraCQL::Type;

=head1 NAME

C<Protocol::CassandraCQL::ColumnMeta> - stores the column metadata of a Cassandra CQL query

=head1 DESCRIPTION

Objects in this class interpret the column metadata from a message frame
containing a C<OPCODE_RESULT> response to a query giving C<RESULT_ROWS> or
C<RESULT_PREPARED>. It provides lookup of column names and type information,
and provides a convenient accessor to the encoding and decoding support
functions, allowing encoding of bytestrings from perl data when executing a
prepared statement, and decoding of bytestrings to perl data when obtaining
query results.

It is also subclassed as L<Protocol::CassandraCQL::Result>.

=cut

=head1 CONSTRUCTOR

=head2 $meta = Protocol::CassandraCQL::ColumnMeta->from_frame( $frame )

Returns a new result object initialised from the given message frame.

=cut

sub from_frame
{
   my $class = shift;
   my ( $frame ) = @_;

   my $self = bless {}, $class;

   $self->{columns} = \my @columns;

   my $flags     = $frame->unpack_int;
   my $n_columns = $frame->unpack_int;

   my $has_gts = $flags & 0x0001;
   my @gts = $has_gts ? ( $frame->unpack_string, $frame->unpack_string )
                      : ();

   foreach ( 1 .. $n_columns ) {
      my @keyspace_table = $has_gts ? @gts : ( $frame->unpack_string, $frame->unpack_string );
      my $colname        = $frame->unpack_string;
      my $type           = Protocol::CassandraCQL::Type->from_frame( $frame );

      my @col = ( @keyspace_table, $colname, undef, $type );

      push @columns, \@col;
   }

   # Now fix up the shortnames
   foreach my $idx ( 0 .. $#columns ) {
      my $c = $columns[$idx];
      my @names;

      my $name = "$c->[0].$c->[1].$c->[2]";
      push @names, $name;

      $name = "$c->[1].$c->[2]";
      push @names, $name if 1 == grep { "$_->[1].$_->[2]" eq $name } @columns;

      $name = $c->[2];
      push @names, $name if 1 == grep { $_->[2] eq $name } @columns;

      $c->[3] = $names[-1];
      $self->{name_to_col}{$_} = $idx for @names;
   }

   return $self;
}

=head1 METHODS

=cut

=head2 $n = $meta->columns

Returns the number of columns

=cut

sub columns
{
   my $self = shift;
   return scalar @{ $self->{columns} };
}

=head2 $name = $meta->column_name( $idx )

=head2 ( $keyspace, $table, $column ) = $meta->column_name( $idx )

Returns the name of the column at the given (0-based) index; either as three
separate strings, or all joined by ".".

=cut

sub column_name
{
   my $self = shift;
   my ( $idx ) = @_;

   my @n = @{ $self->{columns}[$idx] }[0..2];

   return @n if wantarray;
   return join ".", @n;
}

=head2 $name = $meta->column_shortname( $idx )

Returns the short name of the column; which will be just the column name
unless it requires the table or keyspace name as well to make it unique within
the set.

=cut

sub column_shortname
{
   my $self = shift;
   my ( $idx ) = @_;

   return $self->{columns}[$idx][3];
}

=head2 $type = $meta->column_type( $idx )

Returns the type of the column at the given index as an instance of
L<Protocol::CassandraCQL::Type>.

=cut

sub column_type
{
   my $self = shift;
   my ( $idx ) = @_;

   return $self->{columns}[$idx][4];

}

=head2 $idx = $meta->find_column( $name )

Returns the index of the given named column. The name may be given as
C<keyspace.table.column>, or C<table.column> or C<column> if they are unique
within the set. Returns C<undef> if no such column exists.

=cut

sub find_column
{
   my $self = shift;
   my ( $name ) = @_;

   return $self->{name_to_col}{$name};
}

=head2 @bytes = $meta->encode_data( @data )

Returns a list of encoded bytestrings from the given data according to the type
of each column.

=cut

sub encode_data
{
   my $self = shift;
   my @data = @_;

   return map { defined $data[$_] ? $self->column_type( $_ )->encode( $data[$_] ) : undef }
          0 .. $#data;
}

=head2 @data = $meta->decode_data( @bytes )

Returns a list of decoded data from the given encoded bytestrings according to
the type of each column.

=cut

sub decode_data
{
   my $self = shift;
   my @bytes = @_;

   return map { defined $bytes[$_] ? $self->column_type( $_ )->decode( $bytes[$_] ) : undef }
          0 .. $#bytes;
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
