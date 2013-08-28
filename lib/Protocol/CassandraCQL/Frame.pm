#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::Frame;

use strict;
use warnings;

our $VERSION = '0.01';

use Encode qw( encode_utf8 decode_utf8 );

# TODO: At least the lower-level methods of this class should be rewritten in
# efficient XS code

=head1 NAME

C<Protocol::CassandraCQL::Frame> - a byte buffer storing the content of a CQL message frame

=head1 DESCRIPTION

This class provides wire-protocol encoding and decoding support for
constructing and parsing Cassandra CQL message frames. An object represents a
buffer during construction or parsing.

To construct a message frame, create a new empty object and use the C<pack_*>
methods to append data to it, before eventually obtaining the actual frame
bytes using C<bytes>. Each C<pack_*> method returns the frame object, allowing
them to be easily chained:

 my $bytes = Protocol::CassandraCQL::Frame->new
    ->pack_short( 123 )
    ->pack_int( 45678 )
    ->pack_string( "here is the data" )
    ->bytes;

To parse a message frame, create a new object from the bytes in the message,
and use the C<unpack_*> methods to consume the values from it.

 my $frame = Protocol::CassandraCQL::Frame->new( $bytes );
 my $s   = $frame->unpack_short;
 my $i   = $frame->unpack_int;
 my $str = $frame->unpack_string;

=cut

=head1 CONSTRUCTOR

=head2 $frame = Protocol::CassandraCQL::Frame->new( $bytes )

Returns a new frame buffer, optionally initialised with the given byte string.

=cut

sub new
{
   my $class = shift;
   my $bytes = $_[0] // "";
   bless \$bytes, $class;
}

=head1 METHODS

=cut

=head2 $bytes = $frame->bytes

Returns the byte string currently in the buffer.

=cut

sub bytes { ${$_[0]} }

=head2 $frame->pack_short( $v )

=head2 $v = $frame->unpack_short

Add or remove a short value.

=cut

sub pack_short { ${$_[0]} .= pack "S>", $_[1];
                 $_[0] }
sub unpack_short { unpack "S>", substr ${$_[0]}, 0, 2, "" }

=head2 $frame->pack_int( $v )

=head2 $v = $frame->unpack_int

Add or remove an int value.

=cut

sub pack_int { ${$_[0]} .= pack "l>", $_[1];
               $_[0] }
sub unpack_int { unpack "l>", substr ${$_[0]}, 0, 4, "" }

=head2 $frame->pack_string( $v )

=head2 $v = $frame->unpack_string

Add or remove a string value.

=cut

sub pack_string { my $b = encode_utf8( $_[1] );
                  $_[0]->pack_short( length $b );
                  ${$_[0]} .= $b;
                  $_[0] }
sub unpack_string { my $l = $_[0]->unpack_short;
                    decode_utf8( substr ${$_[0]}, 0, $l, "" ) }

=head2 $frame->pack_lstring( $v )

=head2 $v = $frame->unpack_lstring

Add or remove a long string value.

=cut

sub pack_lstring { my $b = encode_utf8( $_[1] );
                   $_[0]->pack_int( length $b );
                   ${$_[0]} .= $b;
                   $_[0] }
sub unpack_lstring { my $l = $_[0]->unpack_int;
                     decode_utf8( substr ${$_[0]}, 0, $l, "" ) }

=head2 $frame->pack_uuid( $v )

=head2 $frame->unpack_uuid

Add or remove a UUID as a plain 16-byte raw scalar

=cut

sub pack_uuid { ${$_[0]} .= pack "a16", $_[1];
                $_[0] }
sub unpack_uuid { substr ${$_[0]}, 0, 16, "" }

=head2 $frame->pack_string_list( $v )

=head2 $v = $frame->unpack_string_list

Add or remove a list of strings from or to an ARRAYref

=cut

sub pack_string_list { $_[0]->pack_short( scalar @{$_[1]} );
                       $_[0]->pack_string($_) for @{$_[1]};
                       $_[0] }
sub unpack_string_list { my $n = $_[0]->unpack_short;
                         [ map { $_[0]->unpack_string } 1 .. $n ] }

=head2 $frame->pack_bytes( $v )

=head2 $v = $frame->unpack_bytes

Add or remove opaque bytes or C<undef>.

=cut

sub pack_bytes { if( defined $_[1] ) { $_[0]->pack_int( length $_[1] ); ${$_[0]} .= $_[1] }
                 else                { $_[0]->pack_int( -1 ) }
                 $_[0] }
sub unpack_bytes { my $l = $_[0]->unpack_int;
                   $l > 0 ? substr ${$_[0]}, 0, $l, "" : undef }

=head2 $frame->pack_short_bytes( $v )

=head2 $v = $frame->unpack_short_bytes

Add or remove opaque short bytes.

=cut

sub pack_short_bytes { $_[0]->pack_short( length $_[1] );
                       ${$_[0]} .= $_[1];
                       $_[0] }
sub unpack_short_bytes { my $l = $_[0]->unpack_short;
                         substr ${$_[0]}, 0, $l, "" }

=head2 $frame->pack_string_map( $v )

=head2 $v = $frame->unpack_string_map

Add or remove a string map from or to a HASH of strings.

=cut

# Don't strictly need to sort the keys but it's nice for unit testing
sub pack_string_map { $_[0]->pack_short( scalar keys %{$_[1]} );
                      $_[0]->pack_string( $_ ), $_[0]->pack_string( $_[1]->{$_} ) for sort keys %{$_[1]};
                      $_[0] }
sub unpack_string_map { my $n = $_[0]->unpack_short;
                        +{ map { $_[0]->unpack_string => $_[0]->unpack_string } 1 .. $n } }

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
