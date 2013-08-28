#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL;

use strict;
use warnings;

our $VERSION = '0.01';

use Exporter 'import';
our @EXPORT_OK = qw();

use Encode ();
use Scalar::Util qw( blessed );

=head1 NAME

C<Protocol::CassandraCQL> - wire protocol support functions for Cassandra CQLv3

=head1 DESCRIPTION

This module provides the basic constants and other support functions required
to communicate with a Cassandra database using C<CQLv3>. It is not in itself a
CQL client; it simply provides the necessary support functions to allow one to
be written.

For a complete client, see instead L<Net::Async::CassandraCQL>.

=cut

=head1 CONSTANTS

The following families of constants are defined, along with export tags:

=head2 OPCODE_* (:opcodes)

Opcodes used in message frames.

=head2 RESULT_* (:results)

Result codes used in C<OPCODE_RESULT> frames.

=head2 TYPE_* (:types)

Type codes used in C<TYPE_ROWS> and C<TYPE_PREPARED> column metadata.

=head2 CONSISTENCY_* (:consistencies)

Consistency levels used in C<OPCODE_QUERY> and C<OPCODE_EXECUTE> frames.

=cut

# See also
#   https://github.com/apache/cassandra/blob/cassandra-1.2/doc/native_protocol.spec

my %CONSTANTS = (
   OPCODE_ERROR        => 0x00,
   OPCODE_STARTUP      => 0x01,
   OPCODE_READY        => 0x02,
   OPCODE_AUTHENTICATE => 0x03,
   OPCODE_CREDENTIALS  => 0x04,
   OPCODE_OPTIONS      => 0x05,
   OPCODE_SUPPORTED    => 0x06,
   OPCODE_QUERY        => 0x07,
   OPCODE_RESULT       => 0x08,
   OPCODE_PREPARE      => 0x09,
   OPCODE_EXECUTE      => 0x0A,
   OPCODE_REGISTER     => 0x0B,
   OPCODE_EVENT        => 0x0C,

   RESULT_VOID          => 0x0001,
   RESULT_ROWS          => 0x0002,
   RESULT_SET_KEYSPACE  => 0x0003,
   RESULT_PREPARED      => 0x0004,
   RESULT_SCHEMA_CHANGE => 0x0005,

   TYPE_CUSTOM    => 0x0000,
   TYPE_ASCII     => 0x0001,
   TYPE_BIGINT    => 0x0002,
   TYPE_BLOB      => 0x0003,
   TYPE_BOOLEAN   => 0x0004,
   TYPE_COUNTER   => 0x0005,
   TYPE_DECIMAL   => 0x0006,
   TYPE_DOUBLE    => 0x0007,
   TYPE_FLOAT     => 0x0008,
   TYPE_INT       => 0x0009,
   TYPE_TEXT      => 0x000A,
   TYPE_TIMESTAMP => 0x000B,
   TYPE_UUID      => 0x000C,
   TYPE_VARCHAR   => 0x000D,
   TYPE_VARINT    => 0x000E,
   TYPE_TIMEUUID  => 0x000F,
   TYPE_INET      => 0x0010,
   TYPE_LIST      => 0x0020,
   TYPE_MAP       => 0x0021,
   TYPE_SET       => 0x0022,

   CONSISTENCY_ANY          => 0x0000,
   CONSISTENCY_ONE          => 0x0001,
   CONSISTENCY_TWO          => 0x0002,
   CONSISTENCY_THREE        => 0x0003,
   CONSISTENCY_QUORUM       => 0x0004,
   CONSISTENCY_ALL          => 0x0005,
   CONSISTENCY_LOCAL_QUORUM => 0x0006,
   CONSISTENCY_EACH_QUORUM  => 0x0007,
);

require constant;
constant->import( $_, $CONSTANTS{$_} ) for keys %CONSTANTS;
push @EXPORT_OK, keys %CONSTANTS;

our %EXPORT_TAGS = (
   'opcodes'       => [ grep { m/^OPCODE_/      } keys %CONSTANTS ],
   'results'       => [ grep { m/^RESULT_/      } keys %CONSTANTS ],
   'types'         => [ grep { m/^TYPE_/        } keys %CONSTANTS ],
   'consistencies' => [ grep { m/^CONSISTENCY_/ } keys %CONSTANTS ],
);

=head1 FUNCTIONS

=cut

=head2 $name = typename( $type )

Returns the name of the given C<TYPE_*> value, without the initial C<TYPE_>
prefix.

=cut

my %typevals = map { substr($_, 5) => __PACKAGE__->$_ } grep { m/^TYPE_/ } keys %CONSTANTS;
my %typenames = reverse %typevals;

sub typename
{
   my ( $type ) = @_;
   return $typenames{$type};
}

=head2 $b = encode( $type, $v )

=head2 $v = decode( $type, $b )

Encode or decode a bytestring for a CQL value of the given type.

=cut

# Method dispatch is kinda slow but easy to maintain
# TODO: find something faster

sub encode
{
   my ( $type, $v ) = @_;

   return undef if !defined $v;

   if( my $code = __PACKAGE__->can( "encode_$type" ) ) {
      return $code->( $v );
   }
   else {
      warn "Not sure how to encode $type";
      return $v;
   }
}

sub decode
{
   my ( $type, $b ) = @_;

   return undef if !defined $b;

   if( my $code = __PACKAGE__->can( "decode_$type" ) ) {
      return $code->( $b );
   }
   else {
      warn "Not sure how to decode $type";
      # Fallback to a text-safe hexbytes representation
      return unpack "H*", $b;
   }
}

# Now the codecs

# ASCII-only bytes
sub encode_ASCII { $_[0] =~ m/^[\x00-\x7f]*$/ or die "Non-ASCII"; $_[0] }
sub decode_ASCII { $_[0] }

# 64-bit integer
sub encode_BIGINT { pack   "q>", $_[0] }
sub decode_BIGINT { unpack "q>", $_[0] }

# blob
sub encode_BLOB { $_[0] }
sub decode_BLOB { $_[0] }

# true/false byte
sub encode_BOOLEAN { pack   "C", !!$_[0] }
sub decode_BOOLEAN { !!unpack "C", $_[0] }

# counter is a 64-bit integer
*encode_COUNTER = \&encode_BIGINT;
*decode_COUNTER = \&decode_BIGINT;

# Not clearly docmuented, but this appears to be an INT decimal shift followed
# by a VARINT
sub encode_DECIMAL {
   require Math::BigFloat;
   my $shift = $_[0] =~ m/\.(\d*)$/ ? length $1 : 0;
   my $n = blessed $_[0] ? $_[0] : Math::BigFloat->new( $_[0] );
   return pack( "L>", $shift ) . encode_VARINT( $n->blsft($shift, 10) );
}

sub decode_DECIMAL {
   require Math::BigFloat;
   my $shift = unpack "L>", $_[0];
   my $n = decode_VARINT( substr $_[0], 4 );
   return scalar Math::BigFloat->new($n)->brsft($shift, 10);
}

# IEEE double
sub encode_DOUBLE { pack   "d>", $_[0] }
sub decode_DOUBLE { unpack "d>", $_[0] }

# IEEE single
sub encode_FLOAT { pack   "f>", $_[0] }
sub decode_FLOAT { unpack "f>", $_[0] }

# 32-bit integer
sub encode_INT { pack   "l>", $_[0] }
sub decode_INT { unpack "l>", $_[0] }

# 'text' seems to come back as 'varchar' but we'll leave them both aliased
*encode_VARCHAR = *encode_TEXT = \&Encode::encode_utf8;
*decode_VARCHAR = *decode_TEXT = \&Encode::decode_utf8;

# miliseconds since UNIX epoch as 64bit uint
sub encode_TIMESTAMP {  pack   "Q>", ($_[0] * 1000) }
sub decode_TIMESTAMP { (unpack "Q>", $_[0]) / 1000  }

# TODO: UUID

# Arbitrary-precision 2s-complement signed integer
# Math::BigInt doesn't handle signed, but we can mangle it
sub encode_VARINT {
   require Math::BigInt;
   my $n = blessed $_[0] ? $_[0] : Math::BigInt->new($_[0]); # upgrade to a BigInt

   my $bytes;
   if( $n < 0 ) {
      my $hex = substr +(-$n-1)->as_hex, 2;
      $hex = "0$hex" if length($hex) % 2;
      $bytes = ~(pack "H*", $hex);
      # Sign-extend if required to avoid appearing positive
      $bytes = "\xff$bytes" if unpack( "C", $bytes ) < 0x80;
   }
   else {
      my $hex = substr $n->as_hex, 2; # trim 0x
      $hex = "0$hex" if length($hex) % 2;
      $bytes = pack "H*", $hex;
      # Zero-extend if required to avoid appearing negative
      $bytes = "\0$bytes" if unpack( "C", $bytes ) >= 0x80;
   }
   $bytes;
}

sub decode_VARINT {
   require Math::BigInt;

   if( unpack( "C", $_[0] ) >= 0x80 ) {
      return -Math::BigInt->from_hex( unpack "H*", ~$_[0] ) - 1;
   }
   else {
      return Math::BigInt->from_hex( unpack "H*", $_[0] );
   }
}

# TODO: INET

=head1 TODO

=over 8

=item *

Reimplement L<Protocol::CassandraCQL::Frame> in XS code for better
performance.

=item *

Use something faster than dynamic method dispatch for C<encode()> and
C<decode()>.

=item *

Codecs for TYPE_UUID, TYPE_INET

=item *

More robust testing of data types, arguments to methods, etc...

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
