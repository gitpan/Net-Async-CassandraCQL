use strict;
use warnings;

use Module::Build;

# If you don't support 64-bit pack formats you will have a bad time and will
# not talk to Cassandra today
eval { pack( "q>", 1 ) eq "\0\0\0\0\0\0\0\1" } or
   die "OS unsupported - no 64bit integers";

my $build = Module::Build->new(
   module_name => 'Net::Async::CassandraCQL',
   requires => {
      'Encode' => 0,
      'Math::BigInt' => 0,
      'IO::Async::Protocol::Stream' => 0,
   },
   build_requires => {
      'IO::Async::Test' => 0,
      'IO::Async::Loop' => 0,
      'Test::HexString' => 0,
      'Test::More' => '0.88', # done_testing
   },
   auto_configure_requires => 0, # Don't add M::B to configure_requires
   license => 'perl',
   create_makefile_pl => 'traditional',
   create_license => 1,
   create_readme  => 1,
);

$build->create_build_script;
