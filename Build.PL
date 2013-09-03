use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
   module_name => 'Net::Async::CassandraCQL',
   requires => {
      'Future' => '0.13',
      'IO::Async::Loop' => '0.54', # ->connect returns Future
      'IO::Async::Protocol::Stream' => 0,
      'Protocol::CassandraCQL' => '0.05', # ->unpack_inet
   },
   build_requires => {
      'IO::Async::Test' => 0,
      'Test::HexString' => 0,
      'Test::Fatal' => 0,
      'Test::More' => '0.88', # done_testing
   },
   auto_configure_requires => 0, # Don't add M::B to configure_requires
   license => 'perl',
   create_makefile_pl => 'small', # since we do active testing here
   create_license => 1,
   create_readme  => 1,
);

$build->create_build_script;
