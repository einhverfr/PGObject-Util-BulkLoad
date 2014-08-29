use 5.006;
use strict;
use warnings FATAL => 'all';
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'PGObject::Util::BulkUpload',
    AUTHOR           => q{Chris Travers <chris.travers@gmail.com>},
    VERSION_FROM     => 'lib/PGObject/Util/BulkUpload.pm',
    ABSTRACT_FROM    => 'lib/PGObject/Util/BulkUpload.pm',
    LICENSE          => 'BSD',
    PL_FILES         => {},
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        #'ABC'              => 1.6,
        #'Foo::Bar::Module' => 5.0401,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'PGObject-Util-BulkUpload-*' },
);
