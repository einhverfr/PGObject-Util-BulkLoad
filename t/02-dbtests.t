use Test::More;
use DBI;

plan skip_all => 'Not set up for db tests' unless $ENV{DB_TESTING};
plan tests => 8;

# SETUP
my $dbh1 = DBI->connect('dbi:Pg:dbname=postgres', 'postgres');
$dbh1->do('CREATE DATABASE pgobject_test_db') if $dbh1;

my $dbh = DBI->connect('dbi:Pg:dbname=pgobject_test_db', 'postgres');

$dbh->do('CREATE TYPE typetest.footype AS (
    foo int,
    bar text,
    baz bigint
)');

