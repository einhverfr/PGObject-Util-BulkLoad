use PGObject::Util::BulkUpload;
use Test::More tests => 9;

sub normalize_whitespace {
    my $string = shift;
    $string =~ s/\s+/ /g;
    $string;
}

my $convert1 = {
   insert_cols => [qw(foo bar baz)], 
   update_cols => [qw(foo bar)],
   key_cols    => ['baz'],
   table       => 'foo',
   tempname    => 'tfoo',
   stmt        => {
           copy => 'COPY "foo"("foo", "bar", "baz") FROM STDIN WITH CSV',
           temp => 'CREATE TEMPORARY TABLE "tfoo" ( LIKE "foo" )',
         upsert => 'WITH UP (
                       UPDATE "foo" SET "foo"."foo" = "tfoo"."foo", "foo"."bar" = "tfoo"."bar"
                         FROM "foo", "tfoo"
                        WHERE "foo"."baz" = "tfoo"."baz"
                    RETURNING "baz"
                  )
                  INSERT INTO "foo" ("foo", "bar", "baz")
                  SELECT "foo", "bar", "baz" FROM "tfoo"
                  WHERE ("baz") NOT IN (SELECT ROW("baz") FROM UP)'
                  },
};

my $convert2 = {
   insert_cols => [qw(foo bar baz)],
   update_cols => [qw(foo)],
   key_cols    => [qw(bar baz)],
   table       => 'foo',
   tempname    => 'tfoo',
   stmt        => {
           copy => 'COPY "foo"("foo", "bar", "baz") FROM STDIN WITH CSV',
           temp => 'CREATE TEMPORARY TABLE "tfoo" ( LIKE "foo" )',
         upsert => 'WITH UP (
                       UPDATE "foo" SET "foo"."foo" = "tfoo"."foo"
                         FROM "foo", "tfoo"
                        WHERE "foo"."bar" = "tfoo"."bar" AND "foo"."baz" = "tfoo"."baz"
                    RETURNING "bar", "baz"
                  )
                  INSERT INTO "foo" ("foo", "bar", "baz")
                  SELECT "foo", "bar", "baz" FROM "tfoo"
                  WHERE ("bar", "baz") NOT IN (SELECT ROW("bar", "baz") FROM UP)'
                  },
};

my $convert3 = {
   insert_cols => [qw(fo"o" bar b"a"z)],
   update_cols => [qw(fo"o" bar)],
   key_cols    => [qw(b"a"z)],
   table       => 'foo',
   tempname    => 'tfoo',
   stmt        => {
           copy => 'COPY "foo"("fo""o""", "bar", "b""a""z") FROM STDIN WITH CSV',
           temp => 'CREATE TEMPORARY TABLE "tfoo" ( LIKE "foo" )',
         upsert => 'WITH UP (
                       UPDATE "foo" SET "foo"."fo""o""" = "tfoo"."fo""o""", "foo"."bar" = "tfoo"."bar"
                         FROM "foo", "tfoo"
                        WHERE "foo"."b""a""z" = "tfoo"."b""a""z"
                    RETURNING "b""a""z"
                  )
                  INSERT INTO "foo" ("fo""o""", "bar", "b""a""z")
                  SELECT "fo""o""", "bar", "b""a""z" FROM "tfoo"
                  WHERE ("b""a""z") NOT IN (SELECT ROW("b""a""z") FROM UP)'
                  },
};

for my $stype (qw(temp copy upsert)){
    my $iter = 0;
    is(normalize_whitespace(PGObject::Util::BulkUpload::statement(%$_)), 
       normalize_whitespace($_->{stmt}->{$stype}),
       "$stype for convert$_->{iter}")
        for map {
          { (%$_, type => $stype, iter => ++$iter) }
        } ($convert1, $convert2, $convert3);
}
