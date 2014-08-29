package PGObject::Util::BulkUpload;

use 5.006;
use strict;
use warnings FATAL => 'all';

use Memoize;
use Text::CSV;

=head1 NAME

PGObject::Util::BulkUpload - Bulk Upload records into PostgreSQL

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

To insert all rows into a table using COPY:

  PGObject::Util::BulkUpload->copy(
      {table => 'mytable', insert_cols => ['col1', 'col2'], dbh => $dbh}, 
      @objects
  );

To copy to a temp table and then upsert:

  PGObject::Util::BulkUpload->upsert(
      {table       => 'mytable', 
       insert_cols => ['col1', 'col2'], 
       update_cols => ['col1'],
       key_cols    => ['col2'],
       dbh         => $dbh}, 
      @objects
  );

Or if you prefer to run the statements yourself:

  PGObject::Util::BulkUpload->statement(
     table => 'mytable', type  => 'temp', tempname => 'foo_123'
  );
  PGObject::Util::BulkUpload->statement(
     table => 'mytable', type  => 'copy', insert_cols => ['col1', 'col2']
  );
  PGObject::Util::BulkUpload->statement(
      type        => 'upsert',
      tempname    => 'foo_123',
      table       => 'mytable',
      insert_cols => ['col1', 'col2'],
      update_cols => ['col1'],
      key_cols    => ['col2']
  );

If you are running repetitive calls, you may be able to trade time for memory 
using Memoize by unning the following:

  PGObject::Util::BulkUpload->memoize_statements;

To unmemoize:

  PGObject::Util::BulkUpload->unmemoize;

To flush cache

  PGObject::Util::BulkUpload->flush_memoization;

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 memoize_statements

This function exists to memoize statement calls, i.e. generate the exact same 
statements on the same argument calls.  This isn't too likely to be useful in
most cases but it may be if you have repeated bulk loader calls in a persistent
script (for example real-time importing of csv data from a frequent source).

=cut

sub memoize_statements {
    memoize 'statement';
}

=head2 unmemoize 

Unmemoizes the statement calls.

=cut

sub unmemoize {
    unmemoize 'statement';
}

=head2 flush_memoization

Flushes the cache for statement memoization.  Does *not* flush the cache for
escaping memoization since that is a bigger win and a pure function accepting
simple strings.

=cut

sub flush_memoization {
    Memoization::flush_cache('statement');
}
 
=head2 statement

=cut

sub _sanitize_ident {
    my($string) = @_;
    $string =~ s/"/""/g;
    qq("$string");
}
memoize '_sanitize_ident';

sub _statement_temp {
    my ($args) = @_;

    "CREATE TEMPORARY TABLE " . _sanitize_ident($args->{tempname}) .
    "( LIKE " . _sanitize_ident($args->{table}) . ")";
}

sub _statement_copy {
    my ($args) = @_;
    croak 'No insert cols' unless $args->{insert_cols};

    "COPY " . _sanitize_ident($args->{table}) . "(" .
      join(', ', map { _sanitize_ident($_) } @{$args->{insert_cols}}) . ') ' .
      "FROM STDIN WITH CSV";
}

sub _statement_upsert {
    my ($args) = @_;
    for (qw(insert_cols update_cols key_cols table tempname)){
       croak "Missing argument $_" unless $args->{$_};
    }
    my $table = _sanitize_ident($arg->{table});
    my $temp = _sanitize_ident($arg->{tempname})

    "WITH up (
     UPDATE $table
        SET " . join(",
            ", map {"$table." . _sanitize_ident($_) . ' = ' .
                    "$temp." _sanitize_ident($_)} @{$args->{update_cols}}) . "
       FROM $table, $temp
      WHERE " . join("
            AND ", map {"$table." . _sanitize_ident($_) . ' = ' .
                    "$temp." _sanitize_ident($_)} @{$args->{key_cols}}) . "
 RETURNING " . join(", ", map {sanitize_ident($_)} @{$args->{key_cols}} ."
)
    INSERT INTO $table (" . join(", ", 
                            map {sanitize_ident($_)} @{$args->{insert_cols}}) . "
    SELECT " . join(", " map {sanitize_ident($_)} @{$args->{insert_cols}}) . "
      FROM $temp 
     WHERE (". join(", ", map {sanitize_ident($_)} @{$args->{key_cols}} .") 
           not in(select row(".join(", ", map {sanitize_ident($_)} @{$args->{key_cols}} .") FROM up)";

}

sub statement {
    my %args = @_;
    $args = shift if $args eq __PACKAGE__;
    croak "Missing argument 'type'" unless $args->{type};
    &{"_statement_$args->{type}"}($args);
}

=head2 upsert

Creates a temporary table named "pg_object.bulkload" and copies the data there


=cut

sub _build_args {
    my ($init_args, $obj);
    my @arglist = qw(table insert_cols update_cols key_cols);
    return { 
       map { for my $val (try { $obj->$_ }, $arglist->{$_}){
                         $_ => $val if defined $val;
                      }
       } @arglist 
    }
}

sub upsert {
    my ($args) = shift;
    $args = shift if $args eq __PACKAGE__;
    try {
       $args->can('foo');
       unshift $args; # args is an object
    };
    my $args = _build_args($args, $_[0]);
    my $dbh = $args->{dbh};

    # pg_temp is the schema of temporary tables.  If someone wants to create
    # a permanent table there, they are inviting disaster.  At any rate this is
    # safe but a plain drop without schema qualification risks losing user data.

    $dbh->do("DROP TABLE IF EXISTS pg_temp.pgobject_bulkloader");
    $dbh->do(statement( %$args, (type => 'temp', 
                              tempname => 'pgobject_bulkloader')
    ));
    copy({(%$args, (table => 'pgobject_bulkloader'))}, @_);
    $dbh->do(statement( %$args, (type => 'upsert', 
                              tempname => 'pgobject_bulkloader')));
    $dbh->do("DROP TABLE pg_temp.pgobject_bulkloader");
}

=head2 copy

=cut

sub _to_csv {
    my ($args) = shift;

    my $csv = Text::CSV->new();
    join("\n", map {
       my $obj = $_;
       $csv->combine(map { $obj->{$_} } @{$args->{cols}});
       $csv->string();
    } @_);
}

sub copy {
    my ($args) = shift;
    $args = shift if $args eq __PACKAGE__;
    try {
       $args->can('foo');
       unshift $args; # args is an object
    };
    my $args = _build_args($args, $_[0]);
    my $dbh = $args->{dbh};
    $dbh->do(statement(%$args));
    $dbh->put_copydata(_to_csv({cols => $args->{insert_cols}, @_));
    $dbh->put_copyend();
}

=head1 AUTHOR

Chris Travers, C<< <chris.travers at gmail.com> >>

=head1 CO-MAINTAINERS

=over

=item Binary.com C<< <perl@binary.com> >>

=back

=head1 BUGS

Please report any bugs or feature requests to C<bug-pgobject-util-bulkupload at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=PGObject-Util-BulkUpload>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc PGObject::Util::BulkUpload


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=PGObject-Util-BulkUpload>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/PGObject-Util-BulkUpload>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/PGObject-Util-BulkUpload>

=item * Search CPAN

L<http://search.cpan.org/dist/PGObject-Util-BulkUpload/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2014 Chris Travers.

This program is distributed under the (Revised) BSD License:
L<http://www.opensource.org/licenses/BSD-3-Clause>

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of Chris Travers's Organization
nor the names of its contributors may be used to endorse or promote
products derived from this software without specific prior written
permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of PGObject::Util::BulkUpload
