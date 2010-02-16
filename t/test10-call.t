
use strict;
use warnings;
use Cwd qw(realpath);
use Test::More;
use DBI;

# test we can connect to a postgres on localhost
# test we can use the plperlu language
# if so, create and execute the test SP

(my $sqlfile = __FILE__) =~ s/\.t/.sql/;
(my $dir = realpath($sqlfile)) =~ s:/t/[^/]+$::;

# create test db
my $dbname = "plperl_call_test".$$;
system("createdb --echo $dbname");

my $dbh = DBI->connect("dbi:Pg:dbname=$dbname")
	or plan skip_all => "Can't connect to local database: $DBI::errstr";

# set @INC - also checks plperlu works
$dbh->do(qq{
	DO '
		warn "Using directory $dir\n";
		# for normal testing via dzil test
		require blib;
		require  lib;
		eval { blib->import("$dir/blib") }
			or eval {  lib->import("$dir/lib") };
	' language plperlu;
})
	or plan skip_all => "Need to use plperlu for test";

plan tests => 1;

my $func = 'call_test10';
$dbh->do("drop function if exists $func()");

my $sql = do { # slurp!
	open my $fh, $sqlfile or die "Can't open $sqlfile: $!";
	local $/;
	<$fh>
};
$dbh->do($sql);

my ($status) = $dbh->selectrow_array("select * from $func()");
is $status, 'PASS';

END {
	$dbh->disconnect if $dbh;
	system("dropdb --echo $dbname");
}
