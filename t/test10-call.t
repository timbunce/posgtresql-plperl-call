
# test we can connect to a postgres on localhost
# test we can use the plperlu language
# if so, create and execute the test SP

(my $sql = __FILE__) =~ s/\.t/.sql/;

my $dbname = "plperl_call_test42";

system("createdb $dbname");
END { system("dropdb $dbname"); }

system("psql $dbname < $sql") == 0 or do {
	require Test::More;
	Test::More::plan(skip_all => "Error loading $sql to create test function");
};

system("echo 'select * from call_test10();' | psql $dbname");
