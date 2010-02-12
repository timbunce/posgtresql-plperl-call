create or replace function test_call() returns text language plperlu as $func$

use lib "/Users/timbo/pg/PostgreSQL-PLPerl-Call/lib";
use PostgreSQL::PLPerl::Call;

use Test::More 'no_plan';

my $row;
my @ary;

# ====== single-value single-row function ======

# --- no arguments
like call('pi()'), qr/^3.14159/;

# bad calls
eval { call('pi()', 42) };
like $@, qr/expected 0 argument/;
eval { call('pi', 42) };
like $@, qr/Can't parse 'pi'/; # error from call() itself

# --- one argument, simple types
is call('abs(int)', -42), 42;
is call('abs(float)', -42.5), '42.5';
is call('bit_length(text)', 'jose'), 32;

# --- one argument, multi-word types
is call('abs(double precision)', -42.5), '42.5';
is call('bit_length(character varying(90))', 'jose'), 32;

# bad calls
eval { call('abs(int)', -42.5) };
like $@, qr/invalid input syntax for integer/;
eval { call('abs(text)', -42.5) };
like $@, qr/function abs\(text\) does not exist/;
eval { call('abs(nonesuchtype)', -42.5) };
like $@, qr/type "nonesuchtype" does not exist/;

# --- multi-argument, simple types
is call('trunc(numeric,int)', 42.4382, 2), '42.43';

# --- unusual types from strings
is call('host(inet)',    '192.168.1.5/24'), '192.168.1.5';
is call('network(inet)', '192.168.1.5/24'), '192.168.1.0/24';
is call('abbrev(cidr)',  '10.1.0.0/16'),    '10.1/16';
is call('numnode(tsquery)', '(fat & rat) | cat'), 5;

spi_exec_query('create temp sequence seqn1 start with 42');
is call('nextval(regclass)', 'seqn1'), 42;
is call('nextval(text)',     'seqn1'), 43;

is call('string_to_array(text, text)', 'xx~^~yy~^~zz', '~^~'), '{xx,yy,zz}';

# --- array and array reference handling
is call('array_dims(text[])', '{a,b,c}'), '[1:3]';
is call('array_dims(text[])', [qw(a b c)]), '[1:3]';
is call('array_dims(text[])', [[1,2,3], [4,5,6]]), '[1:2][1:3]';
is call('array_cat(int[], int[])', [1,2,3], [4,5,6]), '{1,2,3,4,5,6}';


# ====== single-value multi-row function ======

@ary = call('unnest(int[])', '{11,12,13}');
is scalar @ary, 3;
is_deeply \@ary, [ 11, 12, 13 ];

@ary = call('generate_series(int,int)', 10, 19);
is scalar @ary, 10;
is_deeply \@ary, [ 10..19 ];

# scalar context just returns first row
@ary = scalar call('generate_series(int,int)', 10, 19);
is scalar @ary, 1;
is_deeply \@ary, [ 10 ];

@ary = call('generate_series(int,int,int)', 10, 19, 4);
is_deeply \@ary, [ 10, 14, 18 ];

@ary = call('generate_series(timestamp,timestamp,interval)', '2008-03-01', '2008-03-02', '12 hours');
is_deeply \@ary, [ '2008-03-01 00:00:00', '2008-03-01 12:00:00', '2008-03-02 00:00:00' ];


# ====== multi-value (record) returning functions ======

@ary = call('pg_get_keywords()');
cmp_ok scalar @ary, '>', 200;
ok $row = $ary[0];
is ref $row, 'HASH';
ok exists $row->{word},    'should contain a word column';
ok exists $row->{catcode}, 'should contain a catcode column';
ok exists $row->{catdesc}, 'should contain a catdesc column';

# single-record
spi_exec_query(q{
	create or replace function f1(out r1 text, out r2 int) language plperl as $$
		return { r1=>10, r2=>11 };
	$$
});
@ary = call('f1()');
is scalar @ary, 1;
ok $row = $ary[0];
is $row->{r1}, 10;
is $row->{r2}, 11;
spi_exec_query('drop function f1()');

# multi-record
spi_exec_query(q{
	create or replace function f2() returns table (r1 text, r2 int) language plperl as $$
		return_next { r1 => $_, r2 => $_+1 } for 1..5;
		return undef;
	$$
});
@ary = call('f2()');
is scalar @ary, 5;
is $ary[-1]->{r1}, 5;
is $ary[-1]->{r2}, 6;
spi_exec_query('drop function f2()');

# === finish up
Test::More->builder->_ending;
return undef;
$func$;

select * from test_call();
