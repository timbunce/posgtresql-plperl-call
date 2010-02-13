package PostgreSQL::PLPerl::Call;
# vim: sw=4:ts=8:sts=4:et

=head1 NAME

PostgreSQL::PLPerl::Call - Simple interface for calling SQL functions from PostgreSQL PL/Perl

=head1 SYNOPSIS

    use PostgreSQL::PLPerl::Call qw(call);

Returning single-row single-column values:

    $pi = call('pi()'); # 3.14159265358979

    $net = call('network(inet)', '192.168.1.5/24'); # '192.168.1.0/24';

    $seqn = call('nextval(regclass)', $sequence_name);

    $dims = call('array_dims(text[])', '{a,b,c}');   # '[1:3]'

    # array arguments can be perl array references:
    $ary = call('array_cat(int[], int[])', [1,2,3], [2,1]); # '{1,2,3,2,1}'

Returning multi-row single-column values:

    @ary = call('generate_series(int,int)', 10, 15); # (10,11,12,13,14,15)

Returning single-row multi-column values:

    # assuming create function func(int) returns table (r1 text, r2 int) ...
    $row = call('func(int)', 42); # returns hash ref { r1=>..., r2=>... }

Returning multi-row multi-column values:

    @rows = call('pg_get_keywords()'); # ({...}, {...}, ...)

=head1 DESCRIPTION

The C<call> function provides a simple effcicient way to call SQL functions
from PostgreSQL PL/Perl code.

The first parameter is a I<signature> that specifies the name of the function
to call and then, in parenthesis, the types of any arguments as a comma
separated list. For example:

    'pi()'
    'generate_series(int,int)'
    'array_cat(int[], int[])'

The types specify how the I<arguments> to the call should be interpreted.
They don't have to exactly match the types used to declare the function you're
calling.

Any further parameters are used as arguments to the function being called.

=head2 Array Arguments

The argument value corresponding to a type that contains 'C<[]>' can be a
string formated as an array literal, or a reference to a perl array. In the
later case the array reference is automatically converted into an array literal
using the C<encode_array_literal()> function.

=head2 Varadic Functions

Functions with C<varadic> arguments can be called with a fixed number of
arguments by repeating the type name in the signature the same number of times.
For example, given:

    create function vary(VARADIC int[]) as ...

you can call that function with three arguments using:

    call('vary(int,int,int)', $int1, $int2, $int3);

Alternatively, you can append the string 'C<...>' to the last type in the
signature to indicate that the argument is varadic. For example:

    call('vary(int...)', @ints);

=head2 Results

The C<call()> function processes return values in one of four ways depending on
two criteria: single column vs. multi-column results, and list context vs scalar context.

If the results contain a single column with the same name as the function that
was called, then those values are extracted returned directly. This makes
simple calls very simple:

    @ary = call('generate_series(int,int)', 10, 15); # (10,11,12,13,14,15)

Otherwise, the rows are returned as references to hashes:

    @rows = call('pg_get_keywords()'); # ({...}, {...}, ...)

If the C<call()> function was executed in list context then all the values/rows
are returned, as shown above.

If the function was executed in scalar context then an exception will be thrown
if more than one row is returned. For example:

    $foo = call('generate_series(int,int)', 10, 10); # 10
    $bar = call('generate_series(int,int)', 10, 11); # dies


=head2 Performance

Internally C<call()> uses C<spi_prepare()> to create a plan to execute the
function with the typed arguments.

The plan is cached using the call 'signature' as the key. (Minor variations in
the signature will still reuse the same plan because an extra cache entry is
created using a 'normalized' signature.)

=head2 Limitations and Caveats

Requires PostgreSQL 9.0 or later.

Types that contain a comma can't be used in the call signature. That's not a
problem in practice as it only affects 'C<numeric(p,s)>' and 'C<decimal(p,s)>'
and the 'C<,s>' part isn't needed. Typically the 'C<(p,s)>' portion isn't used in
signatures.

The return value of functions that have a C<void> return type should not be
relied upon.

=cut

use strict;
use warnings;
use Exporter;
use Carp;

our @ISA = qw(Exporter);
our @EXPORT = qw(call);

my %sig_cache;
our $debug = 0;


sub parse_signature {
    my $sig = shift;
    $sig =~ m/^ \s* (\S+) \s* \( (.*?) \) (\d+) $/x
        or return;
    my ($spname, $arg_count, @arg_types) = ($1, $3, split(/\s*,\s*/, lc($2), -1));
    s/^\s+// for @arg_types;
    s/\s+$// for @arg_types;

    # if varadic, replace '...' marker with the appropriate number
    # of copies of the preceeding type name
    if (@arg_types and $arg_types[-1] =~ s/\s*\.\.\.//) {
        my $varadic_type = pop @arg_types;
        push @arg_types, $varadic_type
            until @arg_types >= $arg_count;
    }
    else {
        return if $arg_count != @arg_types;
    }

    my $stdsig = "$spname(".join(",",@arg_types).")$arg_count";
    return ($stdsig, $spname, \@arg_types);
}


sub call {
    my $sig = shift;

    # add argument count to sig to handle varadic subs
    $sig .= scalar @_;

    my $how = $sig_cache{$sig} ||= do {

        # get a normalized signature to recheck the cache with
        # and also extract the SP name and argument types
        my ($stdsig, $spname, $arg_types) = parse_signature($sig)
            or croak "Can't parse '$sig'";
        warn "parsed call($sig) => $spname(@$arg_types) => $stdsig\n"
            if $debug;

        # recheck the cache with with the normalized signature
        $sig_cache{$stdsig} ||= [ # else a new entry (for both caches)
            $spname,
            mk_process_args($arg_types),
            mk_process_call($spname, $arg_types),
        ];
    };

    my ($spname, $prepargs, $callsub) = @$how;

    my $rv = $callsub->( $prepargs ? $prepargs->(@_) : @_ );

    my $rows = $rv->{rows};
    my $row1 = $rows->[0] # peek at first row
        or return;        # no row: undef in scalar context else empty list

    my $is_single_column = (keys %$row1 == 1 and exists $row1->{$spname});

    if (wantarray) {                   # list context - all rows

        return map { $_->{$spname} } @$rows if $is_single_column;
        return @$rows;
    }
    elsif (defined wantarray) {        # scalar context - single row

        croak "$sig returned more than one row but was called in scalar context"
            if @$rows > 1;

        return $row1->{$spname} if $is_single_column;
        return $row1;
    }
    # else void context - nothing to do
    return;
}


sub mk_process_args {
    my ($arg_types) = @_;

    # return a closure that pre-processes the arguments of the call
    # else undef if no argument pre-processing is required

    my $hooks;
    my $i = 0;
    for my $type (@$arg_types) {
        if ($type =~ /\[/) {    # ARRAY
            $hooks->{$i} = sub { return ::encode_array_literal(shift) };
        }
        ++$i;
    }

    return undef unless $hooks;

    my $sub = sub {
        my @args = @_;
        while ( my ($argidx, $preproc) = each %$hooks ) {
            $args[$argidx] = $preproc->($args[$argidx]);
        }
        return @args;
    };

    return $sub;
}


sub mk_process_call {
    my ($spname, $arg_types) = @_;

    # return a closure that will execute the query and return result ref
    my $sub;
    if ($arg_types) {

        my $placeholders = join ",", map { '$'.$_ } 1..@$arg_types;
        my $plan = ::spi_prepare("select * from $spname($placeholders)", @$arg_types);
        $sub = sub {
            # XXX need to catch exceptions from here are rethrow using croak
            # to appear to come from the callers location (outside this package)
            warn "calling $spname($placeholders)[@$arg_types](@_)"
                if $debug;
            return ::spi_exec_prepared($plan, @_)
        };

    }
    else {

        # XXX this branch isn't currently used
        $sub = sub {
            my $args = join ",", map { ::quote_nullable($_) } @_;
            warn "calling $spname($args)"
                if $debug;
            return ::spi_exec_query("select * from $spname($args)");
        };

    }
    warn "mk_process_call($spname, @$arg_types): $sub"
        if $debug;

    return $sub;
}

1;
