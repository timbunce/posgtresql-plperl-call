package PostgreSQL::PLPerl::Call;
# vim: sw=4:ts=8:sts=4:et

=for comment

doesn't handle types containing commas, e.g numeric(p,s) 

=cut

use strict;
use warnings;
use Exporter;
use Carp;

our @ISA = qw(Exporter);
our @EXPORT = qw(call);

my %sig_cache;
our $debug;


sub parse_signature {
    my $sig = shift;
    $sig =~ m/^ \s* (\S+) \s* \( (.*?) \) $/x or return;
    my ($spname, @arg_types) = ($1, split(/\s*,\s*/, lc($2), -1));
    s/^\s+// for @arg_types;
    s/\s+$// for @arg_types;
    my $stdsig = "$spname(".join(",",@arg_types).")";
    return ($stdsig, $spname, \@arg_types);
}


sub call {
    my $sig = shift;

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

    # XXX ? switch 'single column' logic to come first and be more like
    # return $row->{$spname} if keys %$row == 1 and exists $row->{$spname};

    if (wantarray) {                   # list context - all rows
        my $rows = $rv->{rows};

        # return empty list if no rows
        return unless my $row1 = $rows->[0]; # peek at first row

        # return all rows as hash refs if more that one column
        return @$rows if keys %$row1 > 1;

        # return all rows as simple list of values if only one column
        return map { $_->{$spname} } @$rows;
    }
    elsif (defined wantarray) {        # scalar context - single row

        # return undef if no rows
        return undef unless my $row = $rv->{rows}[0];

        # return first row as hash ref if more that one column
        return $row if keys %$row > 1;

        # return row as simple column value if only one column
        return $row->{$spname};
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

    # return a closure to execute the query and return result ref
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
