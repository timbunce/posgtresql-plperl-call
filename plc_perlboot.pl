# vim: sw=4:ts=8:sts=4:et
use strict;
use warnings;

my %sig_cache;


sub call {
    my $sig = shift;

    my $how = $sig_cache{$sig} ||= do {

        # get a normalized signature to recheck the cache with
        # and also extract the SP name and argument types
        my ($stdsig, $spname, $arg_types) = parse_signature($sig);

        # recheck the cache with with the normalized signature
        $sig_cache{$stdsig} ||= [
            # create new entry (in both caches)
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
}

sub mk_process_call {
    my ($spname, $arg_types) = @_;

    # return a closure to execute the query and return result ref
    my $sub;
    if ($arg_types) {
        my $placeholders = join ",", map { '$'.$_ } 1..@$arg_types;
        my $plan = spi_prepare("select * from $spname($placeholders)", @$arg_types);
        $sub = sub { return spi_exec_prepared($plan, @_) };
    }
    else {
        $sub = sub {
            my $args = join ",", map { quote_nullable($_) } @_;
            return spi_exec_query("select * from $spname($args)");
        };
    }

    return $sub;
}


=for comment old

sub mkspcaller {
    my ($spname, $arg_types, $single_mode) = @_;

    # create a closure to execute the query
    my $sub;
    if (defined $arg_types) {
        my @arg_types = split /\s*,\s*/, $arg_types;
        my $placeholders = join ",", map { '$'.$_ } 1..@arg_types;
        my $plan = spi_prepare("select * from $spname($placeholders)", @arg_types);
        $sub = sub { return spi_exec_prepared($plan, @_) };
    }
    else {
        $sub = sub {
            my $args = join ",", map { quote_nullable($_) } @_;
            return spi_exec_query("select * from $spname($args)");
        };
    }

    # optionally wrap the closure to return a single value from a single row
    if ($single_mode) {
        my $get_row = $sub; # avoid leak
        $sub = sub {
            my $rv = $get_row->(@_);
            my $row = $rv->{rows}[0]       # first row only
                or return undef;           # return undef if no rows
            return $row if keys %$row > 1; # return record as hashref
            return $row->{$spname};        # return single value
        };
    }

    return $sub;
}
    
sub mkspcaller_for_autoload {
    (my $spname = shift) =~ s/.*:://; # remove SP:: prefix
    our %mkspcaller_cache;
    my $sub = $mkspcaller_cache{ $spname .'+'. scalar @_ }
        ||= mkspcaller($spname, undef, 1);
    return $sub->(@_);
}
=cut

1;
