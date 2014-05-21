#!/usr/bin/perl

use strict;
use warnings;
use utf8;

use Config::Simple;
use IO::Handle;
use IO::Select;
use Socket;
use DBI;
use Encode;

my %cfg;
Config::Simple->import_from("config.cfg", \%cfg);

# Open pipes
my @pipes;
for (0 .. $cfg{'Analizator.nProcesses'} - 1) {
    socketpair(my $p, my $c, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "Socketpair failure: $!\n";
    $p->autoflush(1);
    $c->autoflush(1);
    binmode $p, ":encoding(UTF-8)";
    binmode $c, ":encoding(UTF-8)";
    push @pipes, {
        parent_socket => $p,
        child_socket  => $c
    };
}

my $is_parent;
my $pipeline_index;
my @pids;

# Fork
for (0 .. $cfg{'Analizator.nProcesses'} - 1) {
    my $pid = fork;
    wait_n_die("Cannot fork: $!\n") unless defined $pid;

    $is_parent = $pid;
    unless ($pid) {
        $pipeline_index = $_;
        @pids = ();
        last;
    } else {
        push @pids, $pid;
    }
}

# Entry point
if ($is_parent) {
    dispatch(\%cfg, @pipes);
} else {
    my $index = 0;
    for (@pipes) {
        close $_->{child_socket};
        close $_->{parent_socket} if $index != $pipeline_index;
        $index++;
    }
    start_work(\%cfg, $pipes[$pipeline_index]->{parent_socket});
}
exit 0;

sub wait_n_die {
    my $msg = shift;
    kill 'KILL', @pids;

    waitpid -1, 0;
    die $msg;
}

sub dispatch {
    # Parent process main subroutine
    my $cfg = shift;
    my $select = IO::Select->new;
    my $global_data = pre_dispatch($select, $cfg, @_);
    open my $data, '<:encoding(UTF-8)', $cfg->{'Analizator.News'} or wait_n_die "Can't open $cfg->{'Analizator.News'}: $!\n";
    open my $out, '>', $cfg->{'Analizator.OutFile'} or wait_n_die "Can't open $cfg->{'Analizator.OutFile'}: $!\n";

    my @ready;
    my $count = @_;
    my $times_out = 0;
    while (1) {
        my @handles = $select->can_read($cfg->{'Analizator.WaitTimeout'});
        for (@handles) {
            $times_out = 0;
            my $str = readline $_;

            unless (defined $str) {
                close $_;
                $count--;
                next;
            }

            #utf8::encode($str);
            save_results($out, $str, $global_data);

            my $words = read_news($data);
            if (defined $words) {
                print $_ "$words\n";
            } else {
                close $_;
                $count--;
            }
        }
        unless (@handles) {
            $times_out++;
        }
        last if $count < 1 or $times_out > 20;
    }
    if ($cfg->{'Analizator.LearnMode'}) {
        store_lemmas_cache($global_data);
    }
}

sub pre_dispatch {
    my $select = shift;
    my $cfg = shift;

    my %h = ( config => $cfg );

    while (my $pipeline = shift) {
        close $pipeline->{parent_socket};
        $select->add($pipeline->{child_socket});
    }

    my $dbh = $h{dbh} = DBI->connect("DBI:mysql:$cfg->{'MySQL.DB'}", $cfg->{'MySQL.User'}, $cfg->{'MySQL.Pass'})
        or wait_n_die "Can't connect to database: $DBI::errstr\n";
    $h{requests} = {
        insert_lemma_cache  => $dbh->prepare("insert into lemmas_cache(lemma_id, count) values (?, ?) " .
                                             "on duplicate key update count = ?"),
        get_cache           => $dbh->prepare("select l.name, l.id, c.count from lemmas_cache c join lemmas l " .
                                             "on l.id = c.lemma_id order by count desc limit 20000"),
    };
    return \%h;
}

sub read_lemmas_cache {
    my $req = shift;

    $req->execute;
    return map { $_->[0] => { id => $_->[1], count => $_->[2] } } @{$req->fetchall_arrayref};
}

sub store_lemmas_cache {
    my $data_ptr = shift;
    my %cache = read_lemmas_cache($data_ptr->{requests}->{get_cache});

    print "Storring lemmas cache\n";

    $cache{$_}->{count} += $data_ptr->{frequences}->{$_} for keys %{$data_ptr->{frequences}};

    my @names;
    for (keys %cache) {
        push @names, "'$_'" unless defined $cache{$_}->{id};
    }

    if (@names) {
        my ($slice_start, $slice_size) = (0, $data_ptr->{config}->{'Analizator.InsertSliceSize'});
        while ($slice_start < @names) {
            my $end = $slice_start + $slice_size;
            $end = @names - 1 if $end >= @names;
            my $str = "select name, id from lemmas where name in(" . join(", ", @names[$slice_start .. $end]) . ")";
            my $sth = $data_ptr->{dbh}->prepare($str);
            $sth->execute;
            $cache{ $_->[0] }->{id} = $_->[1] for @{$sth->fetchall_arrayref};
            $slice_start += $end;
            print "$slice_start of " . scalar(@names) . " complete\n";
        }
    }
    print "Request complete\n";

    my (@counts, @ids);
    for (keys %cache) {
        if ($cache{$_}->{id} && $cache{$_}->{count}) {
            push @counts, $cache{$_}->{count};
            push @ids, $cache{$_}->{id};
        }
    }

    $data_ptr->{requests}->{insert_lemma_cache}->execute_array({}, \@ids, \@counts, \@counts);
}

sub parse_input {
    my $str = lc shift;
    $str =~ s/([«»,.;:"'()])/ $1 /g;
    $str =~ s/\s+/ /g;
    $str;
}

sub read_news {
    my $fd = shift;
    my $news_name = 0;
    while (defined $news_name && !$news_name) {
        $news_name = <$fd>;
        chomp $news_name if defined $news_name;
    }

    my $news_data = <$fd>;
    return undef if (!defined $news_name || !defined $news_data);

    $news_name = parse_input $news_name;
    $news_data = parse_input $news_data;

    return "$news_name $news_data";
}

sub save_results {
    my $fd = shift;
    my $data = shift;
    my $glob = shift;

    chomp $data;
    return if $data eq "0";

    if ($glob->{config}->{'Analizator.LearnMode'}) {
        my %freqs = split /\s+/, $data;
        $glob->{frequences}->{$_} += $freqs{$_} for keys %freqs;
    } else {
        my @aa = split /; /, $data;
        print $fd join "\n", @aa;
    }
}

sub start_work {
    # Child process main subroutine
    my $cfg = shift;
    my $socket = shift;
    my $handlers = prepare_handlers();
    $handlers->{config} = $cfg;
    read_grammemes($handlers);

    unless ($cfg->{'Analizator.LearnMode'}) {
        my %cache = read_lemmas_cache($handlers->{requests}->{get_cache});
        $handlers->{cache} = \%cache;
    }

    print $socket "0\n"; # Can read messages from now

    while (1) {
        my $str = readline $socket;
        last unless defined $str;
        $str = process_line($handlers, $str);
        chomp $str;
        print $socket "$str\n";
    }
}

sub prepare_handlers {
    my %h;
    my $dbh = $h{dbh} = DBI->connect("DBI:mysql:$cfg{'MySQL.DB'}", $cfg{'MySQL.User'}, $cfg{'MySQL.Pass'})
        or wait_n_die "Can't connect to database: $DBI::errstr\n";
    $h{requests} = {
        get_grammemes   => $dbh->prepare("select id, description from grammemes"),
        get_cache       => $dbh->prepare("select l.name, l.id, c.count from lemmas_cache c join lemmas l " .
                                         "on l.id = c.lemma_id order by count desc limit 20000"),
    };
    $h{words_frequency} = {};
    return \%h;
}

sub read_grammemes {
    my $h = shift;
    $h->{requests}->{get_grammemes}->execute;
    my %data = map { $_->[0], $_->[1] } @{$h->{requests}->{get_grammemes}->fetchall_arrayref};
    $h->{grammemes} = \%data;
}

sub process_line {
    my $handlers = shift;
    my $line = shift;
    chomp $line;
    my @words = grep { $_ } split / /, $line;

    my $response;
    if ($handlers->{config}->{'Analizator.LearnMode'}) {
        $response = count_frequency($handlers, \@words);
    } else {
        $response = process_words($handlers, \@words);
    }
    return $response;
}

sub process_words {
    my ($h, $words) = @_;

    my @res;
    my @not_cached_words;
    my @not_cached_props;
    my @not_cached;

    my $i = -1;
    for (@$words) {
        $i++;

        my $str = $_;
        utf8::encode($str);
        my $ptr = $h->{cache}->{$str};

        my %item = ( word => $str, id => $ptr->{id} );
        push @res, \%item;
        next unless /^[а-я]+$/;

        unless (defined $ptr && %$ptr) {
            push @not_cached_words, $i;
            push @not_cached, $i;
            next;
        }

        unless (defined $ptr->{properties}) {
            push @not_cached_props, $i;
            push @not_cached, $i;
            next;
        }

        $item{props} = $ptr->{properties};
    }

    request_words($h, \@res, \@not_cached_words) if @not_cached_words;
    request_props($h, \@res, \@not_cached_props) if @not_cached_props;
    for my $index (@not_cached) {
        $res[$index]->{props} = $h->{cache}->{ $res[$index]->{word} }->{properties};
    }

    return join '; ', map {
        my $props = "unknown";
        $props = join ', ', @{$_->{props}} if defined $_->{props};
        "$_->{word}: [$props]";
    } @res;
}

sub request_words {
    my ($h, $words, $indexes) = @_;
    my ($cur_index, $index_step) = (0, $h->{config}->{'Analizator.InsertSliceSize'});
    my $cache = $h->{cache};
    my $grammemes = $h->{grammemes};

    my @names = map { $_->{word} } @$words[ @$indexes ];

    while ($cur_index < @$indexes) {
        my $end = $cur_index + $index_step;
        $end = @$indexes - 1 if $end >= @$indexes;

        my $str = 'select l.id, l.name, ll.name as `parent`, gl.grammem_id from g_list gl join lemmas l on ' .
                  'l.id = gl.lemma_id left outer join lemmas ll on ll.id = l.parent where l.name in ("';
        $str .= join('", "', @names[$cur_index .. $end]) . '")';
        my $sth = $h->{dbh}->prepare($str);
        $sth->execute;

        while (my ($id, $name, $parent, $grammem) = $sth->fetchrow_array) {
            if (defined $parent) {
                next if defined $cache->{$name}->{form_id} && $cache->{$name}->{form_id} != $id;
                $cache->{$name}->{id} = $cache->{$name}->{form_id} = $id;
            }
            push @{$cache->{$name}->{properties}}, $grammemes->{$grammem} if defined $grammemes->{$grammem};
        }

        $cur_index += $index_step;
    }
}

sub request_props {
    my ($h, $words, $indexes) = @_;
    my ($cur_index, $index_step) = (0, $h->{config}->{'Analizator.InsertSliceSize'});
    my $cache = $h->{cache};
    my $grammemes = $h->{grammemes};

    my @ids = map { $_->{id} } @$words[ @$indexes ];
    my %names;

    while ($cur_index < @$indexes) {
        my $end = $cur_index + $index_step;
        $end = @$indexes - 1 if $end >= @$indexes;

        my $str = "select l.name, gl.grammem_id, gg.grammem_id from g_list gl join lemmas l on l.id = gl.lemma_id " .
                  "left outer join lemmas ll on ll.id = l.parent left outer join g_list gg on gg.lemma_id = ll.id " .
                  "where gl.lemma_id in (";
        $str .= join(", ", @ids[$cur_index .. $end]) . ") order by l.name";
        my $sth = $h->{dbh}->prepare($str);
        $sth->execute;

        while (my ($name, $gr_id1, $gr_id2) = $sth->fetchrow_array) {
            $names{$name}->{$gr_id1} = 1;
            $names{$name}->{$gr_id2} = 1;
        }
        $cur_index += $index_step;
    }
    for my $name (keys %names) {
        @{$cache->{$name}->{properties}} = grep { $_ } map { $grammemes->{$_} } keys %{$names{$name}};
    }
}

sub count_frequency {
    my $h = shift;
    my $freq_ptr = $h->{words_frequency};
    my $words = shift;

    for (@$words) {
        $freq_ptr->{$_->{word}}++ if $_->{word} =~ /^[а-я]+$/;
    }

    my @ff = map  { $_ => $freq_ptr->{$_} }
             grep { $freq_ptr->{$_} >= $h->{config}->{'Analizator.WordsMinFreq'} }
             keys %$freq_ptr;

    $h->{words_frequency} = {};

    return join ' ', @ff if @ff;
    return "0";
}
