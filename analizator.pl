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
    pre_dispatch($select, @_);
    open my $data, '<:encoding(UTF-8)', $cfg->{'Analizator.News'} or wait_n_die "Can't open $cfg->{'Analizator.News'}: $!\n";
    open my $out, '>', $cfg->{'Analizator.OutFile'} or wait_n_die "Can't open $cfg->{'Analizator.OutFile'}: $!\n";
    my %global_data = ( config => $cfg );

    my @ready;
    my $count = @_;
    my $times_out = 0;
    while (1) {
        my @handles = $select->can_read($cfg->{'Analizator.WaitTimeout'});
        for (@handles) {
            $times_out = 0;
            my $str = readline $_;
            utf8::encode($str);
            save_results($out, $str, \%global_data);

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
    use Data::Dumper;
    print Dumper $global_data{frequences};
}

sub pre_dispatch {
    my $select = shift;

    while (my $pipeline = shift) {
        close $pipeline->{parent_socket};
        $select->add($pipeline->{child_socket});
    }
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
    }
}

sub start_work {
    # Child process main subroutine
    my $cfg = shift;
    my $socket = shift;
    my $handlers = prepare_handlers();
    $handlers->{config} = $cfg;
    read_grammemes($handlers);

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
        last_id         => $dbh->prepare("select last_insert_id()"),
        get_grammemes   => $dbh->prepare("select id, description from grammemes"),
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
    my @words = map { { word => $_, props => [] } } split / /, $line;

    my $response;
    if ($handlers->{config}->{'Analizator.LearnMode'}) {
        $response = count_frequency($handlers, \@words);
    } else {
        $response = "0"; # TODO
    }
    return $response;
}

sub count_frequency {
    my $h = shift;
    my $freq_ptr = $h->{words_frequency};
    my $words = shift;

    for (@$words) {
        $freq_ptr->{$_->{word}}++ if $_->{word} =~ /^[А-Яа-я]+$/;
    }

    my @ff = map  { $_ => $freq_ptr->{$_} }
             grep { $freq_ptr->{$_} >= $h->{config}->{'Analizator.WordsMinFreq'} }
             keys %$freq_ptr;

    $h->{words_frequency} = {};

    return join ' ', @ff if @ff;
    return "0";
}
