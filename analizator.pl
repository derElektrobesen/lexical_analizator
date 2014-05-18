#!/usr/bin/perl

use strict;
use warnings;

use Config::Simple;
use IO::Handle;
use IO::Select;
use Socket;

my %cfg;
Config::Simple->import_from("config.cfg", \%cfg);

# Open pipes
my @pipes;
for (0 .. $cfg{'Analizator.nProcesses'} - 1) {
=cut
    my ($p, $c);
    socketpair($p, $c, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "$!\n";
    $p->autoflush(1);
    $c->autoflush(1);

    push @pipes, {
        parent_socket => $p,
        child_socket  => $c
    };

=cut
    socketpair(my $p, my $c, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "Socketpair failure: $!\n";
    $p->autoflush(1);
    $c->autoflush(1);
    push @pipes, {
        parent_socket => $p,
        child_socket  => $c
    };
}

my $is_parent;
my $pipeline_index;
my @pids;
=cut
if (my $pid = fork) {
    close $pipes[0]->{parent_socket};

    print {$pipes[0]->{child_socket}} "Hello!";
    waitpid $pid, 0;
} else {
    close $pipes[0]->{child_socket};

    my $m = readline $pipes[0]->{parent_socket};
    print "$$: $m\n";
}
exit 0;
=cut
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
    start_work($pipes[$pipeline_index]->{parent_socket});
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
    open my $data, '<', $cfg->{'Analizator.News'} or wait_n_die "Can't open $cfg->{'Analizator.News'}: $!\n";
    open my $out, '>', $cfg->{'Analizator.OutFile'} or wait_n_die "Can't open $cfg->{'Analizator.OutFile'}: $!\n";

    my @ready;
    my $count = @_;
    while (1) {
        my @handles = $select->can_read($cfg->{'Analizator.WaitTimeout'});
        for (@handles) {
            my $str = readline $_;
            save_results($out, $str);

            my $words = read_news($data);
            if (defined $words) {
                print $_ "$words\n";
            } else {
                close $_;
                $count--;
            }
        }
        last unless $count;
    }
    waitpid -1, 0;
}

sub pre_dispatch {
    my $select = shift;

    while (my $pipeline = shift) {
        close $pipeline->{parent_socket};
        $select->add($pipeline->{child_socket});
    }
}

sub parse_input {
    my $str = shift;
    $str =~ s/([,.;:"'()])/ $1 /g;
    $str =~ s/\s+/ /g;
    $str;
}

sub read_news {
    my $fd = shift;
    my $news_name = 0;
    while (defined $news_name && !$news_name) {
        $news_name = <$fd>;
        chomp $news_name;
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

    print "$$: $data\n";
}

sub start_work {
    # Child process main subroutine
    my $socket = shift;
    my $handlers = prepare_handlers();
    read_grammemes($handlers);

    print $socket "0\n"; # Can read messages from now

    while (1) {
        my $str = readline $socket;
        print "Read: $str\n";
        last unless defined $str;
        $str = process_line($str);
        chomp $str;
        print $socket "$str\n";
    }
}

sub prepare_handlers {

}

sub read_grammemes {
    my $handlers = shift;
}

sub process_line {
    my $line = shift;
    sleep 2;
    return "Processed!";
}
