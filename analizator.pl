#!/usr/bin/perl

use strict;
use warnings;

use Config::Simple;
use IO::Handle;
use IO::Select;

my %cfg;
Config::Simple->import_from("config.cfg", \%cfg);

my $child;

# Open pipes
my @pipes;
for (0 .. $cfg{'Analizator.nProcesses'} - 1) {
    pipe my $p_rd_pipe, my $c_wr_pipe;
    pipe my $c_rd_pipe, my $p_wr_pipe;

    $c_wr_pipe->autoflush(1);
    $p_wr_pipe->autoflush(1);

    push @pipes, {
        parent_reader   => $p_rd_pipe,
        parent_writer   => $p_wr_pipe,
        child_readed    => $c_rd_pipe,
        child_writer    => $c_wr_pipe,
    };
}

my ($wr_pipe, $rd_pipe);
my $is_parent;
my $pipeline_index;
my @pids;

# Fork
for (0 .. $cfg{'Analizator.nProcesses'} - 1) {
    my $pid = fork;
    unless (defined $pid) {
        kill 'KILL', @pids;
        wait -1;
        die "Cannot fork: $!\n";
    }

    $is_parent = $pid;
    unless ($pid) {
        $pipeline_index = $_;
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
        close $_->{child_reader};
        close $_->{child_writer};
        if ($index != $pipeline_index) {
            close $_->{parent_reader};
            close $_->{parent_writer};
        }
        $index++;
    }
    start_work($pipes[$pipeline_index]->{parent_reader}, $pipes[$pipeline_index]->{parent_writer});
}
exit 0;

sub dispatch {
    # Parent process main subroutine
    my $cfg = shift;
    my $select = IO::Select->new;
    my %pipes = pre_dispatch($select, @_);
    open my $data, '<', $cfg->{'Analizator.News'} or die "Can't open $cfg->{'Analizator.News'}: $!\n";

    my @ready;
    while (1) {
        my @handles = $select->can_read($cfg->{'Analizator.WaitTimeout'});
        for (@handles) {
            my $words = read_news($data);
            if (defined $words) {
                $pipes{$_}->{working} = 1;
                my $handle = $pipes{$_}->{writer};
                print $handle $words;
            } else {
                $pipes{$_}->{working} = 0;
            }
        }

        my $working = 0;
        for (keys %pipes) {
            if ($pipes{$_}->{working}) {
                $working = 1;
                last;
            }
        }

        last unless $working;
    }

    for (keys %pipes) {
        close $pipes{$_}->{writer};
        close $_;
    }

    wait -1;
}

sub pre_dispatch {
    my $select = shift;
    my %pipes;

    while (my $pipeline = shift) {
        close $pipeline->{parent_reader};
        close $pipeline->{parent_writer};
        $pipes{$pipeline->{child_reader}} = {
            writer  => $pipeline->{child_writer},
            working => 0,
        };

        $select->add($pipeline->{child_reader});
    }
    return %pipes;
}

sub parse_input {
    join ' ', map { s/([,.;:"'()])/ $1/g } split / /, shift;
}

sub read_news {
    my $fd = shift;
    my $news_name = <$fd>;
    my $news_data = <$fd>;

    my $line;
    while (defined($line = <$fd>) && !$line) {} # Skip empty lines
    return undef if (!defined $news_name || !defined $news_data);

    $news_name = parse_input $news_name;
    $news_data = parse_input $news_data;

    return "$news_name $news_data";
}

sub start_work {
    # Child process main subroutine
    my ($reader, $writer) = @_;
}
