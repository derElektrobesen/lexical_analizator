#!/usr/bin/perl

use strict;
use warnings;

use Config::Simple;
use XML::Parser;
use DBI;

my %cfg;
Config::Simple->import_from("config.cfg", \%cfg);

my $lines_to_read = $cfg{'Program.LinesToRead'};
my $skip = 0;

for (1 .. $cfg{'Program.nProcesses'} - 1) {
    unless (fork) {
        $skip = $_ * $lines_to_read;
        last;
    }
}

my $dbh = DBI->connect("DBI:mysql:$cfg{'MySQL.DB'}", $cfg{'MySQL.User'}, $cfg{'MySQL.Pass'})
    or die "Can't connect to database: $DBI::errstr\n";

my %requests = (
    last_id             => $dbh->prepare("select last_insert_id()"),
    add_grammem         => $dbh->prepare("insert into grammemes(name, alias, description, parent) values (?, ?, ?, ?)"),
    add_lemma_gr        => $dbh->prepare("insert into g_list(lemma_id, grammem_id) values (?, ?)"),
    add_lemma           => $dbh->prepare("insert into lemmas(name, parent) values (?, ?)"),
    get_grammemes       => $dbh->prepare("select name, id from grammemes"),
);

my $xml_parser = XML::Parser->new(
    Style                   => 'Debug',
    'Non-Expat-Options'     => {
        grammemes_parsed        => 0,
        in_lemmas               => 0,
        element                 => {},
        grammemes               => {},
        last_lemma_id           => -1,
    },
    Handlers                => {
        Start                   => \&on_tag_start,
        End                     => \&on_tag_end,
        Char                    => \&on_tag_data,
    },
);

open my $fd, '<', $cfg{'Corpus.XML'} or die "Failure on open: $!\n";

if ($cfg{'Program.SkipLines'}) {
    skip_lines($fd, $cfg{'Program.SkipLines'});
}
if ($skip != 0) {
    sleep $cfg{'Program.GrammemesLoadingTime'}; # Waiting for grammemes loading
}

print "Process $$ started.\n";

skip_lines($fd, $skip);
load_grammemes($xml_parser);
read_file($fd, $xml_parser);
print "Process $$ finished.\n";
exit 0;

sub read_file {
    my $fd = shift;
    my $instance = shift;
    my $line;
    for (1 .. $lines_to_read) {
        $line = <$fd>;
        last unless defined $line;
        next unless $line;

        chomp $line;

        if ($line eq '</grammemes>') {
            $instance->{'Non-Expat-Options'}->{grammemes_parsed} = 1;
            next;
        }

        if ($line eq '<lemmata>') {
            $instance->{'Non-Expat-Options'}->{in_lemmas} = 1;
            next;
        }

        eval {
            $instance->parse($line);
        };
        print "Incorrect parsed:\n$line: $@\n" if $@;
    }
}

sub load_grammemes {
    my $instance = shift;
    my $ptr = $requests{get_grammemes};
    $ptr->execute;

    my %data = map { $_->[0], $_->[1] } @{$ptr->fetchall_arrayref};
    $instance->{'Non-Expat-Options'}->{grammemes} = \%data;
    if (%{$instance->{'Non-Expat-Options'}->{grammemes}}) {
        $instance->{'Non-Expat-Options'}->{grammemes_parsed} = 1;
        $instance->{'Non-Expat-Options'}->{in_lemmas} = 1;
    }
    $ptr->finish;
}

sub skip_lines {
    my ($fd, $count) = @_;
    <$fd> for 1 .. $count;
}

sub on_tag_start {
    my ($instance, $elem, %attrs) = @_;
    my $opts = $instance->{'Non-Expat-Options'};

    # Restrictions are ignored
    my $element = $opts->{element};
    if (!$opts->{grammemes_parsed}) {
        if ($elem eq 'grammeme') {
            $opts->{element} = { parent => $attrs{parent} };
        } elsif ($elem eq 'name') {
            $element->{name_given} = 1;
        } elsif ($elem eq 'alias') {
            $element->{alias_given} = 1;
        } elsif ($elem eq 'description') {
            $element->{description_given} = 1;
        }
    } elsif (!$opts->{in_lemmas} && $elem eq 'lemmata') {
        $opts->{in_lemmas} = 1;
    } elsif ($opts->{in_lemmas}) {
        if ($elem =~ /^(?:l|f)$/) {
            $opts->{element} = { name => $attrs{t} };
            $opts->{element}->{parent} = $opts->{last_lemma_id} if $elem eq 'f';
        } elsif ($elem eq 'g') {
            push @{$element->{grammemes}}, $opts->{grammemes}->{$attrs{v}};
        }
    }
}

sub on_tag_end {
    my ($instance, $elem) = @_;
    my $opts = $instance->{'Non-Expat-Options'};

    if (!$opts->{grammemes_parsed}) {
        if ($elem eq 'grammemes') {
            $opts->{grammemes_parsed} = 1;
        } elsif ($elem eq 'grammeme') {
            add_grammeme($opts->{element}, $opts->{grammemes});
        }
    } elsif ($opts->{in_lemmas}) {
        if ($elem eq 'lemma') {
            $opts->{last_lemma_id} = -1;
        } elsif ($elem =~ /^(?:l|f)$/) {
            $opts->{last_lemma_id} = add_lemma($opts->{element}, $opts->{last_lemma_id});
        }
    }
}

sub on_tag_data {
    my ($instance, $data) = @_;

    return unless $data;

    my $element = $instance->{'Non-Expat-Options'}->{element};
    for (qw( name alias description )) {
        if ($element->{ $_ . "_given" }) {
            $element->{ $_ } = $data;
            $element->{ $_ . "_given" } = 0;
            last;
        }
    }

}

sub last_id {
    $requests{last_id}->execute;
    $requests{last_id}->fetchrow_arrayref()->[0];
}

sub add_grammeme {
    my $elem_ptr = shift;
    my $grams_ptr = shift;

    $elem_ptr->{parent_id} = $grams_ptr->{$elem_ptr->{parent}} if $elem_ptr->{parent};

    $requests{add_grammem}->execute(@$elem_ptr{qw( name alias description parent_id )});
    $grams_ptr->{$elem_ptr->{name}} = last_id;
}

sub add_lemma {
    my $elem_ptr = shift;
    my $last_id = shift;

    $requests{add_lemma}->execute(@$elem_ptr{qw( name parent )});
    my $id = last_id;

    if (defined $elem_ptr->{grammemes}) {
        my @ids = map { $id } (1 .. @{$elem_ptr->{grammemes}});
        $requests{add_lemma_gr}->execute_array({}, \@ids, $elem_ptr->{grammemes});
    }

    return $id unless defined $elem_ptr->{parent};
    $last_id;
}
