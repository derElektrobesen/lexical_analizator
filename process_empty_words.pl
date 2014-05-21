#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use Config::Simple;

my %cfg;
Config::Simple->import_from("config.cfg", \%cfg);

my $dbh = DBI->connect("DBI:mysql:$cfg{'MySQL.DB'}", $cfg{'MySQL.User'}, $cfg{'MySQL.Pass'})
    or die "Can't connect to database: $DBI::errstr\n";

my $sth = $dbh->prepare("select id, parent from lemmas order by id asc");
$sth->execute;
print "Request 1 executed\n";
my $sth_1 = $dbh->prepare("select distinct(lemma_id) from g_list order by lemma_id asc");
$sth_1->execute;
print "Request 2 executed\n";

my %lemmas_ids;
my $fetch_next = 1;
my ($id_1, $id_2, $parent);
while (1) {
    ($id_1, $parent) = $sth->fetchrow_array;
    ($id_2) = $sth_1->fetchrow_array if $fetch_next;
    last unless defined $id_1;
    $fetch_next = 1;
    next if $id_1 == $id_2;
    next unless defined $parent;
    $lemmas_ids{$parent} = $id_1;
    $fetch_next = 0;
}

$sth->finish;
$sth_1->finish;

print "Lemmas processed\n";
print "Lemmas count: " . scalar(keys %lemmas_ids) . "\n";;

my @ids;
my $last_id;
my ($start_index, $delta) = (0, 20000);

my @copy = sort keys %lemmas_ids;

print "Requesting grammemes\n";

while ($start_index < @copy) {
    my $end_index = $start_index + $delta;
    $end_index = @copy - 1 if $end_index >= @copy;
    $sth = $dbh->prepare("select lemma_id, grammem_id from g_list where lemma_id in (" . join(', ', @copy[$start_index .. $end_index]) . ") order by lemma_id");
    $sth->execute;

    while (my ($id, $gid) = $sth->fetchrow_array) {
        push @ids, "($lemmas_ids{$id}, $gid)";
        $last_id = $id;
    }
    $sth->finish;
    $start_index += $delta;
    print "$start_index of " . @copy . " requested\n";
}
%lemmas_ids = ();

print "Grammemes requested\n";

$start_index = 0;

while ($start_index < @ids) {
    my $end = $start_index + $delta;
    $end = @ids - 1 if $end >= @ids;
    my $str = "insert into g_list(lemma_id, grammem_id) values ";
    $str .= join ', ', @ids[$start_index .. $end];
    $sth = $dbh->prepare($str);
    $sth->execute;
    $start_index += $delta;
    print "$start_index of " . @ids . " inserted\n";
}
