#!/usr/bin/perl

use strict;
use warnings;

use WWW::Curl::Easy;

my $home_url = "http://www.mk.ru/news/";
my $content_div_id = "article-content";
my $unimportant_block_class_name = "tools";
my $news_list_class_name = "news_list_big";
my $news_list_tag_name = "ul";
my $article_tag_name = "div";
my $out_f_name = "news_list";

my $year = 2014;
my $month = 4;
my $days_count = 30;

open my $out_f, ">", $out_f_name or die "Error while openning file $out_f_name: $!\n";

my $curl = WWW::Curl::Easy->new;

process_day("$home_url$year/$month/$_/", $curl, $out_f) for (1 .. $days_count);
exit(0);

sub process_day {
    my $cur_url = shift;
    my $curl = shift;
    my $out_f = shift;

    $curl->setopt(CURLOPT_URL, $cur_url);

    print "Processing: $cur_url\n";

    my $response_body;
    $curl->setopt(CURLOPT_WRITEDATA, \$response_body);

    my $ret_code;
    print "An error happen on `$cur_url`: " . $curl->strerr($ret_code) . ": " . $curl->err_buf . "\n" if $ret_code = $curl->perform;

    unless ($response_body =~ m#<$news_list_tag_name\s[^>]*class=["'][^'"]*$news_list_class_name\s*[^'"]*['"][^>]*>(.*)</$news_list_tag_name>#si) {
        die "News list not found\n";
    }

    my $news_list = $1;
    while ($news_list =~ m#(.*)</$news_list_tag_name>#si) { $news_list = $1; }

    my $news_count = 0;
    while ($news_list =~ m#<a[^>]*href=['"]([^'">]+)['"][^>]*>#sig) {
        $curl->setopt(CURLOPT_URL, $1);
        $response_body = "";
        if ($ret_code = $curl->perform) {
            printf "An arror happen on $1: %s: %s\n", $curl->strerr($ret_code), $curl->err_buf;
            next;
        }

        print $out_f process_article($1, \$response_body);
        $news_count++;
    }

    print "$news_count news processed\n\n";
}

sub process_article {
    my $url = shift;
    my $response_ptr = shift;
    unless ($$response_ptr =~ m#<$article_tag_name\s[^>]*id=['"][^'">]*$content_div_id\s*[^'">]*['"][^>]*>(.*)$#si) {
        print "$url\n$content_div_id not found\n";
        return;
    }
    $$response_ptr = $1;

    my @div_blocks = split m#</div>#, $$response_ptr, 3;
    # Skip first div block (tools block)

    my $content = $div_blocks[1]; # Real content

    $content =~ m#<h1>([^<>]+)</h1>#si;
    my $title = $1;

    $content = (split m#</h1>#, $content, 2)[1];
    $content =~ s#<[^>]*>##sig;
    $content =~ s#&[^&;]*;##sg;
    $content =~ s/^\s+(.+)\s+$/$1/sg;

    return "$title.\n$content\n\n";
}
