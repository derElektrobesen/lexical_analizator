lexical_analizator
==================

Text lexical analizator

Description
-----------

Russian text corpus can be downloaded from the following [link](http://opencorpora.org/files/export/dict/dict.opcorpora.xml.bz2).
Downloaded text corpus (in xml format) must be processed with `process_corpus.pl` script.

News corpus can be downloaded with `get_news.pl` script (call `./get_news.pl --help` for more info).
They will be downloaded from [mk.ru website](http://www.mk.ru/) by default.

`config.cfg` file contains configuration for all programs. All parametrs are required.

`analizator.pl` script is main. Run it with prepared database and news list.

If a word form haven't got properties (for example, if a word form is equal as a parent form),
this word can be processed incorrectly. Use `process_empty_words.pl` script to fix it.
