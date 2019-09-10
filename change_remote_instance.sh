#!/bin/bash

cineca="http:\/\/gmql.eu\/gmql-rest\/"
genomic="http:\/\/genomic.elet.polimi.it\/gmql-rest\/"

from=$genomic
to=$cineca

find . -type f | grep -v .git/ | xargs sed -i 's/$to/$to/g'
