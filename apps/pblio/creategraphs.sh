#!/bin/sh

if [ ! -f pblio.data ] ; then
    echo "pblio.data" is missing
    exit 1
fi

./pblioplot.py
./pblioplot.gp
