#!/bin/sh

pydoc3 torf.Torrent | \
    tail -n +6 | \
    sed 's/^..../   /' | \
    sed 's/^\s*$//' | \
    sed '/__repr__/,/^$/ d' | \
    sed '/__dict__/,/^$/ d' | \
    sed '/__weakref__/,/^$/ d'
