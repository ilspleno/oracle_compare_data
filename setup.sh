#!/bin/bash

command curl -sSL https://rvm.io/mpapis.asc | gpg2 --import -
\curl -sSL https://get.rvm.io | bash 

source ./rvm.source
rvm install ruby-2.2.1 --autolibs=1
rvm gemset import

echo
echo "-------- Setup is complete, you can now modify compare.yml -----------"
echo "-------- or use run.sh                                     -----------"
echo

