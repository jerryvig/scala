#!/bin/bash

source /home/diggler/.bash_profile
/usr/bin/links http://www.linkedin.com &
sleep 8;
/usr/bin/killall links
exit 0
