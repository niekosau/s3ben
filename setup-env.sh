#!/bin/bash

set -e

if [ -e /etc/redhat-release ]
then
  sudo dnf -y install python3 python3-pip binutils make git
  sudo pip3 install poetry
fi
poetry env use python3.9
poetry install
