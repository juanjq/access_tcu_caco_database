#!/bin/sh

# Connecting to the databases IPs
# TCU database
gnome-terminal -- ssh lst101 -L27017:127.0.0.1:27017
# CaCo database
gnome-terminal -- ssh lst101 -L27018:127.0.0.1:27018