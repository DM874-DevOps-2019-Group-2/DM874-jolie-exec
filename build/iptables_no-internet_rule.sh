#!/bin/bash

iptables -A OUTPUT -m owner --gid-owner no-internet -j DROP
