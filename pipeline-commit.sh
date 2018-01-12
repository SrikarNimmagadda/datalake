#!/bin/bash -x

make bootstrap

make lint

make test

make pack-lambdas
