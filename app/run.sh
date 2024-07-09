#!/bin/sh
git clone -b $GIT_BRANCH --depth 1 $GIT_REPO git_repo
cd git_repo
python ./run/app.py