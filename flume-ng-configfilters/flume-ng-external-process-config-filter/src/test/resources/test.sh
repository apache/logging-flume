#!/usr/bin/env bash

if [ $1 = "my_password_key" ]; then
    echo "filtered";
fi

if [ $1 = "my_password_key2" ]; then
    echo "filtered2";
fi

exit 0