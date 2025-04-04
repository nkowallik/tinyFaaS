#!/usr/bin/env python3

import typing
import time

def fn(input: typing.Optional[str], headers: typing.Optional[typing.Dict[str, str]]) -> typing.Optional[str]:
    """echo the input"""
    time.sleep(10)
    return input
