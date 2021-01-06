# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 15:51:18 2020

@author: rparvat
"""
import re

def mapper_wc(data):
    # remove puncuation
    data = ''.join(data)
    data = re.sub(r'([^\s\w]|_)+', '', data)
    return [(word.lower().strip(),1) for word in data.split()]