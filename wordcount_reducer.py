# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 15:56:13 2020

@author: rparvat
"""

def reducer_wc(pairs):
    wc_dict = {}
    for word in pairs:
        w = word.split(',')[0]
        if w in wc_dict:
            wc_dict[w] += 1
        else:
            wc_dict[w] = 1
    return wc_dict.items()