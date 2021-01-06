# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 15:57:24 2020

@author: rparvat
"""

def reducer_ii(pairs,fs):
    ii_dict = {}
    for word in pairs:
        w = word.split(',')
        if w[0] in ii_dict:
            ii_dict[w[0]][w[1]] += 1
        else:
            ii_dict[w[0]] = {f:1 if f==w[1] else 0 for f in fs}
    
    #output format
    word_tups = []       
    for word in ii_dict.keys():
        string = ''
        for doc,cnt in ii_dict[word].items():
            if cnt != 0:
                string = string+str(doc)+"|"+str(cnt)+" "
        word_tups.append((word,string.rstrip()))
    return word_tups