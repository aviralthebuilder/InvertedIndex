#!/usr/bin/env python
# coding: utf-8

# In[1]:


sc
sc = SparkContext.getOrCreate()
#time to initialize sc


# In[2]:


sc


# In[3]:


path = "../../Desktop/bbcsports/bbc_sports_articles"


# In[4]:


inputData = sc.wholeTextFiles(path)


# In[5]:


import os
import pandas as pd
import nltk
import operator
from nltk.stem.wordnet import WordNetLemmatizer
lem = WordNetLemmatizer()


# In[7]:


import string
punctuations = string.punctuation

from nltk.corpus import stopwords
stopword = stopwords.words("english")

#now it is time to perform cleaning of the data in order to create an inverted index. As a result, for that we shall
#create a function that will be applied the entire RDD
def cle(text):
    a,b = text
    a= a.replace("file:/Users/aviralsharma/Desktop/bbcsports/bbc_sports_articles/", "")
    text = b
    text = text.replace("\n", " ").replace("\'", " ")
    #getting rid of single character words
    text = " ".join(word for word in text.split(" ") if len(word) > 1)
    #converting everything to lowerCase to decrease the total number of unique
    #words
    cleanText = text.lower()
    #removing punctuations
    cleanText = "".join(word for word in cleanText if word not in punctuations)
    #time to remove the stop words
    words = cleanText.split(" ")
    words = [word for word in words if word not in stopword]
    cleanText = " ".join(words)
    words = cleanText.split(" ")
    words = [lem.lemmatize(word, 'n') for word in words]
    words = [lem.lemmatize(word, 'v') for word in words]
    text = " ".join(words)
    return text+ "[]" + a


# In[8]:


work = inputData.map(lambda x : cle(x))


# In[9]:


work.take(10)


# In[10]:


nextedit = work.map(lambda x: x.split("[]"))


# In[11]:


nextedit.take(10)


# In[12]:


trial = nextedit.map(lambda x: (x[0].split(" "), x[1], 1))


# In[13]:


trial.take(1)


# In[14]:


please = nextedit.map(lambda x: (x[0].split(), x[1]))


# In[15]:


def segregrate(words, doc):
    listTemp = []
    for word in words:
        #if (word,doc) not in listTemp:
        listTemp.append((word, doc))
    return listTemp


# In[16]:


trial = please.flatMap(lambda x: segregrate(x[0], x[1]))


# In[17]:


trial.collect()


# In[18]:


fineseattempt = trial.map(lambda x: ((x[0],x[1]), 1))


# In[19]:


fineseattempt.collect()


# In[20]:


fineseattempt = fineseattempt.reduceByKey(lambda x,y: x + y)


# In[21]:


fineseattempt.collect()


# In[22]:


fineseattempt = fineseattempt.map(lambda x: (x[0][0], x[0][1], x[1]))


# In[23]:


fineseattempt.collect()


# In[24]:


fineseattempt=  fineseattempt.sortBy(lambda a: a[2], ascending = False)


# In[25]:


fineseattempt.collect()


# In[26]:


fineseattempt = fineseattempt.map(lambda x: (x[0], (x[1]+ "(" + str(x[2]) + ")")))


# In[27]:


fineseattempt.collect()


# In[28]:


fineseattempt = fineseattempt.reduceByKey(lambda x,y: x +" "+ y)


# In[29]:


fineseattempt.collect()


# In[36]:


#All in One
oneAttempt = sc.wholeTextFiles(path).map(lambda x : cle(x)).map(lambda x: x.split("[]")).map(lambda x: (x[0].split(), x[1])).flatMap(lambda x: segregrate(x[0], x[1])).map(lambda x: ((x[0],x[1]), 1)).reduceByKey(lambda x,y: x + y).map(lambda x: (x[0][0], x[0][1], x[1])).sortBy(lambda a: a[2], ascending = False).map(lambda x: (x[0], (x[1]+ "(" + str(x[2]) + ")"))).reduceByKey(lambda x,y: x +" "+ y)


# In[37]:


oneAttempt.collect()


# In[ ]:




