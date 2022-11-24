 
from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd #using pandas to find the number of books in the corpus
import spacy #using the spacy library to remove stop words 
from nltk.stem import PorterStemmer #reduces the word to the root node
from nltk.stem import SnowballStemmer
import math
import re 
import string 

threshold = 0.00000001

#Finding the total number of books in the corpus:
test = pd.read_csv('data/books_testset.csv')
M = len(test)

#loading the english language small model of spacy
en = spacy.load('en_core_web_sm')
stopwords = en.Defaults.stop_words

#string of special characters
special_char = r'[' + string.punctuation + ']'

#standardize words that share the same suffix and that normally are derivations of gramatically similar words
#two different stemmers, the snowball is more agressive
porter = PorterStemmer()
snowball = SnowballStemmer(language='english')

#searches for groups that have alphanumerics or ' that are 1 or longer. I.e. breaks a line into words.
WORD_RE = re.compile(r"[\w']+") 


class MR_TFIDF(MRJob): #class that inherits from MRJob 


    def steps(self):
        #need to define the order of mappers and reducers, so that the functions takes in the right key and value
        #only one mapper and reducer per step, so therefore need several steps
        #do we need the combiner between the mapper and reducer??
        return [ 
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(mapper=self.mapper_2, reducer = self.reducer_2),
            MRStep(mapper = self.mapper_3, reducer = self.reducer_3),
            MRStep(mapper = self.mapper_4)
        ]

    def split_and_clean(self, line):  #takes in a key and a value, here the key is ignored
        words = []
        line = re.sub(special_char, ' ', line) #remove special characters from line
        for word in WORD_RE.findall(line): #for each word in that line
            if word.lower() not in stopwords and len(word)>1: #if the word is not a stop word 
                words.append(snowball.stem(word.lower())) #append the root of the word
        return words #return the words

    #key - ignored
    #values - corpus of documents (i.e. the data frame books_metadata)
    def mapper_1(self, _, line):  #each line is a row, i.e. a book 
        idFound = re.search(r"(^\d+),", line)
        descriptionFound = re.search(r",\"(.+)\",", line)
        if idFound: 
            id = int(idFound.group(1))
            if descriptionFound:
                d = descriptionFound.group(1)
                description = str()
                flag = True
                for char in d:
                    if char =='"':
                        flag = False
                    if flag:
                        description += char
                for word in self.split_and_clean(description): 
                    yield (word,id), 1   # returns key-value pairs ((word, id), 1)


    #key - the key which was yielded by the mapper, i.e. (word, id). (key[0] = word, key[1]=id)
    #values - A generator which yields all values yielded by the mapper which correspond to key, i.e. a list of 1's
    def reducer_1(self, word_id, counts): 
        yield (word_id[0], word_id[1]), sum(counts) # returns key-value pairs ((word, id), n)

    #key - (word, id)
    #values - n
    def mapper_2(self, word_id, n):
        word, id = word_id[0], word_id[1], 
        yield id, (word,n) # returns key-value pairs (id, (word,n))

    #key - id
    #values - list of (word,n) pairs 
    def reducer_2(self, id, word_n):
        
        word_list = []
        n_list = []
        N = 0 
        num_el = 0

        for value in word_n:
            word, n = value[0], value[1]

            word_list.append(word)
            n_list.append(n)
            N+=int(n)

            num_el+=1

        for i in range(num_el):
            yield (word_list[i],id), (n_list[i],N)    #returns key-value pairs ((word,id), (n,N))

    #key - (word, id)
    #values - (n, N) (only one pair)
    def mapper_3(self, word_id, n_N):
        word, id = word_id[0], word_id[1]
        n, N = n_N[0], n_N[1]
        yield word, (id, n, N, 1)


    #key - word
    #values - list of key-value (id,n,N,1) pairs
    def reducer_3(self, word, id_n_N_counts):

        id_list = []
        n_list = []
        N_list = []
        m = 0

        num_el = 0

        for value in id_n_N_counts:
            id, n, N, count = value[0], value[1], value[2], value[3]

            id_list.append(id)
            n_list.append(n)
            N_list.append(N)

            m+=count

            num_el +=1

        for i in range(num_el):
            yield (word, id_list[i]), (n_list[i], N_list[i],m)

    def mapper_4(self, word_id, n_N_m):
        word, id = word_id[0], word_id[1]
        n, N, m = n_N_m[0], n_N_m[1], n_N_m[2]

        tf = (n/N) 
        idf = math.log(M/m) 
        tfidf = tf*idf

        if tfidf > threshold:
            yield (word, id), tfidf

    



if __name__ == '__main__':
    MR_TFIDF.run()


