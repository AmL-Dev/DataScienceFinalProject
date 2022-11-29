
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

#ID od the user we want recommendation for
##########################################
recommendation_user_id = "175208"
##########################################

# List of attributes that we take into account 
string_vector_attributes = ['series', 'author', 'publisher']
list_vector_attributes = ['genres', 'awards', 'setting', 'words']
int_vector_attributes = {'pages': 3000}#, 'firstPublishDate': 1000} #attribute and inverse coefficient

#Takes an input string and returns whether it's "empty"
def checkNotEmptyInput(input):
  # Check if input is not a NaN and not empty
  return input == input and input and input != " "

# Finds the user we want to give recommendation to
user_profile = {}
user_suqared_norm = 0

file_name = "user_profiles.txt"
with open(file_name, 'r', encoding='utf-8') as file:
  while (line := file.readline().rstrip()):
    user_profile_data = json.loads(line)
    user_profile_id = list(user_profile_data.keys())[0]
    if user_profile_id == recommendation_user_id:
      # Store the profile vetor
      user_profile = user_profile_data[user_profile_id]
      # Compute the norm of the user vector
      for attribute in string_vector_attributes + list_vector_attributes:
        for key, value in user_profile[attribute].items():
          if checkNotEmptyInput(key) and checkNotEmptyInput(value):
            user_suqared_norm += value**2
      for attribute, inv_coeff in int_vector_attributes.items():
          value = 0
          # Check if there is a value and this value is not a NaN
          if checkNotEmptyInput(user_profile[attribute]):
            try:
              value = (float(user_profile[attribute]) / inv_coeff)
              user_suqared_norm += (value**2)
            except ValueError:
              value = float('nan')
          user_profile[attribute] = value
      break  
      
if user_profile == {}:
  print(f"User id {recommendation_user_id} not found. Please try with another user.")
else:


  class Recomendation(MRJob):    

      def steps(self):
          #need to define the order of mappers and reducers, so that the functions takes in the right key and value
          #only one mapper and reducer per step, so therefore need several steps
          return [ 
              MRStep(mapper=self.mapper_1, reducer=self.reducer_1)
          ]

      # the map step: each book vector is read 
      def mapper_1(self, _, line):
          book = json.loads(line)
          book_id = list(book.keys())[0]
          book = book[book_id]

          #Compute the norm of the book id vector
          suqared_norm = 0
          for attribute in string_vector_attributes:
              # Check if not an empty string or NaN
              if checkNotEmptyInput(book[attribute]):
                  suqared_norm += 1
          for attribute in list_vector_attributes:
              # Check if not an empty list or NaN
              if checkNotEmptyInput(book[attribute]):
                for elmt in book[attribute]:
                  if checkNotEmptyInput(elmt):
                    suqared_norm += 1
          for attribute, inv_coeff in int_vector_attributes.items():
              value = 0
              # Check if there is a value and this value is not a NaN
              if checkNotEmptyInput(book[attribute]):# != "" or book[attribute] != " " or float(book[attribute]) != float(book[attribute]):
                  try:
                      value = (float(book[attribute]) / inv_coeff)
                      suqared_norm += value**2
                  except ValueError:
                      value = float('nan')
              book[attribute] = value
          yield book_id, (suqared_norm, book)
      
      # the reduce step: coompute the cosine similarity between the user profile and every item profile
      # Only recommend unread books
      def reducer_1(self, book_id, value):
          value = list(value)[0]
          book_suqared_norm = list(value)[0]
          book = list(value)[1]
          
          # Check that the user havez not read that book
          if book["title"] not in user_profile["title"]:

            dot_product = 0
            for attribute in string_vector_attributes:
              # Check if the value of the item vector and user vector is not an empty
              if checkNotEmptyInput(book[attribute]) and checkNotEmptyInput(user_profile[attribute]):
                for key, value in user_profile[attribute].items():
                  if key == book[attribute] and checkNotEmptyInput(value):
                    dot_product += value
            for attribute in list_vector_attributes:
              # Check if the value of the item vector and user vector is not an empty
              if checkNotEmptyInput(book[attribute]) and checkNotEmptyInput(user_profile[attribute]):
                for key, value in user_profile[attribute].items():
                  if checkNotEmptyInput(key) and checkNotEmptyInput(value) and key in book[attribute]:
                    dot_product += value
            for attribute, inv_coeff in int_vector_attributes.items():
              dot_product += (book[attribute] * user_profile[attribute])
            

            cosine_similartity = dot_product / (user_suqared_norm*book_suqared_norm) if user_suqared_norm*book_suqared_norm != 0 else 0
            yield (book_id), (cosine_similartity)

  if __name__ == '__main__':
      Recomendation.run()
