
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import json

#ID od the user we want recommendation for
##########################################
recommendation_user_id = "367007"
##########################################

# List of attributes that we take into account 
string_vector_attributes = ['series', 'author', 'publisher']
list_vector_attributes = ['genres', 'awards', 'setting', 'words']
int_vector_attributes = {'pages': 3000} #attribute and inverse coefficient factor

#Takes an input string and returns whether it's "empty"
def Check_not_empty_input(input):
  # Check if input is not a NaN and not empty
  return input == input and input and input != " "

# 1.
# Finds the user we want to give recommendation to
# Then computes the norm of the user vector
user_profile = {}
user_suqared_norm = 0

file_name = "user_profiles.txt"
with open(file_name, 'r', encoding='utf-8') as file:
  while (line := file.readline().rstrip()):
    user_profile_data = json.loads(line)
    user_profile_id = list(user_profile_data.keys())[0]
    
    if user_profile_id == recommendation_user_id: # Seeked user found
      # Store the user profile vetor
      user_profile = user_profile_data[user_profile_id]
      
      # Compute the norm of the user vector
      for attribute in string_vector_attributes + list_vector_attributes:
        for key, value in user_profile[attribute].items():
          if Check_not_empty_input(key) and Check_not_empty_input(value):
            user_suqared_norm += value**2
      for attribute, inv_coeff in int_vector_attributes.items():
          value = 0
          # Check if there is a value and this value is not a NaN
          if Check_not_empty_input(user_profile[attribute]):
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

  # MapReduce class to compute the cosine similarity between user_profile and all books
  class Recomendation(MRJob):    

      # Defines the order of mapper and reducer steps
      def steps(self):
          return [ MRStep(mapper=self.Mapper_compute_norm, reducer=self.Reducer_compute_cosine) ]

      # 2. 
      # Map step: each book vector is read, transformed back into a dictionary and its norm computed 
      def Mapper_compute_norm(self, _, line):
          # Read the input book vector
          book = json.loads(line)
          book_id = list(book.keys())[0]
          book = book[book_id]

          # Compute the norm of the book vector
          suqared_norm = 0
          for attribute in string_vector_attributes:
              if Check_not_empty_input(book[attribute]):
                  suqared_norm += 1
          for attribute in list_vector_attributes:
              if Check_not_empty_input(book[attribute]):
                for elmt in book[attribute]:
                  if Check_not_empty_input(elmt):
                    suqared_norm += 1
          for attribute, inv_coeff in int_vector_attributes.items():
              value = 0
              if Check_not_empty_input(book[attribute]):
                  try:
                      #divide the attribute value by the coeeficient value
                      value = (float(book[attribute]) / inv_coeff)
                      suqared_norm += value**2
                  except ValueError:
                      value = float('nan')
              book[attribute] = value
          yield book_id, (suqared_norm, book)
      
      # 3.
      # Reduce step: compute the cosine similarity between the user profile and every book item profile
      # Only recommend unread books
      def Reducer_compute_cosine(self, book_id, norm_and_book):
          # Read the input book vector
          value = list(norm_and_book)[0]
          book_suqared_norm = value[0]
          book = value[1]
          
          # Check that the user have not already read that book
          if book["title"] not in user_profile["title"]:
            
            # Compute the dot prodcut between the user vector and every book vector
            dot_product = 0
            for attribute in string_vector_attributes:
              if Check_not_empty_input(book[attribute]) and Check_not_empty_input(user_profile[attribute]):
                for key, value in user_profile[attribute].items():
                  if key == book[attribute] and Check_not_empty_input(value):
                    dot_product += value
            for attribute in list_vector_attributes:
              if Check_not_empty_input(book[attribute]) and Check_not_empty_input(user_profile[attribute]):
                for key, value in user_profile[attribute].items():
                  if Check_not_empty_input(key) and Check_not_empty_input(value) and key in book[attribute]:
                    dot_product += value
            for attribute, inv_coeff in int_vector_attributes.items():
              dot_product += (book[attribute] * user_profile[attribute])

            # Compute the cosine simpilarity: X.Y/(||X||.||Y||)
            cosine_similartity = dot_product / (user_suqared_norm*book_suqared_norm) if user_suqared_norm*book_suqared_norm != 0 else 0
            
            yield (book_id), (cosine_similartity)

  if __name__ == '__main__':
      Recomendation.run()
