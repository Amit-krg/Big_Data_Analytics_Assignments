Warm up: Contextual Word Count in Spark (using AWS EMR) 

Objective: Create Spark program to count what people “feel”
==========

Input: First 5,000 1,000 “WET” files from the Common Crawl September 2016

Output: Count of words that appear after the string  “I feel”

your code should be case insensitive (convert strings to lowercase)

“I feel” can appear anywhere and you should only count the one single word appearing after it (this will mean many common words that aren’t “feelings” will also be included; i.e. “that”, “the”, etc…).
