
Run the A Priori algorithm for frequent itemsets over users as baskets.
(This task uses only the user and item elements from the ratings files)
=>For each of the ratings files: 
=>Filter out duplicate item-user pairs
=>From there, filter to items associated with at least 10 users
=>From there, filter to users associated with at least 5 items
=>Treat the remaining users as baskets
=>Produce k_sets where k = 4, using the a priori algorithm to restrict to itemsets with support >= 10.
=>Along the way output the number of itemsets where k=2, k=3, and k=4
From the 4_sets, output a final set of I, {j} meeting the criteria: 
	confidence >= 0.05 and interest >= 0.02. 
	
That is, all I, {j} where I is a 3_set available from A Priori and j is a possible 4th item also found from A Priori where confidence and interest criteria are met. (output pairs: ({tuple_of_item_ids_for_I}, item_id_for_j}
