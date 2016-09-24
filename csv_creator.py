import csv

file_name = 'lenskart_user_data.txt'


print 'Starting to write to lenskart csv..'
with open('lenskart.csv', 'wb') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter='\t',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    lines_read = 0
    for line in open(file_name, 'r').read().split('\n'):
    	if not line:
    		continue
    	if lines_read % 1000 == 0:
    		print 'Lines processed: ', lines_read
    	user_id, product_id =  line.split('|')
    	spamwriter.writerow([user_id.strip(), product_id.strip()])
        lines_read += 1
print 'Finished writing..'