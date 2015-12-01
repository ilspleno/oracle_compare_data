#!/bin/env ruby

require 'oci8'
require 'yaml'
require 'pp'
require 'term/ansicolor'
include Term::ANSIColor
require 'axlsx'

DEBUG = false

if ARGV.index "debug2"
	DEBUG2 = true
else
	DEBUG2 = false
end



@config = YAML::load_file(File.join(__dir__, 'compare.yml'))
@logfile = File.new "compare.log", "w"

def log_it(msg, level = :normal)

	print "#{Time.now} | "

	case level
		when :normal
			print reset
		when :good
			print green
		when :warn
			print yellow
		when :crit
			print red
		else
			print green
	end


	print msg

	print reset, "\n"

	# And now print to file
	@logfile.puts "#{Time.now} | #{msg}"

end

def db_connect(source=true)

	if source
		db_symbol = :source
	else
		db_symbol = :target
	end


	begin
		db = OCI8.new @config[db_symbol][:username], 
			      @config[db_symbol][:password],
			      @config[db_symbol][:dbname]

	rescue Exception => x
		puts "Could not connect to database #{@config[db_symbol][:dbname]}."
		puts x.message
		puts "Please correct and rerun"
		exit 1		
	end	

	return db

end

# Ugh this proc is too long, need to refactor it
def get_rows(db, table, source = true, compare_slice = [])

	# Get the number of items in the source slice, so we know how many bind variables we'll need
	# We know there's always at least one array in compare slice, so we use index 0
	source_rowcount = compare_slice[0].length unless compare_slice.empty?

	# Make sure we have a valid hash in table
	pp table if DEBUG
	sql = "select * from #{table[:schema]}.#{table[:name]}"

	# Add the sample function if we're selecting from the source. DONT want this on the target :)
	sql = sql + " sample(#{table[:sample]})" if source
	
	sql = sql + " where trunc(proxy_stmp) < trunc(sysdate)"
	


	# If we are not querying source table, then we need to put in a where clause to only select rows given in compare_slice
	if !source

		sql = sql + " and "

		# Get number of columns in primary key
		pk_length = table[:check_column].length

		colindex = 1
		(0..(source_rowcount-1)).each do |i|
			sql += "("

			# Each PK column...
			table[:check_column].each do |col|
				sql += "#{col} = :#{col}#{colindex} "
  	 			sql += " and "

			end
			colindex = colindex + 1

			# Get rid of last "and"
			sql.chop!.chop!.chop!.chop! 

			sql += ")"
			
			# Don't put an 'or' on the last line
			sql += " or " unless i == ( source_rowcount - 1)
		end

		
	
	end

	# No matter what, append an order by clause
	sql = sql + " order by "

	table[:check_column].each do |col|
		sql = sql + " #{col},"
	end

	
	# Get rid of final , again
	sql.chop!		

	# create cursor
	puts sql if DEBUG2
	cursor = db.parse sql

	# Assign bind values for target table
	pp compare_slice if DEBUG
	if !source
		compare_index = 0
		table[:check_column].each do |col|
			index = 1
			compare_slice[compare_index].each do |val|

				cursor.bind_param("#{col}#{index}", val)
				index += 1
			end
		compare_index += 1
		end
		
	end

	

	# Get metadata for table
	metadata = cursor.column_metadata

	# Execute cursor
	cursor.exec

	# ... and actual row data
	rows = []
	while r = cursor.fetch
		rows << r
	end

	return metadata, rows

end

def get_slice(results, table)

	metadata = results[0]
	rows = results[1]
	
	row_slices = []
        table[:check_column].each do |col|

		# Get a slice of the results from the table so we can compare the target
		
		# Determine the index (position in array) of the column we're looking for	
		idx = -1
		metadata.each_index { |i| idx = i if metadata[i].name.downcase == col.downcase }

		# Grab out the values from the array for the column that we found
		row_slice = rows.collect { |r| r[idx] }

		if DEBUG
			puts "Returning row_slice:"
		end

		row_slices << row_slice
	end

	return row_slices

end


def show_divergence(table_name, source, target)

	# Remember source and target are ARRAYS. index 0 is metadata, index 1 is result set (array of arrays)

	# Get column names for source and target	
	source_colnames = source[0].map { |x| x.name }	
	target_colnames = target[0].map { |x| x.name }

	# Add source
	@wb.add_worksheet(:name => "Source #{table_name}") do |sheet|
		sheet.add_row source_colnames

		source[1].each do |row|
			sheet.add_row row
		end
	end

	# Add target
	@wb.add_worksheet(:name => "Target #{table_name}") do |sheet|
		sheet.add_row target_colnames

		target[1].each do |row|
			sheet.add_row row
		end
	end


end

def tables_match? (src, tgt)

	all_match = true

	src.each do |row|
		if !(tgt.index row)
			log_it "Unable to match row in target."
			log_it row
			all_match = false
		end
	end

	return all_match
end

# Main

df = File.new "debugfile.txt", "w"

log_it "Beginning run - comparing databases #{@config[:source][:dbname]} and #{@config[:target][:dbname]}"

# Create spreadsheet
@excel = Axlsx::Package.new
@wb = @excel.workbook

@source_db = db_connect
@target_db = db_connect false



# Process each table
@config[:tables].each do |table|

	# Remember each 'table' is actually an array defining the table and what we need to know about it
	log_it "Processing #{table[:name]}"

	# Convert check_column to an array if it's just a single entry
	if table[:check_column].class != Array
		table[:check_column] = [ table[:check_column] ]
        end

	# Results in an array, 0 = metadata, 1 = array of results
	source_results = get_rows @source_db, table

	# Get the slice of data that represents key values
	source_slice = get_slice source_results, table

	# Now get the target database by sending in the key values so we retrieve the same rows
	target_results = get_rows @target_db, table, false, source_slice


	if source_results[1] == target_results[1]
		log_it "All rows (#{source_results[1].length}) match.", :good
	else
		log_it "There are variances between source and target.", :crit
	end


# Old slow way to row compare
#	if tables_match? source_results[1], target_results[1]
#		log_it "All #{source_results[1].length} rows found on target", :good
#	else
#		log_it "There are divergent rows. Total sample size is #{source_results[1].length} rows.", :crit
#	end

	show_divergence(table[:name], source_results, target_results)



end

@excel.serialize "comparison_data.xlsx"

log_it "Run complete"

@source_db.logoff
@target_db.logoff
