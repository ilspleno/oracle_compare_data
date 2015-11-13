#!/bin/env ruby

require 'oci8'
require 'yaml'
require 'pp'
require 'term/ansicolor'
include Term::ANSIColor
require 'axlsx'

DEBUG = false

# Evenutally this will be a YAML config
#@config={}
#@config[:source] = {}
#@config[:source][:dbname] = "EPOP01"
#@config[:source][:username] = 'brian'
#@config[:source][:password] = 'brian'
#
#@config[:target] = {}
#@config[:target][:dbname] = "EPOQ03"
#@config[:target][:username] = "brian"
#@config[:target][:password] = "brian"
#
#@config[:tables] = []
#
#table = { name: 'ctfbadge_fb',sample:  '0.05', check_column: 'blah' }
#@config[:tables].push table
#
#table = { name: 'ctfcesse_m',sample:  '0.015', check_column: 'blah' }
#@config[:tables].push table

@config = YAML::load_file(File.join(__dir__, 'compare.yml'))

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

	# Make sure we have a valid hash in table
	pp table if DEBUG
	sql = "select * from #{table[:schema]}.#{table[:name]}"

	# Add the sample function if we're selecting from the source. DONT want this on the target :)
	sql = sql + " sample(#{table[:sample]})" if source
	
	sql = sql + " where trunc(proxy_stmp) < trunc(sysdate)"
	

	# If we are not querying source table, then we need to put in a where clause to only select rows given in compare_slice
	if !source
		sql = sql + " and #{table[:check_column]} in ("

		puts "Length is #{compare_slice.length}" if DEBUG

		# Put the right number of bind slots in
		(1..(compare_slice.length)).each do |i|
			sql = sql + ":#{i},"
		end
		# Get rid of final ,
		sql.chop!
		sql = sql + ")"

	
	end

	# No matter what, append an order by clause
	sql = sql + " order by #{table[:check_column]}"
		


	# create cursor
	puts sql if DEBUG
	cursor = db.parse sql

	# Assign bind values for target table
	pp compare_slice if DEBUG
	index = 1
	if !source
		compare_slice.each do |value|
			cursor.bind_param(index,value)
			puts "Bound #{index} to #{value} Data Type: #{value.class}" if DEBUG
			index += 1
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

	puts "Fetched #{rows.count} rows" if DEBUG

	return metadata, rows

end

def get_slice(results, table)

	metadata = results[0]
	rows = results[1]

	# Get a slice of the results from the table so we can compare the target
	
	# Determine the index (position in array) of the column we're looking for	
	idx = -1
	metadata.each_index { |i| idx = i if metadata[i].name.downcase == table[:check_column].downcase }

	# Grab out the values from the array for the column that we found
	row_slice = rows.collect { |r| r[idx] }

	if DEBUG
		puts "Returning row_slice:"
		pp row_slice
	end

	return row_slice

end


def show_divergence(table_name, source, target)

	# Remember source and target are ARRAYS. index 0 is metadata, index 1 is result set (array of arrays)

	# Get column names for source and target	
	source_colnames = source[0].map { |x| x.name }	
	target_colnames = target[0].map { |x| x.name }

	# Add source
	@wb.add_worksheet(:name => "Source #{table_name}") do |sheet|
		sheet.add_row ["this", "is", "a", "test"]
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

# Main

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

	# Results in an array, 0 = metadata, 1 = array of results
	source_results = get_rows @source_db, table

	# Get the slice of data that represents key values
	source_slice = get_slice source_results, table

	# Now get the target database by sending in the key values so we retrieve the same rows
	target_results = get_rows @target_db, table, false, source_slice

	if DEBUG
		puts "-----------------------------------" 
		puts source_results[1][0]
		puts "==================================="
		puts target_results[1][0]
	end


	if source_results[1] == target_results[1]
		log_it "All #{source_results[1].length} rows match", :good
	else
		log_it "There are divergent rows.", :crit
	end
	show_divergence(table[:name], source_results, target_results)



end

@excel.serialize "comparison_data.xlsx"

log_it "Run complete"

@source_db.logoff
@target_db.logoff
