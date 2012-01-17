require 'tempfile'

# Streams a file to sort it. Strives for memory efficiency.
def streaming_quicksort(file)
  # If the file is a meg or less in size, finish the sorting in memory
  if file.size < 1_048_576 
    puts "in-memory quicksort #{file.size}"

    sorted = quicksort(file.read.split(',').map { |x| x.to_i })
    file.close

    # Write to a file for merging
    Tempfile.new("sorted").tap do |temp|
      temp << sorted.join(',')
      temp << ','
      temp.flush
      temp.open
    end
  else
    puts "streaming quicksort #{file.size}"

    # We are using the first number as the pivot
    pivot = next_number(file)

    equal = Tempfile.new("equal")
    less = Tempfile.new("less")
    greater = Tempfile.new("greater")

    add_number(equal, pivot)

    # Split up the numbers
    while ((e = next_number(file)) != nil)
      add_number(less, e) if e < pivot
      add_number(greater, e) if e > pivot
      add_number(equal, e) if e == pivot
    end

    # Make sure everything is flushed to the filesystem
    less.flush
    equal.flush
    greater.flush

    # Open for reading
    less.open
    equal.open
    greater.open

    # Close the file, will delete the file if it is a temp file
    file.close

    merge_files(streaming_quicksort(less), equal, streaming_quicksort(greater))
  end
end

# Get the next number from a file.
def next_number(file)
  # Pretty brute force, but just reads characters until
  # we hit a comma.
  retval = ''

  begin
    while ((x = file.readchar) != ',') do
      retval << x
    end
  rescue
    ; # Readchar tosses an exception when it hits EOF.
  end

  if retval.empty?
    nil
  else
    retval.to_i
  end
end

# Add a number to a file.
def add_number(file, int)
  file << "#{int},"
end

# Merge three files.
def merge_files(file1, file2, file3)
  Tempfile.new("sorted").tap do |sorted|
    `cat #{file1.path} #{file2.path} #{file3.path} > #{sorted.path}`
    sorted.open
  end
end

# A reference implementation for in-memory arrays
def quicksort(array)
  return array if array.length <= 1

  pivot = array.shift
  equal = [pivot]
  less = []
  greater = []

  while (array.length > 0)
    e = array.shift

    less << e if e < pivot
    greater << e if e > pivot
    equal << e if e == pivot
  end

  quicksort(less) + equal + quicksort(greater)
end

f = streaming_quicksort(File.new('../data/unsorted'))

`mv #{f.path} ../data/sorted`
