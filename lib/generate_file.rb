TARGET_SIZE = 26_214_400 #5_368_709_120
RAND_MAX = 1_000_000

file = File.open('../data/unsorted', 'w')

while (file.size < TARGET_SIZE) do
  elements = 1_000_000.times.map { rand(1_000_000) }

  file << elements.join(",")
  file << ','
  file.flush
end
