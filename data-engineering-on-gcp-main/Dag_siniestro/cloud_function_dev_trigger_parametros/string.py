id_value = "he-dev-data/he-dev-data-ipress/ipress_clinicas/internacional/nombre_archivo.parquet/1718729942571103"

# Find the index of the last '/'
last_slash_index = id_value.rfind('/')

# Extract the substring from the start up to just before the last '/'
transformed_value = id_value[:last_slash_index]

print(transformed_value)