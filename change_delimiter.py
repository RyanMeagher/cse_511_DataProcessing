def change_csvfile(filepath,output_path,delimiter,out_delimiter):
	with open(filepath,'r') as infile:
		with open(output_path,'w') as outfile:
			for line in infile:
				line=line.split(delimiter)
				line=(out_delimiter.join([str(i) for i in line[0:3]]))+'\n'
				outfile.write(line)
	return 





change_csvfile('abc.dat', 'abc.dat',',','::')


