INPUT=$1
OUTPUT_FOLDER=$2
awk -v odir="$OUTPUT_FOLDER" -F',' '
  { date = substr($4,1,4) }
  !(date in outfile) { outfile[date] = (odir) "/" (date) ".csv" }
  { print > outfile[date] }
' $INPUT