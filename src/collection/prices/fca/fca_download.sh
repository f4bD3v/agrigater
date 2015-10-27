START="01/01/2009"
YEST=`date +%d/%m/%Y -d "yesterday"`
# $1 = [Retail, Wholesale]
python get_fca.py -p "$1" -r "$START" "$YEST
