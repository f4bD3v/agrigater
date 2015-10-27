START="01/01/2009"
YEST=`date -v-1d +%d/%m/%Y`
# $1 = [Retail, Wholesale]
python get_fca.py -p "$1" -r "$START" "$YEST
