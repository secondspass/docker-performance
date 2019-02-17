echo "10 percent";
sleep 5;
python analysis.py -i dal09 -c simulate -p 10;
echo "25 percent";
sleep 5;
python analysis.py -i dal09 -c simulate -p 25;
echo "50 percent";
python analysis.py -i dal09 -c simulate -p 50;
echo "75 percent";
python analysis.py -i dal09 -c simulate -p 75;
echo "100 percent";
python analysis.py -i dal09 -c simulate -p 100;

