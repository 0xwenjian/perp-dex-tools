source env/bin/activate
caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker FOGO --size 4800 --iter 2 --sleep 8631

caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker kSHIB --size 16000 --iter 2 --sleep 4000

caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker hbar --size 1000 --iter 2 --sleep 14400

python3 dashboard_server.py
