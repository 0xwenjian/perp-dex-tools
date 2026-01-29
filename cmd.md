source env/bin/activate
caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker FOGO --size 4800 --iter 2 --sleep 8631

caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker kSHIB --size 16000 --iter 2 --sleep 4000

caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker hbar --size 1000 --iter 2 --sleep 14400

caffeinate -i -s python3 hedge_mode.py --exchange backpack --ticker eth --size  0.02 --iter 2 --sleep 20

python3 dashboard_server.py

python3 hedge_mode.py --exchange backpack_paradex --ticker kSHIB --size 1000 --iter 2 --sleep 20

python3 hedge_mode.py --exchange backpack_paradex --ticker ETH --size 0.02 --iter 2 --sleep 20

caffeinate -i -s python3 hedge_mode.py --exchange backpack_paradex --ticker PYTH --size 1800 --iter 2 --sleep 12600

caffeinate -i -s python3 hedge_mode.py --exchange backpack_paradex --ticker DOT --size 10 --iter 2 --sleep 10