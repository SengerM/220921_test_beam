from TheSetup import connect_me_with_the_setup
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--slot-number',
	metavar = 'N',
	help = 'Number of slot to watch.',
	required = True,
	dest = 'slot_number',
	type = int,
)
args = parser.parse_args()

s = connect_me_with_the_setup()
while True:
	time.sleep(1)
	print(f'{args.slot_number}: {s.measure_bias_voltage(args.slot_number):.2f} V | {s.measure_bias_current(args.slot_number)*1e6:.2f} µA', end='\r', flush=True)
