from TheSetup import connect_me_with_the_setup
import time
import argparse

s = connect_me_with_the_setup()
while True:
	time.sleep(1)
	bias_voltages = [s.measure_bias_voltage(slot_number) for slot_number in s.get_slots_configuration_df().index]
	bias_currents = [s.measure_bias_current(slot_number) for slot_number in s.get_slots_configuration_df().index]
	print('---')
	for sn,V,I in zip(s.get_slots_configuration_df().index, bias_voltages, bias_currents):
		print(f'{sn}: {V:.2f} V | {I*1e6:.2f} µA')
